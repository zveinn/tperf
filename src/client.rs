use anyhow::{bail, Context, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::{Config, TestType};
use crate::db::Db;
use crate::metrics::format_window;
use crate::netutil::{connect_tcp_retry, resolve_retry};
use crate::proto::{read_frame, write_frame, Msg};
use crate::testplan::assignments;

pub async fn run(cfg: Config) -> Result<()> {
    let db_path = cfg.db_path();
    eprintln!(
        "tperf-client: tag={} type={} network={} payload={} workers={} hosts={} db={}",
        cfg.tag,
        cfg.test_type,
        cfg.network,
        cfg.payload_size,
        cfg.workers,
        cfg.hostlist.len(),
        db_path.display()
    );
    for h in &cfg.hostlist {
        eprintln!("tperf-client:   host {h}");
    }

    let db = Db::create(&db_path, &cfg)?;
    let dbh = db.handle();

    let port = cfg.control_port()?;
    let cancel = CancellationToken::new();
    let mut asg = assignments(&cfg.hostlist, matches!(cfg.test_type, TestType::Mesh));
    let test_port = cfg.test_port()?;
    let family = cfg.test_family()?;
    let mut resolved = Vec::new();
    for h in &cfg.hostlist {
        let addr = resolve_retry(h, test_port, family, Duration::from_secs(15), &cancel)
            .await
            .with_context(|| format!("resolve test address for {h}"))?;
        eprintln!("tperf-client: {h} test-addr {addr}");
        resolved.push(addr);
    }
    for a in &mut asg {
        a.bind = resolved[a.self_id as usize].to_string();
        for t in &mut a.targets {
            t.addr = resolved[t.id as usize].to_string();
        }
    }

    let mut writers: Vec<(String, Arc<Mutex<OwnedWriteHalf>>)> = Vec::new();
    let mut readers = JoinSet::new();

    for host in cfg.hostlist.iter() {
        eprintln!("tperf-client: connecting to {host}:{port}");
        let stream = connect_tcp_retry(host, port, Duration::from_secs(30), &cancel)
            .await
            .with_context(|| format!("control connect {host}:{port}"))?;
        let _ = stream.set_nodelay(true);
        let (rd, wr) = stream.into_split();
        writers.push((host.clone(), Arc::new(Mutex::new(wr))));
        let dbh = dbh.clone();
        let name = host.clone();
        readers.spawn(async move { pump_server(name, rd, dbh).await });
    }

    for (i, (host, wr)) in writers.iter().enumerate() {
        let assignment = asg[i].clone();
        let mut g = wr.lock().await;
        write_frame(
            &mut *g,
            &Msg::Start {
                config: cfg.clone(),
                assignment,
            },
        )
        .await
        .with_context(|| format!("send start to {host}"))?;
    }

    eprintln!("tperf-client: test running; send SIGINT/SIGTERM to stop");

    wait_for_signal().await;
    eprintln!("tperf-client: stopping");

    for (host, wr) in &writers {
        let mut g = wr.lock().await;
        if let Err(e) = write_frame(&mut *g, &Msg::Stop).await {
            eprintln!("tperf-client: stop {host}: {e:#}");
        }
    }

    let drain = async { while readers.join_next().await.is_some() {} };
    if tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .is_err()
    {
        eprintln!("tperf-client: timed out waiting for servers to ack stop");
        readers.abort_all();
        while readers.join_next().await.is_some() {}
    }

    for (_, wr) in writers {
        let mut g = wr.lock().await;
        let _ = g.shutdown().await;
    }

    db.shutdown()?;
    eprintln!("tperf-client: wrote {}", db_path.display());
    Ok(())
}

async fn pump_server(
    name: String,
    rd: tokio::net::tcp::OwnedReadHalf,
    db: crate::db::DbHandle,
) -> Result<()> {
    let mut rd = BufReader::new(rd);
    let mut got_start = false;
    loop {
        match read_frame(&mut rd).await {
            Ok(Msg::Ack { cmd }) if cmd == "start" => {
                got_start = true;
                eprintln!("tperf-client: {name} started");
            }
            Ok(Msg::Ack { cmd }) if cmd == "stop" => {
                eprintln!("tperf-client: {name} stopped");
                break;
            }
            Ok(Msg::Metrics { window }) => {
                println!("{}", format_window(&window));
                db.insert(window);
            }
            Ok(Msg::Error { message }) => {
                eprintln!("tperf-client: {name} error: {message}");
                if !got_start {
                    bail!("{name}: {message}");
                }
            }
            Ok(other) => {
                eprintln!("tperf-client: {name} unexpected {other:?}");
            }
            Err(e) => {
                eprintln!("tperf-client: {name} disconnected: {e:#}");
                break;
            }
        }
    }
    Ok(())
}

async fn wait_for_signal() {
    let mut sigterm = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        Ok(s) => s,
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}
