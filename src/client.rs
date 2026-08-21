use anyhow::{bail, Context, Result};
use std::sync::atomic::{AtomicU64, Ordering};
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
use crate::proto::{read_frame, write_frame, Assignment, Msg};
use crate::testplan::{assignments_for_pairs, assignments_mesh, fill_resolved, round_robin_rounds};

/// Traffic time per pairs round. Matches the 1s metric window.
const PAIR_ROUND_SECS: u64 = 1;

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
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            wait_for_signal().await;
            cancel.cancel();
        });
    }

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

    let start_acks = Arc::new(AtomicU64::new(0));
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
        let start_acks = start_acks.clone();
        readers.spawn(async move { pump_server(name, rd, dbh, start_acks).await });
    }

    match cfg.test_type {
        TestType::Mesh => {
            let mut asg = assignments_mesh(&cfg.hostlist);
            fill_resolved(&mut asg, &resolved);
            send_starts(&cfg, &writers, &asg, &start_acks).await?;
            eprintln!("tperf-client: mesh running; send SIGINT/SIGTERM to stop");
            cancel.cancelled().await;
        }
        TestType::Pairs => {
            run_pair_rounds(&cfg, &writers, &resolved, &start_acks, &cancel).await?;
        }
    }

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

async fn run_pair_rounds(
    cfg: &Config,
    writers: &[(String, Arc<Mutex<OwnedWriteHalf>>)],
    resolved: &[std::net::SocketAddr],
    start_acks: &Arc<AtomicU64>,
    cancel: &CancellationToken,
) -> Result<()> {
    let rounds = round_robin_rounds(&cfg.hostlist);
    let pairs_per = rounds.first().map(|r| r.len()).unwrap_or(0);
    let undirected = cfg.hostlist.len() * cfg.hostlist.len().saturating_sub(1) / 2;
    eprintln!(
        "tperf-client: pairs schedule: {} rounds, {} pairs/round, {} undirected pairs (every host vs every other)",
        rounds.len(),
        pairs_per,
        undirected
    );
    eprintln!(
        "tperf-client: {}s per round; send SIGINT/SIGTERM to stop",
        PAIR_ROUND_SECS
    );

    let mut tournament = 0u64;
    while !cancel.is_cancelled() {
        tournament += 1;
        for (i, pairs) in rounds.iter().enumerate() {
            if cancel.is_cancelled() {
                break;
            }
            eprintln!(
                "tperf-client: tournament {tournament}  round {}/{}  {} pairs",
                i + 1,
                rounds.len(),
                pairs.len()
            );
            let mut asg = assignments_for_pairs(&cfg.hostlist, pairs);
            fill_resolved(&mut asg, resolved);
            send_starts(cfg, writers, &asg, start_acks).await?;
            wait_round(cancel, start_acks, writers.len() as u64).await;
        }
        if !cancel.is_cancelled() {
            eprintln!("tperf-client: completed tournament {tournament}; repeating");
        }
    }
    Ok(())
}

async fn send_starts(
    cfg: &Config,
    writers: &[(String, Arc<Mutex<OwnedWriteHalf>>)],
    asg: &[Assignment],
    start_acks: &AtomicU64,
) -> Result<()> {
    start_acks.store(0, Ordering::Relaxed);
    for (i, (host, wr)) in writers.iter().enumerate() {
        let mut g = wr.lock().await;
        write_frame(
            &mut *g,
            &Msg::Start {
                config: cfg.clone(),
                assignment: asg[i].clone(),
            },
        )
        .await
        .with_context(|| format!("send start to {host}"))?;
    }
    Ok(())
}

async fn wait_round(cancel: &CancellationToken, start_acks: &AtomicU64, need: u64) {
    let ack_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while start_acks.load(Ordering::Relaxed) < need {
        if cancel.is_cancelled() {
            return;
        }
        if tokio::time::Instant::now() >= ack_deadline {
            eprintln!(
                "tperf-client: round start acks {}/{need} (continuing)",
                start_acks.load(Ordering::Relaxed)
            );
            break;
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    }
    // 1s of traffic plus a short slack so the server can close and emit the
    // full 1s metric window before we tear the round down.
    let traffic = Duration::from_secs(PAIR_ROUND_SECS) + Duration::from_millis(100);
    tokio::select! {
        _ = cancel.cancelled() => {}
        _ = tokio::time::sleep(traffic) => {}
    }
}

async fn pump_server(
    name: String,
    rd: tokio::net::tcp::OwnedReadHalf,
    db: crate::db::DbHandle,
    start_acks: Arc<AtomicU64>,
) -> Result<()> {
    let mut rd = BufReader::new(rd);
    let mut got_start = false;
    loop {
        match read_frame(&mut rd).await {
            Ok(Msg::Ack { cmd }) if cmd == "start" => {
                got_start = true;
                start_acks.fetch_add(1, Ordering::Relaxed);
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
