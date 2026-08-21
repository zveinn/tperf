use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::{Config, Network};
use crate::dataplane;
use crate::metrics::{
    build_window, cpu_pct, monotonic_ns, realtime_ns, sample_proc, Counters, ProcSample, Snap,
};
use crate::netutil::{parse_host_port, tcp_listen};
use crate::proto::{read_frame, write_frame_locked, Assignment, MetricWindow, Msg};

const WINDOW_NS: u64 = 1_000_000_000;
/// Sleep until this close to the deadline, then spin on CLOCK_MONOTONIC.
const WINDOW_SPIN_NS: u64 = 200_000;

pub async fn run(client_addr: &str) -> Result<()> {
    let (host, port) = parse_host_port(client_addr)?;
    let bind = if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        std::net::SocketAddr::new(ip, port)
    } else {
        crate::netutil::resolve_one(&host, port, None)
            .await
            .with_context(|| format!("resolve control bind {client_addr}"))?
    };

    let listener = tcp_listen(bind).with_context(|| format!("control listen {bind}"))?;
    eprintln!("tperf-server: control listening on {bind}");
    loop {
        let (stream, peer) = listener.accept().await?;
        eprintln!("tperf-server: client {peer}");
        if let Err(e) = handle_conn(stream).await {
            eprintln!("tperf-server: session {peer}: {e:#}");
        }
        eprintln!("tperf-server: client {peer} disconnected");
    }
}

struct RunningTest {
    cancel: CancellationToken,
    metrics_done: Option<oneshot::Receiver<()>>,
    tasks: JoinSet<()>,
}

impl RunningTest {
    async fn stop(mut self) {
        self.cancel.cancel();
        if let Some(rx) = self.metrics_done.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), rx).await;
        }
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
    }
}

async fn handle_conn(stream: TcpStream) -> Result<()> {
    let _ = stream.set_nodelay(true);
    let (rd, wr) = stream.into_split();
    let wr = Arc::new(Mutex::new(wr));
    let mut rd = rd;
    let mut test: Option<RunningTest> = None;
    loop {
        let msg = match read_frame(&mut rd).await {
            Ok(m) => m,
            Err(_) => break,
        };
        match msg {
            Msg::Start { config, assignment } => {
                if let Some(t) = test.take() {
                    t.stop().await;
                }
                match start_test(config, assignment, wr.clone()).await {
                    Ok(t) => {
                        if let Err(e) = write_frame_locked(
                            &wr,
                            &Msg::Ack {
                                cmd: "start".into(),
                            },
                        )
                        .await
                        {
                            t.stop().await;
                            return Err(e);
                        }
                        test = Some(t);
                    }
                    Err(e) => {
                        let _ = write_frame_locked(
                            &wr,
                            &Msg::Error {
                                message: format!("{e:#}"),
                            },
                        )
                        .await;
                    }
                }
            }
            Msg::Stop => {
                if let Some(t) = test.take() {
                    t.stop().await;
                }
                write_frame_locked(&wr, &Msg::Ack { cmd: "stop".into() }).await?;
            }
            other => {
                eprintln!("tperf-server: ignoring unexpected message {other:?}");
            }
        }
    }
    if let Some(t) = test.take() {
        t.stop().await;
    }
    Ok(())
}

async fn start_test(
    config: Config,
    assignment: Assignment,
    wr: Arc<Mutex<OwnedWriteHalf>>,
) -> Result<RunningTest> {
    config.validate().context("config from client")?;
    let family = config.test_family()?;
    let cancel = CancellationToken::new();
    let mut tasks = JoinSet::new();

    let io = match config.network {
        Network::Tcp => {
            dataplane::spawn_tcp(&config, &assignment, family, cancel.clone(), &mut tasks).await?
        }
        Network::Udp => {
            dataplane::spawn_udp(&config, &assignment, family, cancel.clone(), &mut tasks).await?
        }
    };

    eprintln!(
        "tperf-server: start {} {} bind={} targets={} workers={} payload={}",
        config.test_type,
        config.network,
        io.bind_addr,
        assignment.targets.len(),
        config.workers,
        config.payload_size
    );

    let (done_tx, done_rx) = oneshot::channel();
    {
        let cancel = cancel.clone();
        let counters = io.counters.clone();
        let server = assignment.self_name.clone();
        tasks.spawn(async move {
            metrics_loop(server, counters, wr, cancel, done_tx).await;
        });
    }

    Ok(RunningTest {
        cancel,
        metrics_done: Some(done_rx),
        tasks,
    })
}

async fn metrics_loop(
    server: String,
    counters: Arc<Counters>,
    wr: Arc<Mutex<OwnedWriteHalf>>,
    cancel: CancellationToken,
    done: oneshot::Sender<()>,
) {
    let mut prev_total: Snap;
    let mut prev_peers: Vec<Snap>;
    (prev_total, prev_peers) = counters.snapshot_all();
    let mut prev_proc = sample_proc().ok();
    let origin = monotonic_ns();
    let mut mono_start = origin;
    let mut wall_start = realtime_ns();
    let mut window_i: u64 = 1;

    loop {
        let deadline = origin.saturating_add(window_i.saturating_mul(WINDOW_NS));
        // Only emit a window that ran to the 1s grid mark. A stop/round-switch
        // before the deadline drops the partial interval instead of reporting
        // a 0.66s (or 20ms) slice.
        if !wait_until_mono(deadline, &cancel).await {
            let _ = done.send(());
            return;
        }
        emit_window(
            &server,
            &counters,
            &wr,
            &mut prev_total,
            &mut prev_peers,
            &mut prev_proc,
            &mut mono_start,
            &mut wall_start,
        )
        .await;
        window_i = window_i.saturating_add(1);
        if cancel.is_cancelled() {
            let _ = done.send(());
            return;
        }
    }
}

/// Wait until CLOCK_MONOTONIC reaches `deadline_ns`. Returns false if cancelled
/// first. Coarse-sleeps, then spins the last ~200µs so the mark is tight.
async fn wait_until_mono(deadline_ns: u64, cancel: &CancellationToken) -> bool {
    loop {
        let now = monotonic_ns();
        if now >= deadline_ns {
            return true;
        }
        if cancel.is_cancelled() {
            return false;
        }
        let remain = deadline_ns - now;
        if remain > WINDOW_SPIN_NS {
            tokio::select! {
                _ = cancel.cancelled() => return false,
                _ = tokio::time::sleep(Duration::from_nanos(remain - WINDOW_SPIN_NS)) => {}
            }
        } else {
            while monotonic_ns() < deadline_ns {
                if cancel.is_cancelled() {
                    return false;
                }
                std::hint::spin_loop();
            }
            return true;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn emit_window(
    server: &str,
    counters: &Counters,
    wr: &Mutex<OwnedWriteHalf>,
    prev_total: &mut Snap,
    prev_peers: &mut Vec<Snap>,
    prev_proc: &mut Option<ProcSample>,
    mono_start: &mut u64,
    wall_start: &mut u64,
) {
    let mono_end = monotonic_ns();
    let wall_end = realtime_ns();
    let duration_ns = mono_end.saturating_sub(*mono_start).max(1);
    let (tot, peers) = counters.snapshot_all();
    let delta_tot = prev_total.delta(tot);
    let delta_peers: Vec<Snap> = prev_peers
        .iter()
        .zip(peers.iter())
        .map(|(a, b)| a.delta(*b))
        .collect();
    let proc_now = sample_proc().ok();
    let cpu = match (*prev_proc, proc_now) {
        (Some(a), Some(b)) => cpu_pct(a, b, duration_ns),
        _ => 0.0,
    };
    let mem = proc_now.map(|p| p.rss_bytes).unwrap_or(0);
    let window: MetricWindow = build_window(
        server,
        *wall_start,
        wall_end,
        duration_ns,
        delta_tot,
        &delta_peers,
        &counters.names,
        cpu,
        mem,
    );
    let _ = write_frame_locked(wr, &Msg::Metrics { window }).await;
    *prev_total = tot;
    *prev_peers = peers;
    *prev_proc = proc_now;
    *mono_start = mono_end;
    *wall_start = wall_end;
}
