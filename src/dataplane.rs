use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{lookup_host, TcpListener, TcpStream, UdpSocket};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::metrics::{AtomStats, Counters};
use crate::netutil::{
    connect_tcp_addr_retry, connect_tcp_retry, is_loopback_host, join_host_port, local_unicast_ips,
    pick_addr, resolve_retry, tcp_listen, udp_bind, AddrFamily,
};
use crate::proto::Assignment;

const CONNECT_DEADLINE: Duration = Duration::from_secs(30);
const UDP_HDR: usize = 20;
const UDP_PKT: usize = 8192;
const UDP_MAGIC: u32 = 0x5446_5031; // TFP1
const RECV_BUF: usize = 64 * 1024;

pub struct TestIo {
    pub bind_addr: SocketAddr,
    pub counters: Arc<Counters>,
}

pub async fn build_ip_map(
    hosts: &[String],
    port: u16,
    family: Option<AddrFamily>,
) -> HashMap<IpAddr, u32> {
    let mut map = HashMap::new();
    for (id, host) in hosts.iter().enumerate() {
        let query = join_host_port(host, port);
        let addrs = match lookup_host(&query).await {
            Ok(iter) => iter.collect::<Vec<_>>(),
            Err(_) => continue,
        };
        for addr in addrs {
            if family.map(|f| f.matches(addr)).unwrap_or(true) {
                map.insert(addr.ip(), id as u32);
            }
        }
    }
    map
}

pub fn make_payload(size: u64) -> Result<Arc<Vec<u8>>> {
    let n = usize::try_from(size).map_err(|_| anyhow::anyhow!("payload_size too large"))?;
    if n == 0 {
        bail!("payload_size must be >= 1");
    }
    let mut buf = vec![0u8; n];
    for (i, b) in buf.iter_mut().enumerate() {
        *b = (i.wrapping_mul(131) as u8).wrapping_add(0xA5);
    }
    Ok(Arc::new(buf))
}

async fn resolve_bind(
    assignment: &Assignment,
    port: u16,
    family: Option<AddrFamily>,
    cancel: &CancellationToken,
) -> Result<SocketAddr> {
    if !assignment.bind.is_empty() {
        if let Ok(a) = assignment.bind.parse::<SocketAddr>() {
            return Ok(a);
        }
    }
    match resolve_retry(
        &assignment.self_name,
        port,
        family,
        CONNECT_DEADLINE,
        cancel,
    )
    .await
    {
        Ok(a) if !a.ip().is_loopback() || is_loopback_host(&assignment.self_name) => Ok(a),
        Ok(_) | Err(_) => {
            let locals: Vec<SocketAddr> = local_unicast_ips()
                .into_iter()
                .map(|ip| SocketAddr::new(ip, port))
                .collect();
            pick_addr(&locals, family).with_context(|| {
                format!("cannot determine bind address for {}", assignment.self_name)
            })
        }
    }
}

pub async fn spawn_tcp(
    cfg: &Config,
    assignment: &Assignment,
    family: Option<AddrFamily>,
    cancel: CancellationToken,
    tasks: &mut JoinSet<()>,
) -> Result<TestIo> {
    let port = cfg.test_port()?;
    let bind_addr = resolve_bind(assignment, port, family, &cancel).await?;
    let listener = tcp_listen(bind_addr)?;
    let counters = Arc::new(Counters::new(&cfg.hostlist));
    let ip_to_id = Arc::new(build_ip_map(&cfg.hostlist, port, family).await);
    let payload = make_payload(cfg.payload_size)?;

    let io = TestIo {
        bind_addr,
        counters: counters.clone(),
    };

    {
        let cancel = cancel.clone();
        let counters = counters.clone();
        let ip_to_id = ip_to_id.clone();
        tasks.spawn(async move {
            tcp_accept_loop(listener, counters, ip_to_id, cancel).await;
        });
    }

    for t in &assignment.targets {
        for _ in 0..cfg.workers {
            let host = t.name.clone();
            let dest = t.addr.clone();
            let id = t.id;
            let cancel = cancel.clone();
            let counters = counters.clone();
            let payload = payload.clone();
            let peer = counters.peer(id);
            tasks.spawn(async move {
                if let Err(e) =
                    tcp_send_loop(&host, port, &dest, payload, counters, peer, cancel.clone()).await
                {
                    if !cancel.is_cancelled() {
                        eprintln!("tperf-server: send to {host}:{port}: {e:#}");
                    }
                }
            });
        }
    }
    Ok(io)
}

async fn tcp_accept_loop(
    listener: TcpListener,
    counters: Arc<Counters>,
    ip_to_id: Arc<HashMap<IpAddr, u32>>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            acc = listener.accept() => {
                match acc {
                    Ok((stream, peer)) => {
                        let _ = stream.set_nodelay(true);
                        let counters = counters.clone();
                        let ip_to_id = ip_to_id.clone();
                        let cancel = cancel.clone();
                        tokio::spawn(async move {
                            let peer_stats = ip_to_id.get(&peer.ip()).and_then(|id| counters.peer(*id));
                            tcp_recv_loop(stream, counters, peer_stats, cancel).await;
                        });
                    }
                    Err(e) => {
                        if cancel.is_cancelled() {
                            break;
                        }
                        eprintln!("tperf-server: accept: {e}");
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
        }
    }
}

async fn tcp_recv_loop(
    mut stream: TcpStream,
    total: Arc<Counters>,
    peer: Option<Arc<AtomStats>>,
    cancel: CancellationToken,
) {
    let mut buf = vec![0u8; RECV_BUF];
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            n = stream.read(&mut buf) => {
                match n {
                    Ok(0) => break,
                    Ok(n) => {
                        let n = n as u64;
                        total.total.add_recv(n, 0);
                        if let Some(p) = &peer {
                            p.add_recv(n, 0);
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

async fn tcp_send_loop(
    host: &str,
    port: u16,
    dest: &str,
    payload: Arc<Vec<u8>>,
    total: Arc<Counters>,
    peer: Option<Arc<AtomStats>>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut stream = if let Ok(addr) = dest.parse::<SocketAddr>() {
        connect_tcp_addr_retry(addr, CONNECT_DEADLINE, &cancel).await?
    } else {
        connect_tcp_retry(host, port, CONNECT_DEADLINE, &cancel).await?
    };
    loop {
        if cancel.is_cancelled() {
            break;
        }
        match stream.write_all(&payload).await {
            Ok(()) => {
                let n = payload.len() as u64;
                total.total.add_sent(n, 1);
                if let Some(p) = &peer {
                    p.add_sent(n, 1);
                }
            }
            Err(e) => {
                if cancel.is_cancelled() {
                    break;
                }
                total.total.add_drop(1);
                if let Some(p) = &peer {
                    p.add_drop(1);
                }
                return Err(e.into());
            }
        }
    }
    Ok(())
}

pub async fn spawn_udp(
    cfg: &Config,
    assignment: &Assignment,
    family: Option<AddrFamily>,
    cancel: CancellationToken,
    tasks: &mut JoinSet<()>,
) -> Result<TestIo> {
    let port = cfg.test_port()?;
    let self_id = assignment.self_id;
    let bind_addr = resolve_bind(assignment, port, family, &cancel).await?;
    let sock = Arc::new(udp_bind(bind_addr)?);
    let counters = Arc::new(Counters::new(&cfg.hostlist));
    let payload = make_payload(cfg.payload_size)?;
    let seq = Arc::new(
        (0..cfg.hostlist.len())
            .map(|_| std::sync::atomic::AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );
    let expected = Arc::new(
        (0..cfg.hostlist.len())
            .map(|_| Mutex::new(None::<u64>))
            .collect::<Vec<_>>(),
    );

    let io = TestIo {
        bind_addr,
        counters: counters.clone(),
    };

    {
        let sock = sock.clone();
        let counters = counters.clone();
        let expected = expected.clone();
        let cancel = cancel.clone();
        let npeers = cfg.hostlist.len();
        tasks.spawn(async move {
            udp_recv_loop(sock, counters, expected, npeers, self_id, cancel).await;
        });
    }

    for t in &assignment.targets {
        let dest = if let Ok(a) = t.addr.parse::<SocketAddr>() {
            a
        } else {
            match resolve_retry(&t.name, port, family, CONNECT_DEADLINE, &cancel).await {
                Ok(a) => a,
                Err(e) => {
                    eprintln!("tperf-server: resolve {}: {e:#}", t.name);
                    continue;
                }
            }
        };
        for _ in 0..cfg.workers {
            let sock = sock.clone();
            let payload = payload.clone();
            let counters = counters.clone();
            let seq = seq.clone();
            let peer = counters.peer(t.id);
            let dest_id = t.id;
            let cancel = cancel.clone();
            tasks.spawn(async move {
                udp_send_loop(
                    sock, dest, dest_id, self_id, payload, counters, peer, seq, cancel,
                )
                .await;
            });
        }
    }
    Ok(io)
}

fn write_hdr(buf: &mut [u8], sender_id: u32, seq: u64, len: u32) {
    buf[0..4].copy_from_slice(&UDP_MAGIC.to_be_bytes());
    buf[4..8].copy_from_slice(&sender_id.to_be_bytes());
    buf[8..16].copy_from_slice(&seq.to_be_bytes());
    buf[16..20].copy_from_slice(&len.to_be_bytes());
}

fn read_hdr(buf: &[u8]) -> Option<(u32, u64, u32)> {
    if buf.len() < UDP_HDR {
        return None;
    }
    let magic = u32::from_be_bytes(buf[0..4].try_into().ok()?);
    if magic != UDP_MAGIC {
        return None;
    }
    let sender_id = u32::from_be_bytes(buf[4..8].try_into().ok()?);
    let seq = u64::from_be_bytes(buf[8..16].try_into().ok()?);
    let len = u32::from_be_bytes(buf[16..20].try_into().ok()?);
    Some((sender_id, seq, len))
}

fn note_seq(expected: &Mutex<Option<u64>>, seq: u64) -> u64 {
    let mut slot = expected.lock().unwrap_or_else(|e| e.into_inner());
    match *slot {
        None => {
            *slot = Some(seq.saturating_add(1));
            0
        }
        Some(exp) => {
            if seq == exp {
                *slot = Some(exp.saturating_add(1));
                0
            } else if seq > exp {
                let drop = seq - exp;
                *slot = Some(seq.saturating_add(1));
                drop
            } else {
                0
            }
        }
    }
}

async fn udp_recv_loop(
    sock: Arc<UdpSocket>,
    counters: Arc<Counters>,
    expected: Arc<Vec<Mutex<Option<u64>>>>,
    npeers: usize,
    self_id: u32,
    cancel: CancellationToken,
) {
    let mut buf = vec![0u8; 65536];
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            res = sock.recv_from(&mut buf) => {
                match res {
                    Ok((n, _)) => {
                        let Some((sender_id, seq, plen)) = read_hdr(&buf[..n]) else { continue };
                        let payload_bytes = n.saturating_sub(UDP_HDR) as u64;
                        if plen as u64 != payload_bytes {
                            continue;
                        }
                        counters.total.add_recv(payload_bytes, 1);
                        if (sender_id as usize) < npeers && sender_id != self_id {
                            if let Some(p) = counters.peer(sender_id) {
                                p.add_recv(payload_bytes, 1);
                            }
                            if let Some(slot) = expected.get(sender_id as usize) {
                                let d = note_seq(slot, seq);
                                counters.total.add_drop(d);
                                if let Some(p) = counters.peer(sender_id) {
                                    p.add_drop(d);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if cancel.is_cancelled() {
                            break;
                        }
                        eprintln!("tperf-server: udp recv: {e}");
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn udp_send_loop(
    sock: Arc<UdpSocket>,
    dest: SocketAddr,
    dest_id: u32,
    self_id: u32,
    payload: Arc<Vec<u8>>,
    total: Arc<Counters>,
    peer: Option<Arc<AtomStats>>,
    seqs: Arc<Vec<std::sync::atomic::AtomicU64>>,
    cancel: CancellationToken,
) {
    let chunk = UDP_PKT.saturating_sub(UDP_HDR);
    if chunk == 0 {
        return;
    }
    let mut pkt = vec![0u8; UDP_PKT];
    loop {
        if cancel.is_cancelled() {
            break;
        }
        let mut offset = 0usize;
        let mut write_ok = true;
        while offset < payload.len() {
            if cancel.is_cancelled() {
                write_ok = false;
                break;
            }
            let n = (payload.len() - offset).min(chunk);
            let seq = seqs
                .get(dest_id as usize)
                .map(|s| s.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
                .unwrap_or(0);
            write_hdr(&mut pkt, self_id, seq, n as u32);
            pkt[UDP_HDR..UDP_HDR + n].copy_from_slice(&payload[offset..offset + n]);
            match sock.send_to(&pkt[..UDP_HDR + n], dest).await {
                Ok(_) => offset += n,
                Err(_) => {
                    total.total.add_drop(1);
                    if let Some(p) = &peer {
                        p.add_drop(1);
                    }
                    write_ok = false;
                    break;
                }
            }
        }
        if write_ok && offset == payload.len() {
            let n = payload.len() as u64;
            let pkts = payload.len().div_ceil(chunk) as u64;
            total.total.add_sent(n, pkts.max(1));
            if let Some(p) = &peer {
                p.add_sent(n, pkts.max(1));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hdr_roundtrip() {
        let mut buf = [0u8; UDP_HDR];
        write_hdr(&mut buf, 7, 99, 1234);
        let (id, seq, len) = read_hdr(&buf).unwrap();
        assert_eq!(id, 7);
        assert_eq!(seq, 99);
        assert_eq!(len, 1234);
    }

    #[test]
    fn seq_gaps() {
        let slot = Mutex::new(None);
        assert_eq!(note_seq(&slot, 0), 0);
        assert_eq!(note_seq(&slot, 1), 0);
        assert_eq!(note_seq(&slot, 5), 3);
        assert_eq!(note_seq(&slot, 6), 0);
        assert_eq!(note_seq(&slot, 3), 0);
    }
}
