use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::proto::{MetricWindow, PeerMetrics};

const RELAXED: Ordering = Ordering::Relaxed;

#[derive(Debug, Default)]
pub struct AtomStats {
    pub bytes_sent: AtomicU64,
    pub bytes_recv: AtomicU64,
    pub packets_sent: AtomicU64,
    pub packets_recv: AtomicU64,
    pub packets_dropped: AtomicU64,
}

impl AtomStats {
    pub fn snapshot(&self) -> Snap {
        Snap {
            bytes_sent: self.bytes_sent.load(RELAXED),
            bytes_recv: self.bytes_recv.load(RELAXED),
            packets_sent: self.packets_sent.load(RELAXED),
            packets_recv: self.packets_recv.load(RELAXED),
            packets_dropped: self.packets_dropped.load(RELAXED),
        }
    }

    pub fn add_sent(&self, bytes: u64, packets: u64) {
        if bytes > 0 {
            self.bytes_sent.fetch_add(bytes, RELAXED);
        }
        if packets > 0 {
            self.packets_sent.fetch_add(packets, RELAXED);
        }
    }

    pub fn add_recv(&self, bytes: u64, packets: u64) {
        if bytes > 0 {
            self.bytes_recv.fetch_add(bytes, RELAXED);
        }
        if packets > 0 {
            self.packets_recv.fetch_add(packets, RELAXED);
        }
    }

    pub fn add_drop(&self, n: u64) {
        if n > 0 {
            self.packets_dropped.fetch_add(n, RELAXED);
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Snap {
    pub bytes_sent: u64,
    pub bytes_recv: u64,
    pub packets_sent: u64,
    pub packets_recv: u64,
    pub packets_dropped: u64,
}

impl Snap {
    pub fn delta(self, later: Snap) -> Snap {
        Snap {
            bytes_sent: later.bytes_sent.saturating_sub(self.bytes_sent),
            bytes_recv: later.bytes_recv.saturating_sub(self.bytes_recv),
            packets_sent: later.packets_sent.saturating_sub(self.packets_sent),
            packets_recv: later.packets_recv.saturating_sub(self.packets_recv),
            packets_dropped: later.packets_dropped.saturating_sub(self.packets_dropped),
        }
    }
}

pub struct Counters {
    pub total: Arc<AtomStats>,
    pub peers: Vec<Arc<AtomStats>>,
    pub names: Vec<String>,
}

impl Counters {
    pub fn new(hostlist: &[String]) -> Self {
        let peers = hostlist
            .iter()
            .map(|_| Arc::new(AtomStats::default()))
            .collect();
        Self {
            total: Arc::new(AtomStats::default()),
            peers,
            names: hostlist.to_vec(),
        }
    }

    pub fn peer(&self, id: u32) -> Option<Arc<AtomStats>> {
        self.peers.get(id as usize).cloned()
    }

    pub fn snapshot_all(&self) -> (Snap, Vec<Snap>) {
        let total = self.total.snapshot();
        let peers = self.peers.iter().map(|p| p.snapshot()).collect();
        (total, peers)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ProcSample {
    pub utime_ticks: u64,
    pub stime_ticks: u64,
    pub rss_bytes: u64,
}

pub fn sample_proc() -> std::io::Result<ProcSample> {
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let (utime, stime) = parse_stat_cpu(&stat)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "stat"))?;
    let statm = std::fs::read_to_string("/proc/self/statm")?;
    let rss_pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let page = page_size() as u64;
    Ok(ProcSample {
        utime_ticks: utime,
        stime_ticks: stime,
        rss_bytes: rss_pages.saturating_mul(page),
    })
}

fn parse_stat_cpu(stat: &str) -> Option<(u64, u64)> {
    let rparen = stat.rfind(')')?;
    let rest = stat.get(rparen + 2..)?;
    let mut fields = rest.split_whitespace();
    // After comm: state ppid pgrp session tty_nr tpgid flags minflt cminflt majflt cmajflt utime stime
    for _ in 0..11 {
        fields.next()?;
    }
    let utime: u64 = fields.next()?.parse().ok()?;
    let stime: u64 = fields.next()?.parse().ok()?;
    Some((utime, stime))
}

pub fn clk_tck() -> i64 {
    let v = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if v > 0 {
        v
    } else {
        100
    }
}

pub fn page_size() -> i64 {
    let v = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if v > 0 {
        v
    } else {
        4096
    }
}

/// CLOCK_REALTIME nanoseconds.
pub fn realtime_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts) };
    if rc != 0 {
        return 0;
    }
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

/// CLOCK_MONOTONIC nanoseconds.
pub fn monotonic_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    if rc != 0 {
        return 0;
    }
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

pub fn cpu_pct(before: ProcSample, after: ProcSample, duration_ns: u64) -> f64 {
    if duration_ns == 0 {
        return 0.0;
    }
    let dticks = after
        .utime_ticks
        .saturating_sub(before.utime_ticks)
        .saturating_add(after.stime_ticks.saturating_sub(before.stime_ticks));
    let cpu_ns = dticks.saturating_mul(1_000_000_000 / clk_tck() as u64);
    (cpu_ns as f64) / (duration_ns as f64) * 100.0
}

pub fn bps(bytes: u64, duration_ns: u64) -> f64 {
    if duration_ns == 0 {
        return 0.0;
    }
    (bytes as f64) * 8.0 * 1_000_000_000.0 / (duration_ns as f64)
}

#[allow(clippy::too_many_arguments)]
pub fn build_window(
    server: &str,
    wall_start_ns: u64,
    wall_end_ns: u64,
    duration_ns: u64,
    total: Snap,
    peer_snaps: &[Snap],
    names: &[String],
    cpu: f64,
    memory_bytes: u64,
) -> MetricWindow {
    let peers = names
        .iter()
        .zip(peer_snaps.iter())
        .filter(|(_, s)| {
            s.bytes_sent > 0
                || s.bytes_recv > 0
                || s.packets_sent > 0
                || s.packets_recv > 0
                || s.packets_dropped > 0
        })
        .map(|(name, s)| PeerMetrics {
            peer: name.clone(),
            bytes_sent: s.bytes_sent,
            bytes_recv: s.bytes_recv,
            packets_sent: s.packets_sent,
            packets_recv: s.packets_recv,
            packets_dropped: s.packets_dropped,
        })
        .collect();
    MetricWindow {
        server: server.to_string(),
        wall_start_ns,
        wall_end_ns,
        duration_ns,
        bytes_sent: total.bytes_sent,
        bytes_recv: total.bytes_recv,
        send_bps: bps(total.bytes_sent, duration_ns),
        recv_bps: bps(total.bytes_recv, duration_ns),
        packets_sent: total.packets_sent,
        packets_recv: total.packets_recv,
        packets_dropped: total.packets_dropped,
        cpu_pct: cpu,
        memory_bytes,
        peers,
    }
}

pub fn fmt_bps(bps: f64) -> String {
    if bps >= 1e9 {
        format!("{:>7.3} Gbps", bps / 1e9)
    } else if bps >= 1e6 {
        format!("{:>7.3} Mbps", bps / 1e6)
    } else if bps >= 1e3 {
        format!("{:>7.3} Kbps", bps / 1e3)
    } else {
        format!("{:>7.0}  bps", bps)
    }
}

pub fn fmt_bytes(n: u64) -> String {
    const KIB: f64 = 1024.0;
    let v = n as f64;
    if v >= KIB * KIB * KIB {
        format!("{:.2} GiB", v / (KIB * KIB * KIB))
    } else if v >= KIB * KIB {
        format!("{:.2} MiB", v / (KIB * KIB))
    } else if v >= KIB {
        format!("{:.2} KiB", v / KIB)
    } else {
        format!("{n} B")
    }
}

/// Truncate a non-negative value to `n` decimal places (no rounding).
pub fn trunc_decimals(v: f64, n: u32) -> f64 {
    let f = 10f64.powi(n as i32);
    (v * f).trunc() / f
}

pub fn format_window(w: &MetricWindow) -> String {
    let window_s = trunc_decimals(w.duration_ns as f64 / 1e9, 4);
    let name = format!("[{}]", w.server);
    format!(
        "{name:<12} window={window_s:.4}s   send={}   recv={}   drop={:<6} pkts={:<18} cpu={:>5.1}%   rss={}",
        fmt_bps(w.send_bps),
        fmt_bps(w.recv_bps),
        w.packets_dropped,
        format!("{}/{}", w.packets_sent, w.packets_recv),
        w.cpu_pct,
        fmt_bytes(w.memory_bytes),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_and_bps() {
        let a = Snap {
            bytes_sent: 100,
            bytes_recv: 50,
            ..Default::default()
        };
        let b = Snap {
            bytes_sent: 1100,
            bytes_recv: 50,
            ..Default::default()
        };
        let d = a.delta(b);
        assert_eq!(d.bytes_sent, 1000);
        assert_eq!(d.bytes_recv, 0);
        assert!((bps(125_000_000, 1_000_000_000) - 1e9).abs() < 1.0);
    }

    #[test]
    fn parse_stat_sample() {
        let line = "1234 (foo bar) S 1 1 1 0 -1 0 0 0 0 0 10 20 0 0 20 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0";
        let (u, s) = parse_stat_cpu(line).unwrap();
        assert_eq!(u, 10);
        assert_eq!(s, 20);
    }

    #[test]
    fn clocks_move() {
        let a = monotonic_ns();
        let b = monotonic_ns();
        assert!(b >= a);
        let r = realtime_ns();
        assert!(r > 1_000_000_000_000);
    }

    #[test]
    fn format_window_one_line_truncates_duration() {
        let w = MetricWindow {
            server: "srv1".into(),
            wall_start_ns: 0,
            wall_end_ns: 0,
            duration_ns: 97_927_043,
            bytes_sent: 0,
            bytes_recv: 0,
            send_bps: 41.134e9,
            recv_bps: 49.197e9,
            packets_sent: 7683,
            packets_recv: 0,
            packets_dropped: 4,
            cpu_pct: 275.7,
            memory_bytes: (5.43 * 1024.0 * 1024.0) as u64,
            peers: vec![PeerMetrics {
                peer: "srv2".into(),
                bytes_sent: 1,
                bytes_recv: 1,
                packets_sent: 1,
                packets_recv: 1,
                packets_dropped: 0,
            }],
        };
        let s = format_window(&w);
        assert!(!s.contains('\n'), "printout must be a single line: {s}");
        assert!(s.contains("window=0.0979s"), "truncated window, got {s}");
        assert!(
            !s.contains("0.09792"),
            "must not show extra window digits: {s}"
        );
        assert!(!s.contains("srv2"), "no per-peer breakdown: {s}");
        assert!(s.contains("send="));
        assert!(s.contains("recv="));
        assert!(s.contains("drop=4"));
        assert!(s.contains("pkts=7683/0"));
        assert!(s.contains("cpu=275.7%"));
    }
}
