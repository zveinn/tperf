use anyhow::{bail, Context, Result};
use rusqlite::Connection;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::metrics::{bps, fmt_bps};
use crate::proto::PeerMetrics;

#[derive(Debug, Clone)]
pub struct RateStats {
    pub min_bps: f64,
    pub max_bps: f64,
    pub avg_bps: f64,
    pub windows: usize,
}

impl RateStats {
    /// `samples` are (bytes, duration_ns) per window.
    /// avg is duration-weighted; min/max are over per-window bitrates.
    pub fn from_samples(samples: &[(u64, u64)]) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        let mut min_bps = f64::INFINITY;
        let mut max_bps = 0.0;
        let mut bytes: u64 = 0;
        let mut duration_ns: u128 = 0;
        for &(b, d) in samples {
            let r = bps(b, d);
            if r < min_bps {
                min_bps = r;
            }
            if r > max_bps {
                max_bps = r;
            }
            bytes = bytes.saturating_add(b);
            duration_ns = duration_ns.saturating_add(d as u128);
        }
        let avg_bps = if duration_ns == 0 {
            0.0
        } else {
            (bytes as f64) * 8.0 * 1_000_000_000.0 / (duration_ns as f64)
        };
        Some(RateStats {
            min_bps,
            max_bps,
            avg_bps,
            windows: samples.len(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct PairStats {
    pub src: String,
    pub dst: String,
    pub stats: RateStats,
}

pub fn parse_args<I, S>(args: I) -> Result<PathBuf>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args: Vec<String> = args.into_iter().map(|s| s.as_ref().to_string()).collect();
    let mut db: Option<PathBuf> = None;
    let mut i = 0;
    while i < args.len() {
        let a = args[i].as_str();
        if a == "--db" {
            i += 1;
            let path = args
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("--db requires a path"))?;
            db = Some(PathBuf::from(path));
        } else if let Some(path) = a.strip_prefix("--db=") {
            if path.is_empty() {
                bail!("--db requires a path");
            }
            db = Some(PathBuf::from(path));
        } else if a == "-h" || a == "--help" {
            bail!("usage: tperf analyze --db <file>");
        } else {
            bail!("unexpected argument {a:?}; usage: tperf analyze --db <file>");
        }
        i += 1;
    }
    db.ok_or_else(|| anyhow::anyhow!("missing --db; usage: tperf analyze --db <file>"))
}

/// `--db` must name a sqlite database *file*, not a directory.
pub fn require_db_file(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() {
        bail!("--db requires a path to a sqlite database file");
    }
    if path.is_dir() {
        bail!(
            "--db must be a sqlite database file, not a directory: {}",
            path.display()
        );
    }
    if !path.exists() {
        bail!("database file not found: {}", path.display());
    }
    if !path.is_file() {
        bail!("--db must be a sqlite database file: {}", path.display());
    }
    Ok(())
}

pub fn run(db_path: &Path) -> Result<()> {
    let report = load(db_path)?;
    print_report(&report);
    Ok(())
}

struct Report {
    db_path: String,
    tag: String,
    hosts: Vec<String>,
    pairs: Vec<PairStats>,
    total: RateStats,
}

fn load(db_path: &Path) -> Result<Report> {
    require_db_file(db_path)?;
    let conn =
        Connection::open(db_path).with_context(|| format!("open sqlite {}", db_path.display()))?;

    let (tag, hosts_json): (String, String) = conn
        .query_row("SELECT tag, hostlist FROM config WHERE id = 1", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .context("read config row")?;
    let hosts: Vec<String> = serde_json::from_str(&hosts_json).unwrap_or_default();

    let mut stmt = conn.prepare(
        "SELECT server, wall_start_ns, duration_ns, bytes_sent, send_bps, peers_json
         FROM metrics ORDER BY wall_start_ns, server",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(RawRow {
            server: row.get(0)?,
            wall_start_ns: row.get::<_, i64>(1)? as u64,
            duration_ns: row.get::<_, i64>(2)? as u64,
            bytes_sent: row.get::<_, i64>(3)? as u64,
            send_bps: row.get(4)?,
            peers_json: row.get(5)?,
        })
    })?;

    let mut pair_samples: BTreeMap<(String, String), Vec<(u64, u64)>> = BTreeMap::new();
    let mut bucket_bps: BTreeMap<u64, f64> = BTreeMap::new();
    let mut total_samples: Vec<(u64, u64)> = Vec::new();

    for row in rows {
        let row = row.context("read metrics row")?;
        let peers: Vec<PeerMetrics> = serde_json::from_str(&row.peers_json)
            .with_context(|| format!("peers_json for {}", row.server))?;
        for p in peers {
            if p.peer == row.server {
                continue;
            }
            pair_samples
                .entry((row.server.clone(), p.peer))
                .or_default()
                .push((p.bytes_sent, row.duration_ns));
        }
        let bucket = row.wall_start_ns / 1_000_000_000;
        *bucket_bps.entry(bucket).or_insert(0.0) += row.send_bps;
        total_samples.push((row.bytes_sent, row.duration_ns));
    }

    let pairs: Vec<PairStats> = pair_samples
        .into_iter()
        .filter_map(|((src, dst), samples)| {
            RateStats::from_samples(&samples).map(|stats| PairStats { src, dst, stats })
        })
        .collect();

    // Per-interval totals (sum of every server's send rate in that second).
    // min/max come from those intervals; avg is duration-weighted over all
    // server windows so a short last window does not dominate.
    let mut total = RateStats::from_samples(&total_samples)
        .ok_or_else(|| anyhow::anyhow!("no metrics rows in {}", db_path.display()))?;
    if !bucket_bps.is_empty() {
        total.min_bps = bucket_bps.values().copied().fold(f64::INFINITY, f64::min);
        total.max_bps = bucket_bps.values().copied().fold(0.0, f64::max);
        // avg of concurrent total: mean of per-second bucket sums.
        let sum: f64 = bucket_bps.values().sum();
        total.avg_bps = sum / bucket_bps.len() as f64;
        total.windows = bucket_bps.len();
    }

    let display_path = db_path
        .canonicalize()
        .unwrap_or_else(|_| db_path.to_path_buf());
    Ok(Report {
        db_path: display_path.display().to_string(),
        tag,
        hosts,
        pairs,
        total,
    })
}

struct RawRow {
    server: String,
    wall_start_ns: u64,
    duration_ns: u64,
    bytes_sent: u64,
    send_bps: f64,
    peers_json: String,
}

fn print_report(r: &Report) {
    println!(
        "tperf analyze  db={}  tag={}  hosts={}",
        r.db_path,
        r.tag,
        r.hosts.len()
    );
    println!();

    println!("pairs (all time)");
    println!("----------------");
    if r.pairs.is_empty() {
        println!("(no pair samples)");
    } else {
        let label_w = r
            .pairs
            .iter()
            .map(|p| p.src.len() + p.dst.len() + 4)
            .max()
            .unwrap_or(8);
        for p in &r.pairs {
            println!("{}", format_pair_line(p, label_w));
        }
    }

    println!();
    println!("total throughput (all time)");
    println!("---------------------------");
    println!(
        "{:<w$}  min={}   avg={}   max={}",
        "[all]",
        fmt_bps(r.total.min_bps),
        fmt_bps(r.total.avg_bps),
        fmt_bps(r.total.max_bps),
        w = 12
    );

    println!();
    println!("worst pairs");
    println!("-----------");
    let mut worst = r.pairs.clone();
    worst.sort_by(|a, b| {
        a.stats
            .avg_bps
            .partial_cmp(&b.stats.avg_bps)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                a.stats
                    .min_bps
                    .partial_cmp(&b.stats.min_bps)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });
    worst.truncate(5);
    if worst.is_empty() {
        println!("(no pair samples)");
    } else {
        let label_w = worst
            .iter()
            .map(|p| p.src.len() + p.dst.len() + 4)
            .max()
            .unwrap_or(8);
        for (i, p) in worst.iter().enumerate() {
            println!("{:>2}. {}", i + 1, format_pair_line(p, label_w));
        }
    }
}

fn format_pair_line(p: &PairStats, label_w: usize) -> String {
    let label = format!("[{} → {}]", p.src, p.dst);
    format!(
        "{label:<label_w$}  min={}   avg={}   max={}",
        fmt_bps(p.stats.min_bps),
        fmt_bps(p.stats.avg_bps),
        fmt_bps(p.stats.max_bps),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_db_flag() {
        let p = parse_args(["--db", "/tmp/run1.db"]).unwrap();
        assert_eq!(p, PathBuf::from("/tmp/run1.db"));
        let p = parse_args(["--db=./out/mesh-tcp.db"]).unwrap();
        assert_eq!(p, PathBuf::from("./out/mesh-tcp.db"));
        assert!(parse_args(["--db"]).is_err());
        assert!(parse_args::<[&str; 0], _>([]).is_err());
        assert!(parse_args(["live"]).is_err());
    }

    #[test]
    fn require_db_file_rejects_directory() {
        let dir = std::env::temp_dir();
        let err = require_db_file(&dir).unwrap_err().to_string();
        assert!(err.contains("not a directory"), "{err}");
    }

    #[test]
    fn require_db_file_rejects_missing() {
        let err = require_db_file(Path::new("/no/such/tperf-analyze.db"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("not found"), "{err}");
    }

    #[test]
    fn weighted_avg_and_minmax() {
        // 1s @ 10 Gbps (1.25e9 bytes), 1s @ 30 Gbps (3.75e9 bytes)
        let ten_gbps_bytes = 1_250_000_000u64;
        let thirty_gbps_bytes = 3_750_000_000u64;
        let s = RateStats::from_samples(&[
            (ten_gbps_bytes, 1_000_000_000),
            (thirty_gbps_bytes, 1_000_000_000),
        ])
        .unwrap();
        assert_eq!(s.windows, 2);
        assert!((s.min_bps - 10e9).abs() < 1.0);
        assert!((s.max_bps - 30e9).abs() < 1.0);
        assert!((s.avg_bps - 20e9).abs() < 1.0);

        // longer window at 30 Gbps pulls the average up
        let s = RateStats::from_samples(&[
            (ten_gbps_bytes, 1_000_000_000),
            (thirty_gbps_bytes * 3, 3_000_000_000),
        ])
        .unwrap();
        assert!((s.avg_bps - 25e9).abs() < 1.0);
    }

    #[test]
    fn worst_five_are_lowest_avg() {
        let mk = |src: &str, dst: &str, avg: f64| PairStats {
            src: src.into(),
            dst: dst.into(),
            stats: RateStats {
                min_bps: avg / 2.0,
                max_bps: avg,
                avg_bps: avg,
                windows: 1,
            },
        };
        let mut pairs = vec![
            mk("a", "b", 50e9),
            mk("a", "c", 10e9),
            mk("b", "a", 40e9),
            mk("b", "c", 11e9),
            mk("c", "a", 9e9),
            mk("c", "b", 30e9),
        ];
        pairs.sort_by(|a, b| a.stats.avg_bps.partial_cmp(&b.stats.avg_bps).unwrap());
        pairs.truncate(5);
        let names: Vec<_> = pairs
            .iter()
            .map(|p| format!("{}→{}", p.src, p.dst))
            .collect();
        assert_eq!(names, vec!["c→a", "a→c", "b→c", "c→b", "b→a"]);
    }
}
