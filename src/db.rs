use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::mpsc::{self, Sender};
use std::thread::JoinHandle;

use crate::config::Config;
use crate::proto::MetricWindow;

enum Event {
    Metric(MetricWindow),
    Shutdown,
}

#[derive(Clone)]
pub struct DbHandle {
    tx: Sender<Event>,
}

pub struct Db {
    handle: DbHandle,
    thread: Option<JoinHandle<Result<()>>>,
}

fn sidecar(path: &Path, suffix: &str) -> std::path::PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(suffix);
    std::path::PathBuf::from(s)
}

fn remove_db_files(path: &Path) -> Result<()> {
    for p in [
        path.to_path_buf(),
        sidecar(path, "-wal"),
        sidecar(path, "-shm"),
    ] {
        match std::fs::remove_file(&p) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e).with_context(|| format!("removing {}", p.display()));
            }
        }
    }
    Ok(())
}

impl Db {
    pub fn create(path: &Path, cfg: &Config) -> Result<Self> {
        ensure!(
            path.extension().and_then(|e| e.to_str()) == Some("db"),
            "database path must end in .db, got {}",
            path.display()
        );
        remove_db_files(path)
            .with_context(|| format!("removing existing database {}", path.display()))?;
        let cfg_json = serde_json::to_string_pretty(cfg).context("serialize config for sqlite")?;
        let hosts_json = serde_json::to_string(&cfg.hostlist)?;
        let raw_json = serde_json::to_string(&cfg.hostlist_raw)?;
        let path = path.to_path_buf();
        let tag = cfg.tag.clone();

        let (tx, rx) = mpsc::channel::<Event>();
        let thread = std::thread::Builder::new()
            .name("tperf-sqlite".into())
            .spawn(move || {
                let conn = Connection::open(&path)
                    .with_context(|| format!("open sqlite {}", path.display()))?;
                conn.execute_batch(
                    r#"
                    PRAGMA journal_mode = DELETE;
                    PRAGMA synchronous = NORMAL;
                    CREATE TABLE config (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        tag TEXT NOT NULL,
                        json TEXT NOT NULL,
                        hostlist TEXT NOT NULL,
                        hostlist_raw TEXT NOT NULL,
                        saved_at_ns INTEGER NOT NULL
                    );
                    CREATE TABLE metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        tag TEXT NOT NULL,
                        server TEXT NOT NULL,
                        wall_start_ns INTEGER NOT NULL,
                        wall_end_ns INTEGER NOT NULL,
                        duration_ns INTEGER NOT NULL,
                        bytes_sent INTEGER NOT NULL,
                        bytes_recv INTEGER NOT NULL,
                        send_bps REAL NOT NULL,
                        recv_bps REAL NOT NULL,
                        packets_sent INTEGER NOT NULL,
                        packets_recv INTEGER NOT NULL,
                        packets_dropped INTEGER NOT NULL,
                        cpu_pct REAL NOT NULL,
                        memory_bytes INTEGER NOT NULL,
                        peers_json TEXT NOT NULL
                    );
                    CREATE INDEX metrics_server_time ON metrics(server, wall_start_ns);
                    "#,
                )?;
                conn.execute(
                    "INSERT INTO config (id, tag, json, hostlist, hostlist_raw, saved_at_ns)
                     VALUES (1, ?1, ?2, ?3, ?4, ?5)",
                    params![
                        tag,
                        cfg_json,
                        hosts_json,
                        raw_json,
                        crate::metrics::realtime_ns() as i64
                    ],
                )?;
                while let Ok(ev) = rx.recv() {
                    match ev {
                        Event::Metric(w) => insert_metric(&conn, &tag, &w)?,
                        Event::Shutdown => break,
                    }
                }
                Ok(())
            })?;
        Ok(Db {
            handle: DbHandle { tx },
            thread: Some(thread),
        })
    }

    pub fn handle(&self) -> DbHandle {
        self.handle.clone()
    }

    pub fn shutdown(mut self) -> Result<()> {
        let _ = self.handle.tx.send(Event::Shutdown);
        if let Some(t) = self.thread.take() {
            match t.join() {
                Ok(r) => r,
                Err(_) => anyhow::bail!("sqlite thread panicked"),
            }
        } else {
            Ok(())
        }
    }
}

impl DbHandle {
    pub fn insert(&self, w: MetricWindow) {
        let _ = self.tx.send(Event::Metric(w));
    }
}

fn insert_metric(conn: &Connection, tag: &str, w: &MetricWindow) -> Result<()> {
    let peers = serde_json::to_string(&w.peers)?;
    conn.execute(
        "INSERT INTO metrics (
            tag, server, wall_start_ns, wall_end_ns, duration_ns,
            bytes_sent, bytes_recv, send_bps, recv_bps,
            packets_sent, packets_recv, packets_dropped,
            cpu_pct, memory_bytes, peers_json
        ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        params![
            tag,
            w.server,
            w.wall_start_ns as i64,
            w.wall_end_ns as i64,
            w.duration_ns as i64,
            w.bytes_sent as i64,
            w.bytes_recv as i64,
            w.send_bps,
            w.recv_bps,
            w.packets_sent as i64,
            w.packets_recv as i64,
            w.packets_dropped as i64,
            w.cpu_pct,
            w.memory_bytes as i64,
            peers,
        ],
    )?;
    Ok(())
}
