use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::config::Config;

const MAX_FRAME: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Assignment {
    pub self_name: String,
    pub self_id: u32,
    /// Socket address this server should bind for the data plane (ip:port).
    #[serde(default)]
    pub bind: String,
    pub targets: Vec<Target>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Target {
    pub name: String,
    pub id: u32,
    /// Socket address to connect/send to (ip:port).
    #[serde(default)]
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerMetrics {
    pub peer: String,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
    pub packets_sent: u64,
    pub packets_recv: u64,
    pub packets_dropped: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricWindow {
    pub server: String,
    /// CLOCK_REALTIME nanoseconds at window open.
    pub wall_start_ns: u64,
    /// CLOCK_REALTIME nanoseconds at window close.
    pub wall_end_ns: u64,
    /// CLOCK_MONOTONIC delta for the window (nanoseconds).
    pub duration_ns: u64,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
    /// Bits per second, using the measured duration_ns.
    pub send_bps: f64,
    pub recv_bps: f64,
    pub packets_sent: u64,
    pub packets_recv: u64,
    pub packets_dropped: u64,
    pub cpu_pct: f64,
    pub memory_bytes: u64,
    pub peers: Vec<PeerMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Msg {
    Start {
        config: Config,
        assignment: Assignment,
    },
    Stop,
    Ack {
        cmd: String,
    },
    Metrics {
        window: MetricWindow,
    },
    Error {
        message: String,
    },
}

pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, msg: &Msg) -> Result<()> {
    let data = serde_json::to_vec(msg).context("serialize control message")?;
    if data.len() > MAX_FRAME {
        bail!("control message too large ({} bytes)", data.len());
    }
    w.write_u32(data.len() as u32).await?;
    w.write_all(&data).await?;
    w.flush().await?;
    Ok(())
}

pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Msg> {
    let len = r.read_u32().await? as usize;
    if len == 0 || len > MAX_FRAME {
        bail!("invalid control frame length {len}");
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    serde_json::from_slice(&buf).context("deserialize control message")
}

pub async fn write_frame_locked<W: AsyncWrite + Unpin>(
    w: &tokio::sync::Mutex<W>,
    msg: &Msg,
) -> Result<()> {
    let mut guard = w.lock().await;
    write_frame(&mut *guard, msg).await
}
