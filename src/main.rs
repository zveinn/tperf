mod analyze;
mod client;
mod config;
mod dataplane;
mod db;
mod metrics;
mod netutil;
mod proto;
mod server;
mod testplan;

use anyhow::{bail, Result};
use std::env;
use std::path::{Path, PathBuf};

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("tperf: {e:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let mut args = env::args().skip(1);
    let cmd = args.next().unwrap_or_default();

    match cmd.as_str() {
        "client" => {
            let path = args
                .next()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("tperf.toml"));
            let cfg = config::Config::load(&path)?;
            client::run(cfg).await
        }
        "server" => {
            let path = args.next().map(PathBuf::from);
            let bind = load_client_addr(path.as_deref())?;
            server::run(&bind).await
        }
        "analyze" => {
            let db = analyze::parse_args(args)?;
            analyze::run(&db)
        }
        "help" | "" => {
            eprint_usage();
            Ok(())
        }
        other => {
            eprint_usage();
            bail!("unknown command {other:?}");
        }
    }
}

fn load_client_addr(explicit: Option<&Path>) -> Result<String> {
    if let Some(path) = explicit {
        return Ok(config::Config::load(path)?.client_addr);
    }
    let default = PathBuf::from("tperf.toml");
    if default.exists() {
        return Ok(config::Config::load(&default)?.client_addr);
    }
    Ok(config::DEFAULT_CLIENT_ADDR.to_string())
}

fn eprint_usage() {
    eprintln!(
        "usage: tperf <client|server> [config.toml]
       tperf analyze --db <file>

client   load config, write <tag>.db in the working directory, drive the test
server   bind the control socket (client_addr) and wait for start/stop
analyze  summarize pair and total throughput from a sqlite database file"
    );
}
