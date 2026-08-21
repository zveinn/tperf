# tperf

Multi-server TCP/UDP bandwidth tester. One client drives N servers over a
control socket, streams 1-second metric windows back, prints them, and stores
them in SQLite.

IPv4 and IPv6 are both supported.

## Build

```bash
cargo build --release
```

The binary is `target/release/tperf`.

Tagged releases (`v*`) build Linux **amd64** and **arm64** binaries and attach
them to a GitHub release. Download from
[Releases](https://github.com/zveinn/tperf/releases).

## Commands

```text
tperf server [config.toml]
tperf client [config.toml]
tperf analyze --db <file>
```

Default config path is `./tperf.toml`. Test parameters come from the config
file, not from flags.

| Command | What it does |
|---|---|
| `server` | Bind the control socket (`client_addr`) and wait for start/stop |
| `client` | Connect to every host, send the test config, print metrics, write `<tag>.db` |
| `analyze` | Summarize pair and total throughput from a sqlite database file |

The client runs until **Ctrl+C** (SIGINT) or SIGTERM. That sends `stop` to every
server, flushes the last metric window, and closes the database.

Copy the example and edit it:

```bash
cp tperf.toml.example tperf.toml
```

## Config

See [`tperf.toml.example`](tperf.toml.example). Fields:

| Field | Default | Notes |
|---|---|---|
| `client_addr` | `0.0.0.0:7777` | Servers bind this; client dials `hostlist:port` |
| `test_addr` | *(required)* | Data-plane `host:port`. Host must not be `0.0.0.0` / `::`. Port is shared; each server binds its own IP. An IP here selects IPv4 vs IPv6 |
| `network` | `tcp` | `tcp` or `udp` |
| `payload_size` | `100MiB` | Bytes per write (`64KiB`, `1MiB`, raw integers, …) |
| `workers` | `10` | Concurrent writers per destination |
| `type` | `pairs` | `pairs` or `mesh` |
| `hostlist` | *(required)* | Hostnames/IPs, or ellipsis patterns like `srv{1..10}.lab` |
| `tag` | *(required)* | Test id. Database file is `./<tag>.db` |

`hostlist` can be an array or a comma-separated string. `{1..10}` and zero-padded
`{01..10}` expand in place, anywhere in the hostname.

### Test types

- **mesh** — every node sends to every other node at the same time.
- **pairs** — round-robin of disjoint pairs so every host is tested against every
  other. Multiple pairs run at once; a host is in only one pair per round. For
  4 servers that is 3 rounds (`1-2 & 3-4`, then `1-3 & 2-4`, then `1-4 & 2-3`).
  Each round lasts 1 second; the schedule repeats until you stop the client.
  An odd leftover host sits out that round.

## Metrics

Each server measures send/receive throughput, dropped packets (UDP sequence
gaps / failed writes), process CPU %, and RSS. Metric windows are aligned to a
1-second `CLOCK_MONOTONIC` grid: the server waits until the 1s mark (sleep, then
a short spin) and only then snapshots. A stop or pairs round-switch **before**
that mark discards the partial interval — you should not see `window=0.66s`
lines. Throughput is still `bytes × 8 / measured_ns` (typically `1.0000s` after
truncating to 4 decimals; a few hundred nanoseconds of overshoot is normal).

The client prints one line per window per server, and appends the same data
(including per-peer breakdown) to SQLite.

Starting a test **replaces** any existing `./<tag>.db`. One database file per
run.

```bash
tperf analyze --db ./run1.db
```

Reports:

1. min / avg / max from every server to every server (one line per directed pair)
2. total min / avg / max across all servers (one line)
3. the 5 worst pairs by average throughput

`--db` must be a path to the sqlite **file**, not a directory.

## Example

Four servers, then a client in another terminal (or on another machine that can
reach them):

```bash
# on each server
tperf server tperf.toml.example

# on the client, cwd is where run1.db will be created
tperf client tperf.toml.example

# after Ctrl+C
tperf analyze --db ./run1.db
```

Minimal config:

```toml
client_addr = "0.0.0.0:7777"
test_addr = "srv1:9100"
network = "tcp"
payload_size = "100MiB"
workers = 10
type = "pairs"
hostlist = ["srv{1..4}"]
tag = "run1"
```

## Container test

```bash
./scripts/podman-test.sh
```

Builds a release binary and image, then runs 4-server TCP mesh, TCP pairs, UDP
mesh, and (when the network is dual-stack) TCP mesh over IPv6.
