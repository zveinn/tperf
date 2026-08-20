#!/usr/bin/env bash
# Build tperf, run a 4-server podman mesh/pairs/udp (and IPv6 if available).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

IMAGE="${TPERF_IMAGE:-localhost/tperf:test}"
NET="${TPERF_NET:-tperf-net}"
WORKDIR="${ROOT}/out/podman-test"
N=4
RUNTIME_SECS="${TPERF_RUNTIME_SECS:-12}"

log() { printf '==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "missing $1"; }

need podman
need cargo
need python3

cleanup() {
  local i
  for i in $(seq 1 "$N"); do
    podman rm -f "tperf-srv$i" >/dev/null 2>&1 || true
  done
  podman rm -f tperf-client >/dev/null 2>&1 || true
}
cleanup_all() {
  cleanup
  podman network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup_all EXIT

log "building tperf (release)"
cargo build --release

CTX="$(mktemp -d)"
cp "$ROOT/target/release/tperf" "$ROOT/Containerfile" "$CTX/"
log "building image $IMAGE"
podman build -t "$IMAGE" "$CTX"
rm -rf "$CTX"

cleanup_all
mkdir -p "$WORKDIR"
rm -rf "$WORKDIR"/*
trap cleanup_all EXIT

IPV6=0
if podman network create --ipv6 "$NET" >/dev/null 2>&1; then
  IPV6=1
  log "created dual-stack network $NET"
else
  podman network rm "$NET" >/dev/null 2>&1 || true
  podman network create "$NET" >/dev/null
  log "created ipv4 network $NET"
fi

start_servers() {
  local i
  for i in $(seq 1 "$N"); do
    podman run -d --replace \
      --name "tperf-srv$i" \
      --hostname "srv$i" \
      --network "$NET" \
      --network-alias "srv$i" \
      -v "$WORKDIR:/cfg:Z" \
      "$IMAGE" server /cfg/tperf.toml >/dev/null
  done
  local i=0
  while (( i < 50 )); do
    if podman exec tperf-srv1 true >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
    i=$((i + 1))
  done
  # Let aardvark-dns publish the new names before the client resolves.
  sleep 1
}

write_cfg() {
  local type="$1"
  local network="$2"
  local test_addr="$3"
  local tag="$4"
  cat > "$WORKDIR/tperf.toml" <<EOF
client_addr = "0.0.0.0:7777"
test_addr = "${test_addr}"
network = "${network}"
payload_size = "64KiB"
workers = 2
type = "${type}"
hostlist = ["srv{1..${N}}"]
tag = "${tag}"
EOF
}

run_client() {
  local tag="$1"
  # sqlite file is created in the client's cwd
  rm -f "$WORKDIR/${tag}.db" "$WORKDIR/${tag}.db-wal" "$WORKDIR/${tag}.db-shm"
  set +e
  podman run --rm --replace \
    --name tperf-client \
    --hostname client \
    --network "$NET" \
    -v "$WORKDIR:/data:Z" \
    -w /data \
    --entrypoint timeout \
    "$IMAGE" \
    --signal=INT --kill-after=5s "${RUNTIME_SECS}s" \
    /usr/local/bin/tperf client /data/tperf.toml
  local rc=$?
  set -e
  # timeout exits 124 on SIGINT expiry; that is success for this test.
  if [[ "$rc" -ne 0 && "$rc" -ne 124 ]]; then
    die "client run failed for tag=$tag (exit $rc)"
  fi
}

check_db() {
  local tag="$1"
  local expect_send="${2:-1}"
  local db="$WORKDIR/${tag}.db"
  [[ -f "$db" ]] || die "missing sqlite db $db"
  python3 - "$db" "$N" "$expect_send" <<'PY'
import sqlite3, sys
path, n, expect_send = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
c = sqlite3.connect(path)
cfg = c.execute("select tag, json, hostlist from config").fetchone()
assert cfg, "config row missing"
print("config tag=", cfg[0], "hosts=", cfg[2])
rows = c.execute(
    "select server, count(*), sum(bytes_sent), sum(bytes_recv), max(duration_ns), min(duration_ns) "
    "from metrics group by server order by server"
).fetchall()
print("servers with metrics:", len(rows))
for server, cnt, sent, recv, dmax, dmin in rows:
    print(f"  {server}: windows={cnt} sent={sent} recv={recv} duration_ns=[{dmin},{dmax}]")
    assert cnt >= 1, f"{server} has no metric windows"
    if expect_send:
        assert (sent or 0) > 0, f"{server} sent 0 bytes"
        assert (recv or 0) > 0, f"{server} received 0 bytes"
    assert dmin > 0, "duration_ns should be measured"
    # 1s windows should be close to 1e9 ns; allow first/last partial.
    assert dmax < 5_000_000_000, f"implausibly long window {dmax}"
assert len(rows) == n, f"expected metrics from {n} servers, got {len(rows)}"
print("ok")
PY
}

log "TCP mesh"
write_cfg mesh tcp "srv1:9100" mesh-tcp
start_servers
run_client mesh-tcp
check_db mesh-tcp 1
cleanup

log "TCP pairs"
write_cfg pairs tcp "srv1:9100" pairs-tcp
start_servers
run_client pairs-tcp
check_db pairs-tcp 1
cleanup

log "UDP mesh"
write_cfg mesh udp "srv1:9100" mesh-udp
start_servers
run_client mesh-udp
# UDP may drop on a busy bridge; require send, allow some recv.
python3 - "$WORKDIR/mesh-udp.db" "$N" <<'PY'
import sqlite3, sys
path, n = sys.argv[1], int(sys.argv[2])
c = sqlite3.connect(path)
rows = c.execute("select server, sum(bytes_sent), sum(bytes_recv) from metrics group by server").fetchall()
assert len(rows) == n, rows
for server, sent, recv in rows:
    print(f"  {server}: sent={sent} recv={recv}")
    assert (sent or 0) > 0, server
print("udp ok")
PY
cleanup

if [[ "$IPV6" == "1" ]]; then
  log "TCP mesh IPv6"
  write_cfg mesh tcp "[fd10:88:90::1]:9100" mesh-tcp6
  start_servers
  run_client mesh-tcp6
  check_db mesh-tcp6 1
  cleanup
else
  log "skipping IPv6 test (network has no IPv6)"
fi

log "all podman tests passed"
ls -l "$WORKDIR"
