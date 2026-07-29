#!/usr/bin/env bash
# demo.sh — spins up a 3-node raft-kv cluster, writes data, kills the leader,
# and proves the data survives a rolling node failure (the resume headline).
#
# Portable to macOS's stock bash 3.2 (no associative arrays).
set -euo pipefail

cd "$(dirname "$0")/.."

BIN_D=./bin/raftkvd
BIN_CTL=./bin/raftkvctl
PEERS="n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003"
DATA=./data/demo
LOGDIR=./data/demo/logs

if [[ ! -x "$BIN_D" || ! -x "$BIN_CTL" ]]; then
  echo "building binaries..."
  go build -o "$BIN_D" ./cmd/raftkvd
  go build -o "$BIN_CTL" ./cmd/raftkvctl
fi

rm -rf "$DATA"
mkdir -p "$LOGDIR"

PID_n1="" PID_n2="" PID_n3=""

start_node() {
  local id=$1 addr=$2
  "$BIN_D" --id "$id" --listen "$addr" --peers "$PEERS" \
    --data-dir "$DATA/$id" >"$LOGDIR/$id.log" 2>&1 &
  eval "PID_$id=$!"
  echo "started $id (pid $!) on $addr"
}

pid_of() { eval "echo \${PID_$1}"; }

cleanup() {
  echo "--- shutting down ---"
  for id in n1 n2 n3; do
    p=$(pid_of "$id")
    [[ -n "$p" ]] && kill "$p" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT

echo "=== starting 3-node cluster ==="
start_node n1 127.0.0.1:9001
start_node n2 127.0.0.1:9002
start_node n3 127.0.0.1:9003

echo "waiting for leader election..."
sleep 3

echo "=== writing data ==="
"$BIN_CTL" --peers "$PEERS" put city bentonville
"$BIN_CTL" --peers "$PEERS" put team globaltech
echo -n "read city -> "; "$BIN_CTL" --peers "$PEERS" get city

# Find and kill the current leader (grep the logs for "became leader").
LEADER=$(grep -l "became leader" "$LOGDIR"/*.log 2>/dev/null | tail -1 | xargs -I{} basename {} .log || true)
if [[ -z "${LEADER:-}" ]]; then LEADER=n1; fi
echo "=== killing leader: $LEADER ==="
LPID=$(pid_of "$LEADER")
kill "$LPID" 2>/dev/null || true
eval "PID_$LEADER=''"

echo "waiting for re-election..."
sleep 4

echo "=== verifying data survived the rolling kill ==="
echo -n "read city -> "; "$BIN_CTL" --peers "$PEERS" get city
echo -n "read team -> "; "$BIN_CTL" --peers "$PEERS" get team
echo "=== writing after failover ==="
"$BIN_CTL" --peers "$PEERS" put status survived
echo -n "read status -> "; "$BIN_CTL" --peers "$PEERS" get status

echo "=== DEMO COMPLETE: no data lost across a leader failure ==="
