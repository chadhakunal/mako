#!/bin/bash
# Wrapper script for 1shard-1replica microbenchmark with network latency and OWD/headroom
# Usage: ./run_micro_1shard_1replica_latency.sh <duration> <threads> <owd_ms> <headroom_ms> <netem_delay_ms> <netem_jitter_ms>

set -e

cd /root/cse532/mako
export LD_LIBRARY_PATH=/root/cse532/mako/build:$LD_LIBRARY_PATH

# Kill any existing processes and clear tc
pkill -9 luigi_server 2>/dev/null || true
pkill -9 luigi_coordinator 2>/dev/null || true
sudo tc qdisc del dev lo root 2>/dev/null || true
sleep 1

CONFIG="src/deptran/luigi/test/configs/1shard-1replica.yml"
DURATION="${1:-30}"
THREADS="${2:-1}"
OWD="${3:-5}"
HEADROOM="${4:-2}"
NETEM_DELAY="${5:-0}"
NETEM_JITTER="${6:-0}"

echo "=== 1-Shard 1-Replica Microbenchmark ==="
echo "Config: $CONFIG"
echo "Duration: ${DURATION}s"
echo "Threads: ${THREADS}"
echo "OWD: ${OWD}ms"
echo "Headroom: ${HEADROOM}ms"
echo "Network: ${NETEM_DELAY}ms ± ${NETEM_JITTER}ms"
echo ""

# Start single replica (s101)
echo "Starting replica (s101:31850)..."
./build/luigi_server -f "$CONFIG" -P s101 > s101_micro.log 2>&1 &
S0_PID=$!
sleep 3

# Apply network latency AFTER server is up (if specified)
if [ "$NETEM_DELAY" -gt 0 ]; then
  echo "Applying network latency: ${NETEM_DELAY}ms ± ${NETEM_JITTER}ms (pareto)..."
  sudo tc qdisc add dev lo root netem delay ${NETEM_DELAY}ms ${NETEM_JITTER}ms distribution pareto
  tc qdisc show dev lo
fi

# Run coordinator with micro benchmark and OWD/headroom params
echo ""
echo "Running microbenchmark..."
./build/luigi_coordinator -f "$CONFIG" -b micro -d "$DURATION" -t "$THREADS" -w "$OWD" -x "$HEADROOM" 2>&1 | tee coord_micro.log

# Cleanup
echo ""
echo "Cleaning up..."
sudo tc qdisc del dev lo root 2>/dev/null || true
kill $S0_PID 2>/dev/null || true
pkill -9 luigi_server 2>/dev/null || true

echo "Done!"
