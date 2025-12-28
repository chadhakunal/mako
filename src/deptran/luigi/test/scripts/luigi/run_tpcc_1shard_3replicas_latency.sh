#!/bin/bash
# Wrapper script for 1shard-3replica TPCC benchmark with network latency and OWD/headroom
# Usage: ./run_tpcc_1shard_3replicas_latency.sh <duration> <threads> <owd_ms> <headroom_ms> <netem_delay_ms> <netem_jitter_ms>

set -e

cd /root/cse532/mako
export LD_LIBRARY_PATH=/root/cse532/mako/build:$LD_LIBRARY_PATH

# Kill any existing processes and clear tc
pkill -9 luigi_server 2>/dev/null || true
pkill -9 luigi_coordinator 2>/dev/null || true
sudo tc qdisc del dev lo root 2>/dev/null || true
sleep 1

CONFIG="src/deptran/luigi/test/configs/1shard-3replicas.yml"
DURATION="${1:-30}"
THREADS="${2:-1}"
OWD="${3:-5}"
HEADROOM="${4:-2}"
NETEM_DELAY="${5:-0}"
NETEM_JITTER="${6:-0}"
NUM_SHARDS=1
NUM_WAREHOUSES=$((THREADS * NUM_SHARDS))  # Scale warehouses with threads (like Mako)

echo "=== 1-Shard 3-Replica TPC-C Benchmark ==="
echo "Config: $CONFIG"
echo "Duration: ${DURATION}s"
echo "Threads: ${THREADS}"
echo "Warehouses: ${NUM_WAREHOUSES}"
echo "OWD: ${OWD}ms"
echo "Headroom: ${HEADROOM}ms"
echo "Network: ${NETEM_DELAY}ms ± ${NETEM_JITTER}ms"
echo ""

# Start replica 0 (s101) - leader
echo "Starting replica 0 (s101:31850) - leader..."
./build/luigi_server -f "$CONFIG" -P s101 -b tpcc -w "$NUM_WAREHOUSES" > s101_tpcc.log 2>&1 &
S0_PID=$!
sleep 2

# Start replica 1 (s102) - follower
echo "Starting replica 1 (s102:31851) - follower..."
./build/luigi_server -f "$CONFIG" -P s102 -b tpcc -w "$NUM_WAREHOUSES" > s102_tpcc.log 2>&1 &
S1_PID=$!
sleep 2

# Start replica 2 (s103) - follower
echo "Starting replica 2 (s103:31852) - follower..."
./build/luigi_server -f "$CONFIG" -P s103 -b tpcc -w "$NUM_WAREHOUSES" > s103_tpcc.log 2>&1 &
S2_PID=$!

# Wait for all servers to start listening
echo "Waiting for all servers to start listening..."
sleep 3

# Apply network latency AFTER servers are up (if specified)
if [ "$NETEM_DELAY" -gt 0 ]; then
  echo "Applying network latency: ${NETEM_DELAY}ms ± ${NETEM_JITTER}ms (pareto)..."
  sudo tc qdisc add dev lo root netem delay ${NETEM_DELAY}ms ${NETEM_JITTER}ms distribution pareto
  tc qdisc show dev lo
fi

# Run coordinator with TPCC benchmark and OWD/headroom params
echo ""
echo "Running TPCC benchmark..."
./build/luigi_coordinator -f "$CONFIG" -b tpcc -d "$DURATION" -t "$THREADS" -w "$OWD" -x "$HEADROOM" 2>&1 | tee coord_tpcc.log

# Cleanup
echo ""
echo "Cleaning up..."
sudo tc qdisc del dev lo root 2>/dev/null || true
kill $S0_PID $S1_PID $S2_PID 2>/dev/null || true
pkill -9 luigi_server 2>/dev/null || true

echo "Done!"
