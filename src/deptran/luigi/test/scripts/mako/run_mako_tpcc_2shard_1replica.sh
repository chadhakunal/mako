#!/bin/bash
# Mako TPC-C benchmark script for 2-shard 1-replica with network latency
# Usage: ./run_mako_tpcc_2shard_1replica.sh <duration> <threads> <netem_delay_ms> <netem_jitter_ms>

set -e
cd /root/cse532/mako
export LD_LIBRARY_PATH="$(pwd)/build:${LD_LIBRARY_PATH}"

# Kill any existing processes and clear tc
ps aux | grep -i dbtest | awk "{print \$2}" | xargs kill -9 2>/dev/null || true
sudo tc qdisc del dev lo root 2>/dev/null || true
sleep 2

DURATION="${1:-30}"
THREADS="${2:-1}"
NETEM_DELAY="${3:-0}"
NETEM_JITTER="${4:-0}"

echo "=== Mako TPC-C: 2-Shard 1-Replica ==="
echo "Duration: ${DURATION}s"
echo "Threads: ${THREADS}"
echo "Network: ${NETEM_DELAY}ms ± ${NETEM_JITTER}ms"
echo ""

# Apply network latency if specified
if [ "$NETEM_DELAY" -gt 0 ]; then
  echo "Applying network latency: ${NETEM_DELAY}ms ± ${NETEM_JITTER}ms (pareto)..."
  sudo tc qdisc add dev lo root netem delay ${NETEM_DELAY}ms ${NETEM_JITTER}ms distribution pareto
  tc qdisc show dev lo
  echo ""
fi

# Start shard 0 and shard 1 (no replication)
echo "Starting shard 0..."
bash bash/shard.sh 2 0 $THREADS localhost 2>&1 &
SHARD0_PID=$!
sleep 3

echo "Starting shard 1..."
bash bash/shard.sh 2 1 $THREADS localhost 2>&1 &
SHARD1_PID=$!

# Wait for all processes to complete
echo "Running benchmark for ${DURATION} seconds..."
wait $SHARD0_PID 2>/dev/null || true
wait $SHARD1_PID 2>/dev/null || true

# Cleanup
echo ""
echo "Cleaning up..."
sudo tc qdisc del dev lo root 2>/dev/null || true
ps aux | grep -i dbtest | awk "{print \$2}" | xargs kill -9 2>/dev/null || true

echo "Done!"
