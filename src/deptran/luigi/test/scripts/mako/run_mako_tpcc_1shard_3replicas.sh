#!/bin/bash
# Mako TPC-C benchmark script for 1-shard 3-replica with network latency
# Usage: ./run_mako_tpcc_1shard_3replicas.sh <duration> <threads> <netem_delay_ms> <netem_jitter_ms>

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

echo "=== Mako TPC-C: 1-Shard 3-Replica ==="
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

# Start shard 0 with 3 replicas + learner (4 total as required by Paxos config)
echo "Starting shard 0 replicas..."
echo "  Starting leader (localhost)..."
bash bash/shard.sh 1 0 $THREADS localhost 0 1 $DURATION 2>&1 &
PID_LEADER=$!
sleep 2

echo "  Starting follower 1 (p1)..."
bash bash/shard.sh 1 0 $THREADS p1 0 1 $DURATION 2>&1 &
PID_P1=$!
sleep 2

echo "  Starting follower 2 (p2)..."
bash bash/shard.sh 1 0 $THREADS p2 0 1 $DURATION 2>&1 &
PID_P2=$!
sleep 2

echo "  Starting learner..."
bash bash/shard.sh 1 0 $THREADS learner 0 1 $DURATION 2>&1 &
PID_LEARNER=$!

# Wait for all processes to complete
echo "Running benchmark for ${DURATION} seconds..."
wait $PID_LEADER 2>/dev/null || true
wait $PID_P1 2>/dev/null || true
wait $PID_P2 2>/dev/null || true
wait $PID_LEARNER 2>/dev/null || true

# Cleanup
echo ""
echo "Cleaning up..."
sudo tc qdisc del dev lo root 2>/dev/null || true
ps aux | grep -i dbtest | awk "{print \$2}" | xargs kill -9 2>/dev/null || true

echo "Done!"
