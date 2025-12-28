#!/bin/bash
# Mako Cross-Shard Transaction Study
# Tests 2-shard 3-replica with geo-distributed network (150ms)
# Varies cross-shard transaction percentage: 5%, 10%, 15%, 20%, 25%

set -e
cd /root/cse532/mako
export LD_LIBRARY_PATH="$(pwd)/build:${LD_LIBRARY_PATH}"

BASE_RESULTS_DIR="src/deptran/luigi/test/results/cross_shard"

DURATION=30
THREADS=8
NETEM_DELAY=150
NETEM_JITTER=5

echo "=== Mako Cross-Shard Transaction Study ==="
echo "Configuration: 2-shard 3-replica, geo-distributed (150ms)"
echo "Thread count: $THREADS"
echo "Duration: ${DURATION}s"
echo ""

for PCT in 5 10 15 20 25; do
  echo "========================================="
  echo "Running Mako with ${PCT}% cross-shard transactions"
  echo "========================================="
  
  # Create output directory
  OUTPUT_DIR="${BASE_RESULTS_DIR}/${PCT}/mako"
  mkdir -p "$OUTPUT_DIR"
  OUTPUT_FILE="${OUTPUT_DIR}/results.txt"
  
  # Kill any existing processes
  ps aux | grep -i dbtest | awk "{print \$2}" | xargs kill -9 2>/dev/null || true
  sudo tc qdisc del dev lo root 2>/dev/null || true
  sleep 2
  
  # Setup network latency
  sudo tc qdisc add dev lo root netem delay ${NETEM_DELAY}ms ${NETEM_JITTER}ms distribution normal
  
  # Start shard 0 with 3 replicas + learner
  bash bash/shard.sh 2 0 $THREADS localhost 0 1 $DURATION --new-order-remote-item-pct $PCT >> "$OUTPUT_FILE" 2>&1 &
  S0_LEADER=$!
  sleep 2
  bash bash/shard.sh 2 0 $THREADS p1 0 1 $DURATION --new-order-remote-item-pct $PCT >> "$OUTPUT_FILE" 2>&1 &
  S0_P1=$!
  sleep 2
  bash bash/shard.sh 2 0 $THREADS p2 0 1 $DURATION --new-order-remote-item-pct $PCT >> "$OUTPUT_FILE" 2>&1 &
  S0_P2=$!
  sleep 2
  bash bash/shard.sh 2 0 $THREADS learner 0 1 $DURATION --new-order-remote-item-pct $PCT >> "$OUTPUT_FILE" 2>&1 &
  S0_LEARNER=$!
  sleep 2
  
  # Start shard 1 with 3 replicas + learner
  bash bash/shard.sh 2 1 $THREADS localhost 0 1 $DURATION --new-order-remote-item-pct $PCT >> "$OUTPUT_FILE" 2>&1 &
  S1_LEADER=$!
  sleep 2
  bash bash/shard.sh 2 1 $THREADS p1 0 1 $DURATION --new-order-remote-item-pct $PCT >> "$OUTPUT_FILE" 2>&1 &
  S1_P1=$!
  sleep 2
  bash bash/shard.sh 2 1 $THREADS p2 0 1 $DURATION --new-order-remote-item-pct $PCT >> "$OUTPUT_FILE" 2>&1 &
  S1_P2=$!
  sleep 2
  bash bash/shard.sh 2 1 $THREADS learner 0 1 $DURATION --new-order-remote-item-pct $PCT >> "$OUTPUT_FILE" 2>&1 &
  S1_LEARNER=$!
  
  # Wait for all processes
  echo "Running benchmark for ${DURATION} seconds..."
  wait $S0_LEADER 2>/dev/null || true
  wait $S0_P1 2>/dev/null || true
  wait $S0_P2 2>/dev/null || true
  wait $S0_LEARNER 2>/dev/null || true
  wait $S1_LEADER 2>/dev/null || true
  wait $S1_P1 2>/dev/null || true
  wait $S1_P2 2>/dev/null || true
  wait $S1_LEARNER 2>/dev/null || true
  
  # Extract throughput
  THROUGHPUT=$(grep "agg_persist_throughput" "$OUTPUT_FILE" | awk '{print $2}' || echo "N/A")
  LATENCY=$(grep "avg_latency:" "$OUTPUT_FILE" | awk '{print $2}' || echo "N/A")
  
  echo "Result: Throughput=$THROUGHPUT, Latency=$LATENCY"
  echo ""
  
  # Cleanup
  ps aux | grep -i dbtest | awk "{print \$2}" | xargs kill -9 2>/dev/null || true
  sudo tc qdisc del dev lo root 2>/dev/null || true
  sleep 2
done

echo "========================================="
echo "Mako cross-shard study completed!"
echo "Results saved to: $BASE_RESULTS_DIR/{5,10,15,20,25}/mako/results.txt"
echo "========================================="
