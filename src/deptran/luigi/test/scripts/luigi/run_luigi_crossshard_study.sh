#!/bin/bash
# Luigi Cross-Shard Transaction Study
# Tests 2-shard 3-replica with geo-distributed network (150ms)
# Varies cross-shard transaction percentage: 5%, 10%, 15%, 20%, 25%

set -e
cd /root/cse532/mako
export LD_LIBRARY_PATH="$(pwd)/build:${LD_LIBRARY_PATH}"

BASE_RESULTS_DIR="src/deptran/luigi/test/results/cross_shard"
CONFIG="src/deptran/luigi/test/configs/2shard-3replicas.yml"

DURATION=30
THREADS=8
NETEM_DELAY=150
NETEM_JITTER=5

echo "=== Luigi Cross-Shard Transaction Study ==="
echo "Configuration: 2-shard 3-replica, geo-distributed (150ms)"
echo "Config file: $CONFIG"
echo "Thread count: $THREADS"
echo "Duration: ${DURATION}s"
echo ""

for PCT in 5 10 15 20; do
  echo "========================================="
  echo "Running Luigi with ${PCT}% cross-shard transactions"
  echo "========================================="
  
  # Create output directory
  OUTPUT_DIR="${BASE_RESULTS_DIR}/${PCT}/luigi"
  mkdir -p "$OUTPUT_DIR"
  OUTPUT_FILE="${OUTPUT_DIR}/results.txt"
  
  # Kill any existing processes
  pkill -9 -f "luigi_server" 2>/dev/null || true
  pkill -9 -f "luigi_coordinator" 2>/dev/null || true
  sudo tc qdisc del dev lo root 2>/dev/null || true
  sleep 2
  
  # Setup network latency
  sudo tc qdisc add dev lo root netem delay ${NETEM_DELAY}ms ${NETEM_JITTER}ms distribution normal
  
  # Start Shard 0 replicas
  echo "  Starting Shard 0 replicas..."
  ./build/luigi_server -f "$CONFIG" -P s101 > /dev/null 2>&1 &
  S101_PID=$!
  sleep 1
  ./build/luigi_server -f "$CONFIG" -P s102 > /dev/null 2>&1 &
  S102_PID=$!
  sleep 1
  ./build/luigi_server -f "$CONFIG" -P s103 > /dev/null 2>&1 &
  S103_PID=$!
  sleep 1

  # Start Shard 1 replicas
  echo "  Starting Shard 1 replicas..."
  ./build/luigi_server -f "$CONFIG" -P s201 > /dev/null 2>&1 &
  S201_PID=$!
  sleep 1
  ./build/luigi_server -f "$CONFIG" -P s202 > /dev/null 2>&1 &
  S202_PID=$!
  sleep 1
  ./build/luigi_server -f "$CONFIG" -P s203 > /dev/null 2>&1 &
  S203_PID=$!
  sleep 2
  
  # Run Luigi coordinator
  echo "  Starting Coordinator..."
  ./build/luigi_coordinator \
    -f "$CONFIG" \
    -b tpcc \
    -t $THREADS \
    -d $DURATION \
    -r $PCT \
    > "$OUTPUT_FILE" 2>&1
  
  # Extract throughput and latency
  THROUGHPUT=$(grep -E "throughput|Throughput" "$OUTPUT_FILE" | tail -1 || echo "N/A")
  LATENCY=$(grep -E "latency|Latency" "$OUTPUT_FILE" | tail -1 || echo "N/A")
  
  echo "Result: $THROUGHPUT"
  echo "Latency: $LATENCY"
  echo ""
  
  # Cleanup
  pkill -9 -f "luigi_server" 2>/dev/null || true
  pkill -9 -f "luigi_coordinator" 2>/dev/null || true
  sudo tc qdisc del dev lo root 2>/dev/null || true
  sleep 2
done

echo "========================================="
echo "Luigi cross-shard study completed!"
echo "Results saved to: $BASE_RESULTS_DIR/{5,10,15,20,25}/luigi/results.txt"
echo "========================================="
