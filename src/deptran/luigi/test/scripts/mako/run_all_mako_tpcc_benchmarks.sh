#!/bin/bash
# Comprehensive Mako TPC-C benchmark test suite runner
# Runs all 64 tests across configurations, thread counts, and network conditions

set -e
cd /root/cse532/mako

# Network conditions: name latency jitter
declare -a NETWORKS=(
  "same_region 2 0.5"
  "same_continent 30 5"
  "cross_continent 80 10"
  "geo_distributed 150 20"
)

# Configurations
declare -a CONFIGS=(
  "1shard_1replica"
  "1shard_3replicas"
  "2shard_1replica"
  "2shard_3replicas"
)

# Thread counts
declare -a THREADS=(1 2 4 8)

DURATION=30
RESULTS_DIR="src/deptran/luigi/test/results/mako_tpcc"
SCRIPTS_DIR="src/deptran/luigi/test/scripts/mako"
TOTAL_TESTS=64
CURRENT_TEST=1

echo "=== Mako TPC-C Benchmark Test Suite ==="
echo "Total tests: $TOTAL_TESTS"
echo "Duration per test: ${DURATION}s"
echo "Estimated time: ~1.5 hours"
echo ""

# Function to run a single test
run_test() {
  local config=$1
  local threads=$2
  local network_name=$3
  local latency=$4
  local jitter=$5
  
  local script="${SCRIPTS_DIR}/run_mako_tpcc_${config}.sh"
  local output_dir="${RESULTS_DIR}/${config}/t${threads}"
  local output_file="${output_dir}/${network_name}.txt"
  
  echo "[$CURRENT_TEST/$TOTAL_TESTS] Running: ${config}/t${threads}/${network_name}"
  
  # Kill any existing processes and clean network
  ps aux | grep -i dbtest | awk "{print \$2}" | xargs kill -9 2>/dev/null || true
  sudo tc qdisc del dev lo root 2>/dev/null || true
  sleep 2
  
  # Run test and save all output (stats are in stderr, need everything)
  $script $DURATION $threads $latency $jitter > "$output_file" 2>&1
  
  # Cleanup and wait
  ps aux | grep -i dbtest | awk "{print \$2}" | xargs kill -9 2>/dev/null || true
  sudo tc qdisc del dev lo root 2>/dev/null || true
  sleep 10
  
  # Verify result
  if grep -q "agg_persist_throughput" "$output_file"; then
    local throughput=$(grep "agg_persist_throughput" "$output_file" | tail -1 | awk '{print $2}')
    echo "  ✓ Complete - ${throughput} txns/sec"
  else
    echo "  ✗ FAILED - no results found"
  fi
  
  CURRENT_TEST=$((CURRENT_TEST + 1))
}

# Run all tests
for config in "${CONFIGS[@]}"; do
  for threads in "${THREADS[@]}"; do
    for network_spec in "${NETWORKS[@]}"; do
      read -r network_name latency jitter <<< "$network_spec"
      run_test "$config" "$threads" "$network_name" "$latency" "$jitter"
    done
  done
done

echo ""
echo "=== Test Suite Complete ==="
echo "Results saved in: $RESULTS_DIR"
echo "Total tests completed: $TOTAL_TESTS"
