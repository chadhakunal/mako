#!/bin/bash
# Comprehensive benchmark test suite runner
# Runs all 128 tests across configurations, thread counts, and network conditions

set -e
cd /root/cse532/mako

# Network conditions: name owd headroom latency jitter
declare -a NETWORKS=(
  "same_region 5 2 2 0.5"
  "same_continent 40 10 30 5"
  "cross_continent 100 20 80 10"
  "geo_distributed 180 30 150 20"
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

# Benchmarks
declare -a BENCHMARKS=("micro" "tpcc")

DURATION=30
RESULTS_DIR="src/deptran/luigi/test/results"
TOTAL_TESTS=128
CURRENT_TEST=1

echo "=== Comprehensive Benchmark Test Suite ==="
echo "Total tests: $TOTAL_TESTS"
echo "Duration per test: ${DURATION}s"
echo "Estimated time: ~2.5 hours"
echo ""

# Function to run a single test
run_test() {
  local benchmark=$1
  local config=$2
  local threads=$3
  local network_name=$4
  local owd=$5
  local headroom=$6
  local latency=$7
  local jitter=$8
  
  local script="src/deptran/luigi/test/scripts/luigi/run_${benchmark}_${config}_latency.sh"
  local output_dir="${RESULTS_DIR}/${benchmark}/${config}/t${threads}"
  local output_file="${output_dir}/${network_name}.txt"
  
  echo "[$CURRENT_TEST/$TOTAL_TESTS] Running: ${benchmark}/${config}/t${threads}/${network_name}"
  
  # Kill any existing processes and clean network
  pkill -9 luigi_server 2>/dev/null || true
  pkill -9 luigi_coordinator 2>/dev/null || true
  sudo tc qdisc del dev lo root 2>/dev/null || true
  sleep 2
  
  # Run test with logging suppressed (only keep final stats)
  # Redirect verbose logs to /dev/null, keep only the summary
  $script $DURATION $threads $owd $headroom $latency $jitter 2>&1 | \
    grep -E "===|Config:|Duration:|Threads:|OWD:|Headroom:|Network:|Starting|Applying|Running|Waiting|Benchmark|Total|Committed|Aborted|Throughput|Latency|Cleaning|Done" > "$output_file"
  
  # Cleanup and wait
  pkill -9 luigi_server 2>/dev/null || true
  pkill -9 luigi_coordinator 2>/dev/null || true
  sudo tc qdisc del dev lo root 2>/dev/null || true
  sleep 10
  
  # Verify result
  if grep -q "Benchmark Results\|Throughput:" "$output_file"; then
    local throughput=$(grep "Throughput:" "$output_file" | awk '{print $2}')
    echo "  ✓ Complete - ${throughput} txns/sec"
  else
    echo "  ✗ FAILED - no results found"
  fi
  
  CURRENT_TEST=$((CURRENT_TEST + 1))
}

# Skip first test (already completed)
CURRENT_TEST=2

# Run all tests
for benchmark in "${BENCHMARKS[@]}"; do
  for config in "${CONFIGS[@]}"; do
    for threads in "${THREADS[@]}"; do
      for network_spec in "${NETWORKS[@]}"; do
        read -r network_name owd headroom latency jitter <<< "$network_spec"
        
        # Skip the first test (already done)
        if [ "$benchmark" = "micro" ] && [ "$config" = "1shard_1replica" ] && \
           [ "$threads" = "1" ] && [ "$network_name" = "same_region" ]; then
          echo "[1/$TOTAL_TESTS] Skipping: micro/1shard_1replica/t1/same_region (already complete)"
          continue
        fi
        
        run_test "$benchmark" "$config" "$threads" "$network_name" "$owd" "$headroom" "$latency" "$jitter"
      done
    done
  done
done

echo ""
echo "=== Test Suite Complete ==="
echo "Results saved in: $RESULTS_DIR"
echo "Total tests completed: $TOTAL_TESTS"
