#!/bin/bash
# Luigi stored-procedure benchmark wrapper script
# Compatible with Mako CI infrastructure
#
# Usage:
#   bash examples/test_luigi_bench.sh [num_threads] [--benchmark micro|tpcc]
#
# Examples:
#   bash examples/test_luigi_bench.sh 6                    # TPC-C with 6 threads
#   bash examples/test_luigi_bench.sh 4 --benchmark micro  # Micro with 4 threads

set -e

echo "========================================="
echo "Luigi Stored-Procedure Benchmark"
echo "========================================="

# Parse arguments
trd=${1:-6}  # Default to 6 threads
shift || true

benchmark="tpcc"
duration=30

while [[ $# -gt 0 ]]; do
    case $1 in
        --benchmark)
            benchmark="$2"
            shift 2
            ;;
        --duration)
            duration="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

# Clean up old log files
rm -f nfs_sync_*

# Clean up RocksDB data from previous runs
USERNAME=${USER:-$(whoami)}
rm -rf /tmp/${USERNAME}_mako_rocksdb_shard*

# Kill any existing processes
pkill -9 -f luigi_bench 2>/dev/null || true
pkill -9 -f dbtest 2>/dev/null || true
sleep 1

script_name="luigi_bench"
log_prefix="${script_name}_${benchmark}"

path=$(pwd)/src/mako

echo "Configuration:"
echo "  Threads:      $trd"
echo "  Benchmark:    $benchmark"
echo "  Duration:     ${duration}s"
echo "  Config:       $path/config/local-shards2-warehouses${trd}.yml"
echo ""

# For 2-shard setup, start both shards

# Start shard 0 in background
echo "Starting Luigi benchmark shard 0..."
nohup ./build/luigi_bench \
    --shard-config "$path/config/local-shards2-warehouses${trd}.yml" \
    --shard-index 0 \
    -P localhost \
    --num-threads "$trd" \
    --benchmark "$benchmark" \
    --duration "$duration" \
    > "${log_prefix}_shard0.log" 2>&1 &
SHARD0_PID=$!
sleep 2

# Start shard 1 in background
echo "Starting Luigi benchmark shard 1..."
nohup ./build/luigi_bench \
    --shard-config "$path/config/local-shards2-warehouses${trd}.yml" \
    --shard-index 1 \
    -P localhost \
    --num-threads "$trd" \
    --benchmark "$benchmark" \
    --duration "$duration" \
    > "${log_prefix}_shard1.log" 2>&1 &
SHARD1_PID=$!

echo "Waiting for benchmarks to complete (${duration}s + startup)..."
echo "  Shard 0 PID: $SHARD0_PID"
echo "  Shard 1 PID: $SHARD1_PID"

# Wait for both to finish
wait $SHARD0_PID 2>/dev/null || true
SHARD0_EXIT=$?
wait $SHARD1_PID 2>/dev/null || true
SHARD1_EXIT=$?

echo ""
echo "========================================="
echo "Results"
echo "========================================="

# Show results from both shards
echo "--- Shard 0 Results ---"
grep -E "Throughput|Latency|Committed|Duration" "${log_prefix}_shard0.log" 2>/dev/null || echo "No results found"

echo ""
echo "--- Shard 1 Results ---"
grep -E "Throughput|Latency|Committed|Duration" "${log_prefix}_shard1.log" 2>/dev/null || echo "No results found"

echo ""
echo "Full logs:"
echo "  ${log_prefix}_shard0.log"
echo "  ${log_prefix}_shard1.log"

# Cleanup
pkill -9 -f luigi_bench 2>/dev/null || true

exit 0
