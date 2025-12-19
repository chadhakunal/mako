#!/bin/bash
# Simple Luigi test (2 shards, no replication)
# Outputs standardized metrics for comparison with Mako

set -e

echo "========================================="
echo "Luigi Simple Test (No Replication)"
echo "========================================="

# Parse arguments
trd=${1:-6}  # Default to 6 threads per shard
duration=${2:-30}  # Default 30 seconds

script_name="luigi_simple"
log_prefix="${script_name}"

path=$(pwd)/src/mako

echo "Configuration:"
echo "  Threads/shard: $trd"
echo "  Duration:      ${duration}s"
echo "  Shards:        2"
echo "  Replication:   disabled"
echo "  Benchmark:     tpcc"
echo "  Config:        $path/config/local-shards2-warehouses${trd}.yml"
echo ""

# Clean up
rm -f nfs_sync_*
sudo rm -rf /tmp/mako_sync/ 2>/dev/null || rm -rf /tmp/mako_sync/ 2>/dev/null || true
USERNAME=${USER:-$(whoami)}
rm -rf /tmp/${USERNAME}_mako_rocksdb_shard*
pkill -9 -f luigi_bench 2>/dev/null || true
pkill -9 -f dbtest 2>/dev/null || true
sleep 1

# Log files: separate per shard + combined
shard0_log="${log_prefix}_shard0.log"
shard1_log="${log_prefix}_shard1.log"
combined_log="${log_prefix}_combined.log"
rm -f "$shard0_log" "$shard1_log" "$combined_log"

# Start shard 0
echo "Starting Luigi shard 0..."
./build/luigi_bench \
    --shard-config "$path/config/local-shards2-warehouses${trd}.yml" \
    --shard-index 0 \
    -P localhost \
    --num-threads "$trd" \
    --benchmark tpcc \
    --duration "$duration" \
    2>&1 | tee -a "$shard0_log" | sed -u 's/^/[S0] /' >> "$combined_log" &
SHARD0_PID=$!
sleep 1

# Start shard 1
echo "Starting Luigi shard 1..."
./build/luigi_bench \
    --shard-config "$path/config/local-shards2-warehouses${trd}.yml" \
    --shard-index 1 \
    -P localhost \
    --num-threads "$trd" \
    --benchmark tpcc \
    --duration "$duration" \
    2>&1 | tee -a "$shard1_log" | sed -u 's/^/[S1] /' >> "$combined_log" &
SHARD1_PID=$!

echo "Running benchmark for ${duration}s..."
echo "  Shard 0 PID: $SHARD0_PID"
echo "  Shard 1 PID: $SHARD1_PID"

# Wait for completion (duration + startup buffer + grace period)
wait_time=$((duration + 15))
echo "Waiting up to ${wait_time}s for completion..."

# Poll for process completion with timeout
start_wait=$(date +%s)
while true; do
    # Check if both processes have exited
    kill -0 $SHARD0_PID 2>/dev/null
    shard0_alive=$?
    kill -0 $SHARD1_PID 2>/dev/null
    shard1_alive=$?
    
    if [ $shard0_alive -ne 0 ] && [ $shard1_alive -ne 0 ]; then
        echo "Both shards completed"
        break
    fi
    
    # Check timeout
    now=$(date +%s)
    elapsed=$((now - start_wait))
    if [ $elapsed -ge $wait_time ]; then
        echo "Timeout reached (${wait_time}s), forcing shutdown..."
        pkill -9 -f luigi_bench 2>/dev/null || true
        break
    fi
    
    sleep 1
done

# Collect exit codes
wait $SHARD0_PID 2>/dev/null || true
SHARD0_EXIT=$?
wait $SHARD1_PID 2>/dev/null || true
SHARD1_EXIT=$?

# Cleanup any remaining processes
pkill -9 -f luigi_bench 2>/dev/null || true

echo ""
echo "========================================="
echo "Results Parsing"
echo "========================================="

# Extract metrics from both shards
parse_luigi_shard_metrics() {
    local log=$1
    local shard_id=$2
    
    if [ ! -f "$log" ]; then
        echo "ERROR: Log file $log not found"
        return 1
    fi
    
    # Extract throughput (look for "Throughput:" line)
    local tps=$(grep "Throughput:" "$log" 2>/dev/null | tail -1 | awk '{print $2}')
    
    # Extract committed/aborted counts
    local committed=$(grep "Committed:" "$log" 2>/dev/null | tail -1 | awk '{print $2}')
    local aborted=$(grep "Aborted:" "$log" 2>/dev/null | tail -1 | awk '{print $2}')
    
    # Calculate abort ratio
    local abort_ratio="0"
    if [ -n "$committed" ] && [ -n "$aborted" ]; then
        local total=$((committed + aborted))
        if [ $total -gt 0 ]; then
            abort_ratio=$(echo "scale=2; 100.0 * $aborted / $total" | bc)
        fi
    fi
    
    # Extract latency percentiles
    local avg_latency=$(grep "Avg Latency:" "$log" 2>/dev/null | tail -1 | awk '{print $3}')
    local p50_latency=$(grep "P50 Latency:" "$log" 2>/dev/null | tail -1 | awk '{print $3}')
    local p99_latency=$(grep "P99 Latency:" "$log" 2>/dev/null | tail -1 | awk '{print $3}')
    
    echo "Shard $shard_id:"
    echo "  Throughput:   ${tps:-N/A} txns/sec"
    echo "  Committed:    ${committed:-N/A}"
    echo "  Aborted:      ${aborted:-N/A}"
    echo "  Abort ratio:  ${abort_ratio}%"
    echo "  Avg latency:  ${avg_latency:-N/A} us"
    echo "  P50 latency:  ${p50_latency:-N/A} us"
    echo "  P99 latency:  ${p99_latency:-N/A} us"
    
    # Export for comparison script
    echo "$tps" > "/tmp/luigi_s${shard_id}_tps.txt"
    echo "$abort_ratio" > "/tmp/luigi_s${shard_id}_abort.txt"
    echo "$avg_latency" > "/tmp/luigi_s${shard_id}_latency.txt"
}

echo ""
parse_luigi_shard_metrics "${log_prefix}_shard0.log" 0
echo ""
parse_luigi_shard_metrics "${log_prefix}_shard1.log" 1

echo ""
echo "Full logs:"
echo "  ${log_prefix}_shard0.log"
echo "  ${log_prefix}_shard1.log"
echo ""
echo "Luigi simple test completed."

