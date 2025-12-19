#!/bin/bash
# Compare Mako vs Luigi performance (no replication baseline)
# This runs both benchmarks and provides side-by-side comparison

set -e

echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║          Mako vs Luigi Performance Comparison                      ║"
echo "║                  (Simple / No Replication)                         ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""

# Parse arguments
trd=${1:-6}       # Threads per shard
duration=${2:-30}  # Test duration in seconds

echo "Configuration:"
echo "  Threads/shard: $trd"
echo "  Duration:      ${duration}s"
echo "  Shards:        2 (no replication)"
echo "  Benchmark:     TPC-C"
echo ""

path=$(pwd)/src/mako
export LD_LIBRARY_PATH="$(pwd)/build:${LD_LIBRARY_PATH}"

# Cleanup temp files and sync barriers
rm -f /tmp/mako_s*.txt /tmp/luigi_s*.txt
rm -f nfs_sync_*
sudo rm -rf /tmp/mako_sync/ 2>/dev/null || rm -rf /tmp/mako_sync/ 2>/dev/null || true
USERNAME=${USER:-$(whoami)}
rm -rf /tmp/${USERNAME}_mako_rocksdb_shard*
pkill -9 -f dbtest 2>/dev/null || true
pkill -9 -f luigi_bench 2>/dev/null || true
sleep 2

#=============================================================================
# PHASE 1: MAKO BENCHMARK
#=============================================================================
echo "════════════════════════════════════════════════════════════════════"
echo "Phase 1: Running Mako Benchmark"
echo "════════════════════════════════════════════════════════════════════"
echo ""

mako_log_prefix="mako_compare"

# Start Mako shard 0 (output directly to log file)
echo "Starting Mako shard 0..."
./build/dbtest \
    --num-threads "$trd" \
    --shard-index 0 \
    --shard-config "$path/config/local-shards2-warehouses${trd}.yml" \
    -P localhost \
    > "${mako_log_prefix}_shard0.log" 2>&1 &
MAKO_S0_PID=$!
sleep 3

# Start Mako shard 1 (output directly to log file)
echo "Starting Mako shard 1..."
./build/dbtest \
    --num-threads "$trd" \
    --shard-index 1 \
    --shard-config "$path/config/local-shards2-warehouses${trd}.yml" \
    -P localhost \
    > "${mako_log_prefix}_shard1.log" 2>&1 &
MAKO_S1_PID=$!

echo "Running Mako for ${duration}s (plus 30s warmup/loading)..."
echo "  Shard 0 PID: $MAKO_S0_PID"
echo "  Shard 1 PID: $MAKO_S1_PID"
echo "  Logs: ${mako_log_prefix}_shard0.log, ${mako_log_prefix}_shard1.log"
sleep $((duration + 30))

# Stop Mako
echo "Stopping Mako..."
kill $MAKO_S0_PID $MAKO_S1_PID 2>/dev/null || true
wait $MAKO_S0_PID $MAKO_S1_PID 2>/dev/null || true
pkill -9 -f dbtest 2>/dev/null || true

# Thorough cleanup between Mako and Luigi
echo "Cleaning up before Luigi..."
pkill -9 -f "dbtest" 2>/dev/null || true
pkill -9 -f "luigi_bench" 2>/dev/null || true
rm -f nfs_sync_*
sudo rm -rf /tmp/mako_sync/ 2>/dev/null || rm -rf /tmp/mako_sync/ 2>/dev/null || true
rm -rf /tmp/${USERNAME}_mako_rocksdb_shard*
# Wait for ports to be released
echo "Waiting for ports to release..."
sleep 5

#=============================================================================
# PHASE 2: LUIGI BENCHMARK
#=============================================================================
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "Phase 2: Running Luigi Benchmark"
echo "════════════════════════════════════════════════════════════════════"
echo ""

luigi_log_prefix="luigi_compare"

# Start Luigi shard 0 (output directly to log file)
echo "Starting Luigi shard 0..."
./build/luigi_bench \
    --shard-config "$path/config/local-shards2-warehouses${trd}.yml" \
    --shard-index 0 \
    -P localhost \
    --num-threads "$trd" \
    --benchmark tpcc \
    --duration "$duration" \
    > "${luigi_log_prefix}_shard0.log" 2>&1 &
LUIGI_S0_PID=$!
sleep 2

# Start Luigi shard 1 (output directly to log file)
echo "Starting Luigi shard 1..."
./build/luigi_bench \
    --shard-config "$path/config/local-shards2-warehouses${trd}.yml" \
    --shard-index 1 \
    -P localhost \
    --num-threads "$trd" \
    --benchmark tpcc \
    --duration "$duration" \
    > "${luigi_log_prefix}_shard1.log" 2>&1 &
LUIGI_S1_PID=$!

echo "Running Luigi for ${duration}s (plus 30s warmup/loading)..."
echo "  Shard 0 PID: $LUIGI_S0_PID"
echo "  Shard 1 PID: $LUIGI_S1_PID"
echo "  Logs: ${luigi_log_prefix}_shard0.log, ${luigi_log_prefix}_shard1.log"

# Wait for Luigi to load and run
# We use a simple sleep loop with check similar to Mako, but stricter checking
# Increase buffer to 60s to ensure table loading (which takes ~30s) + benchmark + shutdown completes
wait_time=$((duration + 60))
start_wait=$(date +%s)

while true; do
    # Check if processes are alive
    if ! kill -0 $LUIGI_S0_PID 2>/dev/null && ! kill -0 $LUIGI_S1_PID 2>/dev/null; then
        echo "Luigi processes stopped early"
        break
    fi
    
    now=$(date +%s)
    elapsed=$((now - start_wait))
    if [ $elapsed -ge $wait_time ]; then
        echo "Timeout reached (${wait_time}s), stopping Luigi..."
        kill $LUIGI_S0_PID $LUIGI_S1_PID 2>/dev/null || true
        pkill -9 -f luigi_bench 2>/dev/null || true
        sleep 1
        break
    fi
    
    # Show progress
    if [ $((elapsed % 5)) -eq 0 ]; then
        echo "  Waiting... ${elapsed}s/${wait_time}s"
    fi
    sleep 1
done

wait $LUIGI_S0_PID $LUIGI_S1_PID 2>/dev/null || true
pkill -9 -f luigi_bench 2>/dev/null || true

#=============================================================================
# PHASE 3: EXTRACT METRICS
#=============================================================================
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "Phase 3: Extracting Metrics"
echo "════════════════════════════════════════════════════════════════════"
echo ""

# Extract Mako metrics
extract_mako_metrics() {
    local log=$1
    local shard_id=$2
    
    # Mako outputs: "agg_persist_throughput: X txns/sec"
    local tps=$(grep "agg_persist_throughput:" "$log" 2>/dev/null | tail -1 | awk '{print $2}')
    
    # Mako outputs: "NewOrder_remote_abort_ratio: X%"
    local abort=$(grep "NewOrder_remote_abort_ratio:" "$log" 2>/dev/null | tail -1 | awk '{print $2}' | tr -d '%')
    
    # Mako doesn't output avg latency in same format, use placeholder
    local latency=$(grep -E "p50_latency|P50" "$log" 2>/dev/null | tail -1 | awk '{print $2}' || echo "N/A")
    
    echo "${tps:-0}" > "/tmp/mako_s${shard_id}_tps.txt"
    echo "${abort:-0}" > "/tmp/mako_s${shard_id}_abort.txt"
    echo "${latency:-0}" > "/tmp/mako_s${shard_id}_latency.txt"
    
    echo "Mako Shard $shard_id: ${tps:-N/A} TPS, ${abort:-N/A}% abort"
}

# Extract Luigi metrics
extract_luigi_metrics() {
    local log=$1
    local shard_id=$2
    
    # Luigi outputs: "Throughput: X txns/sec"
    local tps=$(grep "Throughput:" "$log" 2>/dev/null | tail -1 | awk '{print $2}')
    
    # Luigi outputs: "Aborted: X (Y%)"
    local aborted=$(grep "Aborted:" "$log" 2>/dev/null | tail -1 | awk '{print $2}')
    local committed=$(grep "Committed:" "$log" 2>/dev/null | tail -1 | awk '{print $2}')
    local abort="0"
    if [ -n "$committed" ] && [ -n "$aborted" ]; then
        local total=$((committed + aborted))
        if [ $total -gt 0 ]; then
            abort=$(echo "scale=2; 100.0 * $aborted / $total" | bc)
        fi
    fi
    
    # Luigi outputs: "Avg Latency: X us"
    local latency=$(grep "Avg Latency:" "$log" 2>/dev/null | tail -1 | awk '{print $3}')
    
    echo "${tps:-0}" > "/tmp/luigi_s${shard_id}_tps.txt"
    echo "${abort:-0}" > "/tmp/luigi_s${shard_id}_abort.txt"
    echo "${latency:-0}" > "/tmp/luigi_s${shard_id}_latency.txt"
    
    echo "Luigi Shard $shard_id: ${tps:-N/A} TPS, ${abort:-N/A}% abort, ${latency:-N/A} us latency"
}

extract_mako_metrics "${mako_log_prefix}_shard0.log" 0
extract_mako_metrics "${mako_log_prefix}_shard1.log" 1
extract_luigi_metrics "${luigi_log_prefix}_shard0.log" 0
extract_luigi_metrics "${luigi_log_prefix}_shard1.log" 1

#=============================================================================
# PHASE 4: COMPARISON TABLE
#=============================================================================
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "Phase 4: Results Comparison"
echo "════════════════════════════════════════════════════════════════════"
echo ""

# Read metrics from temp files
read_metric() {
    local file=$1
    local default=${2:-0}
    if [ -f "$file" ]; then
        cat "$file" | tr -d '[:space:]'
    else
        echo "$default"
    fi
}

mako_s0_tps=$(read_metric "/tmp/mako_s0_tps.txt" "0")
mako_s1_tps=$(read_metric "/tmp/mako_s1_tps.txt" "0")
mako_s0_abort=$(read_metric "/tmp/mako_s0_abort.txt" "0")
mako_s1_abort=$(read_metric "/tmp/mako_s1_abort.txt" "0")
mako_s0_lat=$(read_metric "/tmp/mako_s0_latency.txt" "N/A")
mako_s1_lat=$(read_metric "/tmp/mako_s1_latency.txt" "N/A")

luigi_s0_tps=$(read_metric "/tmp/luigi_s0_tps.txt" "0")
luigi_s1_tps=$(read_metric "/tmp/luigi_s1_tps.txt" "0")
luigi_s0_abort=$(read_metric "/tmp/luigi_s0_abort.txt" "0")
luigi_s1_abort=$(read_metric "/tmp/luigi_s1_abort.txt" "0")
luigi_s0_lat=$(read_metric "/tmp/luigi_s0_latency.txt" "0")
luigi_s1_lat=$(read_metric "/tmp/luigi_s1_latency.txt" "0")

# Calculate totals
if command -v bc >/dev/null 2>&1; then
    mako_total_tps=$(echo "$mako_s0_tps + $mako_s1_tps" | bc 2>/dev/null || echo "0")
    luigi_total_tps=$(echo "$luigi_s0_tps + $luigi_s1_tps" | bc 2>/dev/null || echo "0")
    
    if [ -n "$mako_total_tps" ] && [ "$mako_total_tps" != "0" ]; then
        ratio=$(echo "scale=2; $luigi_total_tps / $mako_total_tps" | bc 2>/dev/null || echo "N/A")
    else
        ratio="N/A"
    fi
else
    mako_total_tps=$((${mako_s0_tps%.*} + ${mako_s1_tps%.*}))
    luigi_total_tps=$((${luigi_s0_tps%.*} + ${luigi_s1_tps%.*}))
    ratio="N/A"
fi

# Print comparison table
echo "┌──────────────────┬─────────────────────┬─────────────────────┐"
echo "│ Metric           │        Mako         │       Luigi         │"
echo "├──────────────────┼─────────────────────┼─────────────────────┤"
printf "│ %-16s │ %19s │ %19s │\n" "Shard 0 TPS" "$mako_s0_tps" "$luigi_s0_tps"
printf "│ %-16s │ %19s │ %19s │\n" "Shard 1 TPS" "$mako_s1_tps" "$luigi_s1_tps"
echo "├──────────────────┼─────────────────────┼─────────────────────┤"
printf "│ %-16s │ %19s │ %19s │\n" "TOTAL TPS" "$mako_total_tps" "$luigi_total_tps"
echo "├──────────────────┼─────────────────────┼─────────────────────┤"
printf "│ %-16s │ %17s%% │ %17s%% │\n" "Shard 0 Abort" "$mako_s0_abort" "$luigi_s0_abort"
printf "│ %-16s │ %17s%% │ %17s%% │\n" "Shard 1 Abort" "$mako_s1_abort" "$luigi_s1_abort"
echo "├──────────────────┼─────────────────────┼─────────────────────┤"
printf "│ %-16s │ %17s us │ %17s us │\n" "Shard 0 Latency" "$mako_s0_lat" "$luigi_s0_lat"
printf "│ %-16s │ %17s us │ %17s us │\n" "Shard 1 Latency" "$mako_s1_lat" "$luigi_s1_lat"
echo "└──────────────────┴─────────────────────┴─────────────────────┘"

echo ""
echo "Summary:"
echo "  Mako:  $mako_total_tps txns/sec"
echo "  Luigi: $luigi_total_tps txns/sec"
if [ "$ratio" != "N/A" ]; then
    echo "  Ratio: ${ratio}x (Luigi/Mako throughput)"
fi

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "Log Files:"
echo "════════════════════════════════════════════════════════════════════"
echo "  Mako:  ${mako_log_prefix}_shard0.log, ${mako_log_prefix}_shard1.log"
echo "  Luigi: ${luigi_log_prefix}_shard0.log, ${luigi_log_prefix}_shard1.log"
echo ""

# Save comparison to CSV
csv_file="comparison_results.csv"
echo "timestamp,system,shard,throughput_tps,abort_pct,latency_us" > "$csv_file"
timestamp=$(date +%Y%m%d_%H%M%S)
echo "$timestamp,mako,0,$mako_s0_tps,$mako_s0_abort,$mako_s0_lat" >> "$csv_file"
echo "$timestamp,mako,1,$mako_s1_tps,$mako_s1_abort,$mako_s1_lat" >> "$csv_file"
echo "$timestamp,luigi,0,$luigi_s0_tps,$luigi_s0_abort,$luigi_s0_lat" >> "$csv_file"
echo "$timestamp,luigi,1,$luigi_s1_tps,$luigi_s1_abort,$luigi_s1_lat" >> "$csv_file"

echo "Results saved to: $csv_file"
echo ""
echo "Comparison complete!"

# Cleanup temp files
rm -f /tmp/mako_s*.txt /tmp/luigi_s*.txt

exit 0
