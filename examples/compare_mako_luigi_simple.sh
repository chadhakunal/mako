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
trd=${1:-6}      # Threads per shard
duration=${2:-30}  # Test duration in seconds

echo "Configuration:"
echo "  Threads/shard: $trd"
echo "  Duration:      ${duration}s"
echo "  Shards:        2 (no replication)"
echo "  Benchmark:     TPC-C"
echo ""

# Cleanup temp files from previous runs
rm -f /tmp/mako_s*.txt /tmp/luigi_s*.txt

echo "════════════════════════════════════════════════════════════════════"
echo "Phase 1: Running Mako Benchmark"
echo "════════════════════════════════════════════════════════════════════"
echo ""

bash ./examples/test_mako_simple.sh $trd $duration
mako_exit=$?

if [ $mako_exit -ne 0 ]; then
    echo ""
    echo "⚠️  WARNING: Mako test exited with code $mako_exit"
    echo "    Continuing with Luigi test anyway..."
fi

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "Phase 2: Running Luigi Benchmark"
echo "════════════════════════════════════════════════════════════════════"
echo ""

# Give system a moment to cleanup
sleep 2

bash ./examples/test_luigi_simple.sh $trd $duration
luigi_exit=$?

if [ $luigi_exit -ne 0 ]; then
    echo ""
    echo "⚠️  WARNING: Luigi test exited with code $luigi_exit"
fi

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "Phase 3: Results Comparison"
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

# Mako metrics
mako_s0_tps=$(read_metric "/tmp/mako_s0_tps.txt" "0")
mako_s1_tps=$(read_metric "/tmp/mako_s1_tps.txt" "0")
mako_s0_abort=$(read_metric "/tmp/mako_s0_abort.txt" "0")
mako_s1_abort=$(read_metric "/tmp/mako_s1_abort.txt" "0")
mako_s0_lat=$(read_metric "/tmp/mako_s0_latency.txt" "0")
mako_s1_lat=$(read_metric "/tmp/mako_s1_latency.txt" "0")

# Luigi metrics
luigi_s0_tps=$(read_metric "/tmp/luigi_s0_tps.txt" "0")
luigi_s1_tps=$(read_metric "/tmp/luigi_s1_tps.txt" "0")
luigi_s0_abort=$(read_metric "/tmp/luigi_s0_abort.txt" "0")
luigi_s1_abort=$(read_metric "/tmp/luigi_s1_abort.txt" "0")
luigi_s0_lat=$(read_metric "/tmp/luigi_s0_latency.txt" "0")
luigi_s1_lat=$(read_metric "/tmp/luigi_s1_latency.txt" "0")

# Calculate totals
if command -v bc >/dev/null 2>&1; then
    mako_total_tps=$(echo "$mako_s0_tps + $mako_s1_tps" | bc)
    luigi_total_tps=$(echo "$luigi_s0_tps + $luigi_s1_tps" | bc)
    mako_avg_lat=$(echo "scale=2; ($mako_s0_lat + $mako_s1_lat) / 2" | bc)
    luigi_avg_lat=$(echo "scale=2; ($luigi_s0_lat + $luigi_s1_lat) / 2" | bc)
    
    # Calculate ratio (avoid division by zero)
    if [ "$(echo "$mako_total_tps > 0" | bc)" -eq 1 ]; then
        ratio=$(echo "scale=3; $luigi_total_tps / $mako_total_tps" | bc)
    else
        ratio="N/A"
    fi
else
    # Fallback to shell arithmetic (less precise)
    mako_total_tps=$((mako_s0_tps + mako_s1_tps))
    luigi_total_tps=$((luigi_s0_tps + luigi_s1_tps))
    mako_avg_lat=$((mako_s0_lat + mako_s1_lat))
    luigi_avg_lat=$((luigi_s0_lat + luigi_s1_lat))
    
    if [ $mako_total_tps -gt 0 ]; then
        ratio=$(( luigi_total_tps * 1000 / mako_total_tps ))
        ratio="0.$ratio"
    else
        ratio="N/A"
    fi
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
printf "│ %-16s │ %17s%% │ %17s%% │\n" "Shard 0 Abort %" "$mako_s0_abort" "$luigi_s0_abort"
printf "│ %-16s │ %17s%% │ %17s%% │\n" "Shard 1 Abort %" "$mako_s1_abort" "$luigi_s1_abort"
echo "├──────────────────┼─────────────────────┼─────────────────────┤"
printf "│ %-16s │ %17s us │ %17s us │\n" "Avg Latency" "$mako_avg_lat" "$luigi_avg_lat"
echo "└──────────────────┴─────────────────────┴─────────────────────┘"

echo ""
echo "Summary:"
echo "  Mako:  $mako_total_tps txns/sec, ${mako_avg_lat} us avg latency"
echo "  Luigi: $luigi_total_tps txns/sec, ${luigi_avg_lat} us avg latency"
if [ "$ratio" != "N/A" ]; then
    echo "  Ratio: ${ratio}x (Luigi/Mako throughput)"
fi

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "Detailed Logs:"
echo "════════════════════════════════════════════════════════════════════"
echo "  Mako:"
echo "    - mako_simple_shard0.log"
echo "    - mako_simple_shard1.log"
echo ""
echo "  Luigi:"
echo "    - luigi_simple_shard0.log"
echo "    - luigi_simple_shard1.log"
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

