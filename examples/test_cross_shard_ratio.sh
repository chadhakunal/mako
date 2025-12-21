#!/bin/bash
# Test Luigi vs Mako at different cross-shard percentages
# Usage: ./test_cross_shard_ratio.sh <threads> <duration> <delay_ms> <jitter_ms>

set -e

trd=${1:-4}
duration=${2:-15}
delay=${3:-50}
jitter=${4:-5}

path=$(dirname "$(dirname "$(realpath "$0")")")
cd "$path"

echo "═══════════════════════════════════════════════════════════════════════════════"
echo "  Cross-Shard Ratio Test: Luigi vs Mako"
echo "  Threads: $trd, Duration: ${duration}s, Network Delay: ${delay}ms ± ${jitter}ms"
echo "═══════════════════════════════════════════════════════════════════════════════"

# Clean up any previous runs
pkill -9 luigi_bench 2>/dev/null || true
pkill -9 dbtest 2>/dev/null || true
rm -f nfs_sync_*
sudo rm -rf /tmp/mako_sync/ 2>/dev/null || true
sudo rm -rf /tmp/*_mako_rocksdb_shard* 2>/dev/null || true
ipcrm -a 2>/dev/null || true
sleep 2

# Add network delay
echo "Adding network delay: ${delay}ms ± ${jitter}ms..."
sudo tc qdisc del dev lo root 2>/dev/null || true
sudo tc qdisc add dev lo root netem delay ${delay}ms ${jitter}ms

# Calculate OWD (delay + half jitter as buffer)
owd_ms=$((delay + jitter/2 + 4))
startup_delay=$((delay * 2 / 100 + 5))

export LD_LIBRARY_PATH="${path}/build:${LD_LIBRARY_PATH}"

results_file="cross_shard_results_${delay}ms.csv"
echo "cross_shard_pct,mako_tps,luigi_tps,speedup,mako_abort,luigi_abort" > "$results_file"

for pct in 5 15 25 35; do
    echo ""
    echo "═══════════════════════════════════════════════════════════════════════════════"
    echo "  Testing ${pct}% cross-shard transactions (BOTH Mako and Luigi)"
    echo "═══════════════════════════════════════════════════════════════════════════════"
    
    # Clean up between runs
    rm -f nfs_sync_*
    sudo rm -rf /tmp/mako_sync/ 2>/dev/null || true
    sudo rm -rf /tmp/*_mako_rocksdb_shard* 2>/dev/null || true
    ipcrm -a 2>/dev/null || true
    
    # Update Mako's cross-shard percentage and rebuild
    echo "Rebuilding Mako with ${pct}% cross-shard..."
    sed -i "s/static int g_new_order_remote_item_pct = [0-9]*;/static int g_new_order_remote_item_pct = ${pct};/" \
        "$path/src/mako/benchmarks/tpcc.cc"
    make -C "$path/build" -j4 dbtest >/dev/null 2>&1
    
    # Run Mako (using dbtest binary)
    echo "Running Mako with ${pct}% cross-shard..."
    pkill -9 dbtest 2>/dev/null || true
    sleep 2
    
    ./build/dbtest --shard-config "$path/src/mako/config/local-shards2-warehouses${trd}.yml" \
        --shard-index 0 -P localhost --num-threads "$trd" \
        > mako_cross_shard0.log 2>&1 &
    mako_pid0=$!
    sleep $startup_delay
    ./build/dbtest --shard-config "$path/src/mako/config/local-shards2-warehouses${trd}.yml" \
        --shard-index 1 -P localhost --num-threads "$trd" \
        > mako_cross_shard1.log 2>&1 &
    mako_pid1=$!
    
    sleep $((duration + 30))
    pkill -9 dbtest 2>/dev/null || true
    sleep 3
    
    # Parse Mako results (uses agg_throughput format)
    mako_tps0=$(grep "agg_throughput:" mako_cross_shard0.log 2>/dev/null | awk '{print $2}' || echo "0")
    mako_tps1=$(grep "agg_throughput:" mako_cross_shard1.log 2>/dev/null | awk '{print $2}' || echo "0")
    mako_abort0=$(grep "NewOrder_remote_abort_ratio:" mako_cross_shard0.log 2>/dev/null | awk '{print $2}' || echo "0")
    mako_abort1=$(grep "NewOrder_remote_abort_ratio:" mako_cross_shard1.log 2>/dev/null | awk '{print $2}' || echo "0")
    mako_tps=$(echo "$mako_tps0 + $mako_tps1" | bc 2>/dev/null || echo "0")
    mako_abort=$(echo "scale=2; ($mako_abort0 + $mako_abort1) / 2" | bc 2>/dev/null || echo "0")
    
    # Run Luigi
    echo "Running Luigi with --cross-shard-pct $pct..."
    pkill -9 luigi_bench 2>/dev/null || true
    rm -f nfs_sync_*
    sudo rm -rf /tmp/mako_sync/ 2>/dev/null || true
    sudo rm -rf /tmp/*_mako_rocksdb_shard* 2>/dev/null || true
    ipcrm -a 2>/dev/null || true
    sleep 2
    
    ./build/luigi_bench --shard-config "$path/src/mako/config/local-shards2-warehouses${trd}.yml" \
        --shard-index 0 -P localhost --num-threads "$trd" --benchmark tpcc --duration "$duration" \
        --owd-ms "$owd_ms" --cross-shard-pct "$pct" > luigi_cross_shard0.log 2>&1 &
    luigi_pid0=$!
    sleep $startup_delay
    ./build/luigi_bench --shard-config "$path/src/mako/config/local-shards2-warehouses${trd}.yml" \
        --shard-index 1 -P localhost --num-threads "$trd" --benchmark tpcc --duration "$duration" \
        --owd-ms "$owd_ms" --cross-shard-pct "$pct" > luigi_cross_shard1.log 2>&1 &
    luigi_pid1=$!
    
    sleep $((duration + 30))
    pkill -9 luigi_bench 2>/dev/null || true
    sleep 3
    
    # Parse Luigi results
    luigi_tps0=$(grep "Throughput:" luigi_cross_shard0.log 2>/dev/null | awk '{print $2}' || echo "0")
    luigi_tps1=$(grep "Throughput:" luigi_cross_shard1.log 2>/dev/null | awk '{print $2}' || echo "0")
    luigi_abort0=$(grep "Aborted:" luigi_cross_shard0.log 2>/dev/null | grep -oP '\d+\.\d+(?=%)' || echo "0")
    luigi_abort1=$(grep "Aborted:" luigi_cross_shard1.log 2>/dev/null | grep -oP '\d+\.\d+(?=%)' || echo "0")
    luigi_tps=$(echo "$luigi_tps0 + $luigi_tps1" | bc 2>/dev/null || echo "0")
    luigi_abort=$(echo "scale=2; ($luigi_abort0 + $luigi_abort1) / 2" | bc 2>/dev/null || echo "0")
    
    # Calculate speedup
    if [ "$mako_tps" != "0" ] && [ -n "$mako_tps" ]; then
        speedup=$(echo "scale=2; $luigi_tps / $mako_tps" | bc 2>/dev/null || echo "N/A")
    else
        speedup="N/A"
    fi
    
    echo ""
    echo "Results for ${pct}% cross-shard:"
    echo "  Mako:  $mako_tps TPS, ${mako_abort}% abort"
    echo "  Luigi: $luigi_tps TPS, ${luigi_abort}% abort"
    echo "  Speedup: ${speedup}x"
    
    echo "${pct},${mako_tps},${luigi_tps},${speedup},${mako_abort},${luigi_abort}" >> "$results_file"
done

# Clean up network delay
echo ""
echo "Cleaning up network delay..."
sudo tc qdisc del dev lo root 2>/dev/null || true

echo ""
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "  Results saved to: $results_file"
echo "═══════════════════════════════════════════════════════════════════════════════"
cat "$results_file"
