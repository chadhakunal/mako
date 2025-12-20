#!/bin/bash
# Compare Mako vs Luigi with tc network delay (Geo-Distributed Simulation)
# Built from working test_mako_tc.sh and test_luigi_tc.sh

set -e

echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║      Mako vs Luigi Performance Comparison (Geo-Distributed)        ║"
echo "║                  Using tc for Network Delay Simulation             ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""

# Parse arguments
trd=${1:-4}
duration=${2:-10}
delay_ms=${3:-50}
jitter_ms=${4:-5}

# Calculate OWD for Luigi (delay + jitter/2 + headroom)
owd_ms=$((delay_ms + jitter_ms/2 + 2))
startup_delay=$((delay_ms * 2 / 100 + 5))

echo "Configuration:"
echo "  Threads/shard: $trd"
echo "  Duration:      ${duration}s"
echo "  Delay:         ${delay_ms}ms ± ${jitter_ms}ms"
echo "  Luigi OWD:     ${owd_ms}ms"
echo ""

path=$(pwd)/src/mako
export LD_LIBRARY_PATH="$(pwd)/build:${LD_LIBRARY_PATH}"

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    pkill -9 -f luigi_bench 2>/dev/null || true
    pkill -9 -f dbtest 2>/dev/null || true
    sudo tc qdisc del dev lo root 2>/dev/null || true
    rm -f nfs_sync_*
    echo "Done."
}
trap cleanup EXIT

# Pre-run cleanup
pkill -9 -f luigi_bench 2>/dev/null || true
pkill -9 -f dbtest 2>/dev/null || true
rm -f nfs_sync_*
sudo rm -rf /tmp/mako_sync/ 2>/dev/null || true
# Clean ALL rocksdb shard directories (any user)
sudo rm -rf /tmp/*_mako_rocksdb_shard* 2>/dev/null || true
sudo rm -rf /dev/hugepages/rtebuf* 2>/dev/null || true
ipcrm -a 2>/dev/null || true
sleep 3

# Add network delay ONCE at the start
echo "Adding network delay..."
sudo tc qdisc del dev lo root 2>/dev/null || true
sudo tc qdisc add dev lo root netem delay ${delay_ms}ms ${jitter_ms}ms
echo "✓ Added ${delay_ms}ms ± ${jitter_ms}ms delay to loopback"
echo ""

#=============================================================================
# PHASE 1: MAKO BENCHMARK
#=============================================================================
echo "════════════════════════════════════════════════════════════════════"
echo "Phase 1: Running Mako Benchmark"
echo "════════════════════════════════════════════════════════════════════"

echo "Starting Mako shard 0..."
./build/dbtest \
    --num-threads "$trd" \
    --shard-index 0 \
    --shard-config "$path/config/local-shards2-warehouses${trd}.yml" \
    -P localhost \
    > mako_geo_shard0.log 2>&1 &
MAKO_S0_PID=$!

echo "  Waiting ${startup_delay}s for shard 0..."
sleep $startup_delay

echo "Starting Mako shard 1..."
./build/dbtest \
    --num-threads "$trd" \
    --shard-index 1 \
    --shard-config "$path/config/local-shards2-warehouses${trd}.yml" \
    -P localhost \
    > mako_geo_shard1.log 2>&1 &
MAKO_S1_PID=$!

echo "  Shard PIDs: $MAKO_S0_PID, $MAKO_S1_PID"
echo "  Running for $((duration + 30))s..."
sleep $((duration + 30))

# Stop Mako
echo "Stopping Mako..."
kill $MAKO_S0_PID $MAKO_S1_PID 2>/dev/null || true
wait $MAKO_S0_PID $MAKO_S1_PID 2>/dev/null || true
pkill -9 -f dbtest 2>/dev/null || true

# Cleanup between benchmarks
echo "Cleaning up before Luigi..."
rm -f nfs_sync_*
sudo rm -rf /tmp/mako_sync/ 2>/dev/null || true
rm -rf /tmp/${USERNAME}_mako_rocksdb_shard*
sudo rm -rf /dev/hugepages/rtebuf* 2>/dev/null || true
ipcrm -a 2>/dev/null || true
sleep 5

#=============================================================================
# PHASE 2: LUIGI BENCHMARK
#=============================================================================
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "Phase 2: Running Luigi Benchmark (OWD=${owd_ms}ms)"
echo "════════════════════════════════════════════════════════════════════"

echo "Starting Luigi shard 0..."
./build/luigi_bench \
    --shard-config "$path/config/local-shards2-warehouses${trd}.yml" \
    --shard-index 0 \
    -P localhost \
    --num-threads "$trd" \
    --benchmark tpcc \
    --duration "$duration" \
    --owd-ms "$owd_ms" \
    > luigi_geo_shard0.log 2>&1 &
LUIGI_S0_PID=$!

echo "  Waiting ${startup_delay}s for shard 0..."
sleep $startup_delay

echo "Starting Luigi shard 1..."
./build/luigi_bench \
    --shard-config "$path/config/local-shards2-warehouses${trd}.yml" \
    --shard-index 1 \
    -P localhost \
    --num-threads "$trd" \
    --benchmark tpcc \
    --duration "$duration" \
    --owd-ms "$owd_ms" \
    > luigi_geo_shard1.log 2>&1 &
LUIGI_S1_PID=$!

echo "  Shard PIDs: $LUIGI_S0_PID, $LUIGI_S1_PID"

wait_time=$((duration + 30))
echo "  Running for ~${wait_time}s..."

start_wait=$(date +%s)
while true; do
    if ! kill -0 $LUIGI_S0_PID 2>/dev/null && ! kill -0 $LUIGI_S1_PID 2>/dev/null; then
        echo "  Processes completed"
        break
    fi
    
    now=$(date +%s)
    elapsed=$((now - start_wait))
    if [ $elapsed -ge $wait_time ]; then
        echo "  Timeout, stopping..."
        kill $LUIGI_S0_PID $LUIGI_S1_PID 2>/dev/null || true
        break
    fi
    sleep 1
done

wait $LUIGI_S0_PID $LUIGI_S1_PID 2>/dev/null || true
pkill -9 -f luigi_bench 2>/dev/null || true

#=============================================================================
# PHASE 3: EXTRACT & COMPARE
#=============================================================================
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "Results Comparison (Network Delay: ${delay_ms}ms ± ${jitter_ms}ms)"
echo "════════════════════════════════════════════════════════════════════"
echo ""

# Extract Mako metrics
mako_s0_tps=$(grep "agg_persist_throughput:" mako_geo_shard0.log 2>/dev/null | tail -1 | awk '{print $2}')
mako_s1_tps=$(grep "agg_persist_throughput:" mako_geo_shard1.log 2>/dev/null | tail -1 | awk '{print $2}')
mako_s0_abort=$(grep "NewOrder_remote_abort_ratio:" mako_geo_shard0.log 2>/dev/null | tail -1 | awk '{print $2}' | tr -d '%')
mako_s1_abort=$(grep "NewOrder_remote_abort_ratio:" mako_geo_shard1.log 2>/dev/null | tail -1 | awk '{print $2}' | tr -d '%')
mako_s0_lat=$(grep -E "p50_latency|P50" mako_geo_shard0.log 2>/dev/null | tail -1 | awk '{print $2}' || echo "N/A")
mako_s1_lat=$(grep -E "p50_latency|P50" mako_geo_shard1.log 2>/dev/null | tail -1 | awk '{print $2}' || echo "N/A")

# Extract Luigi metrics
luigi_s0_tps=$(grep "Throughput:" luigi_geo_shard0.log 2>/dev/null | tail -1 | awk '{print $2}')
luigi_s1_tps=$(grep "Throughput:" luigi_geo_shard1.log 2>/dev/null | tail -1 | awk '{print $2}')
luigi_s0_lat=$(grep "Avg Latency:" luigi_geo_shard0.log 2>/dev/null | tail -1 | awk '{print $3}')
luigi_s1_lat=$(grep "Avg Latency:" luigi_geo_shard1.log 2>/dev/null | tail -1 | awk '{print $3}')

# Luigi abort rate calculation
luigi_s0_abort="0"
luigi_s0_aborted=$(grep "Aborted:" luigi_geo_shard0.log 2>/dev/null | tail -1 | awk '{print $2}')
luigi_s0_committed=$(grep "Committed:" luigi_geo_shard0.log 2>/dev/null | tail -1 | awk '{print $2}')
if [ -n "$luigi_s0_committed" ] && [ -n "$luigi_s0_aborted" ]; then
    total_s0=$((luigi_s0_committed + luigi_s0_aborted))
    if [ $total_s0 -gt 0 ]; then
        luigi_s0_abort=$(echo "scale=2; 100.0 * $luigi_s0_aborted / $total_s0" | bc)
    fi
fi

luigi_s1_abort="0"
luigi_s1_aborted=$(grep "Aborted:" luigi_geo_shard1.log 2>/dev/null | tail -1 | awk '{print $2}')
luigi_s1_committed=$(grep "Committed:" luigi_geo_shard1.log 2>/dev/null | tail -1 | awk '{print $2}')
if [ -n "$luigi_s1_committed" ] && [ -n "$luigi_s1_aborted" ]; then
    total_s1=$((luigi_s1_committed + luigi_s1_aborted))
    if [ $total_s1 -gt 0 ]; then
        luigi_s1_abort=$(echo "scale=2; 100.0 * $luigi_s1_aborted / $total_s1" | bc)
    fi
fi

# Calculate totals
mako_total=0
luigi_total=0
if [ -n "$mako_s0_tps" ] && [ -n "$mako_s1_tps" ]; then
    mako_total=$(echo "$mako_s0_tps + $mako_s1_tps" | bc 2>/dev/null || echo "0")
fi
if [ -n "$luigi_s0_tps" ] && [ -n "$luigi_s1_tps" ]; then
    luigi_total=$(echo "$luigi_s0_tps + $luigi_s1_tps" | bc 2>/dev/null || echo "0")
fi

# Print comparison table (matching compare_mako_luigi_simple.sh format)
echo "┌──────────────────┬─────────────────────┬─────────────────────┐"
echo "│ Metric           │        Mako         │       Luigi         │"
echo "├──────────────────┼─────────────────────┼─────────────────────┤"
printf "│ %-16s │ %19s │ %19s │\n" "Shard 0 TPS" "${mako_s0_tps:-0}" "${luigi_s0_tps:-0}"
printf "│ %-16s │ %19s │ %19s │\n" "Shard 1 TPS" "${mako_s1_tps:-0}" "${luigi_s1_tps:-0}"
echo "├──────────────────┼─────────────────────┼─────────────────────┤"
printf "│ %-16s │ %19s │ %19s │\n" "TOTAL TPS" "$mako_total" "$luigi_total"
echo "├──────────────────┼─────────────────────┼─────────────────────┤"
printf "│ %-16s │ %17s%% │ %17s%% │\n" "Shard 0 Abort" "${mako_s0_abort:-0}" "$luigi_s0_abort"
printf "│ %-16s │ %17s%% │ %17s%% │\n" "Shard 1 Abort" "${mako_s1_abort:-0}" "$luigi_s1_abort"
echo "├──────────────────┼─────────────────────┼─────────────────────┤"
printf "│ %-16s │ %17s us │ %17s us │\n" "Shard 0 Latency" "${mako_s0_lat:-0}" "${luigi_s0_lat:-0}"
printf "│ %-16s │ %17s us │ %17s us │\n" "Shard 1 Latency" "${mako_s1_lat:-0}" "${luigi_s1_lat:-0}"
echo "└──────────────────┴─────────────────────┴─────────────────────┘"

echo ""
echo "Summary (Network Delay: ${delay_ms}ms ± ${jitter_ms}ms):"
echo "  Mako:  $mako_total txns/sec"
echo "  Luigi: $luigi_total txns/sec (OWD=${owd_ms}ms)"

# Calculate ratio
if [ "$mako_total" != "0" ] && [ -n "$mako_total" ]; then
    ratio=$(echo "scale=2; $luigi_total / $mako_total" | bc 2>/dev/null || echo "N/A")
    echo "  Ratio: ${ratio}x (Luigi/Mako throughput)"
fi

echo ""
echo "Log files:"
echo "  Mako:  mako_geo_shard0.log, mako_geo_shard1.log"
echo "  Luigi: luigi_geo_shard0.log, luigi_geo_shard1.log"
echo ""
echo "Comparison complete!"

exit 0

