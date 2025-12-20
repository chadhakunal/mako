#!/bin/bash
# Test Mako with tc network delay (Non-Replicated)
# Matches compare_mako_luigi_simple.sh but with increased startup delays for tc latency

set -e

echo "═══════════════════════════════════════════════════════════"
echo "Mako TC Delay Test (Non-Replicated)"
echo "═══════════════════════════════════════════════════════════"

trd=${1:-4}
duration=${2:-30}
delay_ms=${3:-50}
jitter_ms=${4:-5}

# With tc delay, we need longer sleeps for connection establishment
startup_delay=$((delay_ms * 2 / 100 + 5))  # e.g., 50ms -> ~6s startup delay

echo "Configuration:"
echo "  Threads: $trd"
echo "  Duration: ${duration}s"
echo "  Delay: ${delay_ms}ms ± ${jitter_ms}ms"
echo "  Startup delay: ${startup_delay}s (increased for tc latency)"
echo ""

path=$(pwd)/src/mako
export LD_LIBRARY_PATH="$(pwd)/build:${LD_LIBRARY_PATH}"

# Cleanup function - called on exit
cleanup() {
    echo ""
    echo "Cleaning up..."
    pkill -9 -f dbtest 2>/dev/null || true
    sudo tc qdisc del dev lo root 2>/dev/null || true
    # Also clean sync files in trap to prevent next run issues
    rm -f nfs_sync_* 2>/dev/null || true
    sudo rm -rf /tmp/mako_sync/ 2>/dev/null || true
    echo "Done."
}
trap cleanup EXIT

# Pre-run cleanup - must be thorough to prevent second-run crashes
echo "Cleaning up from previous runs..."
pkill -9 -f dbtest 2>/dev/null || true
rm -f nfs_sync_*
sudo rm -rf /tmp/mako_sync/ 2>/dev/null || true

# Clean ALL rocksdb shard directories (any user)
sudo rm -rf /tmp/*_mako_rocksdb_shard* 2>/dev/null || true

# Additional cleanup for hugepages and shared memory
sudo rm -rf /dev/hugepages/rtebuf* 2>/dev/null || true
ipcrm -a 2>/dev/null || true

# Verify sync files are gone
if ls nfs_sync_* 1>/dev/null 2>&1 || [ -d "/tmp/mako_sync" ]; then
    echo "WARNING: Leftover sync files detected, removing again..."
    rm -f nfs_sync_* 2>/dev/null || true
    sudo rm -rf /tmp/mako_sync/ 2>/dev/null || true
    sleep 1
fi

sleep 3  # Longer sleep to ensure cleanup completes

# Add network delay
echo "Adding network delay..."
sudo tc qdisc del dev lo root 2>/dev/null || true
sudo tc qdisc add dev lo root netem delay ${delay_ms}ms ${jitter_ms}ms
echo "✓ Added ${delay_ms}ms ± ${jitter_ms}ms delay to loopback"
echo ""

# Start Mako shard 0
echo "Starting Mako shard 0..."
./build/dbtest \
    --num-threads "$trd" \
    --shard-index 0 \
    --shard-config "$path/config/local-shards2-warehouses${trd}.yml" \
    -P localhost \
    > mako_tc_shard0.log 2>&1 &
S0_PID=$!

# Wait longer for shard 0 to fully initialize with tc delay
echo "  Waiting ${startup_delay}s for shard 0 to initialize (tc latency)..."
sleep $startup_delay

echo "Starting Mako shard 1..."
./build/dbtest \
    --num-threads "$trd" \
    --shard-index 1 \
    --shard-config "$path/config/local-shards2-warehouses${trd}.yml" \
    -P localhost \
    > mako_tc_shard1.log 2>&1 &
S1_PID=$!

echo "  Shard 0 PID: $S0_PID"
echo "  Shard 1 PID: $S1_PID"
echo ""

# Wait for benchmark
wait_time=$((duration + 30))
echo "Running for ~${wait_time}s (duration + warmup)..."
sleep $wait_time

# Check if processes are still alive
if kill -0 $S0_PID 2>/dev/null; then
    echo "Shard 0 still running, stopping..."
    kill $S0_PID 2>/dev/null || true
fi
if kill -0 $S1_PID 2>/dev/null; then
    echo "Shard 1 still running, stopping..."
    kill $S1_PID 2>/dev/null || true
fi

wait $S0_PID $S1_PID 2>/dev/null || true

# Extract results
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "Results"
echo "═══════════════════════════════════════════════════════════"

s0_tps=$(grep "agg_persist_throughput:" mako_tc_shard0.log 2>/dev/null | tail -1 | awk '{print $2}')
s1_tps=$(grep "agg_persist_throughput:" mako_tc_shard1.log 2>/dev/null | tail -1 | awk '{print $2}')

echo "Shard 0: ${s0_tps:-N/A} TPS"
echo "Shard 1: ${s1_tps:-N/A} TPS"

if [ -n "$s0_tps" ] && [ -n "$s1_tps" ]; then
    total=$(echo "$s0_tps + $s1_tps" | bc 2>/dev/null || echo "N/A")
    echo "Total: $total TPS"
else
    echo ""
    echo "⚠️  No metrics - checking for crashes..."
    grep -iE "segment|abort|error" mako_tc_shard0.log 2>/dev/null | tail -5 || true
    grep -iE "segment|abort|error" mako_tc_shard1.log 2>/dev/null | tail -5 || true
fi

echo ""
echo "Log files: mako_tc_shard0.log, mako_tc_shard1.log"

exit 0
