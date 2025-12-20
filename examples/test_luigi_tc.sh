#!/bin/bash
# Test Luigi with tc network delay (Non-Replicated)
# Based on compare_mako_luigi_simple.sh Luigi section

set -e

echo "═══════════════════════════════════════════════════════════"
echo "Luigi TC Delay Test (Non-Replicated)"
echo "═══════════════════════════════════════════════════════════"

trd=${1:-4}
duration=${2:-10}
delay_ms=${3:-50}
jitter_ms=${4:-5}

# Calculate OWD for Luigi (delay + jitter/2 + headroom)
owd_ms=$((delay_ms + jitter_ms/2 + 2))

# With tc delay, we need longer sleeps for connection establishment
startup_delay=$((delay_ms * 2 / 100 + 5))

echo "Configuration:"
echo "  Threads: $trd"
echo "  Duration: ${duration}s"
echo "  Delay: ${delay_ms}ms ± ${jitter_ms}ms"
echo "  Luigi OWD: ${owd_ms}ms"
echo "  Startup delay: ${startup_delay}s"
echo ""

path=$(pwd)/src/mako
export LD_LIBRARY_PATH="$(pwd)/build:${LD_LIBRARY_PATH}"

# Cleanup function - called on exit
cleanup() {
    echo ""
    echo "Cleaning up..."
    pkill -9 -f luigi_bench 2>/dev/null || true
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
pkill -9 -f luigi_bench 2>/dev/null || true
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

# Start Luigi shard 0
echo "Starting Luigi shard 0..."
./build/luigi_bench \
    --shard-config "$path/config/local-shards2-warehouses${trd}.yml" \
    --shard-index 0 \
    -P localhost \
    --num-threads "$trd" \
    --benchmark tpcc \
    --duration "$duration" \
    --owd-ms "$owd_ms" \
    > luigi_tc_shard0.log 2>&1 &
S0_PID=$!

# Wait for shard 0 to initialize
echo "  Waiting ${startup_delay}s for shard 0 to initialize..."
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
    > luigi_tc_shard1.log 2>&1 &
S1_PID=$!

echo "  Shard 0 PID: $S0_PID"
echo "  Shard 1 PID: $S1_PID"
echo ""

# Wait for benchmark
wait_time=$((duration + 30))
echo "Running for ~${wait_time}s (duration + loading)..."

start_wait=$(date +%s)
while true; do
    if ! kill -0 $S0_PID 2>/dev/null && ! kill -0 $S1_PID 2>/dev/null; then
        echo "Processes completed"
        break
    fi
    
    now=$(date +%s)
    elapsed=$((now - start_wait))
    if [ $elapsed -ge $wait_time ]; then
        echo "Timeout reached, stopping..."
        kill $S0_PID $S1_PID 2>/dev/null || true
        break
    fi
    
    if [ $((elapsed % 10)) -eq 0 ]; then
        echo "  Waiting... ${elapsed}s/${wait_time}s"
    fi
    sleep 1
done

wait $S0_PID $S1_PID 2>/dev/null || true

# Extract results
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "Results"
echo "═══════════════════════════════════════════════════════════"

s0_tps=$(grep "Throughput:" luigi_tc_shard0.log 2>/dev/null | tail -1 | awk '{print $2}')
s1_tps=$(grep "Throughput:" luigi_tc_shard1.log 2>/dev/null | tail -1 | awk '{print $2}')
s0_lat=$(grep "Avg Latency:" luigi_tc_shard0.log 2>/dev/null | tail -1 | awk '{print $3}')
s1_lat=$(grep "Avg Latency:" luigi_tc_shard1.log 2>/dev/null | tail -1 | awk '{print $3}')

echo "Shard 0: ${s0_tps:-N/A} TPS, ${s0_lat:-N/A} us latency"
echo "Shard 1: ${s1_tps:-N/A} TPS, ${s1_lat:-N/A} us latency"

if [ -n "$s0_tps" ] && [ -n "$s1_tps" ]; then
    total=$(echo "$s0_tps + $s1_tps" | bc 2>/dev/null || echo "N/A")
    echo "Total: $total TPS"
else
    echo ""
    echo "⚠️  No metrics - checking for crashes..."
    tail -10 luigi_tc_shard0.log 2>/dev/null || true
    tail -10 luigi_tc_shard1.log 2>/dev/null || true
fi

echo ""
echo "Log files: luigi_tc_shard0.log, luigi_tc_shard1.log"

exit 0
