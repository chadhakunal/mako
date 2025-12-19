#!/bin/bash
# Test that Luigi servers run independently without transactions
# Usage: ./test_luigi_no_txn.sh

set -e

echo "========================================="
echo "Luigi NO Transaction Test (Idle Servers)"
echo "========================================="

script_name="luigi_no_txn"
log_prefix="${script_name}"

path=$(pwd)/src/mako

echo "Configuration:"
echo "  Shards:        2"
echo "  Test:          Servers idle (no transactions sent)"
echo "  Duration:      5 seconds"
echo ""

# Clean up
rm -f nfs_sync_*
USERNAME=${USER:-$(whoami)}
rm -rf /tmp/${USERNAME}_mako_rocksdb_shard*
pkill -9 -f luigi_bench 2>/dev/null || true
sleep 1

# Log files
shard0_log="${log_prefix}_shard0.log"
shard1_log="${log_prefix}_shard1.log"
rm -f "$shard0_log" "$shard1_log"

# Start shard 0 (but don't run benchmark - just initialize)
echo "Starting Luigi shard 0 (idle mode)..."
timeout 5s ./build/luigi_bench \
    --shard-config "$path/config/local-shards2-warehouses6.yml" \
    --shard-index 0 \
    -P localhost \
    --duration 5 \
    --num-threads 0 \
    2>&1 > "$shard0_log" &
SHARD0_PID=$!
sleep 1

# Start shard 1 (idle mode)
echo "Starting Luigi shard 1 (idle mode)..."
timeout 5s ./build/luigi_bench \
    --shard-config "$path/config/local-shards2-warehouses6.yml" \
    --shard-index 1 \
    -P localhost \
    --duration 5 \
    --num-threads 0 \
    2>&1 > "$shard1_log" &
SHARD1_PID=$!

echo "Servers running (no transactions)..."
echo "  Shard 0 PID: $SHARD0_PID"
echo "  Shard 1 PID: $SHARD1_PID"
echo "Waiting 5 seconds..."

sleep 6

# Check if still running
if kill -0 $SHARD0_PID 2>/dev/null; then
    echo "✓ Shard 0 still running independently"
    kill -9 $SHARD0_PID 2>/dev/null || true
else
    echo "✗ Shard 0 exited"
fi

if kill -0 $SHARD1_PID 2>/dev/null; then
    echo "✓ Shard 1 still running independently"
    kill -9 $SHARD1_PID 2>/dev/null || true
else
    echo "✗ Shard 1 exited"
fi

pkill -9 -f luigi_bench 2>/dev/null || true

echo ""
echo "Logs saved to:"
echo "  - $shard0_log"
echo "  - $shard1_log"
