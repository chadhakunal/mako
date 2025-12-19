#!/bin/bash
# Test ONE cross-shard transaction with Luigi
# Usage: ./test_luigi_one_txn.sh

set -e

echo "========================================="
echo "Luigi ONE Transaction Test"
echo "========================================="

script_name="luigi_one_txn"
log_prefix="${script_name}"

path=$(pwd)/src/mako

echo "Configuration:"
echo "  Shards:        2"
echo "  Warehouses:    6 (3 per shard)"
echo "  Benchmark:     tpcc"
echo "  Test Mode:     --test-one (sends 1 cross-shard txn)"
echo ""

# Clean up
rm -f nfs_sync_*
rm -rf /tmp/mako_sync/  # Clean NFS barrier sync files
USERNAME=${USER:-$(whoami)}
rm -rf /tmp/${USERNAME}_mako_rocksdb_shard*
pkill -9 -f luigi_bench 2>/dev/null || true
sleep 1

# Log files
shard0_log="${log_prefix}_shard0.log"
shard1_log="${log_prefix}_shard1.log"
rm -f "$shard0_log" "$shard1_log"

# Start shard 1 FIRST (both shards will send 1 transaction each)
# NFS barrier ensures both are ready before transactions start
echo "Starting Luigi shard 1..."
./build/luigi_bench \
    --shard-config "$path/config/local-shards2-warehouses6.yml" \
    --shard-index 1 \
    -P localhost \
    --test-one \
    2>&1 | tee "$shard1_log" &
SHARD1_PID=$!
echo "Waiting for shard 1 to initialize and start listening..."
sleep 5

# Start shard 0 (the benchmark client will run here)
echo "Starting Luigi shard 0..."
./build/luigi_bench \
    --shard-config "$path/config/local-shards2-warehouses6.yml" \
    --shard-index 0 \
    -P localhost \
    --test-one \
    2>&1 | tee "$shard0_log" &
SHARD0_PID=$!

echo "Running test..."
echo "  Shard 0 PID: $SHARD0_PID"
echo "  Shard 1 PID: $SHARD1_PID"

# Wait for both to finish (should be quick - just 1 transaction + 5 sec wait)
sleep 10

# Check if they're still running
kill -0 $SHARD0_PID 2>/dev/null && echo "Shard 0 still running" || echo "Shard 0 exited"
kill -0 $SHARD1_PID 2>/dev/null && echo "Shard 1 still running" || echo "Shard 1 exited"

# Force kill if still running
pkill -9 -f luigi_bench 2>/dev/null || true

echo ""
echo "========================================="
echo "Checking Logs"
echo "========================================="
echo ""
echo "=== Shard 0 Log (last 50 lines) ==="
tail -50 "$shard0_log"
echo ""
echo "=== Shard 1 Log (last 50 lines) ==="
tail -50 "$shard1_log"
echo ""
echo "Test completed. Check logs:"
echo "  - $shard0_log"
echo "  - $shard1_log"
