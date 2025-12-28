#!/bin/bash
# Run TPCC single-shard benchmark with 1 replica (no replication)

set -e

cd /root/cse532/mako
export LD_LIBRARY_PATH=/root/cse532/mako/build:$LD_LIBRARY_PATH

# Kill any existing processes
pkill -9 luigi_server 2>/dev/null || true
pkill -9 luigi_coordinator 2>/dev/null || true
sleep 1

CONFIG="src/deptran/luigi/test/configs/1shard-1replica.yml"
DURATION="${1:-10}"
THREADS="${2:-1}"
NUM_SHARDS=1
NUM_WAREHOUSES=$((THREADS * NUM_SHARDS))  # Scale warehouses with threads (like Mako)

echo "=== Starting Single-Shard TPCC with 1 Replica (No Replication) ==="
echo "Config: $CONFIG"
echo "Duration: ${DURATION}s"
echo "Threads: ${THREADS}"
echo "Warehouses: ${NUM_WAREHOUSES}"
echo ""

# Start single replica (s101)
echo "Starting replica (s101:31850)..."
./build/luigi_server -f "$CONFIG" -P s101 -b tpcc -w "$NUM_WAREHOUSES" > s101_tpcc.log 2>&1 &
S0_PID=$!
sleep 3

# Run coordinator with TPCC benchmark
echo "Running TPCC benchmark..."
./build/luigi_coordinator -f "$CONFIG" -b tpcc -d "$DURATION" -t "$THREADS" 2>&1 | tee coord_tpcc.log

# Cleanup
echo ""
echo "Cleaning up..."
kill $S0_PID 2>/dev/null || true
pkill -9 luigi_server 2>/dev/null || true

echo "Done!"
echo ""
echo "Log files:"
echo "  - s101_tpcc.log (single replica)"
echo "  - coord_tpcc.log (coordinator)"
