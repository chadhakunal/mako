#!/bin/bash
# Run single-shard microbenchmark with 1 replica (no replication)

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

echo "=== Starting Single-Shard Microbenchmark with 1 Replica (No Replication) ==="
echo "Config: $CONFIG"
echo "Duration: ${DURATION}s"
echo "Threads: ${THREADS}"
echo ""

# Start single replica (s101)
echo "Starting replica (s101:31850)..."
./build/luigi_server -f "$CONFIG" -P s101 > s101_micro.log 2>&1 &
S0_PID=$!
sleep 3

# Run coordinator with micro benchmark
echo "Running microbenchmark..."
./build/luigi_coordinator -f "$CONFIG" -b micro -d "$DURATION" -t "$THREADS" 2>&1 | tee coord_micro.log

# Cleanup
echo ""
echo "Cleaning up..."
kill $S0_PID 2>/dev/null || true
pkill -9 luigi_server 2>/dev/null || true

echo "Done!"
echo ""
echo "Log files:"
echo "  - s101_micro.log (single replica)"
echo "  - coord_micro.log (coordinator)"
