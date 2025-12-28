#!/bin/bash
# Run 2-shard microbenchmark with 1 replica per shard (no replication)

set -e

cd /root/cse532/mako
export LD_LIBRARY_PATH=/root/cse532/mako/build:$LD_LIBRARY_PATH

# Kill any existing processes
pkill -9 luigi_server 2>/dev/null || true
pkill -9 luigi_coordinator 2>/dev/null || true
sleep 1

CONFIG="src/deptran/luigi/test/configs/2shard-1replica.yml"
DURATION="${1:-10}"
THREADS="${2:-1}"

echo "=== Starting 2-Shard Microbenchmark with 1 Replica per Shard (No Replication) ==="
echo "Config: $CONFIG"
echo "Duration: ${DURATION}s"
echo "Threads: ${THREADS}"
echo ""

# Start Shard 0
echo "Starting Shard 0 (s101:31850)..."
./build/luigi_server -f "$CONFIG" -P s101 > s101_micro.log 2>&1 &
S101_PID=$!
sleep 2

# Start Shard 1
echo "Starting Shard 1 (s201:31853)..."
./build/luigi_server -f "$CONFIG" -P s201 > s201_micro.log 2>&1 &
S201_PID=$!
sleep 3

# Run coordinator with micro benchmark
echo "Running microbenchmark..."
./build/luigi_coordinator -f "$CONFIG" -b micro -d "$DURATION" -t "$THREADS" 2>&1 | tee coord_micro.log

# Cleanup
echo ""
echo "Cleaning up..."
kill $S101_PID $S201_PID 2>/dev/null || true
pkill -9 luigi_server 2>/dev/null || true

echo "Done!"
echo ""
echo "Log files:"
echo "  Shard 0:"
echo "    - s101_micro.log (single replica)"
echo "  Shard 1:"
echo "    - s201_micro.log (single replica)"
echo "  Coordinator:"
echo "    - coord_micro.log"
