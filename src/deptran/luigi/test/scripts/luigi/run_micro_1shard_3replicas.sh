#!/bin/bash
# Run single-shard microbenchmark with 3 replicas (Paxos replication)

set -e

cd /root/cse532/mako
export LD_LIBRARY_PATH=/root/cse532/mako/build:$LD_LIBRARY_PATH

# Kill any existing processes
pkill -9 luigi_server 2>/dev/null || true
pkill -9 luigi_coordinator 2>/dev/null || true
sleep 1

CONFIG="src/deptran/luigi/test/configs/1shard-3replicas.yml"
DURATION="${1:-10}"
THREADS="${2:-1}"

echo "=== Starting Single-Shard Microbenchmark with 3 Replicas ==="
echo "Config: $CONFIG"
echo "Duration: ${DURATION}s"
echo "Threads: ${THREADS}"
echo ""

# Start all replicas quickly to ensure all are listening before any try to connect
echo "Starting replica 0 (s101:31850) - leader..."
./build/luigi_server -f "$CONFIG" -P s101 > s101_micro.log 2>&1 &
S0_PID=$!

echo "Starting replica 1 (s102:31851) - follower..."
./build/luigi_server -f "$CONFIG" -P s102 > s102_micro.log 2>&1 &
S1_PID=$!

echo "Starting replica 2 (s103:31852) - follower..."
./build/luigi_server -f "$CONFIG" -P s103 > s103_micro.log 2>&1 &
S2_PID=$!

# Wait for all servers to start listening before they try to connect to each other
# Each server waits 500ms before connecting, so we need to wait longer
echo "Waiting for all servers to start listening..."
sleep 5

# Run coordinator with micro benchmark
echo "Running microbenchmark..."
./build/luigi_coordinator -f "$CONFIG" -b micro -d "$DURATION" -t "$THREADS" 2>&1 | tee coord_micro.log

# Cleanup
echo ""
echo "Cleaning up..."
kill $S0_PID $S1_PID $S2_PID 2>/dev/null || true
pkill -9 luigi_server 2>/dev/null || true

echo "Done!"
echo ""
echo "Log files:"
echo "  - s101_micro.log (replica 0 - leader)"
echo "  - s102_micro.log (replica 1 - follower)"
echo "  - s103_micro.log (replica 2 - follower)"
echo "  - coord_micro.log (coordinator)"
