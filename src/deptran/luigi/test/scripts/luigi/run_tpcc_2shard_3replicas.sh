#!/bin/bash
# Run TPCC 2-shard benchmark with 3 replicas per shard (Paxos replication)

set -e

cd /root/cse532/mako
export LD_LIBRARY_PATH=/root/cse532/mako/build:$LD_LIBRARY_PATH

# Kill any existing processes
pkill -9 luigi_server 2>/dev/null || true
pkill -9 luigi_coordinator 2>/dev/null || true
sleep 1

CONFIG="src/deptran/luigi/test/configs/2shard-3replicas.yml"
DURATION="${1:-10}"
THREADS="${2:-1}"
NUM_SHARDS=2
NUM_WAREHOUSES=$((THREADS * NUM_SHARDS))  # Scale warehouses with threads (like Mako)

echo "=== Starting 2-Shard TPCC with 3 Replicas per Shard ==="
echo "Config: $CONFIG"
echo "Duration: ${DURATION}s"
echo "Threads: ${THREADS}"
echo "Warehouses: ${NUM_WAREHOUSES} (${THREADS} per shard)"
echo ""

# Start Shard 0 replicas
echo "Starting Shard 0 replicas..."
echo "  Starting replica 0 (s101:31850) - leader..."
./build/luigi_server -f "$CONFIG" -P s101 -b tpcc -w "$NUM_WAREHOUSES" > s101_tpcc.log 2>&1 &
S101_PID=$!
sleep 2

echo "  Starting replica 1 (s102:31851) - follower..."
./build/luigi_server -f "$CONFIG" -P s102 -b tpcc -w "$NUM_WAREHOUSES" > s102_tpcc.log 2>&1 &
S102_PID=$!
sleep 2

echo "  Starting replica 2 (s103:31852) - follower..."
./build/luigi_server -f "$CONFIG" -P s103 -b tpcc -w "$NUM_WAREHOUSES" > s103_tpcc.log 2>&1 &
S103_PID=$!
sleep 2

# Start Shard 1 replicas
echo "Starting Shard 1 replicas..."
echo "  Starting replica 0 (s201:31853) - leader..."
./build/luigi_server -f "$CONFIG" -P s201 -b tpcc -w "$NUM_WAREHOUSES" > s201_tpcc.log 2>&1 &
S201_PID=$!
sleep 2

echo "  Starting replica 1 (s202:31854) - follower..."
./build/luigi_server -f "$CONFIG" -P s202 -b tpcc -w "$NUM_WAREHOUSES" > s202_tpcc.log 2>&1 &
S202_PID=$!
sleep 2

echo "  Starting replica 2 (s203:31855) - follower..."
./build/luigi_server -f "$CONFIG" -P s203 -b tpcc -w "$NUM_WAREHOUSES" > s203_tpcc.log 2>&1 &
S203_PID=$!
sleep 3

# Run coordinator with TPCC benchmark
echo "Running TPCC benchmark..."
./build/luigi_coordinator -f "$CONFIG" -b tpcc -d "$DURATION" -t "$THREADS" 2>&1 | tee coord_tpcc.log

# Cleanup
echo ""
echo "Cleaning up..."
kill $S101_PID $S102_PID $S103_PID $S201_PID $S202_PID $S203_PID 2>/dev/null || true
pkill -9 luigi_server 2>/dev/null || true

echo "Done!"
echo ""
echo "Log files:"
echo "  Shard 0:"
echo "    - s101_tpcc.log (replica 0 - leader)"
echo "    - s102_tpcc.log (replica 1 - follower)"
echo "    - s103_tpcc.log (replica 2 - follower)"
echo "  Shard 1:"
echo "    - s201_tpcc.log (replica 0 - leader)"
echo "    - s202_tpcc.log (replica 1 - follower)"
echo "    - s203_tpcc.log (replica 2 - follower)"
echo "  Coordinator:"
echo "    - coord_tpcc.log"
