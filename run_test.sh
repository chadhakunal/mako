#!/bin/bash
cd /root/cse532/mako

THREADS=$1
OWD=$2
HEADROOM=$3

# Kill any existing processes
pkill -9 luigi_server 2>/dev/null || true
pkill -9 luigi_coordinator 2>/dev/null || true
sleep 1

# Start server
./build/luigi_server -f src/deptran/luigi/test/configs/1shard-1replica.yml -P s101 > s101_micro.log 2>&1 &
SERVER_PID=$!
sleep 3

# Run coordinator
./build/luigi_coordinator -f src/deptran/luigi/test/configs/1shard-1replica.yml -b micro -d 10 -t $THREADS -w $OWD -x $HEADROOM 2>&1 | tee src/deptran/luigi/test/results/1shard_1replica_micro_t${THREADS}_owd${OWD}.txt

# Cleanup
kill $SERVER_PID 2>/dev/null || true
pkill -9 luigi_server 2>/dev/null || true
sleep 1
