#!/bin/bash

# Luigi Test Script - 2 shards with 4 threads each, with replication
# Tests the Luigi timestamp-ordered execution protocol

set -e

cd /root/mako

# Clean up
echo "Cleaning up old processes and files..."
pkill -9 dbtest 2>/dev/null || true
rm -f nfs_sync_*
rm -f s0-*.log s1-*.log
sleep 2

echo "Starting Luigi test with 2 shards, 4 threads each..."

# Shard 0 - all 4 roles (leader, p1, p2, learner)
echo "Starting Shard 0 processes..."
./build/dbtest -t 4 -g 0 -q src/mako/config/local-shards2-warehouses4.yml -P localhost -F config/1leader_2followers/paxos4_shardidx0.yml -F config/occ_paxos.yml --is-replicated --use-luigi > s0-leader.log 2>&1 &
./build/dbtest -t 4 -g 0 -q src/mako/config/local-shards2-warehouses4.yml -P p2 -F config/1leader_2followers/paxos4_shardidx0.yml -F config/occ_paxos.yml --is-replicated --use-luigi > s0-p2.log 2>&1 &
./build/dbtest -t 4 -g 0 -q src/mako/config/local-shards2-warehouses4.yml -P learner -F config/1leader_2followers/paxos4_shardidx0.yml -F config/occ_paxos.yml --is-replicated --use-luigi > s0-learner.log 2>&1 &
sleep 1
./build/dbtest -t 4 -g 0 -q src/mako/config/local-shards2-warehouses4.yml -P p1 -F config/1leader_2followers/paxos4_shardidx0.yml -F config/occ_paxos.yml --is-replicated --use-luigi > s0-p1.log 2>&1 &

sleep 3

# Shard 1 - all 4 roles (leader, p1, p2, learner)
echo "Starting Shard 1 processes..."
./build/dbtest -t 4 -g 1 -q src/mako/config/local-shards2-warehouses4.yml -P localhost -F config/1leader_2followers/paxos4_shardidx1.yml -F config/occ_paxos.yml --is-replicated --use-luigi > s1-leader.log 2>&1 &
./build/dbtest -t 4 -g 1 -q src/mako/config/local-shards2-warehouses4.yml -P p2 -F config/1leader_2followers/paxos4_shardidx1.yml -F config/occ_paxos.yml --is-replicated --use-luigi > s1-p2.log 2>&1 &
./build/dbtest -t 4 -g 1 -q src/mako/config/local-shards2-warehouses4.yml -P learner -F config/1leader_2followers/paxos4_shardidx1.yml -F config/occ_paxos.yml --is-replicated --use-luigi > s1-learner.log 2>&1 &
sleep 1
./build/dbtest -t 4 -g 1 -q src/mako/config/local-shards2-warehouses4.yml -P p1 -F config/1leader_2followers/paxos4_shardidx1.yml -F config/occ_paxos.yml --is-replicated --use-luigi > s1-p1.log 2>&1 &

echo "8 processes started. Waiting for 30 seconds..."
sleep 30

echo ""
echo "========================================="
echo "Checking logs for Luigi initialization..."
echo "========================================="

# Check if Luigi was enabled
for log in s0-leader.log s1-leader.log; do
    if [ -f "$log" ]; then
        echo ""
        echo "=== $log ==="
        if grep -q "Luigi" "$log"; then
            echo "✓ Luigi mentioned in log"
            grep -i "Luigi" "$log" | head -5
        else
            echo "✗ No Luigi mentions found"
        fi
        # Show any errors
        if grep -qi "error\|fatal\|abort" "$log"; then
            echo "⚠ Errors found:"
            grep -i "error\|fatal\|abort" "$log" | head -5
        fi
    else
        echo "✗ $log not found"
    fi
done

echo ""
echo "========================================="
echo "Stopping all processes..."
echo "========================================="
pkill -9 dbtest 2>/dev/null || true

echo "Done. Check s0-*.log and s1-*.log for details."
