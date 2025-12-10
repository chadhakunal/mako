#!/bin/bash

# Add network latency (100ms ± 10ms jitter = ~200ms RTT)
# sudo tc qdisc add dev lo root netem delay 100ms 10ms
# echo "Network delay added: 100ms ± 10ms"

# Clean up
pkill -9 dbtest 2>/dev/null; rm -f nfs_sync_*; sleep 2

# Shard 0 - all 4 roles
./build/dbtest -t 4 -g 0 -q src/mako/config/local-shards2-warehouses4.yml -P localhost -F config/1leader_2followers/paxos4_shardidx0.yml -F config/occ_paxos.yml --is-replicated > s0-leader.log 2>&1 &
./build/dbtest -t 4 -g 0 -q src/mako/config/local-shards2-warehouses4.yml -P p2 -F config/1leader_2followers/paxos4_shardidx0.yml -F config/occ_paxos.yml --is-replicated > s0-p2.log 2>&1 &
./build/dbtest -t 4 -g 0 -q src/mako/config/local-shards2-warehouses4.yml -P learner -F config/1leader_2followers/paxos4_shardidx0.yml -F config/occ_paxos.yml --is-replicated > s0-learner.log 2>&1 &
sleep 1
./build/dbtest -t 4 -g 0 -q src/mako/config/local-shards2-warehouses4.yml -P p1 -F config/1leader_2followers/paxos4_shardidx0.yml -F config/occ_paxos.yml --is-replicated > s0-p1.log 2>&1 &

sleep 3

# Shard 1 - all 4 roles
./build/dbtest -t 4 -g 1 -q src/mako/config/local-shards2-warehouses4.yml -P localhost -F config/1leader_2followers/paxos4_shardidx1.yml -F config/occ_paxos.yml --is-replicated > s1-leader.log 2>&1 &
./build/dbtest -t 4 -g 1 -q src/mako/config/local-shards2-warehouses4.yml -P p2 -F config/1leader_2followers/paxos4_shardidx1.yml -F config/occ_paxos.yml --is-replicated > s1-p2.log 2>&1 &
./build/dbtest -t 4 -g 1 -q src/mako/config/local-shards2-warehouses4.yml -P learner -F config/1leader_2followers/paxos4_shardidx1.yml -F config/occ_paxos.yml --is-replicated > s1-learner.log 2>&1 &
sleep 1
./build/dbtest -t 4 -g 1 -q src/mako/config/local-shards2-warehouses4.yml -P p1 -F config/1leader_2followers/paxos4_shardidx1.yml -F config/occ_paxos.yml --is-replicated > s1-p1.log 2>&1 &

echo "8 processes started"

# Wait for all background processes to complete
wait

# Remove network latency
# sudo tc qdisc del dev lo root
echo "Network delay removed"
