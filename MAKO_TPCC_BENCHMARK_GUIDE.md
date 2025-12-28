# Mako TPC-C Benchmark Configuration Analysis

## Overview

Mako uses a modular approach to run TPC-C benchmarks with different configurations. The system is controlled through:
1. **bash/shard.sh** - Main script that launches dbtest with appropriate parameters
2. **Config files** - YAML files defining shard/replica topology
3. **Test scripts** - Wrapper scripts in examples/ directory

## Key Components

### 1. Main Execution Script: `bash/shard.sh`

**Usage**:
```bash
bash/shard.sh <nshard> <shard_index> <threads> <cluster> [is_micro] [is_replicated]
```

**Parameters**:
- `nshard`: Total number of shards (1 or 2)
- `shard_index`: Index of this shard (0, 1, ...)
- `threads`: Number of worker threads (1, 2, 4, 8, etc.)
- `cluster`: Process name (localhost, p1, p2, learner)
- `is_micro`: Optional, "1" for microbenchmark, omit for TPC-C
- `is_replicated`: Optional, "1" for replication, omit for no replication

**Command Built**:
```bash
./build/dbtest --num-threads $threads \
               --shard-index $shard_index \
               --shard-config src/mako/config/local-shards${nshard}-warehouses${threads}.yml \
               -P $cluster \
               [--is-micro] \
               [-F config/1leader_2followers/paxos${threads}_shardidx${shard_index}.yml] \
               [-F config/occ_paxos.yml] \
               [--is-replicated]
```

### 2. Configuration Files

**Location**: `src/mako/config/`

**Naming Pattern**: `local-shards{N}-warehouses{W}.yml`
- N = number of shards (1, 2, 10, etc.)
- W = number of warehouses = number of worker threads

**Structure** (example: local-shards2-warehouses4.yml):
```yaml
shards: 2        # number of shards
replicas: 3      # number of replicas (always 3 in config, but controlled by which processes start)
warehouses: 4    # number of warehouses per shard (= worker threads)

localhost:       # Leader replica
  - name: shard0
    index: 0
    ip: 127.0.0.1
    port: 31000
  - name: shard1
    index: 1
    ip: 127.0.0.1
    port: 31100

p1:             # Follower replica 1
  - name: shard0
    index: 0
    ip: 127.0.0.1
    port: 32000
  - name: shard1
    index: 1
    ip: 127.0.0.1
    port: 32100

p2:             # Follower replica 2
  - name: shard0
    index: 0
    ip: 127.0.0.1
    port: 33000
  - name: shard1
    index: 1
    ip: 127.0.0.1
    port: 33100

learner:        # Learner replica (optional)
  - name: shard0
    index: 0
    ip: 127.0.0.1
    port: 34000
  - name: shard1
    index: 1
    ip: 127.0.0.1
    port: 34100
```

**Key Insight**: The config file defines the topology for ALL replicas, but which replicas actually run is determined by which processes you start.

## Configuration Matrix

### 1-Shard 1-Replica (No Replication)
**Command**:
```bash
bash/shard.sh 1 0 <threads> localhost
```
- Starts only localhost process for shard 0
- No replication flags
- Config: `local-shards1-warehouses<threads>.yml`

### 1-Shard 3-Replica (With Replication)
**Commands** (start all 3):
```bash
bash/shard.sh 1 0 <threads> localhost 0 1  # Leader
bash/shard.sh 1 0 <threads> p1 0 1         # Follower 1
bash/shard.sh 1 0 <threads> p2 0 1         # Follower 2
```
- All processes for same shard (index 0)
- Different cluster names (localhost, p1, p2)
- `is_replicated=1` flag enables Paxos
- Config: `local-shards1-warehouses<threads>.yml`

### 2-Shard 1-Replica (No Replication)
**Commands** (start 2 shards):
```bash
bash/shard.sh 2 0 <threads> localhost  # Shard 0
bash/shard.sh 2 1 <threads> localhost  # Shard 1
```
- Two separate shards (index 0 and 1)
- Both use localhost (no replication)
- Config: `local-shards2-warehouses<threads>.yml`

### 2-Shard 3-Replica (With Replication)
**Commands** (start 6 processes total):
```bash
# Shard 0 replicas
bash/shard.sh 2 0 <threads> localhost 0 1  # Shard 0 Leader
bash/shard.sh 2 0 <threads> p1 0 1         # Shard 0 Follower 1
bash/shard.sh 2 0 <threads> p2 0 1         # Shard 0 Follower 2

# Shard 1 replicas
bash/shard.sh 2 1 <threads> localhost 0 1  # Shard 1 Leader
bash/shard.sh 2 1 <threads> p1 0 1         # Shard 1 Follower 1
bash/shard.sh 2 1 <threads> p2 0 1         # Shard 1 Follower 2
```
- 6 total processes (2 shards × 3 replicas)
- Config: `local-shards2-warehouses<threads>.yml`

## Worker Thread Counts

The `threads` parameter controls:
1. Number of TPC-C worker threads (`--num-threads`)
2. Number of warehouses in the config file (must match)

**Common values**: 1, 2, 4, 6, 8

**Config file requirement**: Must have matching config file
- For 1 thread: `local-shards{N}-warehouses1.yml`
- For 2 threads: `local-shards{N}-warehouses2.yml`
- For 4 threads: `local-shards{N}-warehouses4.yml`
- For 8 threads: `local-shards{N}-warehouses8.yml`

## Example Test Scripts

### 1-Shard No Replication
```bash
# From examples/test_1shard_no_replication.sh
bash bash/shard.sh 1 0 6 localhost
```

### 2-Shard No Replication
```bash
# From examples/test_2shard_no_replication.sh
bash bash/shard.sh 2 0 6 localhost &  # Shard 0
bash bash/shard.sh 2 1 6 localhost &  # Shard 1
```

### 1-Shard With Replication
```bash
# From examples/test_1shard_replication_simple.sh
bash bash/shard.sh 1 0 6 localhost 0 1 &  # Leader
bash bash/shard.sh 1 0 6 p1 0 1 &         # Follower 1
bash bash/shard.sh 1 0 6 p2 0 1 &         # Follower 2
```

## Port Allocation

Ports are allocated based on cluster name and shard index:
- **localhost**: 31000 + (shard_index × 100)
- **p1**: 32000 + (shard_index × 100)
- **p2**: 33000 + (shard_index × 100)
- **learner**: 34000 + (shard_index × 100)

Example for 2 shards:
- Shard 0: localhost=31000, p1=32000, p2=33000, learner=34000
- Shard 1: localhost=31100, p1=32100, p2=33100, learner=34100

## Summary

To run Mako TPC-C benchmarks:

1. **Choose configuration**: 1/2 shards, 1/3 replicas
2. **Choose thread count**: 1, 2, 4, 8
3. **Ensure config file exists**: `local-shards{N}-warehouses{threads}.yml`
4. **Start appropriate processes**:
   - 1 replica: Start only localhost
   - 3 replicas: Start localhost, p1, p2 with `is_replicated=1`
5. **Run for each shard**: Repeat with different shard_index values

The system automatically handles:
- Port allocation
- Paxos configuration (when replicated)
- Warehouse distribution
- Cross-shard coordination
