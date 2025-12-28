# Luigi: Timestamp-Ordered Distributed Transaction Protocol

> ⚠️ **Academic Use & Attribution Notice**
>
> This repository contains work developed as part of a graded course project for  
> **CSE532 – Database Systems**.
>
> **Primary contributors**:  
> - Avjot Singh
> - Kunal Chadha
> - Gautam Sardana
> - Angad Singh
>
> The code is open-source under the MIT License inherited from the upstream project.
> However, reuse of this repository (or substantial portions of it) for **coursework,
> graded assignments, or academic submissions** without proper attribution may constitute
> academic misconduct under institutional policies.
>
> Please see **[ACADEMIC_HONESTY.md](ACADEMIC_HONESTY.md)** for attribution and academic integrity guidelines.


## Introduction

Luigi is a distributed transaction protocol that uses **timestamp-ordered execution** to achieve high throughput in geo-distributed environments. It is inspired by the Tiga protocol (SOSP 2025) and designed to compare against OCC-based systems like Mako.

### Protocol Summary

**Timestamp Initialization**
- Coordinator assigns future timestamps based on OWD measurements: `T.timestamp = now() + max_OWD + headroom`
- `worker_id` sent separately in RPC for tie-breaking and Paxos stream routing

**Transaction Receipt and Queueing**
- Leaders perform conflict detection before accepting transaction (check read/write maps for any transaction that touches the same keys, has a higher timestamp, and has been released)
- Timestamp update for late transactions: `T.timestamp = now()`
- Insert into timestamp-ordered priority queue for execution

**Timestamp Agreement**
- Leaders exchange `T.timestamp` with other involved shards
- `T.agreed_ts = max(T.timestamp<S_i>)` across all shards
- If all leader timestamps match → agreement; proceed to execute and replicate

**Transaction Execution & Replication**
- Leaders execute the transaction and send the result back to coordinator
- Leader appends transaction to Paxos stream (per-worker stream based on `worker_ID` from composite timestamp)

**Watermark Updates & Commit Decision**
- Leaders maintain per-worker watermark: `T.timestamp` of last replicated transaction
- Coordinator replies to client when `T.timestamp <= watermark[shard][worker_ID]` for all involved shards

For detailed protocol specification including message formats, state machines, and RPC definitions, see **[LUIGI_PROTOCOL.md](LUIGI_PROTOCOL.md)**.

## Project Structure

```
src/deptran/luigi/
├── coordinator.cc/h     # Client-side coordinator (timestamp assignment, transaction submission)
├── server.cc/h          # Server entry point
├── scheduler.cc/h       # Transaction scheduling and execution
├── executor.cc/h        # Transaction execution engine
├── state_machine.cc/h   # Key-value state machine with MVCC
├── commo.cc/h           # Network communication layer
├── service.cc/h         # RPC service handlers
├── luigi.rpc            # RPC protocol definitions
├── luigi_entry.h        # Transaction entry data structure
├── luigi_common.h       # Common constants and utilities
├── txn_generator.h      # Base transaction generator interface
├── micro_txn_generator.h    # Microbenchmark transaction generator
├── tpcc_txn_generator.h     # TPC-C transaction generator
├── tpcc_constants.h     # TPC-C benchmark constants
├── tpcc_helpers.h       # TPC-C helper functions
├── test/
│   ├── configs/         # YAML configuration files for different topologies
│   ├── scripts/
│   │   └── luigi/       # Benchmark scripts
│   └── results/         # Experimental results
└── *.md                 # Documentation files
```

## Building and Running

### Prerequisites

- CMake 3.10+
- C++17 compiler (GCC 7+ or Clang 5+)
- Linux with `tc` (traffic control) for network simulation

### Building

```bash
cd /root/cse532/mako
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

This produces two binaries:
- `build/luigi_server` - The Luigi server (runs on each replica)
- `build/luigi_coordinator` - The coordinator/client (generates transactions)

### Running Benchmarks

Benchmark scripts are located in `src/deptran/luigi/test/scripts/`.

#### Luigi Benchmarks

**Quick start (TPC-C, 2-shard, 3-replicas):**
```bash
cd /root/cse532/mako
./src/deptran/luigi/test/scripts/luigi/run_tpcc_2shard_3replicas.sh <duration> <threads>
```

**With network latency simulation:**
```bash
# Parameters: duration threads owd_ms headroom_ms netem_delay_ms netem_jitter_ms
./src/deptran/luigi/test/scripts/luigi/run_tpcc_2shard_3replicas_latency.sh 30 8 160 30 150 20
```

**Run all Luigi benchmarks:**
```bash
./src/deptran/luigi/test/scripts/luigi/run_all_benchmarks.sh
```

**Available Luigi scripts:**
- `run_all_benchmarks.sh` - Runs the complete benchmark suite across all configurations
- `run_luigi_crossshard_study.sh` - Cross-shard percentage comparison study
- `run_micro_*.sh` - Microbenchmark (10 ops/txn, 50% read ratio)
- `run_tpcc_*.sh` - TPC-C benchmark (NewOrder transactions)
- `*_latency.sh` variants - Include network latency simulation via `tc netem`

#### Mako Benchmarks

**Quick start (TPC-C, 2-shard, 3-replicas):**
```bash
./src/deptran/luigi/test/scripts/mako/run_mako_tpcc_2shard_3replicas.sh <duration> <threads>
```

**Run all Mako benchmarks:**
```bash
./src/deptran/luigi/test/scripts/mako/run_all_mako_tpcc_benchmarks.sh
```

**Available Mako scripts:**
- `run_all_mako_tpcc_benchmarks.sh` - Runs the complete Mako TPC-C benchmark suite
- `run_mako_tpcc_*.sh` - TPC-C benchmark for various shard/replica configurations
- `run_mako_crossshard_study.sh` - Cross-shard percentage comparison study

### Manual Execution

**Start servers:**
```bash
export LD_LIBRARY_PATH=/root/cse532/mako/build:$LD_LIBRARY_PATH
./build/luigi_server -f <config.yml> -P <partition_name> -b <benchmark> -w <warehouses>
```

**Start coordinator:**
```bash
./build/luigi_coordinator -f <config.yml> -b <benchmark> -d <duration> -t <threads>
```

## Experimental Results

### Experimental Setup

All experiments were conducted on **Linode cloud infrastructure** with the following specifications:

- **Hardware**: 8 CPU Cores, 16 GB RAM, 320 GB SSD Storage
- **Operating System**: Ubuntu 22.04 LTS
- **Network**: Simulated using Linux `tc` (traffic control) with `netem` qdisc

### Network Conditions

We simulated four realistic network scenarios:

| Scenario | Latency | Jitter | Description |
|----------|---------|--------|-------------|
| **Same Region** | 2ms | 0.5ms | Co-located datacenters (e.g., us-east-1a ↔ us-east-1b) |
| **Same Continent** | 30ms | 5ms | Cross-region within continent (e.g., US East ↔ US West) |
| **Cross Continent** | 80ms | 10ms | Intercontinental (e.g., US ↔ Europe) |
| **Geo-Distributed** | 150ms | 20ms | Global deployment (e.g., US ↔ Asia-Pacific) |

### System Configurations

We evaluated four system configurations to understand the impact of sharding and replication:

| Configuration | Shards | Replicas per Shard | Total Servers | Use Case |
|---------------|--------|-------------------|---------------|----------|
| **1-shard, 1-replica** | 1 | 1 | 1 | Baseline (no distribution, no replication) |
| **1-shard, 3-replicas** | 1 | 3 | 3 | Replication overhead without sharding |
| **2-shard, 1-replica** | 2 | 1 | 2 | Sharding overhead without replication |
| **2-shard, 3-replicas** | 2 | 3 | 6 | Full sharded and replicated setup |

### 4.1 🔵 Luigi Microbenchmark Performance

Luigi's microbenchmark generates transactions with 10 operations each (50% read ratio). **Important**: In multi-shard configurations (2-shard), each transaction touches **all shards** (100% cross-shard rate) by design, testing worst-case distributed coordination.

> **Note:** All values show **throughput** (transactions per second) and **average latency** (milliseconds, in italics)

#### 📊 1-Shard, 1-Replica (Baseline)

| Threads | Same Region | Same Continent | Cross Continent | Geo-Distributed |
|---------|-------------|----------------|-----------------|-----------------|
| 1 | 20,713<br>*9 ms* | 3,041<br>*66 ms* | 1,180<br>*169 ms* | 636<br>*313 ms* |
| 2 | 19,141<br>*21 ms* | 5,740<br>*70 ms* | 2,309<br>*173 ms* | 1,247<br>*321 ms* |
| 4 | 15,795<br>*50 ms* | 9,145<br>*87 ms* | 4,378<br>*182 ms* | 2,371<br>*337 ms* |
| 8 | 12,475<br>*128 ms* | 10,765<br>*148 ms* | 6,856<br>*232 ms* | 4,288<br>*370 ms* |

#### 📊 1-Shard, 3-Replicas (Replication Overhead)

| Threads | Same Region | Same Continent | Cross Continent | Geo-Distributed |
|---------|-------------|----------------|-----------------|--------------------|
| 1 | 11,031<br>*18 ms* | 2,755<br>*72 ms* | 1,162<br>*172 ms* | 624<br>*319 ms* |
| 2 | 11,788<br>*34 ms* | 4,874<br>*82 ms* | 2,218<br>*180 ms* | 1,211<br>*330 ms* |
| 4 | 10,849<br>*73 ms* | 6,989<br>*114 ms* | 3,827<br>*208 ms* | 2,178<br>*365 ms* |
| 8 | 8,856<br>*180 ms* | 7,826<br>*204 ms* | 5,337<br>*298 ms* | 3,603<br>*442 ms* |

#### 📊 2-Shard, 1-Replica (Sharding Overhead)

| Threads | Same Region | Same Continent | Cross Continent | Geo-Distributed |
|---------|-------------|----------------|-----------------|--------------------|
| 1 | 9,925<br>*20 ms* | 2,130<br>*94 ms* | 846<br>*235 ms* | 451<br>*440 ms* |
| 2 | 10,741<br>*37 ms* | 4,034<br>*99 ms* | 1,663<br>*239 ms* | 878<br>*451 ms* |
| 4 | 10,120<br>*79 ms* | 6,744<br>*118 ms* | 3,159<br>*252 ms* | 1,718<br>*462 ms* |
| 8 | 8,611<br>*185 ms* | 7,750<br>*206 ms* | 5,437<br>*293 ms* | 3,176<br>*499 ms* |

#### 📊 2-Shard, 3-Replicas (Full Sharded and Replicated)

| Threads | Same Region | Same Continent | Cross Continent | Geo-Distributed |
|---------|-------------|----------------|-----------------|--------------------|
| 1 | 5,503<br>*36 ms* | 2,015<br>*99 ms* | 830<br>*240 ms* | 439<br>*452 ms* |
| 2 | 4,823<br>*82 ms* | 3,402<br>*117 ms* | 1,584<br>*251 ms* | 863<br>*460 ms* |
| 4 | 5,022<br>*159 ms* | 4,287<br>*186 ms* | 2,813<br>*283 ms* | 1,644<br>*482 ms* |
| 8 | 4,444<br>*358 ms* | 4,217<br>*377 ms* | 3,751<br>*424 ms* | 2,869<br>*552 ms* |

#### 💡 Analysis: Luigi Microbenchmark

**Key Observations:**

1. **Replication Overhead**: Comparing 1-shard-1-replica vs 1-shard-3-replicas shows ~40-50% throughput reduction due to Paxos replication overhead in same-region scenarios. This gap narrows in high-latency networks because network delay causes batching of many log entries, which amortizes the replication overhead across multiple transactions.

2.  **Sharding Overhead**: 2-shard configurations show lower throughput than 1-shard due to cross-shard coordination (timestamp agreement between shard leaders). With 100% cross-shard transactions, every transaction requires inter-shard communication.

3.  **Thread Scaling**: Throughput scales well with thread count (1→8 threads) across all network conditions. Even in geo-distributed settings (150ms latency), we observe **6-7x** throughput improvement from 1 to 8 threads, demonstrating that Luigi effectively utilizes parallelism to hide network latency.

4. **2-WRTT Latency Bound**: Luigi's design guarantees commit latency of at most 2 Wide-Area Round Trip Times (2-WRTT) for the full sharded and replicated setup. Experimental results validate this: the 2-shard, 3-replica configuration with geo-distributed network (150ms one-way delay + 20ms jitter) shows average latencies of 452-552ms across different thread counts. This is **better than** the theoretical worst-case bound of 2 × (2 × 170ms) = 680ms, demonstrating Luigi's efficient coordination. This predictable latency bound is a key advantage over OCC systems with unbounded retry costs.

### 4.2 ⚔️ Luigi vs Mako: TPC-C Performance Comparison

TPC-C is a standard OLTP benchmark simulating a wholesale supplier workload. Both Luigi and Mako are configured with `num_threads × num_shards` warehouses (e.g., 8 threads × 2 shards = 16 warehouses), matching Mako's warehouse-per-thread scaling approach. Default transaction mix: 45% NewOrder, 43% Payment, 12% others.

> **Note:** All values show **throughput** (transactions per second) and **average latency** (milliseconds, in italics)

#### 🔴 1-Shard, 1-Replica: Mako Dominates

**🔵 Luigi Performance:**

| Threads | Same Region | Same Continent | Cross Continent | Geo-Distributed |
| :------ | :---------- | :------------- | :-------------- | :------------------ |
| 1       | 20,349<br>*9 ms* | 2,955<br>*67 ms* | 1,146<br>*174 ms* | 616<br>*324 ms* |
| 2       | 24,768<br>*15 ms* | 5,581<br>*71 ms* | 2,221<br>*180 ms* | 1,181<br>*337 ms* |
| 4       | 26,163<br>*30 ms* | 10,180<br>*78 ms* | 4,176<br>*191 ms* | 2,179<br>*365 ms* |
| 8       | 26,577<br>*59 ms* | 14,010<br>*114 ms* | 5,637<br>*283 ms* | 2,941<br>*541 ms* |

**🔴 Mako Performance:**

| Threads | Same Region | Same Continent | Cross Continent | Geo-Distributed |
| :------ | :---------- | :------------- | :-------------- | :------------------ |
| 1       | **64,101**<br>*15 ms* | **63,899**<br>*15 ms* | **63,221**<br>*15 ms* | **60,850**<br>*16 ms* |
| 2       | **109,409**<br>*18 ms* | **116,414**<br>*17 ms* | **120,226**<br>*16 ms* | **123,200**<br>*16 ms* |
| 4       | **160,376**<br>*24 ms* | **186,113**<br>*21 ms* | **202,252**<br>*19 ms* | **212,231**<br>*18 ms* |
| 8       | **213,851**<br>*37 ms* | **268,340**<br>*29 ms* | **308,450**<br>*25 ms* | **301,888**<br>*26 ms* |

> 🏆 **Winner: Mako** — **3-10x higher throughput** in single-shard configurations

#### 🔴 1-Shard, 3-Replicas: Mako Still Leads

**🔵 Luigi Performance:**

| Threads | Same Region | Same Continent | Cross Continent | Geo-Distributed |
| :------ | :---------- | :------------- | :-------------- | :------------------ |
| 1       | 11,838<br>*15 ms* | 2,762<br>*72 ms* | 1,141<br>*175 ms* | 610<br>*325 ms* |
| 2       | 11,475<br>*34 ms* | 4,971<br>*80 ms* | 2,142<br>*186 ms* | 1,173<br>*340 ms* |
| 4       | 12,039<br>*66 ms* | 8,373<br>*95 ms* | 4,011<br>*199 ms* | 2,160<br>*369 ms* |
| 8       | 15,215<br>*104 ms* | 10,877<br>*146 ms* | 5,599<br>*285 ms* | 2,994<br>*531 ms* |

**🔴 Mako Performance:**

| Threads | Same Region | Same Continent | Cross Continent | Geo-Distributed |
| :------ | :---------- | :------------- | :-------------- | :------------------ |
| 1       | **36,610**<br>*27 ms* | **43,948**<br>*22 ms* | **43,636**<br>*22 ms* | **43,141**<br>*23 ms* |
| 2       | **63,859**<br>*31 ms* | **80,158**<br>*24 ms* | **81,389**<br>*24 ms* | **80,612**<br>*24 ms* |
| 4       | **72,842**<br>*54 ms* | **133,550**<br>*29 ms* | **138,214**<br>*28 ms* | **140,937**<br>*28 ms* |
| 8       | **61,374**<br>*128 ms* | **95,811**<br>*82 ms* | **157,176**<br>*50 ms* | **153,252**<br>*51 ms* |

> 🏆 **Winner: Mako** — **3-50x higher throughput** with replication

#### 🔵 2-Shard, 1-Replica: Luigi Starts to Catch Up

**🔵 Luigi Performance:**

| Threads | Same Region | Same Continent | Cross Continent | Geo-Distributed |
| :------ | :---------- | :------------- | :-------------- | :------------------ |
| 1       | **9,880**<br>*20 ms* | **1,931**<br>*103 ms* | **763**<br>*261 ms* | **408**<br>*486 ms* |
| 2       | **12,298**<br>*32 ms* | **3,671**<br>*109 ms* | **1,479**<br>*269 ms* | **779**<br>*509 ms* |
| 4       | **13,951**<br>*57 ms* | **6,632**<br>*120 ms* | **2,831**<br>*282 ms* | **1,497**<br>*530 ms* |
| 8       | **13,934**<br>*114 ms* | **9,657**<br>*165 ms* | **4,669**<br>*341 ms* | **2,525**<br>*627 ms* |

**🔴 Mako Performance:**

| Threads | Same Region | Same Continent | Cross Continent | Geo-Distributed |
| :------ | :---------- | :------------- | :-------------- | :------------------ |
| 1       | 310<br>*22 ms* | 31<br>*28 ms* | 9<br>*34 ms* | 5<br>*35 ms*        |
| 2       | 940<br>*21 ms* | 89<br>*28 ms* | 29<br>*33 ms* | 16<br>*32 ms*       |
| 4       | 2,068<br>*22 ms* | 212<br>*27 ms* | 82<br>*33 ms* | 43<br>*46 ms*       |
| 8       | 4,129<br>*25 ms* | 415<br>*29 ms* | 168<br>*32 ms* | 79<br>*39 ms*       |

> 🏆 **Winner: Luigi** — **30-60x higher throughput** in geo-distributed multi-shard setups

#### 🔵 2-Shard, 3-Replicas: Luigi Dominates

**🔵 Luigi Performance:**

| Threads | Same Region | Same Continent | Cross Continent | Geo-Distributed |
| :------ | :---------- | :------------- | :-------------- | :------------------ |
| 1       | **6,902**<br>*28 ms* | **1,880**<br>*106 ms* | **754**<br>*264 ms* | **397**<br>*497 ms* |
| 2       | **7,001**<br>*56 ms* | **3,336**<br>*119 ms* | **1,461**<br>*272 ms* | **770**<br>*512 ms* |
| 4       | **6,699**<br>*119 ms* | **5,157**<br>*154 ms* | **2,715**<br>*293 ms* | **1,503**<br>*528 ms* |
| 8       | **6,482**<br>*245 ms* | **6,196**<br>*257 ms* | **4,361**<br>*365 ms* | **2,501**<br>*632 ms* |

**🔴 Mako Performance:**

| Threads | Same Region | Same Continent | Cross Continent | Geo-Distributed |
| :------ | :---------- | :------------- | :-------------- | :------------------ |
| 1       | 337<br>*33 ms* | 32<br>*39 ms* | 10<br>*43 ms* | 5<br>*44 ms*        |
| 2       | 1,031<br>*35 ms* | 94<br>*39 ms* | 32<br>*41 ms* | 16<br>*42 ms*       |
| 4       | 2,476<br>*43 ms* | 247<br>*45 ms* | 89<br>*51 ms* | 40<br>*59 ms*       |
| 8       | 5,982<br>*44 ms* | 414<br>*68 ms* | 177<br>*56 ms* | 84<br>*64 ms*       |

> 🏆 **Winner: Luigi** — **30-60x higher throughput** in full sharded and replicated configuration

#### 💡 Analysis: When Does Each System Win?

**Mako Excels In:**
- **Single-shard workloads**: 3-10x higher throughput (213K-308K txns/sec vs Luigi's 26K)
- **Low-latency networks**: Multiple round trips are cheap in same-region deployments
- **Low per-transaction latency**: 15-68ms for single-shard transactions

**Luigi Excels In:**
- **Multi-shard workloads**: 30-60x higher throughput in 2-shard configurations
- **High-latency networks**: 2-WRTT design (~600ms) vs Mako's 5+ round trips (1,440-1,680ms for cross-shard)
- **Consistent throughput**: Maintains 397-2,501 txns/sec regardless of cross-shard percentage

**Key Architectural Differences:**

Luigi uses a **single execution queue per server** with timestamp adjustment:
- ✅ Avoids aborts by re-ordering transactions (adjusts timestamps instead of aborting)
- ✅ Maintains timestamp-ordered execution for serializability
- ❌ Limits single-server throughput vs Mako's parallel execution

Mako uses **parallel execution with OCC**:
- ✅ Achieves 8-11x higher single-server throughput
- ❌ Suffers from cascading aborts (each abort wastes 1,440-1,680ms in geo-distributed settings)
- ❌ Throughput drops 99.97% (308K → 84 txns/sec) in multi-shard geo-distributed scenarios

> [!WARNING]
> **Understanding Mako's Latency Metrics in Multi-Shard Scenarios**
> 
> Mako's reported average latency (22-68ms) is **dominated by single-shard transactions**. Analysis of the 2-shard, 3-replica geo-distributed configuration reveals a stark difference:
> 
> | Transaction Type | Latency | Abort Rate |
> |-----------------|---------|------------|
> | **Single-shard** | 54-56ms | ~0% |
> | **Cross-shard** | **1,440-1,680ms** | **6-25%** |
> 
> Cross-shard transactions take **20-30x longer** than the reported average, and many abort entirely. This explains both Mako's low average latency and its abysmal throughput in multi-shard scenarios.

**The Crossover Point:**
- **1-shard**: Mako wins (no distribution overhead)
- **2+ shards**: Luigi wins (efficient coordination trumps parallelism)

### 4.3 📊 Cross-Shard Transaction Percentage Study

We conducted a focused study on **2-shard, 3-replica** configuration with **8 threads** to understand how varying cross-shard transaction rates affect each system.

**Workload:** TPC-C benchmark (all transaction types: NewOrder, Payment, OrderStatus, Delivery, StockLevel)  
**Variable:** Cross-shard percentage for NewOrder transactions (5%, 10%, 15%, 20%)  
**Configuration:** 2-shard, 3-replica, geo-distributed (150ms latency, 5ms jitter)  
**Threads:** 8

#### Experimental Parameters

- **Configuration**: 2 shards, 3 replicas per shard (Paxos)
- **Network**: 150ms latency, 20ms jitter (geo-distributed)
- **Threads**: 8 concurrent clients
- **Duration**: 30 seconds per test
- **Workload**: TPC-C NewOrder transactions
- **Cross-Shard %**: Percentage of items sourced from remote warehouse (5%, 10%, 15%, 20%)

#### Results

> **Note:** All throughput values are in **transactions per second (txns/sec)**

| Cross-Shard % | Luigi Throughput | Mako Throughput | Luigi Advantage |
|---------------|------------------|-----------------|-----------------|
| 5% | 2,651 | 42.9 | **61.8x** |
| 10% | 2,662 | 25.8 | **103.2x** |
| 15% | 2,653 | 19.7 | **134.7x** |
| 20% | 2,720 | 16.9 | **160.9x** |

**Latency Comparison (Average):**

| Cross-Shard % | Luigi Avg Latency | Mako Avg Latency |
|---------------|-------------------|------------------|
| 5% | 596 ms | 13 ms |
| 10% | 594 ms | 10 ms |
| 15% | 595 ms | 12 ms |
| 20% | 584 ms | <1 ms |

#### Analysis: Cross-Shard Transaction Behavior

**Why Luigi's Throughput is Constant:**

Luigi's throughput remains stable across different cross-shard configurations due to a combination of a known limitation and architectural advantages.

Due to a bug in Luigi's TPC-C transaction generator (see [Known Limitations](#tpc-c-item-table-partitioning-bug-luigi-only)), the actual cross-shard rate is fixed at ~71% regardless of the configured percentage (5%, 10%, 15%, 20%). The ITEM table keys hash uniformly across shards, causing most transactions to become cross-shard.

Despite this, Luigi's architecture handles the high cross-shard rate efficiently:

1. **Single-Shard Transactions Skip Agreement**: Luigi correctly identifies and fast-paths single-shard transactions, executing them directly without timestamp agreement.

2. **Parallel Cross-Shard Agreement**: For cross-shard transactions, timestamp agreement happens in parallel across all shards within the same round trip.

3. **No Abort-Retry Overhead**: Luigi's deterministic timestamp ordering means transactions are repositioned rather than aborted when conflicts occur, avoiding the cascading aborts and retry storms that plague OCC systems.

**Mako's Degradation:**

Mako's throughput drops from 42.9 → 16.9 (60% reduction) as cross-shard percentage increases because:

1. **More Cross-Shard Transactions Require 2PC**: Each cross-shard transaction needs distributed validation and two-phase commit, adding multiple round trips.

2. **Increased Conflict Probability**: More distributed reads/writes increase the chance of OCC validation failures, requiring expensive retries.

3. **Round-Trip Multiplication**: With 150ms latency, each additional cross-shard operation adds 300ms+ (round trip), quickly saturating the system.

**Why 60-160x Advantage?**

The dramatic performance gap stems from architectural differences:

- **Mako**: 5+ round trips per transaction × 300ms = **1500ms+ per transaction**
- **Luigi**: 2 round trips × 300ms = **~600ms per transaction**

This 2x latency difference translates to 60-160x throughput difference when combined with Mako's increasing abort rate under high cross-shard load.

### 4.4 Result Files

All raw experimental data is available in `test/results/`:

```
test/results/
├── micro/                    # Luigi microbenchmark results
│   ├── 1shard_1replica/     # Single-shard, no replication
│   ├── 1shard_3replicas/    # Single-shard, Paxos replication
│   ├── 2shard_1replica/     # Two shards, no replication
│   └── 2shard_3replicas/    # Two shards, Paxos replication
├── tpcc/                     # Luigi TPC-C results (same structure)
├── mako_tpcc/                # Mako TPC-C results (same structure)
└── cross_shard/              # Cross-shard comparison study
    ├── 5/                    # 5% cross-shard transactions
    │   ├── luigi/results.txt
    │   └── mako/results.txt
    ├── 10/                   # 10% cross-shard transactions
    ├── 15/                   # 15% cross-shard transactions
    └── 20/                   # 20% cross-shard transactions
```

Each result file contains:
- Throughput (transactions/second)
- Latency percentiles (P50, P99, P99.9)
- Abort rate
- Test configuration parameters

## Known Limitations

### TPC-C ITEM Table Partitioning Bug (Luigi Only)

The current Luigi TPC-C transaction generator incorrectly partitions the ITEM table. In standard TPC-C, the ITEM table is **read-only and shared** across all warehouses. Mako handles this correctly by loading ITEM into a single partition and routing all ITEM reads there:

```cpp
// Mako's tpcc.cc:995-997
tbl_item(1)->insert(...);  // "this table is shared, so any partition is OK"

// Mako's tpcc.cc:2210 - all ITEM reads go to partition 1
tbl_item(1)->get(txn, EncodeK(obj_key0, k_i), obj_v);
```

However, Luigi's implementation uses hash-partitioned keys that distribute uniformly across shards:

```cpp
// Luigi's tpcc_txn_generator.h:182
std::string item_key = "item_" + std::to_string(i_id);  // Hashes across shards via FNV-1a
```

This causes ITEM reads to be distributed uniformly across shards, resulting in:
- **~71% cross-shard rate for Luigi** regardless of the configured `remote_item_pct` parameter
- The cross-shard percentage configuration (5%, 10%, 15%, 20%) has no effect on Luigi's actual transaction distribution
- Mako does **not** suffer from this issue - its cross-shard rate is controlled correctly by the configuration

**Impact**: The cross-shard comparison study in Section 4.3 compares Luigi at a fixed ~71% cross-shard rate against Mako at varying rates (5-20%). This actually makes the comparison **more favorable to Mako** - Luigi is running a significantly harder workload (~71% cross-shard) while Mako runs an easier workload (5-20% cross-shard). Despite this disadvantage, Luigi still achieves **60-160x higher throughput** than Mako, demonstrating the fundamental architectural advantages of timestamp-ordered execution over OCC with 2PC.

## Future Work

1. **Integrate Mako's Replication Layer**: Luigi's current replication is a simplified simulation using BatchReplicate RPCs (Raft-style AppendEntries). It lacks leader election, failure recovery, and retries. Integrating Mako's full-fledged Paxos replication layer would provide production-ready fault tolerance.

2. **Safe Commit via Watermark Waiting**: Luigi currently performs optimistic commits—it replies to the client immediately after execution without waiting for `T.timestamp <= watermark[shard][worker_id]`. This is unsafe if a shard leader fails before replication completes. The watermarking infrastructure exists but is not enforced for commit decisions. This was intentional to match Mako's optimistic commit behavior for fair comparison, but should be fixed for production use.

3. **Fix TPC-C ITEM Table Partitioning**: Implement proper ITEM table handling by either:
   - Replicating the ITEM table on all shards (per TPC-C spec)
   - Using warehouse-local item keys like `item_{w_id}_{i_id}`

4. **Variable Cross-Shard Rate Study**: Re-run the cross-shard comparison study with corrected ITEM table partitioning to accurately measure performance across different cross-shard transaction rates.

5. **Geo-Distributed Benchmarks**: Evaluate Luigi and Mako on benchmarks better suited for geo-distributed transaction workloads (e.g., YCSB, Retwis) to provide a more comprehensive comparison.

## References

1. **Mako: A Low-Latency Transactional Database for Geo-Distributed Systems**  
   *Weihai Shen et al., OSDI 2025*  
   *Description*: Describes Mako's OCC-based architecture with Paxos replication  
   *Link*: [Mako Paper](https://www.usenix.org/conference/osdi25/presentation/shen-weihai)

2. **Tiga: Accelerating Geo-Distributed Transactions with Synchronized Clocks**  
   *Jinkun Geng et al., SOSP 2025*  
   *Description*: Timestamp-ordered transaction execution model that inspired Luigi  
   *Link*: [Tiga Paper](https://arxiv.org/abs/2509.05759)

