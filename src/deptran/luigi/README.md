# Luigi: Timestamp-Ordered Transaction Execution for Mako

Luigi is a distributed transaction protocol that uses **timestamp ordering** instead of optimistic concurrency control (OCC). It is designed to work with Mako's replication infrastructure while using Tiga's storage and coordination model.

## Overview

### Motivation

Mako and Luigi employ fundamentally different transaction execution models:

| Aspect | Mako (STO/OCC) | Luigi (Stored Procedure) |
|--------|----------------|--------------------------|
| **Execution location** | Client computes | Server computes |
| **Round trips** | Multiple (read → modify → write) | Single (send all ops upfront) |
| **Validation** | OCC at commit time | Timestamp ordering |
| **Storage needs** | Read-set tracking, write-set buffering | Simple put/get |

Mako's storage engine (Masstree) is tightly integrated with STO (Software Transactional Objects) for OCC validation. Luigi's timestamp-ordered execution requires simple, non-transactional put/get operations that bypass this validation entirely. Rather than engineering fragile wrappers to disable STO checks, we replaced the storage layer with Tiga's memdb—a simpler key-value store designed for stored-procedure execution.

This architectural decision transformed the project from a protocol adapter into a **hybrid engine** combining:
- **Mako's replication infrastructure** (Paxos, transport layer, configuration)
- **Tiga's storage and coordination model** (memdb, stored procedures)

### Protocol Summary

1. **Timestamp Initialization**: Coordinator assigns future timestamps based on One-Way Delay (OWD) measurements: `T.timestamp = now() + max_OWD + headroom`

2. **Transaction Dispatch**: Complete operation set sent upfront to all involved shards

3. **Timestamp Agreement**: Leaders exchange timestamps; `T.agreed_ts = max(T.timestamp)` across all shards

4. **Execution**: If timestamps agree, leaders execute and replicate via Paxos

5. **Commit**: Coordinator confirms commit when watermarks advance past transaction timestamp

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Luigi Benchmark                          │
│                     (luigi_bench_main.cc)                       │
├─────────────────────────────────────────────────────────────────┤
│  LuigiBenchmarkClient  │  TxnGenerators (Micro, TPC-C)         │
├─────────────────────────────────────────────────────────────────┤
│                         LuigiClient                             │
│                   (Dispatch requests to shards)                 │
├─────────────────────────────────────────────────────────────────┤
│    LuigiOWD Service    │    Luigi Transport Setup              │
│  (One-way delay est.)  │  (Delegates to Mako's eRPC)           │
├─────────────────────────────────────────────────────────────────┤
│                    Mako Infrastructure                          │
│         (FastTransport, Configuration, BenchmarkConfig)         │
└─────────────────────────────────────────────────────────────────┘
```

## Key Components

| File | Description |
|------|-------------|
| `luigi_bench_main.cc` | Main entry point for benchmark |
| `luigi_benchmark_client.h/cc` | Benchmark runner with statistics |
| `luigi_client.h/cc` | Client for Luigi dispatch requests |
| `luigi_transport_setup.h/cc` | Transport initialization (delegates to Mako) |
| `luigi_owd.h/cc` | One-Way Delay measurement service |
| `luigi_scheduler.h/cc` | Server-side transaction scheduling |
| `luigi_executor.h/cc` | Transaction execution engine |
| `micro_txn_generator.h` | Micro benchmark workload generator |
| `tpcc_txn_generator.h` | TPC-C workload generator |

## Usage

### Building

```bash
cd /path/to/mako
make -j32
# Binary: ./build/luigi_bench
```

### Running Benchmarks

**Using test script (recommended):**
```bash
# TPC-C with 6 threads, 2 shards
bash examples/test_luigi_bench.sh 6 --benchmark tpcc --duration 30

# Micro benchmark with 4 threads
bash examples/test_luigi_bench.sh 4 --benchmark micro --duration 30
```

**Direct invocation (Mako CI compatible):**
```bash
./build/luigi_bench \
    --shard-config src/mako/config/local-shards2-warehouses6.yml \
    --shard-index 0 \
    -P localhost \
    --num-threads 6 \
    --benchmark tpcc \
    --duration 30
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `-q, --shard-config` | Shard configuration YAML file | (required) |
| `-g, --shard-index` | This shard's index | 0 |
| `-P` | Cluster name (localhost, p1, p2, learner) | localhost |
| `-t, --num-threads` | Worker threads | 1 |
| `--benchmark` | Workload type: micro, micro_single, tpcc | tpcc |
| `--duration` | Test duration in seconds | 30 |
| `--keys` | Keys per shard (micro benchmark) | 100000 |
| `--read-ratio` | Read ratio for micro benchmark | 0.5 |

### Configuration

Luigi reuses Mako's configuration system:
- **Shard configs**: `src/mako/config/local-shards{N}-warehouses{W}.yml`
- **Transport selection**: Set `MAKO_TRANSPORT=erpc` or `MAKO_TRANSPORT=rrr`

## Results

### Performance Comparison: Luigi vs Mako

**Test Environment:**
- **Machine**: Linode 4-core, 8GB RAM
- **Configuration**: 2 shards, 4 threads/shard, TPC-C workload, ~5% cross-shard transactions
- **Network Delay**: Simulated using `tc` (Linux traffic control)
- **Duration**: 15 seconds per test

#### Throughput vs Network Latency

| Network Delay | Mako TPS | Luigi TPS | **Speedup** | Mako Abort Rate | Luigi Abort Rate |
|---------------|----------|-----------|-------------|-----------------|------------------|
| 0ms (local)   | 10,404   | 4,812     | 0.46x       | 1.7%            | **0%**           |
| 25ms (regional) | 447    | 663       | **1.48x**   | 2.0%            | **0%**           |
| 50ms (cross-region) | 226 | 350      | **1.54x**   | 2.2%            | **0%**           |
| 100ms (intercontinental) | 113 | 176 | **1.55x**   | 4.0%            | **0%**           |
| 150ms (high latency) | 69 | 117      | **1.69x**   | 6.3%            | **0%**           |

#### Key Observations

1. **Luigi excels with network latency**: As network delay increases, Luigi's advantage grows from 1.48x to 1.69x
2. **Zero aborts**: Luigi achieves 0% abort rate vs Mako's 1.7-6.3% due to deterministic timestamp ordering
3. **Local performance tradeoff**: Without network delay, Luigi is slower (0.46x) due to timestamp coordination overhead
4. **Abort rate scaling**: Mako's abort rate increases with latency (1.7% → 6.3%), while Luigi remains at 0%

#### Why Luigi Wins with Network Delay

Luigi uses **One-Way Delay (OWD) based timestamps** that eliminate coordination round-trips:
- **Mako**: Each cross-shard transaction requires 2PC coordination (multiple RTTs)
- **Luigi**: Transactions execute deterministically at agreed timestamps (single RTT for dispatch)

The higher the network latency, the more RTTs Luigi saves per transaction.

### Cross-Shard Transaction Ratio Impact

**Test Environment:**
- **Configuration**: 2 shards, 4 threads/shard, TPC-C workload
- **Network Delay**: 50ms ± 5ms
- **Duration**: 10 seconds per test

| Cross-Shard % | Mako TPS | Luigi TPS | **Speedup** | Mako Abort Rate | Luigi Abort Rate |
|---------------|----------|-----------|-------------|-----------------|------------------|
| 5%            | 227      | 345       | **1.51x**   | 3.2%            | **0%**           |
| 15%           | 226      | 188       | 0.83x       | 1.6%            | **0%**           |
| 25%           | 226      | 171       | 0.75x       | 2.7%            | **0%**           |
| 35%           | 227      | 161       | 0.70x       | 2.2%            | **0%**           |

#### Key Observations

1. **Luigi wins at low cross-shard ratios**: At 5% cross-shard, Luigi achieves 1.51x speedup
2. **Mako wins at high cross-shard ratios**: Above ~10% cross-shard, Mako outperforms Luigi
3. **Mako throughput is stable**: Mako maintains ~227 TPS regardless of cross-shard ratio
4. **Luigi throughput degrades**: Luigi TPS drops from 345 → 161 as cross-shard increases (5% → 35%)
5. **Zero aborts for Luigi**: Luigi maintains 0% abort rate at all cross-shard ratios

#### Why This Happens

- **Luigi's overhead per distributed txn**: Each cross-shard transaction requires timestamp agreement across shards, adding per-txn coordination cost
- **Mako's batch efficiency**: Mako batches cross-shard operations and handles conflicts via OCC, which scales better under high cross-shard contention
- **Sweet spot**: Luigi excels in workloads with low cross-shard ratios (typical TPC-C: ~5-10%) and high network latency

### Running Your Own Benchmarks

```bash
# Compare Luigi vs Mako (no network delay)
sudo bash examples/compare_mako_luigi_simple.sh 4 15

# Compare with simulated network delay (50ms ± 5ms jitter)
sudo bash examples/compare_mako_luigi_geo.sh 4 15 50 5

# Test different cross-shard ratios (5%, 15%, 25%, 35%)
sudo bash examples/test_cross_shard_ratio.sh 4 10 50 5

# Luigi-only test
sudo bash examples/test_luigi_simple.sh 4 15
```

### Known Limitations

1. **Storage layer**: Currently uses Tiga's memdb; RocksDB persistence not yet integrated
2. **Replication**: Paxos integration complete but disabled in current benchmarks
3. **OWD**: Currently hardcoded; dynamic OWD measurement pending
4. **Resource constraints**: Tests on 4-core machine; expect better scaling on larger hardware

## References

- [LUIGI_PROTOCOL.md](LUIGI_PROTOCOL.md) - Detailed protocol specification
- Mako: [../../mako/](../../mako/) - Parent system documentation
- Tiga: Original stored-procedure transaction system

## Contact

For questions about Luigi integration with Mako, see the main repository documentation.
