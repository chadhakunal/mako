# Luigi: Timestamp-Ordered Distributed Transactions

Luigi is a distributed transaction protocol that replaces Mako's OCC (Optimistic Concurrency Control) with **timestamp ordering**. The goal is to reduce coordination overhead in geo-distributed settings by using One-Way Delay (OWD) based timestamps to deterministically order transactions, eliminating multi-round-trip 2PC coordination.

## Project Structure

| File | Description |
|------|-------------|
| `luigi_bench_main.cc` | Main entry point, CLI parsing |
| `luigi_benchmark_client.cc/h` | Benchmark driver, stats collection |
| `luigi_client.cc/h` | Client-side transaction dispatch |
| `luigi_server.cc/h` | Server-side request handling |
| `luigi_scheduler.cc/h` | Transaction queue and timestamp ordering |
| `luigi_executor.cc/h` | Transaction execution engine |
| `luigi_state_machine.cc/h` | TPC-C stored procedures |
| `luigi_transport_setup.cc/h` | Network layer (delegates to Mako's eRPC) |
| `luigi_owd.cc/h` | One-Way Delay measurement |
| `tpcc_txn_generator.h` | TPC-C workload generator |
| `micro_txn_generator.h` | Micro benchmark generator |

## Building

```bash
cd /path/to/mako
make -j32 mako-raft
# Binary: ./build/luigi_bench
```

## Running Benchmarks

```bash
# Single Luigi test
./build/luigi_bench \
    --shard-config src/mako/config/local-shards2-warehouses4.yml \
    --shard-index 0 -P localhost --num-threads 4 --benchmark tpcc --duration 30

# Luigi vs Mako with network delay (50ms ± 5ms)
sudo bash examples/compare_mako_luigi_geo.sh 4 15 50 5

# Cross-shard ratio test (rebuilds Mako for each %)
sudo bash examples/test_cross_shard_ratio.sh 4 10 50 5
```

## Results

### Throughput vs Network Latency (5% cross-shard)

| Network Delay | Mako TPS | Luigi TPS | **Speedup** | Mako Abort | Luigi Abort |
|---------------|----------|-----------|-------------|------------|-------------|
| 0ms (local)   | 10,404   | 4,812     | 0.46x       | 1.7%       | **0%**      |
| 25ms          | 447      | 663       | **1.48x**   | 2.0%       | **0%**      |
| 50ms          | 226      | 350       | **1.54x**   | 2.2%       | **0%**      |
| 100ms         | 113      | 176       | **1.55x**   | 4.0%       | **0%**      |
| 150ms         | 69       | 117       | **1.69x**   | 6.3%       | **0%**      |

**Luigi wins with network latency** - speedup grows from 1.48x to 1.69x as delay increases.

### Cross-Shard Ratio Impact (50ms delay)

| Cross-Shard % | Mako TPS | Luigi TPS | **Speedup** | Mako Abort | Luigi Abort |
|---------------|----------|-----------|-------------|------------|-------------|
| 5%            | 97       | 348       | **3.57x**   | 1.8%       | **0%**      |
| 15%           | 47       | 188       | **4.01x**   | 3.9%       | **0%**      |
| 25%           | 35       | 171       | **4.85x**   | 4.4%       | **0%**      |
| 35%           | 28       | 158       | **5.65x**   | 7.5%       | **0%**      |

**Luigi wins at ALL cross-shard percentages!** Speedup *increases* with more cross-shard:
- **Mako degrades faster**: 97 → 28 TPS (71% drop)
- **Luigi degrades slower**: 348 → 158 TPS (55% drop)
- **Mako aborts climb**: 1.8% → 7.5%, Luigi stays at **0%**

### Test Environment

- **Machine**: Linode 4-core, 8GB RAM
- **Config**: 2 shards, 4 threads/shard, TPC-C
- **Network**: Simulated via `tc` (Linux traffic control)
