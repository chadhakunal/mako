# Luigi 2-Shard 1-Replica Network Latency Benchmark - Complete Results

## Executive Summary

Comprehensive performance evaluation of Luigi's 2-shard 1-replica configuration across four network conditions and four thread counts (16 total tests).

**Key Finding**: 2-shard performance scales well with thread count in high-latency scenarios, similar to 1-shard patterns.

---

## Complete Results Matrix

| Network Condition | 1 Thread | 2 Threads | 4 Threads | 8 Threads | Peak |
|-------------------|----------|-----------|-----------|-----------|------|
| **Same Metro** (2ms ± 0.5ms) | **8,451** | 8,778 | 7,932 | 6,411 | 2T |
| **Same Continent** (30ms ± 5ms) | 2,041 | 3,864 | **6,187** | 6,368 | 8T |
| **Cross-Continent** (80ms ± 10ms) | 819 | 1,601 | 3,070 | **4,639** | 8T |
| **Cross-Region** (150ms ± 20ms) | 443 | 850 | 1,675 | **3,076** | 8T |

*All values in txns/sec*

---

## Detailed Results by Thread Count

### Thread Count 1

| Network | Throughput | Avg Latency | P50 | P99 | P99.9 |
|---------|------------|-------------|-----|-----|-------|
| Same Metro | 8,451 txns/sec | 23.3 ms | 22.5 ms | 42.6 ms | 156.5 ms |
| Same Continent | 2,041 txns/sec | 97.2 ms | 99.5 ms | 143.6 ms | 236.0 ms |
| Cross-Continent | 819 txns/sec | 241.1 ms | 249.7 ms | 386.2 ms | 495.0 ms |
| Cross-Region | 443 txns/sec | 442.7 ms | 462.0 ms | 619.4 ms | 621.7 ms |

### Thread Count 2

| Network | Throughput | Avg Latency | P50 | P99 | P99.9 |
|---------|------------|-------------|-----|-----|-------|
| Same Metro | 8,778 txns/sec | 45.2 ms | 42.9 ms | 97.0 ms | 162.6 ms |
| Same Continent | 3,864 txns/sec | 102.7 ms | 104.6 ms | 201.1 ms | 214.2 ms |
| Cross-Continent | 1,601 txns/sec | 246.5 ms | 253.7 ms | 452.9 ms | 487.4 ms |
| Cross-Region | 850 txns/sec | 463.6 ms | 476.8 ms | 693.6 ms | 716.9 ms |

### Thread Count 4

| Network | Throughput | Avg Latency | P50 | P99 | P99.9 |
|---------|------------|-------------|-----|-----|-------|
| Same Metro | 7,932 txns/sec | 100.0 ms | 97.9 ms | 196.5 ms | 258.9 ms |
| Same Continent | 6,187 txns/sec | 128.2 ms | 127.4 ms | 231.1 ms | 289.2 ms |
| Cross-Continent | 3,070 txns/sec | 257.0 ms | 260.0 ms | 438.7 ms | 502.7 ms |
| Cross-Region | 1,675 txns/sec | 469.6 ms | 477.8 ms | 724.0 ms | 1012.6 ms |

### Thread Count 8

| Network | Throughput | Avg Latency | P50 | P99 | P99.9 |
|---------|------------|-------------|-----|-----|-------|
| Same Metro | 6,411 txns/sec | 246.8 ms | 244.0 ms | 512.0 ms | 602.0 ms |
| Same Continent | 6,368 txns/sec | 247.8 ms | 246.1 ms | 437.6 ms | 509.7 ms |
| Cross-Continent | 4,639 txns/sec | 339.0 ms | 341.6 ms | 568.8 ms | 630.5 ms |
| Cross-Region | 3,076 txns/sec | 509.9 ms | 520.4 ms | 912.3 ms | 1144.2 ms |

---

## Scaling Analysis

### Same Metro (2ms ± 0.5ms)
- **Peak**: 2 threads at 8,778 txns/sec
- **Scaling**: Slight improvement 1T→2T (1.04x), then degradation with more threads
- **Pattern**: Low latency shows diminishing returns with high thread counts

### Same Continent (30ms ± 5ms)
- **Peak**: 8 threads at 6,368 txns/sec
- **Scaling**: Strong improvement through all thread counts (1.89x → 1.60x → 1.03x)
- **Total improvement**: 3.12x from 1T to 8T

### Cross-Continent (80ms ± 10ms)
- **Peak**: 8 threads at 4,639 txns/sec
- **Scaling**: Consistent positive scaling (1.95x → 1.92x → 1.51x)
- **Total improvement**: 5.66x from 1T to 8T

### Cross-Region (150ms ± 20ms)
- **Peak**: 8 threads at 3,076 txns/sec
- **Scaling**: Strong positive scaling (1.92x → 1.97x → 1.84x)
- **Total improvement**: 6.95x from 1T to 8T

---

## Comparison: 1-Shard vs 2-Shard (1-Replica)

### Thread Count 1

| Network | 1-Shard | 2-Shard | Ratio |
|---------|---------|---------|-------|
| Same Metro | 10,429 | 8,451 | 0.81x |
| Same Continent | 2,811 | 2,041 | 0.73x |
| Cross-Continent | 1,133 | 819 | 0.72x |
| Cross-Region | 614 | 443 | 0.72x |

**Analysis**: 2-shard shows ~20-30% throughput reduction compared to 1-shard due to cross-shard coordination overhead.

### Thread Count 8

| Network | 1-Shard | 2-Shard | Ratio |
|---------|---------|---------|-------|
| Same Metro | 5,235 | 6,411 | 1.22x |
| Same Continent | 4,701 | 6,368 | 1.35x |
| Cross-Continent | 4,521 | 4,639 | 1.03x |
| Cross-Region | 3,261 | 3,076 | 0.94x |

**Analysis**: With 8 threads, 2-shard actually **outperforms** 1-shard in low/medium latency scenarios, suggesting better parallelization.

---

## Key Insights

1. **Multi-shard scales better with threads**: 2-shard shows stronger scaling with thread count than 1-shard

2. **Cross-shard overhead is manageable**: ~20-30% overhead for single-threaded, but diminishes with concurrency

3. **High-latency benefits most**: Cross-Region shows 6.95x improvement from 1T to 8T

4. **Optimal thread count varies**:
   - Low latency: 1-2 threads
   - Medium latency: 4-8 threads  
   - High latency: 8+ threads

5. **2-shard can outperform 1-shard**: With sufficient threads, 2-shard leverages parallelism better

---

## System Stability

- **Zero aborts** across all 16 test configurations
- **100% commit rate** demonstrates robust multi-shard coordination
- **Consistent behavior** across varying network conditions
- **No mutex crashes** after fix implementation

---

## Test Configuration

- **Duration**: 10 seconds per test
- **Benchmark**: Micro (read-only transactions)
- **Topology**: 2 shards, 1 replica per shard
- **Total tests**: 16 (4 network conditions × 4 thread counts)
- **Total transactions**: 639,851 committed

---

## Files Generated

### Individual Results (16 files)
- `2shard_1replica_t{1,2,4,8}_{metro,continent,crosscontinent,crossregion}.txt`

### Summary
- `2shard_1replica_complete_analysis.md` (this file)

All files located in: `src/deptran/luigi/test/results/`
