# 1-Shard 3-Replica Microbenchmark Results

## Summary

All 16 tests completed successfully on the resized machine (15GB RAM) after fixing the coordinator bug. This configuration demonstrates the performance impact of replication with quorum-based consensus.

## Results Table

| Threads | Network Condition | Throughput (txns/sec) | Transactions Completed |
|---------|------------------|----------------------|------------------------|
| 1 | Metro (2ms±0.5ms) | 11,306 | 113,110 |
| 1 | Continent (30ms±5ms) | 2,814 | 28,344 |
| 1 | Cross-Continent (80ms±10ms) | 1,130 | 11,498 |
| 1 | Cross-Region (150ms±20ms) | 594 | 6,096 |
| 2 | Metro (2ms±0.5ms) | 11,663 | 116,634 |
| 2 | Continent (30ms±5ms) | 4,850 | 48,500 |
| 2 | Cross-Continent (80ms±10ms) | 2,144 | 21,442 |
| 2 | Cross-Region (150ms±20ms) | 1,186 | 11,861 |
| 4 | Metro (2ms±0.5ms) | 11,076 | 110,761 |
| 4 | Continent (30ms±5ms) | 6,661 | 66,614 |
| 4 | Cross-Continent (80ms±10ms) | 3,816 | 38,164 |
| 4 | Cross-Region (150ms±20ms) | 2,153 | 22,110 |
| 8 | Metro (2ms±0.5ms) | 9,162 | 92,288 |
| 8 | Continent (30ms±5ms) | 7,963 | 79,634 |
| 8 | Cross-Continent (80ms±10ms) | 5,512 | 55,121 |
| 8 | Cross-Region (150ms±20ms) | 3,604 | 37,295 |

## Latency Analysis

### Average Latency (microseconds)

| Threads | Metro | Continent | Cross-Continent | Cross-Region |
|---------|-------|-----------|-----------------|--------------|
| 1 | 17,571 | 70,807 | 175,252 | 331,008 |
| 2 | 33,839 | 82,175 | 184,276 | 333,501 |
| 4 | 71,806 | 119,339 | 208,041 | 365,513 |
| 8 | 173,790 | 125,104 | 181,054 | 438,443 |

### P50 Latency (microseconds)

| Threads | Metro | Continent | Cross-Continent | Cross-Region |
|---------|-------|-----------|-----------------|--------------|
| 1 | 16,605 | 68,368 | 169,934 | 319,474 |
| 2 | 31,747 | 79,646 | 179,108 | 320,944 |
| 4 | 69,125 | 115,488 | 202,455 | 353,881 |
| 8 | 164,895 | 121,095 | 175,881 | 419,037 |

### P99 Latency (microseconds)

| Threads | Metro | Continent | Cross-Continent | Cross-Region |
|---------|-------|-----------|-----------------|--------------|
| 1 | 35,038 | 122,017 | 229,560 | 593,661 |
| 2 | 71,559 | 127,317 | 262,810 | 699,295 |
| 4 | 153,234 | 193,569 | 363,544 | 694,735 |
| 8 | 276,823 | 219,056 | 297,595 | 831,809 |

### Latency Observations

**Metro Network**:
- Latency increases significantly with thread count (T1: 17ms → T8: 174ms avg)
- Caused by increased contention for replication quorum
- P99 latency grows from 35ms to 277ms

**Cross-Region Network**:
- Latency dominated by network delay (~320-440ms)
- Thread count has less impact on latency
- Replication overhead is relatively small compared to network latency

## Key Findings

### Peak Performance
- **Highest Throughput**: 11,663 txns/sec (T2 metro)
- **Best for Low Latency**: T2 metro configuration
- **Best for High Latency**: T8 crossregion (3,604 txns/sec)

### Thread Scaling Analysis

**Metro Network (2ms):**
- T1: 11,306 txns/sec
- T2: 11,663 txns/sec (+3.2%) ← **Peak**
- T4: 11,076 txns/sec (-5.0%)
- T8: 9,162 txns/sec (-17.3%)

**Observation**: Very flat scaling in metro network. Replication quorum synchronization limits the benefit of additional threads in low-latency scenarios.

**Cross-Region Network (150ms):**
- T1: 594 txns/sec
- T2: 1,186 txns/sec (+99.7%)
- T4: 2,153 txns/sec (+81.5%)
- T8: 3,604 txns/sec (+67.4%) ← **Best for high latency**

**Observation**: Excellent scaling with thread count in high-latency scenarios. More threads effectively hide both network latency and replication overhead.

### Network Latency Impact

| Network | T1 | T2 | T4 | T8 |
|---------|----|----|----|----|
| Metro (2ms) | 11,306 | 11,663 | 11,076 | 9,162 |
| Continent (30ms) | 2,814 | 4,850 | 6,661 | 7,963 |
| Cross-Continent (80ms) | 1,130 | 2,144 | 3,816 | 5,512 |
| Cross-Region (150ms) | 594 | 1,186 | 2,153 | 3,604 |

**Key Insight**: Replication overhead is most visible in low-latency networks. Higher thread counts provide better scaling in high-latency networks.

## Replication Overhead Analysis

Comparing with 1-shard 1-replica baseline:

| Threads | Network | 1-Replica | 3-Replica | Overhead |
|---------|---------|-----------|-----------|----------|
| 1 | Metro | 23,916 | 11,306 | -52.7% |
| 2 | Metro | 24,906 | 11,663 | -53.2% |
| 4 | Metro | 19,957 | 11,076 | -44.5% |
| 8 | Metro | 14,849 | 9,162 | -38.3% |
| 1 | Cross-Region | 646 | 594 | -8.0% |
| 2 | Cross-Region | 1,272 | 1,186 | -6.8% |
| 4 | Cross-Region | 2,406 | 2,153 | -10.5% |
| 8 | Cross-Region | 4,291 | 3,604 | -16.0% |

**Key Findings**:
1. **Metro Network**: 38-53% overhead from replication
2. **Cross-Region Network**: 7-16% overhead from replication
3. **Overhead decreases** as network latency increases (network latency dominates)
4. **Overhead decreases** with higher thread counts in metro (better parallelization)

## Performance Characteristics

### Optimal Thread Count by Network Condition
- **Metro**: 2 threads (11,663 txns/sec)
- **Continent**: 8 threads (7,963 txns/sec)
- **Cross-Continent**: 8 threads (5,512 txns/sec)
- **Cross-Region**: 8 threads (3,604 txns/sec)

### Scaling Efficiency
- **Low Latency (Metro)**: Minimal scaling due to replication quorum bottleneck
- **High Latency (Cross-Region)**: Excellent scaling up to 8 threads (6.1x improvement)

## Replication Behavior

### Quorum Requirements
- **Total Replicas**: 3 (1 leader + 2 followers)
- **Quorum Size**: 2 (majority)
- **Replication Method**: Batched asynchronous replication with quorum acknowledgement

### Watermark Exchange
- Bidirectional watermark propagation between leader and followers
- Watermark updates only after quorum is reached
- Watermark thread runs every 100ms

## Test Configuration

- **Duration**: 10 seconds per test
- **Config File**: `src/deptran/luigi/test/configs/1shard-3replicas.yml`
- **Shards**: 1
- **Replicas**: 3 (1 leader + 2 followers)
- **Machine**: 15GB RAM (resized)
- **Network Emulation**: tc qdisc with pareto distribution

## Bug Fix

The 1-shard 3-replica configuration initially failed with the coordinator exiting immediately after initialization. The bug was fixed, enabling all tests to complete successfully.

## Comparison with 2-Shard 3-Replica

| Config | Metro (T4) | Continent (T8) | Cross-Region (T8) |
|--------|-----------|----------------|-------------------|
| 1-Shard 3-Replica | 11,076 | 7,963 | 3,604 |
| 2-Shard 3-Replica | 8,552 | 6,763 | 3,127 |
| **Difference** | **+29% faster** | **+18% faster** | **+15% faster** |

**Analysis**: Single-shard configuration outperforms 2-shard due to:
1. No cross-shard coordination overhead
2. Simpler transaction routing
3. Lower contention with fewer shards

## Conclusion

The 1-shard 3-replica configuration demonstrates:
1. **Consistent performance** across thread counts in low-latency scenarios (~11K txns/sec)
2. **Excellent scaling** with thread count for high-latency networks (6x improvement)
3. **Replication overhead** of 38-53% in metro, but only 7-16% in cross-region
4. **Quorum synchronization** limits scaling in low-latency scenarios
5. **Better performance** than 2-shard 3-replica due to simpler coordination
