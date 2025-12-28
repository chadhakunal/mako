# 1-Shard 1-Replica Microbenchmark Results

## Summary

All 16 tests completed successfully on the resized machine (15GB RAM). This configuration represents the baseline performance without replication overhead.

## Results Table

| Threads | Network Condition | Throughput (txns/sec) | Transactions Completed |
|---------|------------------|----------------------|------------------------|
| 1 | Metro (2ms±0.5ms) | 23,916 | 239,162 |
| 1 | Continent (30ms±5ms) | 3,099 | 30,993 |
| 1 | Cross-Continent (80ms±10ms) | 1,189 | 11,894 |
| 1 | Cross-Region (150ms±20ms) | 646 | 6,469 |
| 2 | Metro (2ms±0.5ms) | 24,906 | 249,062 |
| 2 | Continent (30ms±5ms) | 5,780 | 57,803 |
| 2 | Cross-Continent (80ms±10ms) | 2,362 | 23,621 |
| 2 | Cross-Region (150ms±20ms) | 1,272 | 12,721 |
| 4 | Metro (2ms±0.5ms) | 19,957 | 199,572 |
| 4 | Continent (30ms±5ms) | 9,838 | 98,383 |
| 4 | Cross-Continent (80ms±10ms) | 4,490 | 44,901 |
| 4 | Cross-Region (150ms±20ms) | 2,406 | 24,062 |
| 8 | Metro (2ms±0.5ms) | 14,849 | 148,493 |
| 8 | Continent (30ms±5ms) | 12,293 | 122,937 |
| 8 | Cross-Continent (80ms±10ms) | 7,183 | 71,832 |
| 8 | Cross-Region (150ms±20ms) | 4,291 | 42,918 |

## Latency Analysis

**Note**: The 1-shard 1-replica result files use an older output format that doesn't include detailed latency statistics (P50, P99, average). Only transaction counts and throughput are available.

For latency comparisons, refer to the 1-shard 3-replica and 2-shard 3-replica analysis documents which include comprehensive latency data.

## Key Findings

### Peak Performance
- **Highest Throughput**: 24,906 txns/sec (T2 metro)
- **Best for Low Latency**: T2 metro configuration
- **Best for High Latency**: T8 crossregion (4,291 txns/sec)

### Thread Scaling Analysis

**Metro Network (2ms):**
- T1: 23,916 txns/sec
- T2: 24,906 txns/sec (+4.1%) ← **Peak**
- T4: 19,957 txns/sec (-19.9%)
- T8: 14,849 txns/sec (-25.6%)

**Observation**: Peak performance at T2. Beyond 2 threads, overhead from thread contention and context switching reduces throughput in low-latency scenarios.

**Cross-Region Network (150ms):**
- T1: 646 txns/sec
- T2: 1,272 txns/sec (+96.9%)
- T4: 2,406 txns/sec (+89.2%)
- T8: 4,291 txns/sec (+78.3%) ← **Best for high latency**

**Observation**: Linear scaling with thread count in high-latency scenarios. More threads effectively hide network latency.

### Network Latency Impact

| Network | T1 | T2 | T4 | T8 |
|---------|----|----|----|----|
| Metro (2ms) | 23,916 | 24,906 | 19,957 | 14,849 |
| Continent (30ms) | 3,099 | 5,780 | 9,838 | 12,293 |
| Cross-Continent (80ms) | 1,189 | 2,362 | 4,490 | 7,183 |
| Cross-Region (150ms) | 646 | 1,272 | 2,406 | 4,291 |

**Key Insight**: As network latency increases, higher thread counts become more beneficial. T8 shows the best scaling for high-latency networks.

## Performance Characteristics

### Optimal Thread Count by Network Condition
- **Metro**: 2 threads (24,906 txns/sec)
- **Continent**: 8 threads (12,293 txns/sec)
- **Cross-Continent**: 8 threads (7,183 txns/sec)
- **Cross-Region**: 8 threads (4,291 txns/sec)

### Scaling Efficiency
- **Low Latency (Metro)**: Poor scaling beyond 2 threads due to contention
- **High Latency (Cross-Region)**: Excellent scaling up to 8 threads (6.6x improvement)

## Test Configuration

- **Duration**: 10 seconds per test
- **Config File**: `src/deptran/luigi/test/configs/1shard-1replica.yml`
- **Shards**: 1
- **Replicas**: 1 (no replication)
- **Machine**: 15GB RAM (resized)
- **Network Emulation**: tc qdisc with pareto distribution

## Comparison with Multi-Replica Configurations

This baseline configuration (no replication) shows the maximum achievable throughput. Comparing with 1-shard 3-replica:

| Config | Metro (T2) | Continent (T8) | Cross-Region (T8) |
|--------|-----------|----------------|-------------------|
| 1-Shard 1-Replica | 24,906 | 12,293 | 4,291 |
| 1-Shard 3-Replica | 11,663 | 7,963 | 3,604 |
| **Overhead** | **53% slower** | **35% slower** | **16% slower** |

**Analysis**: Replication overhead is most significant in low-latency scenarios where the replication quorum becomes the bottleneck. In high-latency scenarios, network latency dominates and replication overhead is less pronounced.

## Conclusion

The 1-shard 1-replica configuration demonstrates:
1. **Peak performance** at 2 threads for low-latency networks
2. **Excellent scaling** with thread count for high-latency networks
3. **Baseline performance** for comparison with replicated configurations
4. **Thread contention** becomes significant beyond 2 threads in low-latency scenarios
