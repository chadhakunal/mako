# Luigi Testing Guide for LLMs

This guide explains how to run Luigi vs Mako benchmarks and vary key parameters.

## Quick Test Commands

```bash
# Build everything
cd /root/mako && make -j4 mako-raft

# Simple Luigi vs Mako comparison (no network delay)
sudo bash examples/compare_mako_luigi_simple.sh 4 15

# With network delay (recommended for meaningful results)
sudo bash examples/compare_mako_luigi_geo.sh <threads> <duration> <delay_ms> <jitter_ms>
sudo bash examples/compare_mako_luigi_geo.sh 4 15 50 5

# Cross-shard ratio test (varies both Mako and Luigi)
sudo bash examples/test_cross_shard_ratio.sh <threads> <duration> <delay_ms> <jitter_ms>
sudo bash examples/test_cross_shard_ratio.sh 4 10 50 5
```

## Parameters You Can Vary

### 1. Network Delay (Most Important)

Add simulated network latency using `tc`:

```bash
# Add delay
sudo tc qdisc add dev lo root netem delay 50ms 5ms   # 50ms ± 5ms

# Remove delay
sudo tc qdisc del dev lo root

# Check current delay
tc qdisc show dev lo
```

**Typical values:**
- 0ms = local/same datacenter
- 25ms = regional (US East to US Central)
- 50ms = cross-region (US East to US West)
- 100ms = intercontinental (US to Europe)
- 150ms = high latency (US to Asia)

### 2. Cross-Shard Transaction Percentage

**For Luigi** - use CLI option:
```bash
./build/luigi_bench --cross-shard-pct 15 ...
```

**For Mako** - must edit source and rebuild:
```bash
# Edit this line in src/mako/benchmarks/tpcc.cc:
# static int g_new_order_remote_item_pct = 5;

sed -i 's/g_new_order_remote_item_pct = [0-9]*/g_new_order_remote_item_pct = 15/' \
    src/mako/benchmarks/tpcc.cc
make -C build -j4 dbtest
```

**Typical values:** 5%, 15%, 25%, 35%

### 3. Thread Count

```bash
# Luigi
./build/luigi_bench --num-threads 6 ...

# Mako
./build/dbtest --num-threads 6 ...
```

**Note:** Must use matching shard config file (e.g., `local-shards2-warehouses6.yml` for 6 threads)

### 4. Test Duration

```bash
# Luigi
./build/luigi_bench --duration 30 ...

# Mako runs until killed, typically wait duration + 30s then pkill
```

### 5. OWD (One-Way Delay) for Luigi

```bash
./build/luigi_bench --owd-ms 56 ...
```

**Rule of thumb:** `owd_ms = network_delay + jitter/2 + 4`

## Running Manual Tests

### Luigi Only (2 shards)

```bash
# Terminal 1: Shard 0
./build/luigi_bench \
    --shard-config src/mako/config/local-shards2-warehouses4.yml \
    --shard-index 0 -P localhost --num-threads 4 \
    --benchmark tpcc --duration 15 --owd-ms 56

# Terminal 2: Shard 1 (start 5s after shard 0)
./build/luigi_bench \
    --shard-config src/mako/config/local-shards2-warehouses4.yml \
    --shard-index 1 -P localhost --num-threads 4 \
    --benchmark tpcc --duration 15 --owd-ms 56
```

### Mako Only (2 shards)

```bash
# Terminal 1: Shard 0
./build/dbtest \
    --shard-config src/mako/config/local-shards2-warehouses4.yml \
    --shard-index 0 -P localhost --num-threads 4

# Terminal 2: Shard 1 (start 5s after shard 0)
./build/dbtest \
    --shard-config src/mako/config/local-shards2-warehouses4.yml \
    --shard-index 1 -P localhost --num-threads 4

# Wait ~45s then kill both
pkill -9 dbtest
```

## Parsing Results

### Luigi Output

```
=== Luigi Benchmark Results ===
Throughput: 350.45 TPS        # <-- Main metric
Committed: 5234
Aborted: 0 (0.00%)            # <-- Should be 0%
Avg Latency: 2.85 ms
```

Parse with:
```bash
grep "Throughput:" luigi.log | awk '{print $2}'
grep "Aborted:" luigi.log | grep -oP '\d+\.\d+(?=%)'
```

### Mako Output

```
agg_throughput: 97.29 ops/sec           # <-- Main metric
agg_abort_rate: 0.05 aborts/sec
NewOrder_remote_abort_ratio: 1.78 %     # <-- Cross-shard abort rate
```

Parse with:
```bash
grep "agg_throughput:" mako.log | awk '{print $2}'
grep "NewOrder_remote_abort_ratio:" mako.log | awk '{print $2}'
```

## Cleanup Between Runs

**Always run this between tests:**

```bash
pkill -9 luigi_bench dbtest 2>/dev/null
rm -f nfs_sync_*
sudo rm -rf /tmp/mako_sync/ /tmp/*_mako_rocksdb_shard*
ipcrm -a 2>/dev/null
sudo tc qdisc del dev lo root 2>/dev/null
sleep 3
```

## Common Issues

### "Address already in use"
Wait longer between runs, or run cleanup commands above.

### Processes getting "Killed"
This is normal - the test scripts use `pkill -9` to stop processes after duration.

### Low Mako throughput
Check if network delay is applied. Mako is very sensitive to latency.

### Luigi TPS = 0
Check that both shards started and can communicate. Shard 0 must start first.

## Expected Results Summary

At **50ms network delay**:

| Cross-Shard % | Expected Luigi Speedup |
|---------------|------------------------|
| 5%            | ~3.5x                  |
| 15%           | ~4.0x                  |
| 25%           | ~4.8x                  |
| 35%           | ~5.6x                  |

Luigi should always have **0% abort rate**. Mako aborts increase with cross-shard %.
