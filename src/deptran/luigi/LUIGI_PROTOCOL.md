# Luigi Protocol - Timestamp-Ordered Transaction Execution

## Quick Reference for LLMs

**Key Files:**
- Coordinator: `src/mako/benchmarks/sto/Transaction.cc` → `try_commit_luigi()`
- Client RPC: `src/mako/lib/shardClient.cc` → `remoteLuigiDispatch()`
- Server Handler: `src/mako/lib/server.cc` → `HandleLuigiDispatch()`
- Scheduler: `src/deptran/luigi/luigi_scheduler.cc` → `LuigiDispatchFromRequest()`
- Executor: `src/deptran/luigi/luigi_executor.cc` → `Execute()`
- OWD Service: `src/deptran/luigi/luigi_owd.cc` → `getExpectedTimestamp()`
- RPC Types: `src/mako/lib/common.h` → `luigi_dispatch_request_t`

**Enable with:** `./dbtest --use-luigi ...`

---

## 1. Core Concept: Deadline-Based Execution

Luigi replaces Mako's OCC (Lock → Validate → Install) with deadline-based execution:

```
Traditional OCC:          Luigi:
┌─────────────┐           ┌─────────────┐
│ Execute Txn │           │ Execute Txn │  (reads happen, writes computed)
├─────────────┤           ├─────────────┤
│ Get Locks   │           │ Get Timestamp│  (expected_time = now + OWD + headroom)
├─────────────┤           ├─────────────┤
│ Validate    │           │ Dispatch    │  (send to shard leaders)
├─────────────┤           ├─────────────┤
│ Install     │           │ Wait Reply  │  (shards queue, wait, execute at deadline)
├─────────────┤           ├─────────────┤
│ Unlock      │           │ Check Match │  (verify execute_timestamps agree)
└─────────────┘           └─────────────┘
```

**Key Insight:** The coordinator pre-computes ALL write values. Shards receive complete write operations and simply apply them at the agreed deadline.

---

## 2. Timestamp Calculation

### 2.1 One-Way Delay (OWD) Measurement

The `LuigiOWD` service runs a background thread that:
1. Pings all remote shards every `PING_INTERVAL_MS` (100ms)
2. Measures RTT using `checkRemoteShardReady()` warmup RPC
3. Estimates OWD = RTT / 2
4. Maintains exponential moving average: `new_owd = 0.8 * old_owd + 0.2 * measured_owd`

```cpp
// In luigi_owd.cc
void LuigiOWD::pingLoop() {
    while (running_) {
        for (int dst_shard : getRemoteShards()) {
            auto start = std::chrono::steady_clock::now();
            shard_client_->checkRemoteShardReady(dst_shard);
            auto end = std::chrono::steady_clock::now();
            uint64_t rtt_ms = duration_cast<milliseconds>(end - start).count();
            updateOWD(dst_shard, rtt_ms);  // stores rtt/2
        }
        std::this_thread::sleep_for(milliseconds(PING_INTERVAL_MS));
    }
}
```

### 2.2 Expected Timestamp Formula

```
expected_time = current_system_time_ms + max_owd(involved_shards) + HEADROOM_MS

Where:
- current_system_time_ms: Wallclock time when transaction dispatches
- max_owd: Maximum one-way delay among all involved shards
- HEADROOM_MS: Safety margin (default 10ms)
```

**Why this works:** By the time the RPC arrives at the shard leader, the deadline will be in the near future, giving all shards time to:
1. Receive the transaction
2. Queue it by deadline
3. Execute when deadline arrives (all shards execute at ~same wall-clock time)

---

## 3. Transaction ID Generation

Each transaction needs a globally unique ID for:
1. Tie-breaking when deadlines match
2. Tracking in the agreement protocol
3. Deduplication

```cpp
// In Transaction.cc try_commit_luigi()
uint64_t txn_id = (uint64_t(threadid_) << 48) | TThread::increment_id;

// Structure:
// Bits 63-48: Thread ID (16 bits) - unique per worker thread
// Bits 47-0:  Increment ID (48 bits) - monotonically increasing per thread
```

This guarantees uniqueness without coordination.

---

## 4. Read/Write Set Handling

### 4.1 What the Coordinator Sends

The coordinator collects operations from TransItems and sends:

| Field | Read Operations | Write Operations |
|-------|-----------------|------------------|
| `table_id` | Table containing the key | Table containing the key |
| `op_type` | `LUIGI_OP_READ` (0) | `LUIGI_OP_WRITE` (1) |
| `key` | The key being read | The key being written |
| `value` | Empty | **Pre-computed write value** |

```cpp
// In Transaction.cc try_commit_luigi()
for (each TransItem) {
    if (item.has_write()) {
        // Extract pre-computed value from TransItem
        table_ids.push_back(table_id);
        op_types.push_back(LUIGI_OP_WRITE);
        keys.push_back(key);
        values.push_back(write_value);  // Already computed!
    } else if (item.has_read()) {
        table_ids.push_back(table_id);
        op_types.push_back(LUIGI_OP_READ);
        keys.push_back(key);
        values.push_back("");  // No value for reads
    }
}
```

### 4.2 Why Reads Are Sent

Even though writes are pre-computed, reads are sent for:
1. **Durability:** The shard logs what values were read for replay
2. **Validation (optional):** Could verify versions haven't changed
3. **Returning values:** Some protocols need read results for verification

### 4.3 Server-Side Filtering

Each shard only executes operations for keys it owns:

```cpp
// In luigi_scheduler.cc or luigi_executor.cc
void LuigiScheduler::LuigiDispatchFromRequest(...) {
    for (const auto& op : ops) {
        int key_shard = getShardForKey(op.key);
        if (key_shard == this->partition_id_) {
            entry->local_keys_.push_back(op_index);  // Mark as local
        }
    }
}
```

---

## 5. Protocol Flow (Step-by-Step)

### Phase 1: Coordinator Prepares Transaction

```
┌─────────────────────────────────────────────────────────────────┐
│ Transaction.cc::try_commit_luigi()                              │
├─────────────────────────────────────────────────────────────────┤
│ 1. Collect all TransItems (reads and writes already executed)  │
│ 2. Generate txn_id = (thread_id << 48) | increment_id          │
│ 3. Identify involved_shards from table_ids                     │
│ 4. Call LuigiOWD::getExpectedTimestamp(involved_shards)        │
│    → returns expected_time = now + max_owd + headroom           │
│ 5. Build ops[] with {table_id, op_type, key, value}            │
│ 6. Call shardClient->remoteLuigiDispatch(...)                  │
└─────────────────────────────────────────────────────────────────┘
```

### Phase 2: RPC Dispatch to Shard Leaders

```
┌─────────────────────────────────────────────────────────────────┐
│ shardClient.cc::remoteLuigiDispatch()                           │
├─────────────────────────────────────────────────────────────────┤
│ 1. Group operations by destination shard                       │
│ 2. For each shard, create LuigiDispatchRequestBuilder           │
│ 3. Serialize: {req_nr, txn_id, expected_time, num_ops, ops[]}  │
│ 4. Call client->InvokeLuigiDispatch() - BLOCKING                │
│ 5. Collect responses into execute_timestamps map                │
└─────────────────────────────────────────────────────────────────┘
```

### Phase 3: Server Receives and Queues

```
┌─────────────────────────────────────────────────────────────────┐
│ server.cc::HandleLuigiDispatch()                                │
├─────────────────────────────────────────────────────────────────┤
│ 1. Parse luigi_dispatch_request_t                              │
│ 2. Build std::vector<LuigiOp> from serialized data             │
│ 3. Call luigi_scheduler_->LuigiDispatchFromRequest(             │
│        txn_id, expected_time, ops, reply_callback)              │
│ 4. Wait on condition_variable for reply_callback                │
│ 5. Return luigi_dispatch_response_t with execute_timestamp     │
└─────────────────────────────────────────────────────────────────┘
```

### Phase 4: Scheduler Processes Transaction

```
┌─────────────────────────────────────────────────────────────────┐
│ luigi_scheduler.cc::LuigiDispatchFromRequest()                  │
├─────────────────────────────────────────────────────────────────┤
│ 1. Create LuigiLogEntry with:                                   │
│    - proposed_ts_ = expected_time                               │
│    - tid_ = txn_id                                              │
│    - ops_ = operations vector                                   │
│    - reply_cb_ = callback to send response                     │
│ 2. Identify local_keys_ (keys this shard owns)                 │
│ 3. Push to incoming_txn_queue_                                  │
│                                                                 │
│ HoldReleaseTd thread picks up:                                  │
│ 4. CONFLICT CHECK: For each local key:                         │
│    if (proposed_ts_ <= last_released_deadline_[key]) {          │
│        proposed_ts_ = max(last_released_deadline_[key]) + 1;    │
│    }                                                            │
│ 5. Insert into priority_queue_[{proposed_ts_, txn_id}]          │
│ 6. WAIT: Sleep until now >= proposed_ts_                        │
│ 7. Move to ready_txn_queue_ for execution                       │
│ 8. Update last_released_deadline_[key] for each key             │
└─────────────────────────────────────────────────────────────────┘
```

### Phase 5: Executor Runs Transaction

```
┌─────────────────────────────────────────────────────────────────┐
│ luigi_executor.cc::Execute()                                    │
├─────────────────────────────────────────────────────────────────┤
│ 1. For multi-shard: Run agreement protocol (see Section 6)    │
│ 2. ExecuteAllOps():                                             │
│    a. For each op in local_keys_:                               │
│       - If READ: call read_cb_(table_id, key, &value)           │
│       - If WRITE: call write_cb_(table_id, key, value)          │
│ 3. TriggerReplication():                                        │
│    - Call replication_cb_(entry) for Paxos durability          │
│ 4. Invoke reply_cb_(status, execute_timestamp, read_results)   │
└─────────────────────────────────────────────────────────────────┘
```

### Phase 6: Coordinator Checks Consensus

```
┌─────────────────────────────────────────────────────────────────┐
│ Transaction.cc::try_commit_luigi() (continued)                  │
├─────────────────────────────────────────────────────────────────┤
│ 1. Receive execute_timestamps from all shards                  │
│ 2. CHECK: All execute_timestamps must match                    │
│    if (any differ) → ABORT                                      │
│    if (all same)   → COMMIT                                     │
│ 3. Call stop(committed, ...)                                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 6. Multi-Shard Agreement (Tiga-Style)

When a transaction spans multiple shards, they must agree on the final execute_timestamp.

### 6.1 Why Agreement is Needed

Different shards may bump the timestamp due to local conflicts:
- Shard 0: No conflicts, proposed_ts = 100
- Shard 1: Conflict with key K, bumps to proposed_ts = 105

They must agree on the MAX (105) so both execute at the same logical time.

### 6.2 Three Cases

```
After exchanging proposals, each shard determines its case:

Case 1: My proposal == agreed_ts (everyone agrees)
        → Execute immediately

Case 2: My proposal > others (I have the highest)
        → I am the "leader" for this agreement
        → Re-queue at agreed_ts
        → Send CONFIRM messages to others

Case 3: My proposal < agreed_ts (someone else higher)
        → Wait for CONFIRM from the leader
        → Then execute
```

### 6.3 Agreement Protocol Sequence

```
Transaction T touches Shard 0 and Shard 1

Step 1: Initial Proposals
   S0: proposed_ts=100  ──────────────────►  S1: proposed_ts=105
   S0: receives 105     ◄──────────────────  S1: receives 100

Step 2: Compute agreed_ts = max(100, 105) = 105

Step 3: Determine Case
   S0: 100 < 105 → Case 3 (wait for confirm)
   S1: 105 == 105 → Case 2 (I'm leader, send confirm)

Step 4: Confirmation
   S1: sends CONFIRM(txn_id, agreed_ts=105) to S0
   S0: receives CONFIRM, moves to ready_queue

Step 5: Execution
   Both execute at agreed_ts=105
```

---

## 7. Data Structures

### 7.1 RPC Request (common.h)

```cpp
struct luigi_dispatch_request_t {
    uint64_t req_nr;          // Sequence number for response matching
    uint64_t txn_id;          // Globally unique transaction ID
    uint64_t expected_time;   // Deadline (ms since epoch)
    uint16_t num_ops;         // Count of operations
    char ops_data[];          // Serialized operations (variable length)
};

// Each operation in ops_data:
// [table_id: 2B][op_type: 1B][klen: 2B][vlen: 2B][key: klen B][value: vlen B]
```

### 7.2 Log Entry (luigi_entry.h)

```cpp
struct LuigiLogEntry {
    uint64_t proposed_ts_;      // Initially expected_time, may be bumped
    uint64_t agreed_ts_;        // Final agreed timestamp (multi-shard)
    txnid_t tid_;               // Transaction ID
    
    std::vector<LuigiOp> ops_;  // All operations
    std::vector<int32_t> local_keys_;  // Indices of ops this shard owns
    
    std::atomic<uint32_t> agree_status_;  // INIT → FLUSHING → CONFIRMING → COMPLETE
    std::atomic<uint32_t> exec_status_;   // INIT → EXECUTING → DONE
    
    std::function<void(status, commit_ts, read_results)> reply_cb_;
};
```

### 7.3 Scheduler Queues (luigi_scheduler.h)

```cpp
class SchedulerLuigi {
    // Incoming transactions from RPC handler
    ConcurrentQueue<std::shared_ptr<LuigiLogEntry>> incoming_txn_queue_;
    
    // Priority queue ordered by (deadline, txn_id)
    // Transactions wait here until their deadline
    std::map<std::pair<uint64_t, txnid_t>, std::shared_ptr<LuigiLogEntry>> priority_queue_;
    
    // Ready to execute (deadline passed, agreement complete)
    ConcurrentQueue<std::shared_ptr<LuigiLogEntry>> ready_txn_queue_;
    
    // Per-key last released deadline (for conflict detection)
    std::unordered_map<int32_t, uint64_t> last_released_deadlines_;
};
```

---

## 8. Key Invariants

1. **Timestamp Ordering:** Transactions execute in timestamp order per key
2. **Agreement:** Multi-shard transactions agree on execute_timestamp before executing
3. **Pre-computed Writes:** Coordinator computes all write values; shards just apply them
4. **Unique txn_id:** No two transactions share the same (thread_id, increment_id)
5. **Deadline Safety:** expected_time is always in the future (OWD + headroom)

---

## 9. Enabling Luigi

```bash
# Build
make

# Run with Luigi enabled
./dbtest --use-luigi --shard-config config.yaml --num-threads 4 ...
```

The `--use-luigi` flag:
1. Sets `BenchmarkConfig::getInstance().setUseLuigi(1)`
2. Starts `LuigiOWD` background ping thread
3. Initializes `SchedulerLuigi` in each `ShardServer`
4. Routes `Sto::try_commit()` → `try_commit_luigi()`

---

## 10. Current Limitations

### 10.1 Worker Thread Blocks on Server Response

**Current Behavior:**
```
Worker Thread                     Shard Leader
     │                                 │
     │──── LuigiDispatch RPC ─────────►│
     │                                 │
     │         BLOCKED                 │ (queued, waiting for deadline)
     │         WAITING                 │
     │         HERE                    │ (deadline arrives, execute)
     │                                 │
     │◄──── Response ──────────────────│
     │                                 │
     │ (can now process next txn)      │
```

The worker thread calls `remoteLuigiDispatch()` which is a **blocking RPC**:
- `shardClient.cc` uses `client->InvokeLuigiDispatch()` which blocks
- The worker cannot start the next transaction until ALL shards respond
- With high network latency or many shards, this limits throughput

**Why it blocks:**
```cpp
// In shardClient.cc::remoteLuigiDispatch()
client->InvokeLuigiDispatch(txn_nr, requests_per_shard, 
    continuation, error_continuation, timeout);
// ^^^ This call blocks until all responses arrive
```

### 10.2 Server Handler Also Blocks

In `server.cc::HandleLuigiDispatch()`:
```cpp
// Wait for completion (with timeout)
std::unique_lock<std::mutex> lock(completion_mutex);
completion_cv.wait_for(lock, std::chrono::seconds(10), [&]{ return completed; });
```

The eRPC server thread blocks waiting for the Luigi scheduler to:
1. Queue the transaction
2. Wait for deadline
3. Execute
4. Trigger replication

This ties up server resources during the waiting period.

### 10.3 Sequential Transaction Processing

Because of blocking, each worker processes transactions sequentially:
```
Time:  |---Txn1---|---Txn2---|---Txn3---|
       dispatch   dispatch   dispatch
       wait       wait       wait
       commit     commit     commit
```

Instead of pipelining:
```
Time:  |---Txn1---|
          |---Txn2---|
             |---Txn3---|
       dispatch all, overlap waiting
```

---

## 11. Future Improvements

### 11.1 Async/Pipelined Dispatch (High Priority)

**Goal:** Worker dispatches transaction and immediately starts next one.

```cpp
// Proposed: Non-blocking dispatch
void remoteLuigiDispatchAsync(
    txn_id, expected_time, ops,
    std::function<void(status, execute_ts)> completion_callback);

// Worker pipeline:
for (auto& txn : transactions) {
    txn.execute();  // Do reads, compute writes
    pending_txns.push_back(txn);
    shardClient->remoteLuigiDispatchAsync(txn, [&](status, ts) {
        // Handle completion asynchronously
        txn.finalize(status, ts);
        pending_txns.remove(txn);
    });
}
// Periodically: poll for completions
```

**Implementation Steps:**
1. Add `PendingLuigiTxn` tracking structure
2. Modify `InvokeLuigiDispatch` to be non-blocking
3. Add completion callback mechanism
4. Restructure `try_commit_luigi()` for async flow

### 11.2 Server-Side Async Response

**Goal:** Don't block eRPC threads waiting for deadline.

```cpp
// Current (blocking):
HandleLuigiDispatch() {
    scheduler->LuigiDispatch(..., callback);
    wait(callback);  // BLOCKS
    return response;
}

// Proposed (async):
HandleLuigiDispatch() {
    scheduler->LuigiDispatch(..., [=](result) {
        // Send response when ready (deferred)
        sendAsyncResponse(result);
    });
    return;  // Don't block, return immediately
}
```

Requires eRPC deferred response support or separate response path.

### 11.3 Batching Transactions

**Goal:** Amortize RPC overhead by batching multiple transactions.

```
Current:   Txn1 ──RPC──►  Txn2 ──RPC──►  Txn3 ──RPC──►

Batched:   [Txn1, Txn2, Txn3] ──single RPC──►
```

Benefits:
- Fewer network round trips
- Better deadline alignment
- Reduced per-transaction overhead

### 11.4 Speculative Execution

**Goal:** Start executing before deadline if no conflicts detected.

```
Current:
    receive ─── wait ─── deadline ─── execute

Speculative:
    receive ─── no conflicts? ─── execute early (tentatively)
                                        │
                     confirm at deadline ▼
```

Risk: Must handle late-arriving conflicting transactions.

### 11.5 Adaptive Headroom

**Goal:** Dynamically adjust headroom based on observed delays.

```cpp
// Instead of fixed HEADROOM_MS = 10:
uint64_t adaptive_headroom = 
    base_headroom + 
    percentile_99(recent_delays) - median(recent_delays);
```

Reduces latency when network is stable, increases safety when unstable.

### 11.6 Read-Only Transaction Fast Path

**Goal:** Skip agreement for read-only transactions.

```cpp
if (transaction.isReadOnly()) {
    // No writes → no conflicts → no agreement needed
    // Just read at current time, return immediately
    return executeReadsLocally();
}
```

---

## 12. Current Implementation Status

| Component | Status | Location |
|-----------|--------|----------|
| OWD Service | ✅ Complete | `luigi_owd.cc` |
| Coordinator Logic | ✅ Complete | `Transaction.cc` |
| Client RPC | ✅ Complete (blocking) | `shardClient.cc`, `client.cc` |
| Server Handler | ✅ Complete (blocking) | `server.cc` |
| Scheduler Core | ✅ Complete | `luigi_scheduler.cc` |
| Executor | ✅ Complete | `luigi_executor.cc` |
| Multi-shard Agreement | 🚧 Partial | `luigi_scheduler.cc` |
| Paxos Integration | ⏳ TODO | `replication_cb_` stub |
| Async Dispatch | ⏳ TODO | See 11.1 |
| Transaction Batching | ⏳ TODO | See 11.3 |
| Performance Tuning | ⏳ TODO | - |

---

## 13. Tracing a Transaction (Example)

```
Worker Thread 5, Transaction #42, accessing keys on Shard 0 and Shard 1:

1. try_commit_luigi() called
   - txn_id = (5 << 48) | 42 = 0x0005000000000002A
   - involved_shards = [0, 1]

2. LuigiOWD::getExpectedTimestamp([0, 1])
   - current_time = 1000000 ms
   - owd[0] = 0 ms (local)
   - owd[1] = 25 ms
   - max_owd = 25 ms
   - expected_time = 1000000 + 25 + 10 = 1000035 ms

3. remoteLuigiDispatch() sends to Shard 0 and Shard 1
   - Request: {txn_id=0x5..2A, expected_time=1000035, ops=[...]}

4. Shard 0 receives at t=1000005 ms
   - proposed_ts = 1000035
   - No conflicts → stays 1000035
   - Queued in priority_queue_

5. Shard 1 receives at t=1000030 ms
   - proposed_ts = 1000035
   - Conflict with key K (last_released=1000040)
   - BUMPED: proposed_ts = 1000041

6. Agreement Protocol
   - S0 proposes 1000035, S1 proposes 1000041
   - agreed_ts = max = 1000041
   - S0: Case 3, waits for confirm
   - S1: Case 2, sends confirm to S0

7. Execution at t=1000041
   - Both shards execute their local ops
   - Both reply with execute_timestamp=1000041

8. Coordinator receives both responses
   - execute_timestamps = {0: 1000041, 1: 1000041}
   - Match! Transaction COMMITTED
```
