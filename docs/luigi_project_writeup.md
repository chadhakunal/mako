# Luigi: Timestamp-Ordered Transaction Execution for Mako

**Code Repository:** https://github.com/chadhakunal/mako (branch: `luigi/owd+execution`)

**README:** See `src/deptran/luigi/LUIGI_PROTOCOL.md` for detailed protocol documentation.

---

## 1. Introduction & Motivation

Mako is a geo-replicated distributed transaction system that uses Optimistic Concurrency Control (OCC) with a lock-validate-install commit protocol. While OCC works well for low-contention workloads, it suffers from increased abort rates under contention because transactions may conflict during the validation phase.

I implemented **Luigi**, a Tiga-style timestamp-ordered execution protocol for Mako. The core idea is simple: instead of optimistically executing and then validating, we assign each transaction a future timestamp (deadline) and execute transactions in timestamp order. This deterministic ordering eliminates validation conflicts entirely.

The key insight from Tiga is that if all shard leaders agree on when to execute a transaction, and they all execute in the same timestamp order, we get serializability without traditional concurrency control. Luigi brings this idea to Mako's architecture.

---

## 2. Technical Approach

### 2.1 Core Protocol

Luigi replaces Mako's OCC commit path with deadline-based execution:

```
OCC (before):                    Luigi (after):
┌─────────────┐                  ┌─────────────┐
│ Execute Txn │                  │ Execute Txn │  
│ Lock Keys   │       →          │ Get Deadline│  (now + OWD + headroom)
│ Validate    │                  │ Dispatch    │  (send to all shards)
│ Install     │                  │ Poll Result │  (wait for completion)
└─────────────┘                  └─────────────┘
```

The coordinator computes ALL write values upfront, calculates a future deadline, and dispatches the complete transaction to all involved shard leaders. Each leader queues the transaction by deadline and executes when the time comes.

### 2.2 One-Way Delay (OWD) Service

A critical component is accurately estimating network latency to set appropriate deadlines. I implemented an OWD measurement service that:

1. Runs a background ping thread that measures RTT to all remote shards every 100ms
2. Estimates OWD = RTT/2 with exponential moving average smoothing
3. Provides `getExpectedTimestamp()` which returns `now + max_owd(involved_shards) + headroom`

This ensures the deadline is far enough in the future that all shards receive the transaction before it needs to execute.

### 2.3 Async Dispatch with Polling

A major design challenge was avoiding blocking server threads. Mako's RPC model expects handlers to return quickly. If a leader blocks waiting for its deadline, it starves other transactions.

I implemented a two-phase async approach:

**Phase 1 - Dispatch:** Coordinator sends transaction to all shards. Each shard immediately returns `QUEUED` and hands off to a background scheduler thread. Server thread is freed instantly.

**Phase 2 - Poll:** Coordinator sleeps until the expected deadline, then polls shards for completion. When a shard finishes execution, it stores the result in a map. The poll RPC returns `COMPLETE` with results.

This decouples the server thread from execution timing entirely.

### 2.4 Multi-Shard Agreement

For transactions touching multiple shards, we need all leaders to agree on the final execution timestamp. I implemented Tiga's three-case agreement protocol:

- **Case 1:** All shards propose the same timestamp → execute immediately
- **Case 2:** My timestamp is the max → wait for others to confirm they've repositioned
- **Case 3:** My timestamp is smaller → reposition to agreed timestamp, send confirmation

The agreement is fully async - leaders exchange proposals via RPC and the scheduler re-enqueues transactions when agreement completes.

### 2.5 Conflict Detection & Priority Queue

Following Tiga's Algorithm 1, I track `last_released_deadline` per key. When a new transaction arrives:

1. Find max `last_released_deadline` among all keys it touches
2. If transaction's deadline ≤ max, bump it to max+1 (leader privilege)
3. Insert into priority queue sorted by (deadline, txn_id)
4. Release to execution when current time ≥ deadline

This ensures serializable ordering even when transactions have overlapping key sets.

---

## 3. Implementation Details

### Files Created/Modified

| File | Purpose |
|------|---------|
| `luigi_scheduler.cc/h` | Priority queue, conflict detection, hold-release timing |
| `luigi_executor.cc/h` | Agreement state machine, operation execution |
| `luigi_owd.cc/h` | One-way delay measurement service |
| `luigi_entry.h` | Transaction entry struct with status fields |
| `luigi_service.cc/h` | RPC service for leader-to-leader communication |
| `luigi_rpc_setup.cc/h` | RPC client setup for agreement protocol |
| `server.cc/h` | Async dispatch handler, status check handler, replication |
| `shardClient.cc/h` | Two-phase polling dispatch from coordinator |
| `Transaction.cc` | `try_commit_luigi()` commit path |
| `common.h` | Request/response structs and status codes |

### Key Data Structures

```cpp
// Priority queue entry
std::map<std::pair<uint64_t, uint64_t>, shared_ptr<LuigiLogEntry>> priority_queue_;
// Key: (deadline_timestamp, txn_id) - ensures deterministic ordering

// Conflict detection
std::unordered_map<int32_t, uint64_t> last_released_deadlines_;
// Key: conflict_key (hash of table_id + key)

// Async result storage
std::unordered_map<uint64_t, LuigiTxnResult> luigi_completed_txns_;
// Key: txn_id, Value: {status, commit_ts, read_results}
```

---

## 4. Technical Challenges

### Challenge 1: Server Thread Blocking

**Problem:** Initial design had the server thread wait until the deadline to execute. This blocked all other RPCs on that thread.

**Solution:** Completely async model. Server immediately returns `QUEUED`, scheduler threads handle timing, coordinator polls for results. This required adding a status check RPC and result storage mechanism.

### Challenge 2: Callback vs Polling for Async Completion

**Problem:** How does the coordinator know when execution is done? Considered three options:
- (A) Callback RPC from leader to coordinator
- (B) Coordinator polls leaders
- (C) Hybrid with timeout

**Solution:** Chose polling (B) because coordinators are transient worker threads without RPC server infrastructure. Polling is simpler and the coordinator already knows when to expect completion (after the deadline).

### Challenge 3: Multi-Shard Agreement Complexity

**Problem:** Tiga's agreement protocol has subtle state transitions. Getting the phase-2 confirmation logic correct was tricky - when do we reset counters? When is agreement truly complete?

**Solution:** Careful state machine implementation with atomic status flags. Case 2 resets `item_count_` and waits for phase-2 confirmations. When they arrive with the agreed timestamp, `all_match` becomes true and we transition to `AGREE_COMPLETE`.

### Challenge 4: Integration with Mako's Replication

**Problem:** Mako uses async Paxos replication. How does Luigi integrate?

**Solution:** Fire-and-forget model matching Mako's existing approach. After local execution, we serialize the writes and call `add_log_to_nc()`. Transaction is marked complete immediately - replication happens in background. Paxos handles failures via its own recovery.

### Challenge 5: OWD Measurement Accuracy

**Problem:** Inaccurate OWD estimates cause either too-early execution (transaction hasn't arrived) or unnecessary waiting.

**Solution:** Exponential moving average (80% old, 20% new) smooths out noise. Added configurable headroom (default 10ms) as safety margin. The OWD service runs continuously so estimates stay fresh.

---

## 5. Current Status & Results

### What's Working
- ✅ Complete Luigi protocol implementation
- ✅ OWD measurement service with background pinging
- ✅ Async dispatch with polling (server threads never block)
- ✅ Multi-shard agreement (all 3 cases)
- ✅ Conflict detection with leader privilege timestamp bumping
- ✅ Priority queue with deterministic ordering
- ✅ Integration with Mako's Paxos replication
- ✅ Clean build with no errors or warnings

### What Needs Testing
- End-to-end transaction execution
- Multi-shard scenarios with agreement
- Performance comparison vs OCC under contention
- Failure scenarios and recovery

---

## 6. Lessons Learned

1. **Start with the threading model.** Understanding which threads handle what is critical. I initially missed that Mako server handlers must return quickly, which led to a complete redesign of the async model.

2. **Async is harder but necessary.** The polling approach added complexity (status check RPC, result storage, cleanup) but was essential for correctness. Blocking alternatives would have broken Mako's RPC model.

3. **State machines need careful design.** The agreement protocol has subtle transitions. Drawing out the state diagram and tracing through all cases before coding would have saved debugging time.

4. **Integration matters.** Luigi doesn't exist in isolation - it needs to work with Mako's existing OWD ping infrastructure, replication, and RPC layer. Understanding these interfaces upfront made integration smoother.

5. **Fire-and-forget replication simplifies things.** By treating replication as a background task (matching Mako's model), we avoided complex completion tracking and kept the critical path fast.

---

## 7. Future Work

1. **Performance evaluation:** Run TPC-C and micro-benchmarks comparing Luigi vs OCC under varying contention levels.

2. **Failure handling:** Test and improve handling of shard failures during agreement.

3. **Dynamic headroom:** Adjust OWD headroom based on observed variance rather than using a fixed value.

4. **Speculative execution:** Current implementation waits for agreement before execution. Could add speculative execution for lower latency (with rollback on mismatch).

---

## Appendix: How to Run

```bash
# Build
cd /root/mako/build
make mako txlog

# Enable Luigi in benchmarks
./dbtest --use-luigi ...

# See detailed protocol docs
cat src/deptran/luigi/LUIGI_PROTOCOL.md
```
