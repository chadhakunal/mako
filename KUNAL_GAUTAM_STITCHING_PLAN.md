# Kunal + Gautam: What Needs Stitching

This document lists the specific integration points between Kunal's OWD/client-side work and Gautam's scheduler/executor/server-side work.

---

## Quick Summary

| Component | Kunal Has | Gautam Has | Integration Needed |
|-----------|-----------|------------|-------------------|
| Future timestamp | `expected_time` from OWD | `send_time + bound` in scheduler | Wire Kunal's `expected_time` → Gautam's `proposed_ts_` |
| RPC request struct | `expected_time` field | `send_time` + `bound` fields | Align struct fields |
| Server handler | N/A | `HandleLuigiDispatch` scaffolding | Extract timestamp from request, pass to scheduler |
| Multi-shard detection | Knows involved shards in `try_commit_luigi()` | `IsMultiShard()` returns `false` | Pass shard info from client, implement detection |
| Leader agreement | N/A | RPC stubs in `luigi_service` | Complete the RPCs, wire into executor |

---

## 1. Connect OWD-Based Timestamp to Scheduler

### The Gap
- **Kunal's side**: In `Transaction::try_commit_luigi()`, calculates `expected_time_us` using `LuigiOWD::getExpectedTimestamp(involved_shards)`
- **Gautam's side**: `LuigiLogEntry` has `proposed_ts_` (called `local_deadline_`), but it's not set from OWD

### What to Do
In `server.cc::HandleLuigiDispatch()`:
```cpp
// Currently (Gautam's code):
luigi_scheduler_->LuigiDispatchFromRequest(req, reply_cb);

// Change to:
// Extract expected_time from request and pass to scheduler
uint64_t expected_ts = req->expected_time;  // Kunal's field
luigi_scheduler_->LuigiDispatchFromRequest(req, expected_ts, reply_cb);
```

In `luigi_scheduler.cc::LuigiDispatchFromRequest()`:
```cpp
// Set proposed_ts_ from the OWD-derived timestamp
entry->local_deadline_ = expected_ts;  // or entry->proposed_ts_ = expected_ts
```

### Files to Touch
- `src/mako/lib/server.cc` - Extract `expected_time` from request
- `src/deptran/luigi/luigi_scheduler.cc` - Accept and use the timestamp
- `src/deptran/luigi/luigi_scheduler.h` - Update function signature

---

## 2. Align the RPC Request Struct

### The Gap
Kunal's struct (in `common.h`):
```cpp
struct luigi_dispatch_request_t {
    uint64_t expected_time;        // Timestamp from OWD
    // ...
};
```

Gautam's struct (also in `common.h`):
```cpp
struct luigi_dispatch_request_t {
    uint64_t send_time;            // When coordinator sent
    uint32_t bound;                // Deadline offset
    // ...
};
```

These are **the same struct** but with different field names/semantics!

### What to Do
Pick one convention. Recommendation: Use Kunal's `expected_time` since it already carries the computed future timestamp:
```cpp
struct luigi_dispatch_request_t {
    uint64_t expected_time;        // = now() + max_OWD + headroom (Kunal computes this)
    // Remove send_time + bound, or keep for debugging
};
```

Then in Gautam's scheduler, just use `expected_time` directly as `proposed_ts_`.

### Files to Touch
- `src/mako/lib/common.h` - Unify the struct definition
- `src/deptran/luigi/luigi_scheduler.cc` - Use the unified field

---

## 3. Pass Involved Shards from Client to Server

### The Gap
- **Kunal's side**: `try_commit_luigi()` knows which shards are involved (it loops over `shard_to_keys`)
- **Gautam's side**: `IsMultiShard()` returns `false` because it doesn't know which shards are involved

### What to Do
Option A: Add shard info to the request struct:
```cpp
struct luigi_dispatch_request_t {
    uint16_t num_involved_shards;
    uint16_t involved_shards[8];   // Max 8 shards, adjust as needed
    // ...
};
```

Then in `LuigiExecutor::IsMultiShard()`:
```cpp
bool IsMultiShard() {
    return entry->num_involved_shards > 1;
}
```

Option B: Each shard infers other shards from the same `txn_id` arriving at multiple places.

Recommendation: **Option A** is simpler and more explicit.

### Files to Touch
- `src/mako/lib/common.h` - Add shard list to request
- `src/mako/benchmarks/sto/Transaction.cc` - Fill in the shard list
- `src/deptran/luigi/luigi_executor.cc` - Use the shard list in `IsMultiShard()`

---

## 4. Complete Leader Agreement RPCs

### The Gap
- **Kunal's side**: N/A (client-side only)
- **Gautam's side**: Has `luigi_service.cc` with RPC stubs, but they're not called

### What to Do
In `LuigiExecutor::PerformLeaderAgreement()`:
1. If `IsMultiShard()` is true, send `proposed_ts_` to other leaders via `luigi_service` RPC
2. Receive their `proposed_ts_` values
3. Compute `agreed_ts = max(all proposed_ts)`
4. Apply the 3-case logic:
   - **Case 1**: All timestamps match → execute immediately
   - **Case 2**: Timestamps differ but within tolerance → wait for confirmation
   - **Case 3**: Timestamps differ significantly → requeue with updated timestamp

### Files to Touch
- `src/deptran/luigi/luigi_executor.cc` - Implement agreement logic
- `src/deptran/luigi/luigi_service.cc` - Complete RPC handlers
- `src/deptran/luigi/luigi_scheduler.cc` - Add `RequeueForReposition()` for Case 3

---

## 5. Return Commit Timestamp to Coordinator

### The Gap
- **Kunal's side**: `try_commit_luigi()` expects `commit_timestamp` in the response and checks all shards return the same value
- **Gautam's side**: Response struct exists but `commit_timestamp` isn't set from `agreed_ts_`

### What to Do
In `LuigiExecutor` after agreement + execution:
```cpp
response->commit_timestamp = entry->agreed_ts_;  // or agreed_deadline_
```

Then Kunal's code in `try_commit_luigi()` will correctly check consensus.

### Files to Touch
- `src/deptran/luigi/luigi_executor.cc` - Set `commit_timestamp` in response
- `src/mako/lib/server.cc` - Ensure response is properly returned

---

## Summary Checklist for Gautam

- [ ] **Task 1**: In `HandleLuigiDispatch`, extract `expected_time` from request and pass to scheduler
- [ ] **Task 2**: In `LuigiDispatchFromRequest`, set `proposed_ts_ = expected_time`
- [ ] **Task 3**: Unify the request struct (use `expected_time`, remove `send_time+bound` or keep for debug)
- [ ] **Task 4**: Add involved shards to request struct, implement `IsMultiShard()` properly
- [ ] **Task 5**: In `PerformLeaderAgreement`, call `luigi_service` RPCs to exchange timestamps
- [ ] **Task 6**: Implement the 3-case agreement logic with proper requeue on Case 3
- [ ] **Task 7**: Set `response->commit_timestamp = agreed_ts_` before returning

---

## Testing the Stitching

After integration, this flow should work:

1. Client calls `try_commit_luigi()`
2. Client computes `expected_time = now() + max_OWD + headroom` using `LuigiOWD`
3. Client sends request to all involved shards
4. Each shard's `HandleLuigiDispatch` extracts `expected_time`, passes to scheduler
5. Scheduler sets `proposed_ts_ = expected_time`, enqueues in priority queue
6. When deadline reached, executor calls `PerformLeaderAgreement`
7. Leaders exchange `proposed_ts` via RPC, compute `agreed_ts = max()`
8. Leaders execute in `agreed_ts` order
9. Response with `commit_timestamp = agreed_ts` sent back to client
10. Client checks all shards returned same `commit_timestamp`, commits or aborts

---

## Files Reference

| File | Owner | Integration Touch Points |
|------|-------|-------------------------|
| `src/mako/benchmarks/sto/Transaction.cc` | Kunal | Fill shard list in request |
| `src/mako/luigi/luigi_owd.cc` | Kunal | No changes needed |
| `src/mako/lib/common.h` | Both | Unify request struct |
| `src/mako/lib/server.cc` | Gautam | Extract timestamp, pass to scheduler |
| `src/deptran/luigi/luigi_scheduler.cc` | Gautam | Use `expected_time` as `proposed_ts_` |
| `src/deptran/luigi/luigi_executor.cc` | Gautam | `IsMultiShard()`, agreement RPCs, set `commit_timestamp` |
| `src/deptran/luigi/luigi_service.cc` | Gautam | Complete RPC handlers |

