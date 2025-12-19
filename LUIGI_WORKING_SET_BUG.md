# Luigi Working Set Bug - Current Status

## Problem Summary
Shard 1 crashes with `terminate called after throwing an instance of 'int'` due to `map::at` exception when executing TPC-C transactions.

## Root Cause
The `working_set` map (TPC-C transaction parameters) arrives **empty** on Shard 1, causing `.at()` calls in `ExecuteNewOrder()` to fail.

## Investigation Timeline

### 1. Initial Crash
- **Symptom**: Shard 1 crashes with coroutine exception (`int`)
- **Log**: `[EXEC-THREAD] Exception: map::at`
- **Location**: `luigi_state_machine.cc:405` when calling `working_set.at(TPCC_VAR_W_ID)`

### 2. Missing working_set Discovery
- Added logging to trace working_set through the pipeline:
  - Client serialization
  - Server deserialization
  - State machine execution

- **Finding**: `working_set` had only 1 entry instead of 30-50 expected entries

### 3. Hex Dump Analysis
Added hex dumps of `working_set_data` buffer at each stage:

**Client Side (Serialization)**:
```
LuigiDispatchBuilder: Serialized 51 working_set entries, ws_len=421
  first 16 bytes: e9 03 00 00 01 00 30 eb 03 00 00 01 00 30 ec 03
  ✅ CORRECT: e9 03 00 00 = 0x03e9 = var_id 1001 (TPCC_VAR_W_ID)
```

**Shard 0 Server Side (Local Requests)**:
```
[SERVER-DISPATCH] TXN-50: Parsing working_set with 42 entries
  first 16 bytes: e9 03 00 00 01 00 30 eb 03 00 00 01 00 30 ec 03
  ✅ CORRECT DATA RECEIVED
```

**Shard 1 Server Side (Remote RPC Requests)**:
```
[SERVER-DISPATCH] TXN-208: Parsing working_set with 24 entries
  first 16 bytes: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
  ❌ ALL ZEROS - DATA NOT TRANSMITTED
```

### 4. Diagnosis

**What Works**:
- ✅ Client-side serialization: `working_set` is correctly written to `working_set_data` buffer
- ✅ Local shard requests: Data arrives intact when using direct `ReceiveRequest()` call
- ✅ Move constructor fix: Added `ws_len_` to move operations (previously missing)

**What's Broken**:
- ❌ Remote RPC requests: `working_set_data` arrives as all zeros at Shard 1
- ❌ This affects all cross-shard transactions from Shard 0 → Shard 1

## Technical Details

### DispatchRequest Structure
```cpp
struct DispatchRequest {
  uint16_t target_server_id;
  uint32_t req_nr;
  uint64_t txn_id;
  uint64_t expected_time;
  uint32_t txn_type;
  uint16_t num_ops;
  uint16_t num_involved_shards;
  uint16_t num_working_set;     // ← Says 24, 30, 42, etc. (correct)
  uint16_t involved_shards[kMaxShards];
  char ops_data[...];           // Fixed-size array
  char working_set_data[...];   // ← Fixed-size array, but RPC sends all zeros
};
```

### GetTotalSize Calculation
```cpp
size_t GetTotalSize() const {
  return sizeof(luigi::DispatchRequest)
         - sizeof(request_->ops_data)
         - sizeof(request_->working_set_data)
         + msg_len_   // Actual ops data used
         + ws_len_;   // Actual working_set data used
}
```

### RPC Transmission
```cpp
// luigi_client.cc:362-366
rpc_transport_->SendBatchRequestToAll(
    nullptr,
    luigi::kLuigiDispatchReqType,
    sender_rpc_id,
    sizeof(luigi::DispatchResponse),
    data_to_send);  // Contains {char* buf, size_t len} pairs
```

## Current Hypothesis

The RPC transport (`FastTransport::SendBatchRequestToAll`) is **NOT respecting the variable-length size** from `GetTotalSize()`. Instead, it's likely:

1. Sending only the fixed struct header (up to `working_set_data` offset)
2. NOT sending the actual `working_set_data` contents
3. Or, sending the full struct but not copying variable data correctly

### Evidence
- `num_working_set` field is transmitted correctly (shows 24, 30, 42)
- But `working_set_data` buffer is all zeros on receive
- This suggests the struct header is copied, but not the trailing variable data

## Fixes Applied So Far

### 1. Move Constructor Bug (FIXED)
**File**: `luigi_client.cc:26-32`
```cpp
LuigiDispatchBuilder::LuigiDispatchBuilder(LuigiDispatchBuilder &&other) noexcept
    : request_(other.request_),
      msg_len_(other.msg_len_),
      ws_len_(other.ws_len_) {  // ← Added (was missing)
  other.request_ = nullptr;
  other.msg_len_ = 0;
  other.ws_len_ = 0;  // ← Added
}
```

**Result**: Fixed local shard requests ✅, but remote RPC still broken ❌

## Next Steps

### Option 1: Fix RPC Transport
Investigate `FastTransport::SendBatchRequestToAll()` and ensure it sends the full `size` bytes, not just `sizeof(DispatchRequest)`.

**Likely culprit**: The RPC backend might be hardcoded to send `sizeof(DispatchRequest)` instead of using the provided size.

**Files to check**:
- `src/mako/lib/fasttransport.cc` - FastTransport implementation
- `src/mako/lib/rrr_rpc_backend.cc` - RRR RPC backend
- Look for where `DispatchRequest` messages are sent/received

### Option 2: Change Message Format
Instead of using variable-length trailing data in fixed arrays, use a properly sized message structure or send working_set as a separate RPC payload.

### Option 3: Serialize to Byte Stream
Create a flat byte buffer for the entire message (header + ops + working_set) and send that, rather than relying on struct layout.

## ROOT CAUSE FOUND!

**The bug is in how `working_set_data` is written!**

### The Problem
`AddWorkingSetEntry()` writes to `request_->working_set_data`, which is at a FIXED compile-time offset (after the huge `ops_data` array). But when we serialize with `GetTotalSize()`, we only send a SMALL portion of the struct.

**Evidence**:
- Client logs show bytes 66-81 are ALL ZEROS even BEFORE RPC send
- `working_set_data` field is at offset ~20KB in the struct (after full `ops_data` array)
- But we only send ~300-400 bytes total via RPC

### The Fix
`AddWorkingSetEntry()` must write to the ACTUAL location where working_set will be serialized, which is:
```
header (66 bytes) + actual_ops_data_length + working_set_data
```

NOT to `request_->working_set_data` (which is at a huge offset).

## Status
- **Shard 0**: ✅ Works for local transactions (lucky - pointers point to same memory)
- **Shard 1**: ❌ Crashes on first cross-shard transaction (map::at exception)
- **Issue**: `working_set_data` written to wrong offset in struct
- **Blocking**: All multi-shard Luigi benchmarks

## Files Modified
1. `luigi_common.h` - Added working_set fields to DispatchRequest
2. `luigi_client.h/cc` - Added SetWorkingSet() and fixed move constructor
3. `luigi_benchmark_client.cc` - Call SetWorkingSet() before dispatch
4. `luigi_server.cc` - Parse working_set from request
5. `luigi_scheduler.h/cc` - Accept working_set parameter
6. `luigi_state_machine.cc` - Use working_set in Execute methods

---

**Date**: 2025-12-18
**Last Test**: `examples/test_luigi_simple.sh 6 10`
**Logs**: `luigi_simple_shard0.log`, `luigi_simple_shard1.log`
