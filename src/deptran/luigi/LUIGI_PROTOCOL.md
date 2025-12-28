# Luigi Protocol Specification

This document provides a detailed specification of the Luigi protocol, including message formats, state machines, and RPC definitions.

## Table of Contents

1. [Protocol Summary](#1-protocol-summary)
2. [System Architecture](#2-system-architecture)
3. [RPC Definitions](#3-rpc-definitions)
4. [Timestamp Management](#4-timestamp-management)
5. [Transaction Lifecycle](#5-transaction-lifecycle)
6. [Cross-Shard Agreement](#6-cross-shard-agreement)
7. [Paxos Replication](#7-paxos-replication)
8. [State Machines](#8-state-machines)
9. [Data Structures](#9-data-structures)

---

## 1. Protocol Summary

### Timestamp Initialization
- Coordinator assigns future timestamps based on OWD measurements: `T.timestamp = now() + max_OWD + headroom`
- `worker_id` sent separately in RPC for tie-breaking and Paxos stream routing

### Transaction Receipt and Queueing
- Leaders perform conflict detection before accepting transaction (check read/write maps for any transaction that touches the same keys, has a higher timestamp, and has been released)
- Timestamp update for late transactions: `T.timestamp = now()`
- Insert into timestamp-ordered priority queue for execution

### Timestamp Agreement
- Leaders exchange `T.timestamp` with other involved shards
- `T.agreed_ts = max(T.timestamp<S_i>)` across all shards
- If all leader timestamps match → agreement; proceed to execute and replicate

### Transaction Execution (on agreement success)
- Leaders execute the transaction and send the result back to coordinator

### Transaction Replication (on agreement success)
- Leader appends transaction to Paxos stream
- Per-worker Paxos stream; `worker_ID` from composite timestamp determines which stream replicates

### Watermark Updates
- Leaders maintain per-worker watermark: `T.timestamp` of last replicated transaction
- Update per-worker watermark upon successful replication
- Periodically exchange watermarks with other shard leaders

### Transaction Commit Decision
- Coordinator replies back to client with result when `T.timestamp <= watermark[shard][worker_ID]` for all involved shards

---

### Protocol Phases

```
Phase 1: Timestamp Assignment & Dispatch
┌────────────┐          ┌────────────┐          ┌────────────┐
│ Coordinator│──────────│   Shard 0  │          │   Shard 1  │
│            │  Dispatch│   Leader   │          │   Leader   │
│            │─────────►│            │          │            │
│            │  Dispatch│            │          │            │
│            │──────────┼────────────┼─────────►│            │
└────────────┘          └────────────┘          └────────────┘

Phase 2: Agreement & Execution (server-side)
┌────────────┐          ┌────────────┐          ┌────────────┐
│ Coordinator│          │   Shard 0  │◄────────►│   Shard 1  │
│            │          │   Leader   │ Propose/ │   Leader   │
│            │          │            │ Confirm  │            │
│            │◄─────────│            │          │            │
│            │  Reply   │            │          │            │
│            │◄─────────┼────────────┼──────────│            │
└────────────┘          └────────────┘          └────────────┘
```

---

## 2. System Architecture

### Components

```
┌─────────────────────────────────────────────────────────────────┐
│                         Coordinator                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ Transaction │  │  Timestamp  │  │     Communication       │  │
│  │  Generator  │  │  Assigner   │  │        Layer            │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Shard Leader                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   Service   │  │  Scheduler  │  │       Executor          │  │
│  │  (RPC Rx)   │─►│  (Queuing)  │─►│   (State Machine)       │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│                          │                      │                │
│                          ▼                      ▼                │
│                   ┌─────────────┐        ┌─────────────┐        │
│                   │  Agreement  │        │   Paxos     │        │
│                   │   Module    │        │ Replication │        │
│                   └─────────────┘        └─────────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

### Key Files

| Component | File | Description |
|-----------|------|-------------|
| Coordinator | `coordinator.cc/h` | Timestamp assignment, transaction dispatch |
| Server | `server.cc/h` | Server entry point, configuration |
| Service | `service.cc/h` | RPC handlers |
| Scheduler | `scheduler.cc/h` | Transaction queuing, agreement coordination |
| Executor | `executor.cc/h` | Transaction execution |
| State Machine | `state_machine.cc/h` | Key-value store with MVCC |
| Communication | `commo.cc/h` | Network layer (eRPC-based) |
| RPC Definitions | `luigi.rpc` | Protocol buffer-style RPC definitions |

---

## 3. RPC Definitions

The Luigi RPC interface is defined in `luigi.rpc`. Only the RPCs actually used in the implementation are documented here.

### 3.1 Dispatch

Sent from coordinator to shard leaders to submit a transaction. Synchronous RPC that waits for execution.

```protobuf
Dispatch(
    i64 txn_id,                   // Globally unique transaction ID
    i64 expected_time,            // Assigned execution timestamp
    i32 worker_id,                // Worker ID for tie-breaking and stream routing
    vector<i32> involved_shards,  // All shards involved in this transaction
    string ops_data               // Serialized read/write operations
) → (
    i32 status,                   // Success/failure code
    i64 commit_timestamp,         // Final agreed timestamp
    string results_data           // Serialized read results
)
```

### 3.2 OwdPing

Sent from coordinator to shard leaders to measure one-way delay.

```protobuf
OwdPing(
    i64 send_time                 // Coordinator's local time when sent
) → (
    i32 status                    // Acknowledgment
)
```

### 3.3 DeadlineBatchPropose

Batched timestamp proposals between shard leaders. Includes piggybacked watermarks.

```protobuf
DeadlineBatchPropose(
    vector<i64> tids,                 // Transaction IDs
    i32 src_shard,                    // Sending shard ID
    vector<i64> proposed_timestamps,  // Proposed timestamps for each txn
    vector<i64> watermarks            // Per-worker watermarks (piggybacked)
) → (
    i32 status                        // Acknowledgment
)
```

### 3.4 DeadlineBatchConfirm

Batched timestamp confirmations from the shard with highest timestamp.

```protobuf
DeadlineBatchConfirm(
    vector<i64> tids,                 // Transaction IDs
    i32 src_shard,                    // Sending shard ID
    vector<i64> agreed_timestamps     // Agreed timestamps for each txn
) → (
    i32 status                        // Acknowledgment
)
```

### 3.5 BatchReplicate

Raft-style AppendEntries for batched replication (leader → followers).

```protobuf
BatchReplicate(
    i32 worker_id,                    // Worker stream ID
    i64 prev_committed_slot,          // Last committed slot (consistency check)
    vector<i64> slot_ids,             // Slot IDs for each entry
    vector<i64> txn_ids,              // Transaction IDs
    vector<i64> timestamps,           // Execution timestamps
    vector<string> log_entries        // Serialized log entries
) → (
    i32 status,                       // Accept/reject
    i64 last_appended_slot            // Last successfully appended slot
)
```

> **Note:** Non-batched versions (DeadlinePropose, DeadlineConfirm, Replicate) are defined in `luigi.rpc` but not used in the current implementation.

---

## 4. Timestamp Management

### 4.1 Timestamp and Worker ID

Luigi uses two separate fields for transaction ordering:

- **`timestamp`** (i64): Execution deadline in milliseconds since epoch
- **`worker_id`** (i32): Unique coordinator thread ID, sent separately in the Dispatch RPC

This separation provides:
- **Temporal ordering**: Higher timestamp = later in logical time
- **Tie-breaking**: When timestamps match, `worker_id` determines order
- **Paxos stream routing**: Each `worker_id` has its own replication stream

### 4.2 Timestamp Assignment

The coordinator assigns timestamps using the formula:

```
T.timestamp = now() + max_OWD(involved_shards) + headroom
```

Where:
- `now()`: Current wall-clock time in milliseconds
- `max_OWD`: Maximum one-way delay to any involved shard
- `headroom`: Safety margin (configurable, typically 10-30ms)

### 4.3 OWD Measurement

The coordinator periodically pings shards to measure one-way delay:

```cpp
// Ping loop (runs every 100ms)
for each shard:
    start = now()
    send OwdPing(start) to shard
    wait for OwdPingReply
    rtt = now() - start
    owd = rtt / 2

    // Exponential moving average
    owd_estimate[shard] = 0.8 * owd_estimate[shard] + 0.2 * owd
```

---

## 5. Transaction Lifecycle

### 5.1 Complete Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│  COORDINATOR                                                              │
├──────────────────────────────────────────────────────────────────────────┤
│  1. Generate transaction (read set, write set)                           │
│  2. Identify involved shards from key hashing                            │
│  3. Compute timestamp: T = now() + max_OWD + headroom                    │
│  4. Send Dispatch(txn_id, T, ops) to all involved shard leaders          │
│  5. Wait for all DispatchReply messages                                  │
│  6. Verify all agreed_ts values match                                    │
│  7. Return result to client                                              │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  SHARD LEADER                                                             │
├──────────────────────────────────────────────────────────────────────────┤
│  1. Receive Dispatch request                                             │
│  2. Check for timestamp conflicts with pending transactions              │
│  3. If conflict: bump timestamp to max(conflicting_ts) + 1               │
│  4. Insert into timestamp-ordered priority queue                         │
│  5. If multi-shard: run agreement protocol (see Section 6)               │
│  6. Wait until timestamp deadline arrives                                │
│  7. Execute transaction (apply reads/writes to state machine)            │
│  8. Replicate via Paxos (see Section 7)                                  │
│  9. Send DispatchReply with agreed_ts and read results                   │
└──────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Single-Shard Fast Path

Single-shard transactions skip agreement:

```
Coordinator                    Shard Leader
     │                              │
     │──── Dispatch(T=100) ────────►│
     │                              │ Queue at T=100
     │                              │ Wait until now() >= 100
     │                              │ Execute
     │                              │ Replicate
     │◄──── Reply(agreed_ts=100) ───│
     │                              │
```

### 5.3 Multi-Shard Transaction

Multi-shard transactions require agreement:

```
Coordinator              Shard 0 Leader         Shard 1 Leader
     │                        │                       │
     │── Dispatch(T=100) ────►│                       │
     │── Dispatch(T=100) ─────┼──────────────────────►│
     │                        │                       │
     │                        │◄─ DeadlinePropose ────│ (T=105)
     │                        │── DeadlinePropose ───►│ (T=100)
     │                        │                       │
     │                        │   agreed_ts = max(100, 105) = 105
     │                        │                       │
     │                        │◄─ DeadlineConfirm ────│ (T=105)
     │                        │                       │
     │                        │   Execute at T=105    │   Execute at T=105
     │                        │   Replicate           │   Replicate
     │◄─ Reply(agreed=105) ───│                       │
     │◄─ Reply(agreed=105) ───┼───────────────────────│
     │                        │                       │
```

---

## 6. Cross-Shard Agreement

### 6.1 Agreement Protocol

When a transaction spans multiple shards, leaders must agree on a common execution timestamp:

```
Step 1: Exchange Proposals
    Each shard leader sends its proposed_ts to all other involved shards

Step 2: Compute Agreement
    agreed_ts = max(proposed_ts from all shards)

Step 3: Determine Role
    If my_proposed_ts == agreed_ts:
        I am the "winner" - send DeadlineConfirm to others
    Else:
        Wait for DeadlineConfirm from winner

Step 4: Execute
    All shards execute at agreed_ts
```

### 6.2 Conflict Detection and Resolution

Before queuing a transaction, the scheduler checks for conflicts:

```cpp
for each key in transaction.write_set:
    if last_released_ts[key] >= proposed_ts:
        // Conflict! Transaction would execute before a committed write
        proposed_ts = last_released_ts[key] + 1

for each key in transaction.read_set:
    if last_released_ts[key] >= proposed_ts:
        // Conflict! Transaction would read stale data
        proposed_ts = last_released_ts[key] + 1
```

This ensures serializable execution by repositioning late transactions.

### 6.3 Agreement State Machine

```
                    ┌─────────────┐
                    │    INIT     │
                    └──────┬──────┘
                           │ Receive Dispatch
                           ▼
                    ┌─────────────┐
                    │   QUEUED    │
                    └──────┬──────┘
                           │ Multi-shard?
              ┌────────────┴────────────┐
              │ No                      │ Yes
              ▼                         ▼
       ┌─────────────┐           ┌─────────────┐
       │    READY    │           │  PROPOSING  │
       └──────┬──────┘           └──────┬──────┘
              │                         │ All proposals received
              │                         ▼
              │                  ┌─────────────┐
              │                  │  AGREEING   │
              │                  └──────┬──────┘
              │                         │ Confirm sent/received
              │                         ▼
              │                  ┌─────────────┐
              └─────────────────►│  EXECUTING  │
                                 └──────┬──────┘
                                        │ Execution complete
                                        ▼
                                 ┌─────────────┐
                                 │ REPLICATING │
                                 └──────┬──────┘
                                        │ Paxos complete
                                        ▼
                                 ┌─────────────┐
                                 │  COMPLETE   │
                                 └─────────────┘
```

---

## 7. Paxos Replication

### 7.1 Replication Model

Each shard uses Paxos for fault-tolerant replication:
- **Leader**: Handles client requests, proposes log entries
- **Followers**: Accept/reject proposals, apply committed entries

```
Leader                    Follower 1               Follower 2
   │                           │                        │
   │── Replicate(slot, txn) ──►│                        │
   │── Replicate(slot, txn) ───┼───────────────────────►│
   │                           │                        │
   │◄─ ReplicateReply(ok) ─────│                        │
   │◄─ ReplicateReply(ok) ─────┼────────────────────────│
   │                           │                        │
   │   (Majority achieved - committed)                  │
   │                           │                        │
```

### 7.2 Log Structure

```
┌────────────────────────────────────────────────────────────────┐
│  Paxos Log (per shard)                                         │
├──────┬──────────────────────────────────────────────────────────┤
│ Slot │  Entry                                                   │
├──────┼──────────────────────────────────────────────────────────┤
│  1   │  {txn_id=0x1, ts=100, ops=[W(k1,v1), W(k2,v2)]}         │
│  2   │  {txn_id=0x2, ts=105, ops=[R(k1), W(k3,v3)]}            │
│  3   │  {txn_id=0x3, ts=110, ops=[W(k1,v4)]}                   │
│ ...  │  ...                                                     │
└──────┴──────────────────────────────────────────────────────────┘
```

### 7.3 Commit Condition

A transaction is committed when:
1. Executed on the leader
2. Replicated to a majority of replicas in each involved shard

---

## 8. State Machines

### 8.1 Scheduler State Machine

```cpp
class Scheduler {
    // Incoming transactions from RPC layer
    ConcurrentQueue<Entry> incoming_queue_;

    // Timestamp-ordered pending transactions
    PriorityQueue<Entry, by_timestamp> pending_queue_;

    // Ready for execution (deadline passed, agreement complete)
    ConcurrentQueue<Entry> ready_queue_;

    // Per-key last committed timestamp
    HashMap<Key, Timestamp> last_released_ts_;

    // Background threads
    void HoldReleaseTd();  // Moves entries from pending to ready
    void ExecTd();         // Executes ready transactions
};
```

### 8.2 State Machine (Key-Value Store)

```cpp
class StateMachine {
    // Multi-version storage
    HashMap<Key, vector<{Timestamp, Value}>> data_;

    // Read at timestamp T
    Value Read(Key k, Timestamp T) {
        auto& versions = data_[k];
        // Find largest timestamp <= T
        return versions.upper_bound(T)->value;
    }

    // Write at timestamp T
    void Write(Key k, Value v, Timestamp T) {
        data_[k].emplace_back({T, v});
    }
};
```

---

## 9. Data Structures

### 9.1 Transaction Entry

```cpp
struct LuigiLogEntry {
    txnid_t txn_id_;              // Unique transaction ID
    uint64_t proposed_ts_;        // Initially assigned timestamp
    uint64_t agreed_ts_;          // Final agreed timestamp

    vector<Operation> ops_;       // Read/write operations
    vector<uint32_t> shards_;     // Involved shards

    // Agreement tracking
    map<uint32_t, uint64_t> proposals_;  // Shard -> proposed_ts
    atomic<AgreementState> agree_state_;

    // Execution tracking
    atomic<ExecState> exec_state_;
    map<string, string> read_results_;

    // Callback to send reply
    function<void(DispatchReply)> reply_cb_;
};
```

### 9.2 Operation

```cpp
struct Operation {
    OpType type;     // READ or WRITE
    string key;      // Key being accessed
    string value;    // Value (for writes) or empty (for reads)
    uint32_t shard;  // Shard owning this key
};
```


## Appendix A: Configuration

### YAML Configuration Format

```yaml
# Example: 2-shard, 3-replicas per shard
site:
  server:
    s101: "127.0.0.1:31850"  # Shard 0, Replica 0 (Leader)
    s102: "127.0.0.1:31851"  # Shard 0, Replica 1
    s103: "127.0.0.1:31852"  # Shard 0, Replica 2
    s201: "127.0.0.1:31853"  # Shard 1, Replica 0 (Leader)
    s202: "127.0.0.1:31854"  # Shard 1, Replica 1
    s203: "127.0.0.1:31855"  # Shard 1, Replica 2

partition:
  - name: "shard0"
    leader: "s101"
    members: ["s101", "s102", "s103"]
  - name: "shard1"
    leader: "s201"
    members: ["s201", "s202", "s203"]
```

### Command Line Options

**Server:**
```bash
./luigi_server -f <config.yml> -P <partition_name> -b <benchmark> -w <warehouses>
```

**Coordinator:**
```bash
./luigi_coordinator -f <config.yml> -b <benchmark> -d <duration> -t <threads> -w <owd_ms> -x <headroom_ms>
```

---

## Appendix B: Performance Characteristics

### Throughput Factors

1. **Thread count**: Linear scaling up to CPU saturation
2. **Network latency**: Lower latency → higher throughput
3. **Cross-shard rate**: More cross-shard → more agreement overhead
4. **Replication factor**: More replicas → more replication overhead

---

*For implementation details and benchmark results, see [README.md](README.md).*
