# Luigi Complete Flow Trace

## Architecture Wiring (VERIFIED ✅)

### Step 1: Initialize OWD Service
- `luigi_bench_main.cc:208` - Initialize `LuigiOWD` for calculating expected timestamps
- Status: ✅ Working

### Step 2: Setup Transport Infrastructure  
- `luigi_bench_main.cc:215` → `setup_luigi_transport()` → `mako::setup_erpc_server()`
- Creates 6 FastTransport threads (one per eRPC server)
- Each listens on ports 31011-31016
- Creates HelperQueues for REMOTE partitions:
  - **Shard 0:** queues for par_id 6-11 (warehouses on shard 1)
  - **Shard 1:** queues for par_id 0-5 (warehouses on shard 0)
- Status: ✅ Working

### Step 3: Create Luigi Server
- `luigi_bench_main.cc:228` - Create `LuigiReceiver`
- `luigi_bench_main.cc:231` - Initialize `SchedulerLuigi`
  - Creates 3 background threads:
    - `HoldReleaseTd`: Conflict detection & priority queue
    - `ExecTd`: Execute transactions  
    - `WatermarkTd`: Broadcast watermarks
- Status: ✅ Working

### Step 4: Setup Luigi Helper Threads
- `luigi_bench_main.cc:237` → `setup_luigi_helper()`
- Creates helper threads that:
  - Pull from HelperQueues (populated by FastTransport)
  - Call `LuigiReceiver::ReceiveRequest()`
  - Send responses back
- Status: ✅ Working

### Step 5: Create Benchmark Client
- `luigi_bench_main.cc:245` - Create `LuigiBenchmarkClient`
- `luigi_bench_main.cc:247` - Initialize (creates `LuigiClient`)
- Status: ✅ Working

### Step 6: Run Benchmark
- `luigi_bench_main.cc:274` - Start worker threads
- Each thread runs transaction loop
- Status: ✅ Working

## Transaction Flow (WITH LOGGING)

### Client Side (Transaction Generation & Dispatch)

1. **WorkerThread** (`luigi_benchmark_client.cc:135`)
   - Generates transactions in loop
   - Calls `DispatchOneTransaction()`

2. **DispatchOneTransaction** (`luigi_benchmark_client.cc:160`)
   - Calls `generator_->GetTxnReq()` to generate transaction
   - Assigns unique txn_id
   - Logs: `[CLIENT-DISPATCH] TXN-X: Generated`
   - Calls `DispatchRequest()`

3. **DispatchRequest** (`luigi_benchmark_client.cc:198`)
   - Creates `LuigiDispatchBuilder` for each target shard
   - **Sets target_server_id** = source_shard * warehouses_per_shard
     - Shard 0: target_server_id = 0  
     - Shard 1: target_server_id = 6
   - Logs: `[CLIENT-DISPATCH] TXN-X: Sending FROM shard_A TO shard_B, target_server_id=Y`
   - **Filters out local shard** (skips if shard_id == local_shard)
   - Calls `luigi_client_->InvokeDispatch()`

4. **LuigiClient::InvokeDispatch** (`luigi_client.cc:200`)
   - Logs: `[LUIGI-CLIENT] TXN-X: InvokeDispatch for N shards`
   - Calls `rpc_transport_->SendBatchRequestToAll()`
   - Logs: `[LUIGI-CLIENT] TXN-X: RPC sent`

### Network Layer (Transport)

5. **FastTransport::SendBatchRequestToAll**
   - Sends request over network to remote shard
   - Request contains: txn_id, ops, target_server_id, etc.

6. **FastTransport::RequestHandler** (on receiving shard)
   - Receives incoming request from network
   - Extracts `target_server_id` from request  
   - Looks up queue: `queue_holders_[target_server_id]`
   - **ERROR HERE:** If target_server_id not in queues → "No helper queue found"
   - If found: Enqueues to HelperQueue

### Server Side (Request Handling & Execution)

7. **LuigiHelperServer::Run** (`luigi_helper.cc:50`)
   - Pulls requests from HelperQueue
   - Logs: `[HELPER-SERVER] Shard-X: Pulled request #N`
   - Calls `receiver_->ReceiveRequest()`

8. **LuigiReceiver::ReceiveRequest** (`luigi_server.cc:42`)
   - Logs: `[RECEIVER] Request #N: type=14`
   - Routes to `HandleDispatch()`
   - Logs: `[RECEIVER] Request #N: Handled`

9. **LuigiReceiver::HandleDispatch** (`luigi_server.cc:148`)
   - Parses operations from request
   - Logs: `[RECEIVER-DISPATCH] TXN-X: HandleDispatch called`
   - Calls `scheduler_->LuigiDispatchFromRequest()`

10. **SchedulerLuigi::LuigiDispatchFromRequest** (`luigi_scheduler.cc:88`)
    - Creates `LuigiLogEntry`
    - Logs: `[SCHEDULER-DISPATCH] TXN-X: Received dispatch`
    - Enqueues to `incoming_txn_queue_`
    - Logs: `[SCHEDULER-QUEUE] TXN-X: Enqueued`

11. **SchedulerLuigi::HoldReleaseTd** (`luigi_scheduler.cc:226`)
    - Background thread pulls from `incoming_txn_queue_`
    - Logs: `[HOLD-RELEASE-THREAD] Dequeued N txns`
    - Does conflict detection
    - Adds to `priority_queue_`
    - When deadline passes: moves to `ready_txn_queue_`
    - Logs: `[HOLD-RELEASE-THREAD] TXN-X: Released to ready queue`

12. **SchedulerLuigi::ExecTd** (`luigi_scheduler.cc:341`)
    - Background thread pulls from `ready_txn_queue_`
    - Logs: `[EXEC-THREAD] Dequeued N txns`
    - Calls `executor_.Execute()`

13. **LuigiExecutor::Execute** (`luigi_executor.cc:25`)
    - Logs: `[EXECUTOR] TXN-X: Execute called`
    - Calls `ExecuteViaStateMachine()` or `ExecuteAllOps()`
    - Calls `scheduler_->Replicate()`

14. **SchedulerLuigi::Replicate** (`luigi_scheduler.cc:767`)
    - Logs: `[SCHEDULER-REPLICATE] TXN-X: Replicating`
    - Updates watermarks
    - **TODO:** Integrate Paxos here

15. **Callback** - Back to `LuigiReceiver::HandleDispatch`
    - Stores result
    - Returns response to client

## Current Issues

### Issue: Queue Routing
**Symptom:** "No helper queue found for server_id 0"

**Root Cause:** `target_server_id` mismatch

**Current Setting:**
- Shard 0 → Shard 1: target_server_id = 0 (source shard 0 * 6)
- Shard 1 → Shard 0: target_server_id = 6 (source shard 1 * 6)

**Queue Setup:**
- Shard 0 queues: par_id 6-11  
- Shard 1 queues: par_id 0-5

**What Happens:**
- Shard 0 sends with target_server_id=0 → Shard 1 looks for queue 0 ✅ FOUND
- Shard 1 sends with target_server_id=6 → Shard 0 looks for queue 6 ✅ FOUND

**So why the error?** Need to investigate further - maybe OWD pings or other control messages?

## Logging Added

All touchpoints now have logging:
- `[CLIENT-*]` - Client-side transaction generation
- `[LUIGI-CLIENT]` - LuigiClient RPC layer
- `[HELPER-SERVER]` - Helper thread queue pulling
- `[RECEIVER]` - Request reception and routing  
- `[SCHEDULER-*]` - Scheduler queueing and coordination
- `[EXECUTOR]` - Transaction execution
- `[TRACE]` - Main initialization steps

