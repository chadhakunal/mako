# Luigi Architecture Analysis

## Current Issue: Client-Only Mode

Luigi benchmark currently runs in **CLIENT-ONLY** mode:
- Each shard generates transactions and sends requests
- **BUT**: No servers exist to receive/handle those requests!
- Result: All requests fail with RPC errors (cannot connect to remote shards)

## Mako's Working Architecture

### 1. Transport Layer
`setup_erpc_server()` creates FastTransport threads that:
- Listen on network ports  
- Receive incoming RPC requests
- Enqueue them into `HelperQueue`s

### 2. Server Layer  
`setup_helper()` creates helper threads that:
- Pull requests from `HelperQueue`s
- Create `ShardServer` → `ShardReceiver` 
- Call `ReceiveRequest()` to handle business logic
- Send responses back

### 3. Client Layer
Benchmark workers:
- Generate transactions
- Use `ShardClient` to send requests via transport
- Wait for responses

## What Luigi Needs

For Luigi to work in 2-shard mode, **each process must be both client AND server**:

```
Process A (Shard 0):
  [Client Side]               [Server Side]
  - Generates txns      -->   - Receives requests from Shard 1
  - Sends to Shard 1          - Handles via LuigiReceiver
                              - Sends responses back

Process B (Shard 1):
  [Client Side]               [Server Side]
  - Generates txns      -->   - Receives requests from Shard 0  
  - Sends to Shard 0          - Handles via LuigiReceiver
                              - Sends responses back
```

## Implementation Options

### Option 1: Integrate with Mako's Helper Architecture (Recommended)
Follow Mako's pattern:
1. Keep `setup_luigi_transport()` (creates transports) ✅
2. Add `setup_luigi_helper()` (creates server threads):
   ```cpp
   void helper_server_luigi(...) {
     LuigiReceiver *receiver = new LuigiReceiver(config_file);
     receiver->InitScheduler(shard_idx);
     
     while (true) {
       // Pull from HelperQueue
       TransportRequestHandle *req = queue->fetch_one_req();
       
       // Handle request  
       size_t resp_len = receiver->ReceiveRequest(
         req->GetRequestType(),
         req->GetRequestBuffer(),
         req->GetResponseBuffer()
       );
       
       // Send response
       req->EnqueueResponse(resp_len);
     }
   }
   ```

3. Call both in `luigi_bench_main.cc`:
   ```cpp
   setup_luigi_transport(...);  // Creates transports
   setup_luigi_helper(...);      // Creates servers
   // Now both client and server sides work!
   ```

### Option 2: Single-Shard Testing
Test Luigi with NO cross-shard communication:
- Run only 1 shard
- All transactions are local
- No RPC requests needed
- Validates Luigi's core transaction logic

### Option 3: Mock Server
Create a minimal mock server just for testing:
- Responds to all requests with "OK"
- Doesn't execute actual Luigi logic  
- Just proves transport connectivity

## Why Current Code Fails

```
Shard 0 Process:
  ✅ Transport created (listening on port 31011-31016)
  ✅ Client sends request to Shard 1 
  ❌ NO SERVER to handle incoming requests from Shard 1!

Shard 1 Process:
  ✅ Transport created (listening on port 32011-32016)
  ✅ Client sends request to Shard 0
  ❌ NO SERVER to handle incoming requests from Shard 0!

Result: Both fail with "RPC error: 2" (connection/timeout)
```

## Next Steps

1. **Immediate**: Test with single shard to validate core Luigi logic
   ```bash
   # Modify test script to run only 1 shard
   ./build/luigi_bench --shard-config ... --shard-index 0 --num-threads 6
   ```

2. **Short-term**: Implement `setup_luigi_helper()` following Mako's pattern

3. **Long-term**: Consider if Luigi should use queues or direct RPC handling

## Key Files

- `src/mako/benchmarks/rpc_setup.cc` - Mako's server setup (reference)
- `src/deptran/luigi/luigi_transport_setup.cc` - Luigi transport (current)
- `src/deptran/luigi/luigi_receiver.cc` - Luigi request handler (exists but unused)
- `src/deptran/luigi/luigi_scheduler.cc` - Luigi business logic (exists but unreachable)

## Trace: How Mako Request Flows

1. Client: `ShardClient::InvokeGet()` → `transport->SendRequestToShard()`
2. Transport (Shard 1): Receives packet → Enqueues to `HelperQueue[par_id]`
3. Helper thread: `queue->fetch_one_req()` → `receiver->ReceiveRequest()`  
4. `ShardReceiver::HandleGetRequest()` → DB lookup → Write response
5. Transport: `req->EnqueueResponse()` → Sends packet back
6. Client: Receives response → Callback → Transaction continues

Luigi has steps 1-2 and 5-6, but **missing step 3-4** (the server side)!

