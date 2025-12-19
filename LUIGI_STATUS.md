# Luigi Status Summary

## ✅ **WORKING**
1. **Single shard execution**: 80.95 tps achieved
2. **Complete architecture wiring**:
   - Transport layer (FastTransport + eRPC) ✅
   - Helper servers (pull from queues) ✅
   - LuigiReceiver (request handling) ✅
   - SchedulerLuigi (3 background threads) ✅
   - LuigiExecutor (transaction execution) ✅
3. **Cross-shard RPC communication**: Messages send and receive successfully
4. **Local transaction handling**: All-local transactions work perfectly

## ❌ **NOT WORKING**
1. **Multi-shard crashes**: Shard 1 crashes with `terminate called after throwing an instance of 'int'`
   - Crash happens AFTER successfully handling cross-shard requests
   - Crash is in a background thread (scheduler or executor)
   - Not in the main RPC handler thread

## 🔍 **Current Investigation**

### Crash Details
- **Location**: Background thread (likely scheduler or executor)
- **Error**: `terminate called after throwing an instance of 'int'`
- **Timing**: AFTER `[RECEIVER] Request #0: Handled` log
- **Hypothesis**: Some Mako STO code (`Transaction::Abort()`) is being called even though Luigi doesn't use STO

### Next Steps
1. Find where the `int` exception originates (not from Luigi's code)
2. Check if there's shared infrastructure (logging, verification macros) that throws `int`
3. Add exception handling in scheduler/executor background threads
4. Consider wrapping all background thread loops in try-catch

## 📊 **Performance**
- Single shard: **80.95 txns/sec** (with 2 threads, 10s duration)
- Multi-shard: **CRASHES**

## 🏗️ **Architecture** (Fully Traced)
See `LUIGI_FLOW_TRACE.md` for complete end-to-end flow documentation.

**Client → Transport → Helper → Receiver → Scheduler → Executor → Response**

