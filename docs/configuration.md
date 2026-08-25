# Configuration

Planner settings use Flink's normal configuration surface and can be supplied in `flink-conf.yaml`,
the SQL client, or `TableConfig` (for example, `SET 'streamfusion.native.enabled' = 'false'`). A
same-named `-Dstreamfusion.*` system property remains a compatibility fallback. Task-runtime tuning
settings are still JVM properties before 1.0; they are called out below and will become
serialized operator configuration rather than process-global state before a stable release.

## Acceleration control

- **`-Dstreamfusion.native.enabled=false`** — master switch; run entirely on stock Flink.
- **`-Dstreamfusion.operator.<name>.enabled=false`** — keep one operator on the host (e.g. leave a
  lone cheap `filter` on a row source, which can't earn back the transpose round-trip). A switch
  covers every shape that reaches the same native operator — e.g. `groupAggregate` also covers the
  two-phase global half, `windowRank` also covers window deduplication. All default on.
  Optional connector operators only activate once the matching connector extension and format JAR
  are installed; otherwise the plan falls back to Flink's own path.
- **`-Dstreamfusion.expression.<NAME>.allowIncompatible=true`** — opt into the faster pure-Rust
  path for expressions that otherwise use a byte-exact JVM upcall or fall back (`UPPER`/`LOWER`,
  `REGEXP_EXTRACT`, `DATE_FORMAT`/`EXTRACT` over `TIMESTAMP_LTZ`, `ROUND` on float, transcendental
  math). Off by default — parity-first. See the per-operator/expression pages under
  [Operators](operators/index.md) for exactly which functions this affects and how they can diverge.
- **`-Dstreamfusion.plan.shareSources=false`** — disable the substitution pass's own source-sharing
  (two branches scanning the same native source normally collapse into one read); restores one
  source per branch.
- **`-Dstreamfusion.exchange.zeroCopyLocal=...`** — vouch for a single-TaskManager deployment so the
  columnar exchange can hand off batches by ownership transfer instead of Arrow IPC on a same-process
  edge. See [Zero-copy local shuffle](optimizations/zero-copy-local-shuffle.md).
- **`-Dstreamfusion.exchange.coalesceRows`** (default 4096) / **`-Dstreamfusion.exchange.coalesceLatencyMs`**
  (default 50) — re-assemble processing-sized batches after the keyed exchange fragments them across
  parallel subtasks. See [Post-exchange batch coalescing](optimizations/post-exchange-coalescing.md).

## Memory

See [Memory management](memory-management.md) for the authoritative pool, covered consumers,
exhaustion behavior, sizing, and metrics.

- **`taskmanager.memory.task.off-heap.size`** — the single TaskManager-wide authority for
  StreamFusion memory. It must be greater than zero. Native operator state and DataFusion working
  memory and Arrow FFI buffers reserve from this shared cap. A denied reservation fails with a
  `NativeMemoryLimitException` naming this normal Flink setting.
- **`state.backend.rocksdb.memory.*`** — Flink's normal RocksDB memory-control options size the
  native stores' shared block cache and write-buffer manager under the RocksDB backend; there are
  no StreamFusion-specific state memory settings. See [RocksDB backend](backends/rocksdb.md).

### Off-heap sizing

Size `taskmanager.memory.task.off-heap.size` for the peak aggregate of all StreamFusion consumers in
one TaskManager, not per operator:

- Arrow FFI buffers are process-wide and bounded by in-flight batches.
- Native operator state and DataFusion working reservations vary with the query. The in-memory state
  backend fails when it cannot reserve more. Under the RocksDB backend, state lives in RocksDB's own
  memtables and block cache (bounded by `state.backend.rocksdb.memory.*`), and only the per-batch
  working set reserves from this cap.

Kafka client buffers are owned by Flink's Java source/sink and remain under the normal Flink/JVM
memory model; StreamFusion charges only its Arrow batches and native codec/operator work here.

### Live metrics

StreamFusion exports the shared pool and operator state to the Flink UI/metrics reporter:

- `nativeOffHeapCapacityBytes` — configured TaskManager task off-heap capacity.
- `nativeOffHeapReservedBytes` / `nativeOffHeapAvailableBytes` — current aggregate usage/headroom.
- `nativeOffHeapPeakBytes` — process-wide reservation high-water mark.
- `nativeOffHeapDeniedReservations` — number of rejected growth requests.
- `nativeArrowAllocatorBytes` — the process-wide Arrow FFI allocator's current footprint.
- `nativeStateBytes` — tracked state drawing on that budget, sampled per batch.

## Diagnostics

- **`-Dstreamfusion.logFallbackReasons=true`** — substitution is silent by default; this logs each
  plan node that stayed on Flink and why, as the plan is decided. `EXPLAIN` shows native nodes (e.g.
  `NativeCalc`) directly for an accelerated plan.

See [Benchmarks](benchmarks.md) for how to reproduce the throughput numbers, and
[Optimizations](optimizations/index.md) for how these flags map to specific performance techniques.
