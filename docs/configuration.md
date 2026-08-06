# Configuration

StreamFusion-specific runtime flags are JVM system properties. Flink-owned settings continue to use
Flink's normal configuration surface.

## Acceleration control

- **`-Dstreamfusion.native.enabled=false`** — master switch; run entirely on stock Flink.
- **`-Dstreamfusion.operator.<name>.enabled=false`** — keep one operator on the host (e.g. leave a
  lone cheap `filter` on a row source, which can't earn back the transpose round-trip). A switch
  covers every shape that reaches the same native operator — e.g. `groupAggregate` also covers the
  two-phase global half, `windowRank` also covers window deduplication. All default on.
  `kafkaSource` (and `flussSource`) only activate once the matching connector extension and format
  JAR are installed; otherwise the plan falls back to Flink's own connector path.
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
exhaustion behavior, Paimon flushing, sizing, and metrics.

- **`taskmanager.memory.task.off-heap.size`** — the single TaskManager-wide authority for
  StreamFusion memory. It must be greater than zero. Native operator state and DataFusion working
  memory, Arrow FFI buffers, native Kafka consumer queues, and native exactly-once producer queues
  all reserve from this shared cap. A denied reservation fails with a
  `NativeMemoryLimitException` naming this normal Flink setting.
- **`-Dstreamfusion.state.paimon.write-buffer-mb`** (default 64) — flush a Paimon backend's native
  in-memory write buffer into immutable local files once it reaches this size. StreamFusion may
  lower the effective threshold under TaskManager-wide memory pressure. This local flush is
  independent of Flink checkpoint timing; a later checkpoint snapshots and uploads the already
  materialized local files.

### Off-heap sizing

Size `taskmanager.memory.task.off-heap.size` for the peak aggregate of all StreamFusion consumers in
one TaskManager, not per operator:

- **`-Dstreamfusion.kafka.prefetch-mb`** (default 256, capped at 2 GiB) — the native Kafka source's
  off-heap prefetch budget, **per source subtask**.
- The native Kafka **sink** mirrors the Java client's `buffer.memory` (default 32 MiB) **per live
  producer**. Allow for an active producer and the next checkpoint's warming producer during
  handover.
- Arrow FFI buffers are process-wide and bounded by in-flight batches.
- Native operator state and DataFusion working reservations vary with the query. The in-memory state
  backend fails when it cannot reserve more. The Paimon backend proactively flushes its write buffer
  to local files at its threshold or when shared headroom is low; an allocation that still cannot
  reserve from the shared cap fails normally.

Worked example: 4 Kafka source subtasks and 2 sink subtasks on one TaskManager at the defaults need
at least `4 × 256 MiB + 2 × 32 MiB ≈ 1.1 GiB` of task off-heap before Arrow's in-flight batches and
native operator state, and temporarily another 64 MiB if both sinks have a warming producer. The
Java Kafka producer's own heap allocations remain under Flink/JVM heap sizing; only the native
exactly-once producer path is charged here.

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
