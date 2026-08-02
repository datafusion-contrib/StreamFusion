# Configuration

Every runtime flag is a JVM system property, mirroring DataFusion Comet's config surface.

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

- **`-Dstreamfusion.memory.accounting.enabled`** (default on) — native stateful operators reserve an
  operator-scope share of the slot's managed memory from Flink's `MemoryManager` and bound their
  state by it, failing with a `NativeMemoryLimitException` naming the remedy rather than an
  unattributed OOM.
- **`-Dstreamfusion.memory.arrow.max-mb`** — optional cap on the shared Arrow FFI allocator (the
  buffers crossing the native↔JVM boundary). Uncapped by default; an allocation past the cap fails
  naming the knob instead of growing the process.

### Off-heap sizing

Some native memory appears in **no Flink memory figure** — not heap, not managed memory, not
network buffers — and must be budgeted under `taskmanager.memory.task.off-heap.size` (or equivalent
container headroom), or a backpressured job gets OOM-killed with no attribution:

- **`-Dstreamfusion.kafka.prefetch-mb`** (default 256, capped at 2 GiB) — the native Kafka source's
  off-heap prefetch budget, **per source subtask**.
- The native Kafka **sink** mirrors the Java client's `buffer.memory` (default 32 MiB) **per sink
  subtask**.
- The Arrow FFI allocator above is process-wide and, absent a cap, transient and small at steady
  state — bounded by in-flight batches, not held state.

Worked example: 4 Kafka source subtasks and 2 sink subtasks on one TaskManager at the defaults need
`4 × 256 MiB + 2 × 32 MiB ≈ 1.1 GiB` of task off-heap before Arrow's in-flight batches. Native
*operator state* is the one exception — with accounting on (the default) it draws on Flink's managed
memory, already inside Flink's process model, and needs no extra off-heap allowance.

### Live metrics

Every accounted native operator exports three metrics to the Flink UI/metrics reporter, alongside
its ordinary JVM numbers:

- `nativeStateBudgetBytes` — the operator's reserved managed-memory budget.
- `nativeStateBytes` — tracked state drawing on that budget, sampled per batch.
- `nativeArrowAllocatorBytes` — the process-wide Arrow FFI allocator, sampled per batch.

## Diagnostics

- **`-Dstreamfusion.logFallbackReasons=true`** — substitution is silent by default; this logs each
  plan node that stayed on Flink and why, as the plan is decided. `EXPLAIN` shows native nodes (e.g.
  `NativeCalc`) directly for an accelerated plan.

See [Benchmarks](benchmarks.md) for how to reproduce the throughput numbers, and
[Optimizations](optimizations/index.md) for how these flags map to specific performance techniques.
