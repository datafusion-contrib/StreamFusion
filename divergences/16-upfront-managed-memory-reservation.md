# Memory accounting: reserve the budget up front, no per-grow JNI upcall

**Kind:** structural — how native state is accounted against the host's memory budget.
**Diverges from:** Comet's `MemoryPool → JNI → CometTaskMemoryManager → TaskMemoryManager`
per-allocation upcall.
**Forced by parity:** no — forced by Flink's memory-manager semantics.

## Comet's mechanism, and why it does not transplant

Comet implements DataFusion `MemoryPool`s whose `try_grow`/`shrink` cross JNI on every
grant and defer to Spark's `TaskMemoryManager.acquireExecutionMemory`. That fits Spark
because Spark's unified memory manager is *cooperative*: it hands out partial grants and
asks sibling consumers to spill, so asking on every grow buys real elasticity — a task's
native pool can borrow whatever its peers are not using.

Flink's `MemoryManager.reserveMemory` is binary: the reservation either fits the slot's
remaining managed memory or throws. There are no partial grants and no spill negotiation.
A well-behaved consumer must also stay inside the fraction Flink computed for it from the
declared managed-memory weights — exceed it and you starve the slot's other consumers
(e.g. a RocksDB state backend). Cap the pool at its fraction and per-grow upcalls buy
nothing: every grow inside the cap succeeds, every grow past it fails, which is exactly
what a local check against a fixed budget decides — without a JNI crossing per batch.

## What we do

The reserve-up-front model Flink itself uses for its flagship off-heap consumer (the
RocksDB block cache via `getSharedMemoryResourceForManagedMemory`):

- The native operator's transformation declares an operator-scope
  `ManagedMemoryUseCase.OPERATOR` weight (`streamfusion.memory.operator-weight-mb`).
- At open, the operator computes its fraction's byte size and reserves it once
  (`MemoryManager.reserveMemory`); at close it releases (`releaseAllMemory`). Flink sees
  the full budget for the operator's lifetime, exactly as it sees RocksDB's cache.
- The budget crosses JNI once, at handle creation. Natively it becomes a bounded
  DataFusion pool (`GreedyMemoryPool`) with a per-operator `MemoryReservation` — the same
  seam Comet uses, so DataFusion-executed fragments plug into the identical pool later.
- The operator tracks its state footprint incrementally (per touched group, DataFusion
  `Accumulator::size` + key size) and resizes the reservation per batch. A denied resize
  is a `NativeMemoryLimitException` naming the budget and the remedy — the accounted
  replacement for an unattributed container OOM, since streaming operators here have no
  runtime spill to fall back to (research §4).

## Scope: what draws on the budget, and what deliberately does not

- **Operator state** — every stateful native operator tracks its footprint against its
  reservation (the list lives in the readme's "Controlling acceleration" section).
- **DataFusion-executed fragments** — where an operator delegates work to DataFusion's
  execution (the interval/window joins run their match as a `HashJoinExec` over the
  buffered batches), that plan runs under a `TaskContext` built on a `RuntimeEnv` sharing
  the operator's bounded pool (comet's `RuntimeEnvBuilder::with_memory_pool`), so the
  join's transient build side draws on the same budget as the buffered state and a denial
  fails with the same remedy message.
- **File scans are NOT pool-wired, on purpose.** DataFusion 53's scan path (the Parquet
  opener/source and the datasource file streams) registers no memory consumers — only its
  `ParquetSink` does, and our sink writes via arrow's `ArrowWriter` directly. Wiring a pool
  into the scan's `TaskContext` would enforce nothing, while plumbing a budget to the
  reader would add host surface (a `BulkFormat` has no `MemoryManager` access) for zero
  effect. Revisit if a DataFusion upgrade adds scan-side reservations.
- **The Java-side FFI Arrow allocator** (`NativeAllocator.SHARED`) stays outside the
  budget: its buffers are transient per-batch and refcounted — accounting is the pool's
  job, not the allocator's (see the class comment). Revisit only if transpose buffering
  ever becomes long-lived.
- **No runtime spill**: a denied reservation is a hard failure because these streaming
  operators have nothing to spill to; the structural answer is disaggregated/cached state
  (ticket 37).

## What this costs, and the escape hatch

Elasticity: one operator cannot borrow another's unused budget mid-flight (Comet's
task-shared pool allows that within a task). In exchange the hot path pays zero JNI and
Flink's slot arithmetic stays honest. If cross-operator sharing ever matters, the
Rust-side seam is already the `MemoryPool` trait — swapping the per-operator
`GreedyMemoryPool` for a slot-shared pool handle (the `OpaqueMemoryResource` route) is a
contained change that does not touch the operators' accounting.
