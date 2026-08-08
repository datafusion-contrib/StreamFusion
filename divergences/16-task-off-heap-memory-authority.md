# Memory accounting: TaskManager task off-heap is the shared authority

**Kind:** structural — how native memory is accounted against the host's process model.  
**Influenced by:** Comet's `MemoryPool → JNI → TaskMemoryManager` shared-pool design.  
**Forced by parity:** no — this is a resource-control decision.

## Why Flink task off-heap

Flink's managed-memory manager is designed around declared use-case weights and up-front binary
reservations. It does not expose Spark's partial execution-memory grants or cooperative spill
contract. StreamFusion also has native allocations that do not fit an operator managed-memory
weight because Arrow batches cross operator lifetimes.

The normal `taskmanager.memory.task.off-heap.size` setting already represents memory owned by task
code but allocated outside the JVM heap. StreamFusion therefore treats that configured size as a
real runtime cap, rather than only a process-sizing hint.

## What we do

- A process-wide JVM pool is initialized from `taskmanager.memory.task.off-heap.size` and rejects
  aggregate reservations beyond that capacity.
- Each native operator receives an owner handle. Its DataFusion `MemoryPool` crosses JNI when the
  operator's per-bundle footprint grows or shrinks, so all operators share unused headroom.
- The shared Arrow allocator reserves and releases bytes with Arrow buffer lifetimes.
- A denied reservation surfaces as `NativeMemoryLimitException` and names the Flink setting to
  increase. Metrics expose capacity, current/available/peak bytes, denials, and Arrow usage.

State footprint remains incremental and off the per-row hot path. The in-memory state backend cannot
spill and fails on denial. The RocksDB backend can flush its mutable buffer into local files
when it reaches its configured threshold or memory is tight; Flink checkpoints later make those
files durable, just as checkpointing a local-disk state backend is separate from its ordinary flushes.

## Deliberate exclusions

The Parquet and Fluss sources are not yet wired to this pool. Flink's Java Kafka client memory remains
part of the JVM heap, not task off-heap. The cap is process-local, matching Flink's ordinary
one-TaskManager-per-process deployment model.
