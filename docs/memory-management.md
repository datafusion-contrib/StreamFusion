# Memory management

StreamFusion uses Flink's normal `taskmanager.memory.task.off-heap.size` setting as the single
memory authority for one TaskManager process. Every StreamFusion task in that process shares the
same pool, so spare capacity is available to whichever operator or connector needs it rather than
being divided into fixed per-slot or per-operator slices.

The setting must be greater than zero. It is already part of Flink's TaskManager process-memory
model, so increasing it also increases the total process memory Flink derives unless total process
memory is configured explicitly. StreamFusion does **not** reserve Flink managed memory; the one
consumer *sized* from the managed-memory share is the RocksDB backend's native store pool, which
follows Flink's `state.backend.rocksdb.memory.*` semantics (see the
[RocksDB backend](backends/rocksdb.md)) but allocates natively outside the managed pool.

## What is charged

The shared cap currently covers:

- native operator state and DataFusion working-memory reservations; and
- Arrow buffers allocated by the shared Arrow C Data Interface allocator.

Reservations are acquired before the corresponding native capacity is made available and are
released with their owner. Arrow allocations are charged and released at their actual allocation
lifetime; operator and zero-copy batch handles also release ownership on normal close and task
cancellation.

Parquet sink writer allocations are not yet connected to this authority. Small allocations made
inside native libraries, allocator metadata, thread stacks, loaded libraries, and other process
overhead are also outside the reservation pool. The cap is therefore an allocation authority for
the large StreamFusion-controlled consumers above, not an RSS or container hard limit. Leave
process headroom for those costs and for the JVM.

## Exhaustion and spilling

A reservation that would cross the TaskManager-wide cap is denied with
`NativeMemoryLimitException`; its message identifies the consumer and
`taskmanager.memory.task.off-heap.size`.

The in-memory state backend cannot spill, so a denied growth request fails the task and Flink's
configured restart policy applies. Under the RocksDB backend, native state is written through to
RocksDB at every batch, so an operator's resident working set stays bounded by its batch size;
RocksDB's own memtables and block cache hold the rest, governed by Flink's
`state.backend.rocksdb.memory.*` options rather than the task off-heap pool (see the
[RocksDB backend](backends/rocksdb.md) memory section).

## Sizing

Size `taskmanager.memory.task.off-heap.size` for the peak aggregate across every StreamFusion task
running in one TaskManager:

- Add peak Arrow in-flight buffers plus native operator state and DataFusion working memory.

Flink's Kafka consumer and producer buffers are Java-client memory and use Flink's normal JVM
memory model. Use the native high-water mark to size the Arrow and operator working set.

## Metrics

StreamFusion registers these gauges with each native operator or connector metric group:

- `nativeOffHeapCapacityBytes` — configured TaskManager task off-heap capacity;
- `nativeOffHeapReservedBytes` — current aggregate reservations;
- `nativeOffHeapAvailableBytes` — remaining shared capacity;
- `nativeOffHeapPeakBytes` — process-wide reservation high-water mark;
- `nativeOffHeapDeniedReservations` — rejected growth requests;
- `nativeArrowAllocatorBytes` — current shared Arrow allocator footprint; and
- `nativeStateBytes` — the operator's sampled native state footprint.

See [Configuration](configuration.md#memory) for the related settings and
[RocksDB backend](backends/rocksdb.md) for the RocksDB write path, memory pool, and checkpoint
semantics.
