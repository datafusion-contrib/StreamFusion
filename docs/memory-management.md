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
Native DataFusion pools cache the Java authority class on the calling thread, so
worker callbacks retain the correct application classloader. The internal bounded
runtime additionally keeps its owner registration alive through the pool's final
reference, including asynchronously cancelled workers; it does not reclaim their
balance at Java operator close. Bounded sort copies borrowed Java input into compact
native buffers under a reusable reservation before DataFusion retains it. Output
uses the reverse handoff: native reserves the copy, Java imports/charges its Arrow
buffers, then acknowledges that transfer so native can retire its copy reservation.
Temporary overlap covers actual handoff allocations rather than charging every
retained sort run through both allocators. Spillable sort state leaves headroom
within the same process cap for the next input/copy/output; other consumers may use
that headroom. See the [bounded runtime](batch-execution-runtime.md) for the lifetime
and exhaustion controls.
Mini-batch cleanup skips native flushes when initialization has not acquired a state handle or
completed bundle setup. This also applies after the handle is closed: cleanup must preserve the
original initialization failure rather than enter JNI with an absent handle. Partially acquired
handles and reservations are still released by normal close; initialized operators retain their
normal end-of-input and checkpoint flushes.
Before a row-to-Arrow conversion hands its batch to a consumer, the converter owns every allocated
vector. A rejected allocation or a row conversion error closes those vectors, including the
changelog sidecar, before propagating the original failure. Cleanup errors are attached as
suppressed exceptions so they cannot hide the allocation or conversion error.

Parquet sink writer allocations are not yet connected to this authority. Small allocations made
inside native libraries, allocator metadata, thread stacks, loaded libraries, and other process
overhead are also outside the reservation pool. The cap is therefore an allocation authority for
the large StreamFusion-controlled consumers above, not an RSS or container hard limit. Leave
process headroom for those costs and for the JVM.

## Arrow allocation during startup

A connector can create its decoder schema before another task initializes the process memory
budget. StreamFusion tracks those early Arrow allocations and adopts the live balance when the
TaskManager budget becomes available. Buffers already released do not consume that budget.
Initialization publishes the capacity only after the existing buffers are charged, so a source
thread can release them safely while other tasks start.

If the configured cap cannot cover the live startup buffers, initialization fails with the normal
memory-limit exception. Their ownership remains intact for cleanup or a later initialization with
sufficient capacity. Startup schema and buffer overhead contributes to the same cap and high-water
mark as later allocations; no separate configuration or per-operator allocator is introduced.

## Arrow schema and buffer ownership

C Data import transfers the array release callback using the existing Comet-style ownership
pattern. Imported arrays retain their input buffers after producer objects close. Field and
root schema metadata survive both ordinary and cached-schema import; cached declarations
remain immutable.

Flink sources can carry actual NULL slots beneath a NOT NULL logical hint. Import widens only
those top-level physical Arrow fields before batch coalescing, shuffle or IPC validates the
batch. It shares the same arrays and validity buffers, keeps the SQL logical type unchanged
and leaves Flink's later sink constraints in place. No additional data copy, reservation or
release callback is introduced. Calc column projections and JVM callback arguments also retain
field metadata needed to reconstruct Flink's collection copy categories.

## Exhaustion and spilling

A reservation that would cross the TaskManager-wide cap is denied with
`NativeMemoryLimitException`; its message identifies the consumer and
`taskmanager.memory.task.off-heap.size`.
Arrow and fixed-reservation failures include the capacity, aggregate reserved
bytes, and available bytes sampled when the request was rejected. Native JNI
reservation denials log the same snapshot together with the owner id and its
registered category/task name; the native exception retains that owner id.
Optional caches may recover from a denial, so a denial log alone does not imply
that the task failed. Other tasks can release memory immediately afterwards;
later metric samples need not match the rejection snapshot.

With fine-grained resource scheduling, increase the affected task/vertex's
off-heap resource request and let the scheduler derive the TaskManager capacity.
Verify the actual runtime `taskmanager.memory.task.off-heap.size` against
`nativeOffHeapCapacityBytes`; a job-level setting can be superseded by the
scheduler's process specification. Increasing a JVM direct-buffer maximum or
RocksDB's managed-memory budget alone does not enlarge this shared pool.

The in-memory state backend cannot spill, so a denied growth request fails the task and Flink's
configured restart policy applies. Under the RocksDB backend, native state is written through to
RocksDB at every batch, so an operator's resident working set stays bounded by its batch size;
RocksDB's own memtables and block cache hold the rest, governed by Flink's
`state.backend.rocksdb.memory.*` options rather than the task off-heap pool (see the
[RocksDB backend](backends/rocksdb.md) memory section).

RocksDB Top-N reconciles its reservation with the current read-through working set, not the
cumulative size of persisted partition keys. Append-only (including Top-1), retracting and
update-fast rankers check the live working set before committing a bundle and return its charge
after decoding the output and releasing that working set. An active mini-batch still charges
hydrated rows and retained preimage payloads; payloads shared with the working set are counted
conservatively in that estimate. The separately reserved winner cache is not included a second
time in the completed operator-state charge. The memory backend retains its incremental charge
for every resident partition. Checkpoint/state formats and key-group assignment are unchanged.

`RocksTopNMemoryAccountingTest` reproduces the shared-pool lifecycle through the production JNI
entry points: 100 disjoint-key batches of 16 rows complete under a 128 KiB cap in both immediate
and mini-batch modes. Run it with the repository's configured Flink/JDK build:

```sh
mvn -pl streamfusion-runtime -am -Dtest=RocksTopNMemoryAccountingTest \
  -Dmaven.javadoc.skip=true -Dsurefire.failIfNoSpecifiedTests=false test
```

Native `topn::rocks_memory_tests` additionally covers all three strategies, existing-key
hydration, wide preimages, real budget denials, cache ownership and checkpoint/TTL recovery.

Constant append Top-1 additionally retains clean decoded winners under a 64 MiB
FIFO limit. That cache uses a separate reservation against the same task off-heap
pool as the operator. Cache bytes include retained row buffers, payload ownership,
keys and container overhead; a value header or a sort-row header already held in
its container is counted once. Cache growth reserves its estimated upper bound before retaining values; unused
reservation is returned after eviction. Memory denial clears and disables the cache rather
than failing the query; ordinary write-through state remains in RocksDB. Fresh
unique keys bypass cache admission, and checkpoint/canonical scans clear it.
The `topn.cache.bytes` gauge reports its accounted estimate. The large allocation
authority remains separate from RocksDB's managed memtable/block-cache pool.


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

## Reused-JVM test isolation

Allocator and pool-limit tests save the process capacity before each test and restore it after
releasing their reservations. They must not leave the pool unconfigured for subsequent operator
harnesses, whose mock TaskManager configuration has no off-heap setting. Test-only resets reject
live reservations rather than erasing their accounting, so restoration cannot hide a leak.
The production requirement for a positive task off-heap setting is unchanged.
