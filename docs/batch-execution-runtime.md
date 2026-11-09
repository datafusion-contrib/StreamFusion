# Bounded execution runtime

This runtime executes the [admitted batch SQL sorts](operators/batch.md). It also
provides native controls for future bounded operators; those controls alone do not
admit additional SQL operators. Whole-query admission remains mandatory.

## Execution

One bounded DataFusion execution owns persistent input/output streams for the task.
Each input has one pending Arrow batch, explicit EOF, producer backpressure and
separate reader/driver wakeups. An empty input slot means pending, not completion.
Offering another batch into an occupied slot fails rather than extending a queue.
EOF follows the final offered batch, including an empty or short batch.

The driver returns an output batch, a set of inputs needing data, or completion.
Input demand returns control to Flink; it never waits for another batch that the
same mailbox must deliver. Separate driver wakeups also cover DataFusion operators
that consume inputs on worker tasks. Output is drained incrementally rather than
collected into a result vector or concatenated into one batch.

The shared bounded JNI bridge preserves that protocol. While waiting on asynchronous native
I/O it yields periodically so Java can check task interruption. The deadline cannot
preempt a synchronous CPU-bound poll or synchronous filesystem call. Cancellation
closes inputs and drops the execution stream; DataFusion owns cancellation of any
workers spawned by its operators. A consumed input cannot be replayed: Flink task
retry must construct a fresh execution and replay its source/shuffle input.

The native sorter accepts signed integer keys, explicit ascending/
descending direction and NULL placement. Its Arrow input and output remain columnar. Sorted LIMIT/FETCH uses the same driver with DataFusion's
TopK sort and a subsequent offset stream, preserving Flink's local/global stages.
TopK retains candidates under the shared memory authority; unlike the unrestricted
external sorter, its candidate heap does not spill and reports memory denial when
it cannot fit. No per-task cap is silently raised. Unrestricted sort uses
DataFusion's external sorter, the existing shared Tokio runtime, Flink task spill directories, and the TaskManager off-heap authority.
It does not add a distributed scheduler or change Arrow IPC serialization.

The bounded task driver and Java Arrow lifecycle are shared independently of the
physical plan factory. The sort factory supplies the existing DataFusion sort and
optional offset plans; its JNI data ABI, memory ownership and output acknowledgement
remain unchanged. The shared lifecycle permits a declared result schema different
from the input, and a factory may supply an input schema for initialization at EOF
when no input batches arrive. These hooks do not themselves admit new SQL operators.

## Memory lifetime

JNI memory callbacks retain the Java authority class as a global reference obtained
on the calling Java thread. Native workers use that reference rather than resolving
an application class through an attached thread's default classloader.

The bounded task registers its own memory owner. Reservations and runtime contexts
retain the pool, and the pool's final drop closes the registration. Java operator
close does not reclaim a balance while a cancelled DataFusion worker still holds
it. Existing streaming operators preserve their Java-owned registration contract.
See the [reference comparison](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/bounded-datafusion-execution.md).

The bridge stages one borrowed input, closes the Java view, and copies its logical
rows into independent native Arrow buffers under a reusable workspace reservation.
It then releases the borrowed C Data buffers before DataFusion retains the copy.
The workspace remains bounded to one maximum input batch and is freed at EOF;
DataFusion charges its retained sort state separately. This avoids charging all
retained input through both Java and DataFusion. A transient overlap during copying
covers the actual source and destination allocations.

Output is copied into compact independent buffers under a native reservation.
Copying logical rows matters: a small slice must not copy or pin an entire sorted
run. After Java imports and charges the output, it acknowledges the handoff and
native retires the output reservation. The imported batch may outlive its producing
operator. The bridge refuses another poll until that acknowledgement. Import
failure releases the remaining C Data callback and reservation.

A schema probe rejects representations whose copy operation could retain borrowed
buffers, including views and dictionaries. The planner checks both declared boundary
schemas and actual native Calc output below a sort. Physical top-level nullability
is normalized without changing SQL logical types, since source hints need not
exclude actual NULL slots.

Spillable DataFusion reservations preserve headroom for the next Arrow batch,
copy workspace and output import within the same process off-heap authority. The
headroom grows with observed batch size; ordinary Arrow/unspillable allocations
can use it. It is neither a second memory cap nor a per-operator partition.
A batch that cannot fit still fails with the existing memory-limit exception.

## Shuffle and recovery

The admitted exchange is Flink's blocking SINGLETON partition transformation over
`ArrowBatch`, using ordinary Arrow IPC serialization. It introduces no process-local
handles. Flink owns persistence, transfer and replay. Required singleton distribution
is preserved through native Calc and transpose nodes, and consumers remain at
parallelism/max-parallelism one so global order survives later edges.

The runtime gauges `batchSortSpillCount`, `batchSortSpilledBytes` and
`batchSortReservedBytes` snapshot DataFusion metrics on the task thread. Metric
reporters read the snapshot without racing JNI teardown. The reserved-byte gauge
covers the sort's native pool, not all process Arrow allocations.

## Validation and performance

Native controls cover input demand and wakeups, one-slot backpressure, EOF, schema
rejection, prevention of replay, empty results, memory denial, independent
worker-driven inputs and global SUM/COUNT results for empty/all-NULL inputs. A sort
control sorts 131,072 rows under a 256 KiB pool, asserts spill before EOF, bounds
output batches and verifies memory/file release.

JNI operator controls cover multi-key order/NULL placement, EOF, a 262,144-row sort
under a 2 MiB task pool with positive pre-EOF spill files and DataFusion spill
metrics, cleanup after success/cancellation, creation failure, memory denial, and
task interruption. Copy controls verify compact variable-width slices and release
of borrowed input. An output can outlive its producer with only its Arrow charge
remaining. Aggregation is currently a native adapter test, not admitted batch SQL.

SQL controls compare ordered results across multiple output batches, empty input,
parallelism-two unchained producers and unsupported-key fallback. Sorted-limit
controls cover local/global candidates, offsets crossing Arrow batches, repeated
keys, beyond-end offsets, empty input and zero-row limits. A downstream
sink fails once, then recovers all 2,051 sorted rows from the completed blocking
Arrow shuffle: native sort/exit reach attempt 1 while producers stay on attempt 0.
The packaged loader executes the same multi-batch sort through its isolated payload.
Unchanged upstream signed-integer fixtures require positive native work; the STRING
key fixture requires an explicit fallback. Both supported Flink lines are covered.

The [sort benchmarks](benchmarks/batch-sort-groundwork.md) report separate prepared
operation and complete-job measurements. Complete jobs retain scans, both transposes,
blocking shuffle, JNI and sink completion. The previous StreamFusion behavior for
these queries was whole-query stock Flink fallback.
