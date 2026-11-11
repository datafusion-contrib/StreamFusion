# Bounded execution runtime

This runtime executes the [admitted batch SQL sorts and global aggregates](operators/batch.md). It also
provides native controls for future bounded operators; those controls alone do not
admit additional SQL operators. Whole-query admission remains mandatory.

## Execution

Stateless Calc, union, retained-view sharing and row/Arrow conversion build their
transformations in `planner.common`. Streaming entry nodes retain their existing
public names and `StreamExecNode` markers; batch nodes compose the mode-neutral
implementation under `BatchExecNode`. The batch wrapper and transformation contexts
retain their existing identifiers and allocation order. Historical `stream-exec-*`
context names in the shared implementation are compatibility identities, not mode
admission. Batch-specific file-source lowering is injected at the conversion boundary
after the complete batch reuse graph is known. Both modes use the same source-reuse
configuration policy, without calling the other mode's scan.

The batch planner, scan, physical/exec nodes, graph boundary rewrites and operator
specifications live in `planner.batch`. The public `NativePlanner.installBatch`
return type and root-package `BatchPlanScan` remain as compatibility entry points.
Expression encoding stays shared: `ExpressionPlanning` exposes admission and
predicate construction through `common.EncodedExpression`, while the mutable
encoder stays internal. Common type probes, explain metadata and the single
reuse-barrier counter serve both modes. Runtime mode remains independent of source
boundedness; this package split does not relocate streaming mini-batch helpers,
runtime operators, JNI entry points or serializer snapshots.

Streaming scan, rewrite context, matchers and physical/exec implementations live
in `planner.stream`, with the same mode package in both Flink compatibility source
sets and optional connector modules. Root `PhysicalPlanScan` remains the public
installation return type; it inherits the one `stream.StreamPlanScan` implementation.
Historical public stateless mode-marker entries and connector service-provider
names remain compatibility entry points. Source-sharing identity prefixes, node
contexts and transformation names keep their previous values. Compiler and
documentation source filters cover both compatibility entries and nested
connector implementations, preserving deployment artifact ownership.

Optional connector planner extensions also use the common explain metadata and
reuse-barrier counter. Their module-owned source sets must import those shared
contracts explicitly, including the Flink 1.18-only Hudi and Paimon 1.0 modules.
The contracts remain in the connector-neutral runtime payload; they do not pull
optional connector implementations into core.

Expression admission reads producer representation and aliasing through
`common.ExpressionInput`, rather than recognizing streaming physical-node classes.
The streaming graph retains responsibility for proving those properties; unknown
columnar producers keep the existing collection fallback. Shared collection and
string expression rules remain separate from stream graph-boundary checks. Both
modes consume `common.EncodedExpression` for input pruning and remapping, keeping
the mutable expression encoder internal.

Optional connector rules contribute `common.StreamRule` definitions through the
existing `NativePlannerExtension` service. Their callbacks receive only
`common.StreamPlanningContext` services for admission diagnostics, source reuse
and original-node/state eligibility. The stream scan owns rule ordering,
changelog/configuration gates, probe-failure caching and substitution counts.
Provider discovery still publishes a contribution atomically and excludes its
sharing hooks when registration fails. Connector rules do not need access to the
scan's mutable rewrite state or a shared Java package.

`PlannerModeBoundaryTest` checks compiled batch/common dependencies, including
method bodies, to reject streaming ExecNodes and reverse common-to-batch coupling.
SQL regression checks retain positive native consumption, the existing perimeter
transpose count and historical simplified task names. The packaged loader tests
exercise both planning modes through the isolated deployment payload.

One bounded DataFusion execution owns persistent input/output streams for the task.
The Rust `engine::batch` module owns bounded execution, task scheduling, sort,
aggregate and join plans. Its sibling modules retain shared expression semantics,
Arrow/JNI transport, memory authority and streaming state. The batch module keeps
the existing JNI exports and native library boundary; this organization does not
change execution algorithms, admission or add a scheduler.

Each input has one pending Arrow batch, explicit EOF, producer backpressure and
separate reader/driver wakeups. An empty input slot means pending, not completion.
Offering another batch into an occupied slot fails rather than extending a queue.
EOF follows the final offered batch, including an empty or short batch.

The driver returns an output batch, a set of inputs needing data, or completion.
Input demand returns control to Flink; it never waits for another batch that the
same mailbox must deliver. Separate driver wakeups also cover DataFusion operators
that consume inputs on worker tasks. Output is drained incrementally rather than
collected into a result vector or concatenated into one batch.

The shared JNI bounded driver preserves that protocol. While waiting on asynchronous native
I/O it yields periodically so Java can check task interruption. The deadline cannot
preempt a synchronous CPU-bound poll or synchronous filesystem call. Cancellation
closes inputs and drops the execution stream; DataFusion owns cancellation of any
workers spawned by its operators. A consumed input cannot be replayed: Flink task
retry must construct a fresh execution and replay its source/shuffle input.

The native sorter accepts signed integer, UTF-8 string, BOOLEAN, DECIMAL and
DATE keys, explicit ascending/
descending direction and NULL placement. Its Arrow input and output remain columnar. Sorted LIMIT/FETCH uses the same driver with DataFusion's
TopK sort and a subsequent offset stream, preserving Flink's local/global stages.
TopK retains candidates under the shared memory authority; unlike the unrestricted
external sorter, its candidate heap does not spill and reports memory denial when
it cannot fit. No per-task cap is silently raised. Unrestricted sort uses
DataFusion's external sorter, the existing shared Tokio runtime, Flink task spill directories, and the TaskManager off-heap authority.
It does not add a distributed scheduler or change Arrow IPC serialization.

## Aggregate plans

Each Flink aggregate stage builds one DataFusion `AggregateExec` in single mode,
then projects into the host's output schema. This uses DataFusion's existing
accumulators without exposing its private partial-state schema on the network.
Local/final stages and the blocking exchange remain Flink's. Ordinary SUM stores
one nullable value at the declared integer width; COUNT stores BIGINT; MIN/MAX
store one fixed-width value, including Boolean, Decimal128, Date32 and Time32
milliseconds. Decimal precision/scale and the host's logical TIME precision survive
both stages. Noncanonical TIME units use a small expression adapter matching the
host reader before DataFusion chooses its extremum; no custom accumulator is added.
Integer AVG partials contain BIGINT sum and count. Final integer AVG divides only
when count is nonzero, then narrows to the declared signed integer type. Widened
integer SUM followed by a wrapping narrowing cast preserves Flink's modular
arithmetic through local and merge stages. Integer SUM0 and merged COUNT coalesce NULL to
zero. Decimal SUM/SUM0/AVG wrap the existing Flink-compatible decimal accumulators,
including reset-on-overflow SUM, sticky-overflow AVG/SUM0, zero initial SUM0 and
exact `(DECIMAL(38,s), BIGINT)` AVG host states. Only local AVG evaluation exposes
a temporary struct that is projected back to the two host columns before output;
DataFusion spill state remains internal. Each call retains its own FILTER; merge consumes already filtered states.

A local unfiltered COUNT also records whether any input existed, independently
of aggregate filters. It suppresses the local row on truly empty input, preserving
neutral partial rows for nonempty but all-NULL/all-filtered input. A final aggregate
must initialize and run even when Java receives no input batch. Its serialized
input/output schemas permit that initialization and the output schema can differ
from the input. Sort uses the same lifecycle with an unchanged output schema.
Existing sorter-named JNI data methods remain shared ABI entry points; separate
aggregate factory/capability methods keep an older DSO a planning-time fallback.

Verified integer/string/boolean/date/decimal grouping keys prefix the same host accumulator schema. Each grouped
HashAgg stage uses DataFusion's existing single-mode grouped hash stream, which
spills sorted private state and replays it internally when its memory reservation
is denied. Only the projected Flink state crosses task boundaries. Key-only stages
use the same grouping machinery for DISTINCT and emit no row for empty input.
DataFusion copies grouping values into its key storage and accumulators own their
values after consumption. Variable-width key-encoding workspace is charged before
evaluation;
borrowed Arrow input is released before requesting another batch.

## Memory lifetime

JNI memory callbacks retain the Java authority class as a global reference obtained
on the calling Java thread. Native workers use that reference rather than resolving
an application class through an attached thread's default classloader.

The bounded task registers its own memory owner. Reservations and runtime contexts
retain the pool, and the pool's final drop closes the registration. Java operator
close does not reclaim a balance while a cancelled DataFusion worker still holds
it. Existing streaming operators preserve their Java-owned registration contract.
See the [reference comparison](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/bounded-datafusion-execution.md).

Retaining plans stage one borrowed input, close the Java view, and copy its logical
rows into independent native Arrow buffers under a reusable workspace reservation.
It then releases the borrowed C Data buffers before DataFusion retains the copy.
The workspace remains bounded to one maximum input batch and is freed at EOF;
DataFusion charges its retained sort state separately. This avoids charging all
retained input through both Java and DataFusion. A transient overlap during copying
covers the actual source and destination allocations.

The admitted fixed-width global reductions consume the imported Arrow batch directly:
DataFusion's SUM/COUNT/MIN/MAX accumulators retain only scalar values. The original
Arrow allocation remains charged until its C Data reference drops. The next input
demand is returned after consumption, without retained Java Arrow input or an
ownership copy. An explicit reusable workspace reserves expression temporaries
before evaluation: 32 bytes per row per underlying function plus fixed padding,
with the input's compact-copy budget additionally covering filtered payloads.
This includes widening casts, literal arrays, 16-byte decimal arguments,
TIME normalization, validity and selection buffers;
DataFusion's global aggregate reservation alone accounts for accumulator growth
after evaluation. Grouped plans reserve projection space for the maximum output
batch and retain it across every spill-replay/output batch. Once projection
temporaries retire, `MemoryReservation.split` transfers their charged budget to the
compact result/copy handoff; the next poll replenishes projection space after the
output acknowledgement. This avoids a second charge for the same working budget
while keeping the existing strict reservation authority. The workspace is freed
on completion or cancellation. This borrowing
policy is explicit and is not used for sorts
or future aggregates that retain input buffers.

Output is copied into compact independent buffers under a native reservation.
Copying logical rows matters: a small slice must not copy or pin an entire sorted
run. After Java imports and charges the output, it acknowledges the handoff and
native retires the output reservation. The imported batch may outlive its producing
operator. The bridge refuses another poll until that acknowledgement. Import
failure releases the remaining C Data callback and reservation.

A schema probe rejects representations whose copy operation could retain borrowed
buffers, including views and dictionaries. The planner checks both declared boundary
schemas and actual native Calc output below a retaining bounded operator. Physical top-level nullability
is normalized without changing SQL logical types, since source hints need not
exclude actual NULL slots.

Spillable DataFusion reservations preserve headroom for the next Arrow batch,
copy workspace and output import within the same process off-heap authority. The
headroom grows with observed batch size; ordinary Arrow/unspillable allocations
can use it. It is neither a second memory cap nor a per-operator partition.
A batch that cannot fit still fails with the existing memory-limit exception.

## Shuffle and recovery

Admitted SINGLETON and verified-key HASH exchanges use Flink's blocking partition transformations over
`ArrowBatch`, using ordinary Arrow IPC serialization. It introduces no process-local
handles. Flink owns persistence, transfer and replay. Required singleton distribution
is preserved through native Calc and transpose nodes, and final consumers remain at
parallelism/max-parallelism one. This preserves both global order and keyless final
aggregation. Partial aggregate states use the same serializer as ordinary batches.

HASH routing reuses the existing Arrow key splitter and partitioner with
ordinary IPC, including when streaming local zero-copy is enabled. Batch routing
binds to the deployed writer's subpartitions/target groups; the streaming path
continues to require its exact recoverable key-group namespace. Grouped native
exec nodes advertise ANY input to Flink's exchange graph processor because their
explicit Arrow partition transformation already enforces distribution. Advertising
HASH there would insert a stock RowData hash partitioner after the Arrow splitter.
The physical relational hash trait remains intact, and a final native group forces
a compatible Arrow exchange when the stock optimizer had elided one.

The runtime gauges `batchSortSpillCount`, `batchSortSpilledBytes` and
`batchSortReservedBytes` snapshot DataFusion metrics on the task thread. Metric
reporters read the snapshot without racing JNI teardown. The reserved-byte gauge
covers the sort's native pool, not all process Arrow allocations. Aggregates expose
the corresponding `batchAggregateSpillCount`, `batchAggregateSpilledBytes` and
`batchAggregateReservedBytes` snapshots. Global scalar state does not spill; grouped state uses DataFusion's
spill/replay path through the same host-owned task spill directories.

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
remaining. Global aggregate controls add signed-width overflow, independent filters,
AVG partial merging, COUNT(*) over zero-column inputs, and local/final empty defaults.

SQL controls compare ordered results across multiple output batches, empty input,
parallelism-two unchained producers and unsupported-key fallback. Sorted-limit
controls cover local/global candidates, offsets crossing Arrow batches, repeated
keys, beyond-end offsets, empty input and zero-row limits. A downstream
sink fails once, then recovers all 2,051 sorted rows from the completed blocking
Arrow shuffle: native sort/exit reach attempt 1 while producers stay on attempt 0.
The packaged loader executes the same multi-batch sort through its isolated payload.
Unchanged upstream integer and STRING-key fixtures require positive native work
on both supported Flink lines. Standalone sorted-limit fixtures retain their
explicit performance fallback.

The [sort benchmarks](benchmarks/batch-sort-groundwork.md) report separate prepared
operation and complete-job measurements. Complete jobs retain scans, both transposes,
blocking shuffle, JNI and sink completion. The previous StreamFusion behavior for
these queries was whole-query stock Flink fallback.

Global SQL aggregate controls additionally replay completed partial states after a
final consumer failure, with producers remaining on their first attempt. The
packaged payload executes both populated and empty aggregate results. The
[aggregate comparisons](benchmarks/batch-aggregates.md) record rejected standalone
candidates and admitted compositions; only the latter qualify current SQL islands.

Fixed-width extrema controls additionally compare every typed replay result to
stock Flink after consumer failure. The producer attempt remains zero while the
consumer reaches attempt one. Native time controls exercise second/microsecond/
nanosecond inputs whose millisecond conversion truncates or wraps before comparison.
Variable-width and timestamp-struct extrema remain outside the borrowed scalar
accumulator policy.


The decimal SUM adapter uses the existing Flink-compatible `DecimalSumAccumulator`
through DataFusion's UDAF interface. DataFusion still owns input consumption,
grouping and task lifetime; no streaming state store or window operator is reused.
The one-field DECIMAL(38,s) host state preserves overflow-to-NULL/reset semantics
that differ from DataFusion's builtin decimal SUM. TPC-H Q6 verification covers
the raw/local/final SQL path; the native control also covers empty/all-NULL and
explicit overflow/merge states. Complete CSV Q6 performance remains neutral.

## Bounded hash joins

The same bounded task driver now supports two input ports, a per-port EOF and an
explicit input-demand mask. It uses one staged handoff at a time and the same
compact input/output reservations as sort. DataFusion's `HashJoinExec` retains
the build state under the shared pool; the host scheduler supplies only the input
currently requested. The initial hash table does not spill. Reservation denial
remains a task failure within the existing authority, rather than an automatic
limit increase or a runtime fallback.

The planner keeps Flink's build-side choice and restores its logical output order
when swapping DataFusion's build/probe inputs. Blocking hash/broadcast frames remain
Arrow IPC. Flink's deadlock-breakup processor may insert another task boundary for
a shared source; a graph adapter preserves that boundary with Arrow forwarding
when its producer is columnar. The optional host multiple-input fusion cannot
wrap Arrow nodes because it hardcodes RowData types, so a graph containing native
joins skips that processor. This is required only in the deployed batch planner;
the stock-planner optimizer-hook installation retains whole-query join fallback.

Nested-loop and sort-merge joins share the same two-input lifecycle. Residual
predicates use the existing encoded expression compiler and DataFusion JoinFilter
with explicit logical left/right column mapping when the build side is swapped.
The initial predicate whitelist excludes JVM callbacks and unverified collection
or original-string representations. Merge joins sort both native inputs with the
existing spillable external sorter, then use DataFusion's merge operator.

A scalar SINGLE_VALUE over a planner-proven singleton uses DataFusion's first-value
aggregate. The proof accepts only Calc/singleton exchange over a final global
aggregate; other cardinalities retain the host guard and its multiple-row error.

Shared unary aggregate subplans use the existing native retained-view share
operator. The planner first invokes Flink's own subplan reuser, counts actual
aggregate consumers, and preserves only that explicitly shared unary subtree's
digests through the host clone/reuse cycle. Each branch takes its own Arrow view;
there is no additional native result cache or memory pool. Disabling native source
sharing or either Flink reuse option restores independent subplans.

### Native Parquet input

A final graph pass can replace an admitted file-source/entry-transpose boundary
with the optional Parquet decoder's Arrow-producing source, after Flink has
resolved source reuse. The optional module owns source admission and file decoding;
core only supplies the extension hook and retained-view sharing. A shared source is
converted only if every direct consumer accepts the same Arrow representation.
Flink retains file discovery, assignment, seekable filesystem access and source
checkpoint serialization. Native checkpoints count emitted logical rows in a
whole-file split; restoration replays/skips those rows. This does not claim stock
Parquet byte-split checkpoint interchange. Source queues and decoder workspace
share the existing process-wide task-off-heap authority.
