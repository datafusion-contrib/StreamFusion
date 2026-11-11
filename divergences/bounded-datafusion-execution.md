# Bounded DataFusion task execution

The implementation was designed after consulting Comet's execution JNI driver,
scan and plan memory pool, and Arroyo's stateless executor, buffered join execution,
incremental aggregate and cloneable stream futures.

It follows their shared structure: compile a DataFusion physical plan, supply Arrow
input through a pending/EOF scan, retain its task context, and poll its output stream.
It does not introduce another distributed runtime or collect an entire result.

Flink delivers input through a push/mailbox operator, unlike Spark's pull iterator.
The adapter therefore reports input demand to the host instead of recursively
calling Java for another batch. A one-slot port provides backpressure, and a
separate driver waker handles scans polled by DataFusion workers. The JNI driver
also yields during pending native I/O so the task thread can check interruption.

Arroyo resets bounded physical execution for a streaming batch/window. Here a
bounded Flink task executes its plan once across all incoming batches, with one
explicit EOF per input. Task retry rebuilds that execution; inputs are not rewindable.

Arroyo's ordinary SQL planner rejects non-windowed ORDER BY and LIMIT. Bounded
sorted limits instead follow Comet's `SortExec.with_fetch` followed by
`GlobalLimitExec` for the offset. Flink still owns its local/global physical stages
and singleton exchange; each bounded native task applies only its own bounds.
A logical zero-row limit still consumes input before returning no output, so it
does not suppress upstream evaluation or failures in an existing sort-limit stage.

Comet's plan memory pool waits for spawned-worker reservations to be returned before
releasing its host memory manager. StreamFusion can keep its independent Java owner
registration alive through the last pool reference instead. DataFusion reservations
and worker contexts retain that reference, so asynchronous cancellation cannot
prematurely return their balance to the shared authority. The cached Java authority
class follows Comet's retained JVM-reference pattern.

Unlike a synchronous Calc, the sorter retains inputs across calls. StreamFusion's
Arrow Java authority cannot transfer a borrowed allocation's charge directly into
DataFusion's working reservation. The bounded bridge therefore copies input into
compact native buffers, releases its borrowed Java reference, and retains only one
reusable copy workspace in addition to DataFusion's state. Output is also compacted
under a reservation, then acknowledged after Java import. This costs two copies but
avoids keeping every retained run charged to both owners or exporting tiny slices
that pin an entire native run. View/dictionary representations are not admitted.
Comet's importer supplies the model for releasing unconsumed C Data callbacks on
failure, including a helper in Arrow's package to inspect its C struct lifetime.

Spillable reservations leave space for input and output handoff within the existing
process authority; there is no separate operator cap or unbounded pool. The
[runtime design](../docs/batch-execution-runtime.md) documents this contract and
the blocking Arrow shuffle/replay controls used for SQL admission.


## Bounded global aggregation

Reference-first sources for aggregate state were Arroyo's planner
`extension/aggregate.rs`, worker `arrow/tumbling_aggregating_window.rs` and
`arrow/mod.rs`, plus Comet's `execution/planner/aggregate.rs`. Arroyo splits partial
schemas from finish plans and resets pending execution per window. Comet builds
DataFusion partial/final/single stages and normalizes merge schemas. Bounded Flink
instead supplies its own released physical local/global contracts and EOF.

Each host stage runs DataFusion single aggregation and a projection into Flink's
exact accumulator columns. Flink local integer AVG uses BIGINT sum/count, narrow SUM keeps
its input width, and local empty input emits no row. These differ from DataFusion's
private partial schema/default global output. A local auxiliary COUNT suppresses
only truly empty input; it is independent of aggregate FILTERs. Widening SUM to
BIGINT then using the existing wrapping cast preserves modular narrow arithmetic;
Integer AVG uses separate SUM/COUNT with guarded signed division. Decimal stages
wrap the existing host-compatible accumulators. No new arithmetic algorithm or
streaming retraction machinery is added. The host's partial-state Arrow schema
remains the wire contract.

The primitive global accumulators consume each imported batch without retaining
its buffers. Their explicit borrowing policy avoids the sort driver's input copy;
Comet-style Arrow C Data lifetime and the existing authority keep the original
allocation charged until consumption. Retaining plans keep independent copies.
This policy must be revisited before admitting variable-width or other accumulators
that retain Arrow input references.

DataFusion's ungrouped stream reserves accumulator growth after evaluating an input;
its reservation does not budget evaluated expression arrays or per-call filter
copies. The bounded driver therefore reserves primitive cast/literal/selection
workspace before execution and each input, adding the input copy-size bound for
filtered payloads. A reusable reservation survives through final projection/output
copying and is released at EOF. This keeps borrowing compatible with the existing
shared authority; it does not add a custom accumulator or another memory cap.
The comparison also consulted Comet's `memory_pools/plan_pool.rs` reservation
lifetime, alongside DataFusion 54's `aggregates/no_grouping.rs` allocation boundary.


The fixed-width MIN/MAX extension also consults the released Flink declarative
min/max buffer types and the Arrow TIME reader. DataFusion's existing Boolean,
Decimal128 and Date32/Time32 accumulators keep scalar state, so they share the
borrowing policy and expression reservation. A noncanonical TIME expression first
matches the host's millisecond read, including Java integer wrapping, before
DataFusion compares values. Casting only the chosen result could select a different
extremum. Timestamp structs and variable-width values are not admitted by this
extension: their retained-buffer ownership and semantics require separate proof.


## Bounded keyed aggregation and Arrow hash routing

The grouped extension follows Comet's `execution/planner/aggregate.rs`: group
expressions and DataFusion accumulators form the physical aggregate, while a
projection restores the host state layout. Arroyo's aggregate extension similarly
separates key fields, partial schema and finish projection. Flink supplies the
local/final stages; DataFusion 54 single-mode grouped hashing supplies spill and
sorted replay. Signed keys and fixed-width accumulators copy scalar state and do
not retain the imported Arrow batch.

The comparison includes Comet's `memory_pools/plan_pool.rs`, `task_shared.rs`,
`fair_pool.rs`, `unified_pool.rs` and `spill_replay.rs`, alongside the existing
StreamFusion task pool. Comet's current replay exception targets DataFusion 55
stream/consumer names and permits overcommit. StreamFusion keeps its released
DataFusion 54 and existing hard reservation authority. Projection workspace is
retained across grouped output and transferred with `MemoryReservation.split`
into the copied-output handoff, then replenished after acknowledgement. No new
pool or overcommit exception is introduced. DataFusion reports hash-state growth
after processing a batch; pool reservations follow that established estimate-based
contract rather than tracking every native allocation or process RSS.

Batch HASH reuses the Arrow projected-key splitter and key-group partitioner, but
binds their routing count to the deployed writer. Streaming key-group recovery
checks remain unchanged. Flink's batch graph processor may leave consumer
parallelism unresolved and configure the runtime partitioner using its writer's
target groups. Native groups use explicit Arrow distributions and ANY exec input
properties to avoid insertion of stock RowData hash exchanges. Every final native
group establishes a compatible route even if a stock source trait elided hashing;
stock batch generated field hashing and streaming BinaryRow hashing are not assumed
interchangeable. Frames remain Arrow IPC on Flink's blocking shuffle and stage replay.

Typed batch reductions reuse DataFusion grouping storage for integer, UTF-8,
boolean, date and decimal keys. Decimal SUM/AVG/SUM0 reuse StreamFusion's existing
Flink-compatible decimal accumulators through thin DataFusion UDAFs. The AVG
adapter exposes Flink's two local columns through a temporary struct projection;
it does not substitute DataFusion's decimal rounding or overflow semantics. SUM0
uses the same sticky-overflow arithmetic as AVG and preserves NULL propagation
when merging host partials. This is a host-state/schema adaptation, not a second
aggregation store or memory pool. Comet's aggregate and sort planners and Arroyo's
UDAF aggregate construction remain the structural references.

Bounded hash joins follow Comet's `HashJoinExec` construction and build-side swap,
and Arroyo's persistent two-input DataFusion execution structure. Flink's mailbox
and input selection supply the two pending Arrow streams; we do not use Arroyo's
window/reset lifecycle for bounded SQL. Flink's optional RowData-only multiple-input
fusion is skipped for graphs containing native joins. Its deadlock resolver still
runs, and inserted forwarding boundaries after Arrow producers retain Arrow IPC.
The released DataFusion hash table is reservation-bounded but does not spill; this
is an explicit limitation of the current candidate, not another memory pool or
permission to exceed the shared authority.

Cartesian elimination follows DataFusion 54's `eliminate_cross_join` linked-input
reconstruction, applied to a narrow verified host physical pattern instead of
running a second logical optimizer. A projection and optional blocking exchange
restore Flink's column order and distribution. Explicit strategy hints, residual
conditions and NULL-safe equality are excluded from reassociation.

Computed batch sharing uses the existing streaming share operator and Arrow
retained-view ownership. Flink expands an intermediate table to a shared RelNode
before its SameRelObjectShuttle/SubplanReuser cycle; we preserve only explicitly
counted unary aggregate subtrees through that cycle. Arbitrary native nodes keep
their per-instance digest barriers. This follows Flink's subplan reuse and Comet's
exchange reuse rather than adding a cache or independent memory manager.

## Bounded native filesystem Parquet source

Comet supplies native Arrow readers per host partition; Arroyo's filesystem source
uses parquet-rs record batches and restores a logical record offset. StreamFusion
uses the existing optional Parquet decoder over Flink-owned seekable streams, under
Flink's stock bounded file enumerator and SourceReader. It restores logical row
counts and transfers owned Arrow buffers through the established C Data bridge.
The initial decoder API is whole-file, so the source deliberately enumerates one
split per file rather than interpreting Flink's stock Parquet byte splits as whole
files. File count limits source parallelism; stock source savepoint compatibility
is not claimed. Splitting individual row groups is follow-up work, not a reason to
change Flink's coordinator or add another memory pool. Shared scans use the existing
retained-view share operator. Source-reader teardown additionally releases queued
Arrow records because Flink's heap-oriented close path only shuts down fetchers.
