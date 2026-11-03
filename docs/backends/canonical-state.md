# Canonical state format

StreamFusion uses Flink's canonical savepoint container and full-snapshot writer. Operators
bound to a verified stock contract project directly into the corresponding released Flink state
schema. Their qualified scope is documented below. Unbound operators retain reserved managed
state containing StreamFusion's logical representation, which still requires StreamFusion on restore.
Flink owns state metadata, key-group indexes, stream handles, redistribution and integrity checks.

Representative canonical conversion paths now exist in all **15 operator families**, including
tested draft adapters awaiting consolidation. Every family remains partial; this count excludes
connector protocols and is not a percentage of overall completion or individual state variants.

Each non-empty key group has a versioned `SFCS` header in
`__streamfusion_canonical_native_state_v1` and zero or more 4 MiB chunk states. The header records:

- the canonical format version;
- a stable operator-state identifier;
- the processing-time cleanup deadline, when applicable;
- the chunk count and total payload length; and
- a CRC32C checksum of the reassembled payload.

All header integers are big-endian. The byte layout is `SFCS` (4 bytes), format version (`i32`),
UTF-8 operator-id length (`i32`) and bytes, processing-time deadline (`i64`), chunk count (`i32`),
payload length (`i32`), and CRC32C (`u32`). Chunk `n` is stored in the reserved value state
`__streamfusion_canonical_native_state_v1_chunk_n` under the same Flink key group. This naming and
envelope remain an internal native-recovery boundary.

The chunks contain the operator's logical Arrow IPC snapshot sections. Memory and RocksDB operators
produce and consume the same representation. Direct RocksDB state is decoded from its typed table
only when a canonical savepoint is requested, and restoring onto the RocksDB backend decodes the
sections once at open and bulk-writes them back into the typed table; ordinary checkpoints and
native-format savepoints retain the incremental SST path.

Reading or writing these reserved states temporarily selects each owned key group. The operation
restores the hosting backend's exact active key and key-group context afterward, including an unset
context, so repeated checkpoints cannot leak a synthetic partition key into normal keyed state.

When installing a native partition, the writer checksums the payload directly after its
four-byte key-group prefix and copies each managed chunk from that same array. It does not
first allocate another full payload array. Managed chunks remain independently owned, so later
changes to the input partition cannot change a completed snapshot. The saved header, checksum,
chunk names and restore format are unchanged.

## Compatibility contract

Canonical savepoints can move a StreamFusion job between the memory and RocksDB backends and can be
redistributed by key group when parallelism changes. Unknown format versions, a different operator
identifier, missing chunks, invalid lengths, and checksum mismatches fail restore before native code
interprets the payload.

The reserved-state envelope above is a StreamFusion compatibility format. Restoring an unbound
operator's envelope requires its StreamFusion implementation. Operators bound to the stock contracts
below instead write stock operator state directly and can use the qualified stock handoff paths.
Neither format makes arbitrary job upgrades safe: keys, aggregate definitions, join shape and the
operator state schema must remain compatible with the saved artifact.

## Timestamp layout upgrade

The lossless timestamp layout changes native row encodings. Current writers use canonical version 5
(asynchronous Top-N version 6), raw keyed-state version 3 and RocksDB metadata version 4. Recovery
checks these versions before interpreting native payloads. Earlier snapshots and savepoints require
the StreamFusion binary that wrote them; this release does not convert old state in place. Existing
stateful jobs must finish or replay from their durable inputs when upgrading. Keep their previous
binary and checkpoints until that transition is complete. Already-wrapped values in an old snapshot
cannot be reconstructed from that snapshot alone.

New checkpoints preserve the millisecond component and fractional nanos on memory and RocksDB,
including canonical transitions between those backends. Format changes do not alter Flink key-group
assignment or the timestamp precision rules used by its BinaryRow key serializer.

The local-window boundary correction also changes the meaning of retained window-rank/join
payloads and their firing thresholds in nonzero-offset LTZ jobs. A previous epoch-boundary snapshot
cannot be read as local-boundary state. The common checkpoint envelopes therefore advance together
and reject earlier versions before decoding, including older asynchronous canonical state. This
upgrade requires restarting from durable inputs; it does not provide an in-place state migration.

## Stock-compatible transfer groundwork

[P0 #569](https://github.com/datafusion-contrib/StreamFusion/issues/569) is adding direct projection
into released stock operator state schemas. The state accessor for this work reads and writes
Flink's serialized TTL envelope with its original entry timestamp. Conversion can therefore copy
state without expiring it or refreshing its age through the running job's clock wrapper.
Operator adapters, automatic restore binding and cross-engine qualification remain part of #569.

### Canonical import batches

A typed RocksDB store releases its decoded working rows after each bundle. A canonical restore
that transfers one logical partition in several batches must hydrate previously committed rows
before merging the next batch. Otherwise a non-resident value is mistaken for an absent value
and overwritten. The restore hook uses actual serialized keys and preserves raw TTL write ages;
ordinary input reads continue to apply normal expiry checks. This is correctness groundwork for
stock-compatible state projection; benchmarking remains deferred until that functionality is complete.

Update-fast native checkpoint rows preserve their original INSERT/UPDATE_AFTER kind alongside
payloads and unique keys. Memory snapshots add a kind column only when needed; typed RocksDB
adds an optional tagged row-kind trailer after its existing rows and optional first-bound state.
Readers accept older kind-less snapshots as INSERT rows, while all-INSERT values keep their
previous representation. Projected fixed ranks also retain the first overflow sort group visited
by Flink, so a later version of an already retained hidden key keeps its update identity after
restore. These changes apply to ordinary native checkpoints as well as canonical handoff.

## Stock-schema restore inventory

The stock-schema import work under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569)
uses a read-only inventory validator. It checks declared descriptor names and state kinds, exact
namespace serializers per descriptor, and timer key/namespace serializers per service. These
declarations can mix void, window-end and session-window namespaces; matching only a serializer's
class is insufficient. Unknown descriptors or timer services are errors even when empty. Pending
timers require explicit adapter ownership. An adapter can declare an unused service with its exact
namespace only when its captured queue is empty; Flink's known empty user-timer queues remain the
default exception. Aggregate declarations include their DISTINCT/null-marker and extrema data views.

Inspection captures released Flink snapshot resources and checks queue emptiness through their
state iterator. It does not open/register timer queues, consume records, expire or refresh TTL
values, or build a serialized native snapshot. Captured resources are released on success and
failure. A failed inventory check closes and disposes the stock import view before native backend
creation. This check belongs to stock canonical import; ordinary native checkpoints keep their
existing restore path.


## Physical grouping-key projections

Flink's generated grouping-key projection reserves variable slots for NULL non-compact
DECIMAL and high-precision timestamp fields. Its row serializer instead omits those slots.
The rows are logically equal but have different bytes and hashes, so re-encoding only while
writing a savepoint would leave live state and key-group routing inconsistent.

Native callers can select the stock generated root projection layout with a leading
`Native.STOCK_KEY_PROJECTION` in the recursive timestamp/type descriptor. Unprefixed
descriptors retain the existing serializer layout. This supplies exact grouping-key bytes;
stock operator identities, state serializers and a complete state adapter are separate
requirements under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).

Byte/hash controls compare production Rust encoding with released Flink-generated selectors
for decimal precision 18/19/38, millisecond/nanosecond timestamps, negative epochs, NULLs,
nested generic rows and arrays. These controls pass against Flink 2.2.1 and 1.18.1. They do
not establish cross-version savepoint interoperability. Benchmarking and tuning remain
deferred until the full functional handoff is complete.

The shared stock canonical capture helper freezes managed operator state alongside managed keyed
state at the same synchronous barrier boundary. Both snapshot futures are captured before the temporary keyed view
is cleared. A synchronous capture failure cancels both futures without replacing the original
error. This preserves stock operator metadata such as UNION list distribution without an
intermediate native snapshot. Window watermark support prepares the released `watermark` UNION
list with its `LongSerializer` and minimum-on-restore rule, including Flink's maximum-value sentinel;
it rejects unexpected operator/broadcast state and incompatible metadata. These foundations do not
yet qualify native windows for stock savepoint interchange under #569.

The native SESSION transfer primitive visits scalar accumulators and actual merged bounds
directly from memory or typed RocksDB, in batches of at most 1,024 sessions. The persistent
reader decodes bounded chunks rather than hydrating the full session map or serializing a
native snapshot. Import validates schemas and rejects duplicate or still-overlapping sessions,
including conflicts with earlier import batches; subsequent input can bridge restored sessions.
NULL keys remain distinct from ordinary keys. Receiver failures leave the live state available
for retry, and restored memory is reserved against the operator budget and released on close.
Explicit COUNT DISTINCT slots export their scalar cardinality and memberships separately.
Import checks the complete view before making the restored operator available; duplicate,
orphaned or excessive memberships cannot be used as a valid restore. Counted views preserve
frequencies independently of their scalar. Export restores draining aggregate factories before
calling the receiver so callback failure leaves live state intact.
Typed RocksDB session state still supports scalar accumulators; membership state retains the
existing snapshot-backed native checkpoint path.
Native controls cover both stores, keyless state, bridging, malformed namespaces, bounded export
and memory-limit cleanup. This foundation alone does not qualify the whole SESSION family.

The native tumbling transfer primitive exposes running scalar accumulator fields, grouping keys
and window bounds directly from memory or typed RocksDB. Transfer batches contain at most 1,024
rows; the initial RocksDB reader uses its decoded state view after draining pending updates.
Export can be retried after a failed receiver without losing live state. Import rejects malformed
schemas, bounds and duplicate keyed windows before committing a batch. Resource-limit failures
retain their native memory-limit exception, and borrowed Arrow buffers are released before Java
failures are raised. Restored stock watermarks convert from inclusive timestamps to the native
exclusive firing frontier. This primitive produces no intermediate serialized native snapshot;
full window-family integration and qualification remain part of #569.

The shared-window transfer primitive preserves HOP panes, CUMULATE prefixes and a separate
pending timer ledger, including timers whose last pane has expired. It reuses the running
shared-slice lifecycle for memory and typed RocksDB and supports fixed-offset assignment grids.
Import retains the same Arrow key converter across state and timer batches: retained owned keys
must remain associated with the converter that produced them. Regression controls transfer
1,539 keys in bounded batches, re-export without new input and continue window firing on both
stores. These native controls establish the transfer primitive; stock planner and whole-job
qualification are documented separately as they complete under #569. Benchmarking remains deferred.

The native fixed-window transfer primitive keeps SQL offsets separate from time-zone shifts.
Assignment uses their difference; inverse boundary rendering retains the time zone. CUMULATE
normalizes the SQL offset by maximum size, preserving its prefix base even when the offset
exceeds a step. Pane assignment, pending timers and import validation use the same grid.
Ordinary native checkpoints preserve the offset and reject partitions with different grids.
The legacy zero-offset checkpoint representation stays readable. Independent tumbling state
has no shared timer ledger: its timer visitor is empty and shared-timer import is rejected.
The native suite passes 1,050 controls (two existing ignores); sixty-four released-operator
controls additionally compare offset continuations and exact DISTINCT counters across both
state paths and four time zones. Full planner/process qualification remains part of #569;
canonical export still visits live state directly, without an intermediate native snapshot.

The native final-window merge mode gives each incoming partial only to the final end it names.
It does not expand that end across overlapping HOP windows. CUMULATE retains the variable prefix
start derived from the maximum-size grid and SQL offset. Direct canonical import validates
these bounds before committing a batch, while native checkpoints retain the mode and reject
mixed-mode partitions. Legacy slice-mode snapshots remain readable. The handoff control checks
exact per-end counts, one/two/three-step cumulative bounds, rejected malformed prefixes and raw
checkpoint continuation across memory and typed RocksDB. The full native suite passes 1,050
checks with two existing ignores. Canonical export remains a direct live-state visit; the new
mode does not introduce an intermediate native snapshot or a performance gate.

Window operators bind final-window mode before installing the time-zone calendar. This keeps
regional calendar setup from re-enabling shared slices for a final HOP/CUMULATE merge. The job
graph retains the mode on ordinary native checkpoint recovery; runtime evidence reads the same
binding. Java/JNI recovery controls merge additional partials after restore and ensure that a
closed end stays closed on both native memory and typed RocksDB.

Local windows already emit every pending partial before a checkpoint barrier. A dedicated
stock-compatible local mode on Flink 2.2 now returns empty snapshot futures after that drain,
so the stateless stage saves neither native keyed metadata nor serializer references. Missing
the drain fails the snapshot without consuming the pending partials; it never emits them after
the barrier. Scalar and counted-DISTINCT controls check all four managed/raw state collections,
exact frequencies and continued input without replay. Stateful operators and the ordinary local
mode retain the existing snapshot path by default. The version bridge keeps Flink 1.18's final
snapshot lifecycle unchanged; this foundation does not establish 1.18 stock interoperability.

When an adapter reconstructs a key from logical fields, ordinary row serialization is not the
same operation as Flink's generated key projection. The projection reserves NULL variable slots
for non-compact decimals and timestamps; omitting them changes binary equality and key-group
hashes. The runtime projection helper uses stock binary writers with those exact NULL rules,
without loading planner classes on workers. It produces binary rows for stock serializers;
no StreamFusion serializer is added to savepoint metadata. Controls compare bytes and hashes
with released generated selectors across compact/non-compact types, nested values, writer reuse,
row kinds, empty keys and multiword NULL bitmaps.

The native deferred-partial API retains owned Arrow batches until its caller drains them on
progress or a barrier. It selects namespaces at arrival, registers late-input timers immediately
and leaves a past shared slice without a fabricated trigger when a buffer flush arrives late.
A later input can make that retained slice contribute again. Buffer bytes count against the
operator reservation; imported buffers and dictionary children are copied into native ownership.
Snapshots reject pending partials and grid changes cannot reinterpret queued rows. Direct
canonical visitors remain the export path; this adds no intermediate native snapshot.

The full native regression passes 1,050 checks with two existing ignores. Twelve focused
controls also pass in an isolated foundation checkout, including deferred namespace handoff,
missing-drain rejection, immutable pending grids and dictionary ownership. Runtime integration
and broader buffer-capacity qualification remain tracked by
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569); benchmarking is deferred.

Fixed-window firing records the earliest remaining epoch deadline for UTC and fixed-offset
state as well as regional calendars. Typed RocksDB firing combines persisted deadlines with
pending buffered COUNT state; an idle restore must not require another record to discover its
next timer. A focused native control compares all three fixed shapes between memory and the
typed store through partial and complete firing. The integration native regression passes
1,059 checks with two existing ignores. Processing-time stock-state import and automatic timer
rearming remain part of the runtime qualification in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569); benchmarking is deferred.

The native SESSION primitive can bind a stock admission watermark independently of its firing
frontier. Expired new sessions are dropped and expired merges retain the released host failure;
fixed-offset native bounds are rendered in the stock local domain for diagnostics. The binding
owns its JVM callback and is transient: callers rebind it from stock UNION state after native
checkpoint restore, leaving existing checkpoint formats unchanged. Thirty-three direct JNI
controls cover both stores, keyless state, four zones, inclusive MAX watermarks, monotonic
admission, input release on rejection and required rebinding after restore. Runtime/planner
qualification is separate; this primitive alone does not establish complete SESSION coverage.
Benchmarking remains deferred under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).


The native window Top-N transfer interface visits retained input rows directly, without first
encoding a native checkpoint. It preserves rank/tie order within each window and partition and
splits even a single large persisted list into batches of at most 1,024 rows. Memory and typed
RocksDB use the same transfer schema. Import validates all rows of each batch before updating
state, and callers install the exclusive firing frontier in the configured native time domain
before importing rows, including for an empty savepoint. JNI controls verify buffer release on
invalid input and receiver failure, unchanged retained rows on rejection, and complete retry in
tie order after a failed export. These primitives leave ordinary checkpoint formats unchanged;
stock state-schema and planner qualification remain separate parts of
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.


The bounded OVER native bridge optionally retains complete original frame inputs before processing.
Memory owns the captured Arrow buffers; typed RocksDB stores a parallel, key-group-prefixed input
table with the same frame positions and an explicit retention marker. Eviction and key cleanup
remove both rows, and native checkpoint restore/rescaling retain the marker and owned payloads.
Native key-group snapshots carry optional frame input payloads; older snapshots remain readable
for ordinary execution but cannot invent missing original rows. Direct canonical frame export
reads live inputs without producing a native snapshot first. This foundation does not yet admit
stock bounded OVER canonical interchange: its complete stock state and timer projection remains
part of [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).


Bounded OVER native arithmetic now retains primitive SUM's nullable sum and independent live
count alongside SUM0, COUNT and widened AVG state. Native frame checkpoints carry those fields
without serializing evaluated averages. The persistent reader accepts unmarked, version 1 and
expanded version 2 frame values; memory readers accept the preceding fold-column layouts.
Native migration preserves known old fields and marks unrecorded arithmetic explicitly. It cannot
recover history an old implementation discarded. Direct stock bounded projection remains
unqualified until the full released accumulator/view/buffer/counter/timer contract is installed;
ordinary native checkpoint migration is not a stock conversion step.

Bounded event-time RANGE also retains the complete registered timestamp set and last-fired
timestamp independently of the current cleanup deadline and processed frame. Superseded cleanup
timers can advance late admission without processing a row. Native memory/key-group snapshots
and a separate key-group-prefixed RocksDB table preserve that protocol through recovery and
rescaling; cleanup removes it with the key. Old native snapshots can seed their former cutoff,
but cannot reconstruct missing historical timers. This closes an information-loss gap for the
future stock adapter without qualifying bounded stock canonical import/export. Performance
validation remains deferred for [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).

The direct bounded OVER frame bridge transfers original inputs and raw arithmetic in one
partition batch, with the raw fold and per-key native protocol on its final row. Memory reads
live frames; RocksDB visits one partition at a time. Neither path creates a native savepoint as
an intermediate. Import owns input buffers, validates the schema/position/key/protocol before
installing state, and never reconstructs a raw accumulator by replay. Persistent import rejects
unowned key groups and duplicate partitions. Normal native checkpoint layouts are unchanged.
This bridge does not yet write or read Flink's released bounded state descriptors: complete
stock counters/views and complete automatic planner coverage remain required by
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Performance work is deferred.


The direct event-time RANGE protocol also visits keys without processed frames. Typed keys,
optional cleanup/last-trigger state, all queued timestamps and input-map membership are transferred
independently of retained rows and raw arithmetic. This includes empty callbacks left behind by
Flink's wrapping cleanup deadlines. Native firing preserves their later admission cutoff and
emits completed RANGE data in timestamp order. Native RANGE checkpoint timer metadata can carry
a NULL cleanup deadline; prior non-NULL timer metadata remains readable. Frame payload layouts
are unchanged. The stock operator adapter and complete #569 acceptance matrix require separate
qualification; benchmarking remains deferred.


### Stock-schema projections for native operators

A bound operator restores released Flink canonical handles into a stock-keyed projection before
allocating its native state. Descriptor kinds, key and namespace serializers, and timer queues
are validated first. The runtime native backend keeps its batch-key serializer. Native checkpoint
handles go through the ordinary backend and do not materialize the projection. Bindings match the
complete native class and operator ID in Flink's subtask description; an unbound class that only
shares a name prefix keeps its own handles.

At a canonical savepoint barrier the operator visits live native state into that projection and
uses Flink's canonical writer to capture keyed and managed operator state together. It then clears
the temporary projection. It does not serialize and reread a native checkpoint. Projection cleanup
belongs to the task backend lifetime, including failed initialization and native close failures;
cleanup errors are retained without hiding the original failure. Ordinary processing and native
checkpoints retain their normal state paths.

The released 2.2 arithmetic event-time RANGE contract binds `accState`, `inputState`,
`cleanupTsState`, `lastTriggeringTsState` and the stock void-namespace event timer queue. Input
buckets contain full original rows under `ListSerializer<RowDataSerializer>`, with stock long
bucket keys. Generated accumulator fields are copied as raw values. A generated hidden input
count counts retained rows, including NULL aggregate arguments; it is independent of a visible
`COUNT(value)`. All registered cleanup/data callbacks, pending-only keys and orphan last-trigger
state transfer directly. Arrow RowKind field declarations agree across retained and pending rows.

The integration fixtures cover heap/RocksDB in each engine, nullable and empty keys, complete and
pending-only frames, invalid state, direct re-export, clean classpaths and ordinary native recovery.
The automatic portable-plan binding is still under consolidation. Bounded ROWS counters/buckets,
remaining RANGE views, the full version/rescale matrix and other #569 requirements remain open.
Benchmarking and tuning remain deferred.

## Canonical metadata identity projection

The Flink 2.2 component can project native operator identities to the original stock identities
while publishing canonical savepoint metadata, and reverse that mapping while restoring it.
It retains subtask state handles, coordinator state, master state, parallelism and finished
markers. Payload streams and their ownership stay with the configured Flink storage provider;
ordinary checkpoints and native-format savepoints retain their metadata bytes.

Unknown durable state, even a zero-byte coordinator handle, cannot be omitted. Only explicitly
identified empty native helpers may lose finished markers on export. Import reconstructs their
finished status from upstream operators: partial status propagates only over pointwise boundaries
at unchanged parallelism, and a union requires all its inputs to finish. This preserves bounded
branches when the runtime graph inserts transposes or exchanges.

Publication validates the complete metadata before finalizing the provider's metadata stream.
Relative file handles retain their relocation behavior, temporary reader context files are
removed, and cancellation can close a blocked storage read or write. Twenty isolated clean-build
controls cover identity/state ownership, finished and union branches, relocation, malformed or
unmapped state, stale/disabled configuration and failure/cancellation cleanup. Automatic installation for qualified stock compiled plans is described below; the remaining
operator and submission coverage stays open in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
Benchmarking of canonical handoff remains deferred.

### Binding the complete logical topology

The Flink 2.2 graph-binding component derives original primary operator identities from an
explicit mapping of replaced logical operators. It matches unchanged stages around those
anchors, including connector-created writer/committer operators. It preserves union edge
multiplicity and input positions, and rejects incomplete, ambiguous or changed topology.

Only known empty transpose, batch-splitting and sharing helpers may be elided. A concrete
operator wrapped in a simple factory cannot be replaced merely by reusing its display name;
its replacement needs an explicit logical binding. Generated stock factories may compile the
same stage into different generated class names. Seven isolated graph controls cover those
boundaries, repeated connector names, expanded sinks, unions/repeated edges and invalid bindings;
the combined clean graph/metadata run passes 27 controls. This component does not itself install
an automatic planner workflow or qualify stateful exchange reassembly.

### Automatic stock compiled-plan binding

The unmerged Flink 2.2 planner retains a stock compiled plan as the portable artifact. Compiling
SQL writes stock JSON; explaining or executing that artifact with StreamFusion enabled lowers a
private runtime copy. The copy binds event-time window joins to the actual stock key serializer,
list/namespace/timer layout and resolved maximum parallelism, and installs canonical metadata
identity projection automatically. The original JSON remains executable on stock Flink. Ordinary
SQL submission identities and the other state families are still being consolidated in #569.

Stateless Calc and optional connector source extensions retain their existing admission checks
when loading a stock artifact. Connectors without a compiled-source extension stay stock. The
Kafka extension uses its existing source whitelist; this planner support does not qualify broker
offset, transaction or fault-recovery interchange. Unaligned window-join exchange reassembly
currently retains the stock join state owner rather than eliding its pending work.

The clean compiled-plan qualification includes 46 complete-job window-join cases, 50 planner
controls, unchanged released restore fixtures and existing ordinary SQL/planner regression checks.
The shared Flink 1.18 planner retains its previous implementation; its regression checks do not
qualify Flink 1.18 stock canonical interchange. Canonical conversion runs only at the canonical
savepoint boundary; ordinary native checkpoints retain their existing paths. Benchmarking remains
deferred while the full #569 functionality is completed.

### Event-time interval cache projection

The released Flink 2.2.1 interval component projects live native rows into stock timestamp-keyed
map buckets of row/matched-flag tuples, the two independent cleanup value states and the shared
void-namespace event timer queue. Timer-only keys survive even without cached rows. Full inventory,
key/row/timestamp, serializer and callback ownership validation finishes before native import;
rows then enter retained state directly with owned buffers. Stock metadata contains only released
Flink serializer classes. Native canonical export takes no intermediate native snapshot.

The explicit component binding passes 240 positive and 45 rejection/guard controls, plus typed
key native-checkpoint continuation. The processing-time adapter is described below; wider
full-job/version/rescale/failure coverage remains open in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). See the
[interval contract](../operators/joins/interval-join.md#released-stock-event-time-state-adapter)
for the exact qualified types and remaining nonpositive/overflow-time boundaries. Benchmarking
remains deferred.

Interval and window join input normalization now shares the same logical-schema boundary.
Transport row-kind/cache companions do not become retained SQL fields. Older tagged interval
native IPC normalizes on memory/persistent recovery without changing its frame layout, row
identity/match flags or independent cleanup timers. Direct canonical export still visits live
state. The clean normalization component passes 1,161 native tests (two existing ignores) and
618 Java controls; these overlap the adapter qualification above.

### Automatic interval binding

The stock compiled-plan workflow now installs the interval state contract and canonical identity
projection during normal initialization. It resolves the original stock maximum parallelism and
binds it to both native exchanges and the state owner. Portable JSON remains stock-executable.
Twenty-six clean-process interval jobs pass across both stores/orders and all four join kinds,
including repeated switches, no-input export, native-checkpoint recovery and 64-key rescaling
through 1 → 3 → 2 tasks. Custom sources, transactional sink effects and both transposes remain in
the jobs, while stock workers exclude StreamFusion. The clean workflow passes 198 controls.

The unchanged released event-time and processing-time interval fixtures each import ten pending
rows and process five new native rows. The three interval and eight window programs all pass on
both engines. Native execution includes eight native state-owner programs, two verified stock
window SEMI/ANTI paths and one impossible-interval graph with no join state owner. The latter is
not native-join credit. Ordinary SQL identities and the remaining full #569 matrix remain open.
Benchmarking stays deferred.

### Processing-time interval payloads and callbacks

The released Flink 2.2.1 processing-time interval adapter preserves original SQL rows separately
from each row's arrival clock. A private physical BIGINT column holds that clock in native state;
joined output projects it out. Canonical export writes the original payload and matched flag into
the stock tuple list and uses the independent clock as the stock map bucket. Native export visits
live state directly without an intermediate native snapshot. Original stock row/key serializers
remain unchanged and require no StreamFusion classes on restore.

Flink represents a PROCTIME attribute as a NULL placeholder even when the logical type is NOT
NULL. The native physical and transfer schemas permit that placeholder, and export preserves its
NULL bit. Residual predicates retain their original logical left/right field positions despite
the private clock. Original NULL or arbitrary timestamp payloads are never replaced by the clock.

Processing-time state consumes the stock processing `user-timers` queue and requires an empty
event queue. The event-time contract applies the converse rule. Both modes reject a mismatched
native operator. Import validates all cache, key, serializer and callback ownership before any
native row is installed. Idle and already-due callbacks rearm from the earliest retained native
deadline after import or ordinary native checkpoint recovery. End of input does not force future
processing-time callbacks to fire, matching stock behavior. Unbound legacy native jobs retain
their existing stamping and finish paths.

Controlled-clock coverage comprises 208 positive processing-time cases and 17 rejection/guard
cases: all four join kinds, both stock/native stores, both switch orders, NULL/original payloads,
nine scalar key layouts, no-input re-export, timer-only keys, overdue callbacks on open, end of
input and native checkpoint continuation. The stock compiled-plan loader binds both clock modes
to the original stock operator identity, key serializer and resolved maximum parallelism. Its
clean planner/regression selection passes 175 checks, including 29 interval planner controls.
The expanded complete-job matrix is qualified below. Remaining #569 requirements retain their
separate scope; component and upstream passes do not establish every supported job variant.
Benchmarking remains deferred.

## Independent temporal cleanup partitions

Native temporal join snapshots retain cleanup-only key groups even when neither row side holds
records in that group. Memory and persistent logical exports take the union of both row-side
inventories and independent cleanup-state groups. Versioned native snapshots now retain exact
deadlines together with signed cursors and independent event callbacks. Restore and redistribution therefore retain timer-only state rather than
silently omitting its key group.

Two native regressions first reproduce the loss on each original backend, then verify both
source/target stores, 64 negative keys, empty/mixed row sides, exact deadline ages, redistribution
across three subtasks, repeated export and silent expiry. The clean native suite passes 1,166
tests with two existing ignores; targeted Java operator, SQL/backend and key-group checks also
pass. Those regressions establish native logical-snapshot retention; direct stock temporal
interchange is qualified separately below. Benchmarking remains deferred.

### Complete event-time and processing-time interval handoffs

All 84 clean-process interval job cases pass against uninterrupted stock baselines. The matrix
contains 32 continued-input switches and 32 residual-predicate switches across both clock modes,
four join kinds, both native stores and both engine orders; eight FULL-join no-input re-exports;
four INNER-join native checkpoint recoveries; and eight FULL-join 64-key rescale cases through
1 → 3 → 2 tasks at maximum parallelism 128. The checkpoint cases trigger a real checkpoint,
cancel, restore and then export a canonical savepoint. Native execution is verified on every
claimed continued-input native leg. Each job retains two custom sources, a transactional sink
and both transposes; stock processes exclude StreamFusion, portable metadata contains no
StreamFusion serializer classes and committed sink effects match stock.

Processing-time complete jobs use a one-day matching range so process startup does not determine
expiry. The separate controlled-clock cases verify cleanup at exact deadlines, original/NULL
payloads, idle/overdue callbacks and EOF. The combined clean job/control selection passes 457
checks with no failures, errors or skips. These overlap the component and planner cohorts.
Wider ordinary-SQL identity/type/version/failure and connector-protocol requirements remain open
in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.

## Released temporal bookkeeping contract

A shared 48-transition trace pins temporal probe cursors, minimum registered timer values and
physical callback ownership to released Flink 2.2.1. Eighteen stock-operator cases exercise both
backends and canonically restore every transition onto the opposite backend (96 restores).
Cases include cursor-only state, signed cursor overflow/output ordering, callback-only state after
TTL cleanup, right-only callback cleanup refresh, timer reordering, extreme timestamps and
retained build versions when no callback is queued. The internal native metadata helper and its
versioned codec follow this trace and reject malformed metadata without conflating MIN with
absence. The clean native suite passes 1,168 tests with two existing ignores.

Both native stores now execute and recover this bookkeeping. They prune versions only when the
owning key has a due callback, preserve signed per-key indices, and account orphan callbacks until
they fire. Four-section raw snapshots and versioned persistent metadata preserve original clocks,
with frozen previous-writer fixtures qualifying legacy raw and persistent recovery.

## Stock temporal canonical interchange

Released Flink 2.2.1 INNER/LEFT temporal operator fixtures now transfer live state directly in
both directions. The adapter writes stock `left`/`right` maps with their original row serializers,
signed probe indices, version timestamps and row kinds; independent `next-index`, `timer` and
optional `cleanup-timestamp` values; and the actual Void-namespace event/processing queues.
Cursor-only, cleanup-only and orphan-callback state survive even when both row caches are empty.
Export visits native memory or typed persistent tables in bounded row chunks without creating a
native snapshot first. Import preflights the complete stock inventory, serializers, key/row/time
agreement and callback ownership before any native mutation.

The clean qualification contains 80 positive controls and 32 malformed-state rejections. The
72 trace cases combine both join kinds and both stock/native stores, with 384 canonical handoffs
across the shared 48-transition released trace. Every next native action is checked against stock;
continued stock actions after export are also checked. Ordinary native checkpoints alternate with
stock-schema re-imports. Eight additional cases preserve NULL payloads, all four row kinds,
last-write-wins build versions, late probes and continued execution in both engine orders.
Rejected descriptors, callback/value mismatches and corrupt row keys/timestamps leave native state
empty. Exported serializer snapshots restore with StreamFusion and planner classes blocked.
Strict Arrow ownership checks remain enabled across every handoff and teardown.

The independent build passes 1,184 native tests (two existing ignores) and 185 Java checks on
2.2.1, including the controls above and 73 overlapping stock/runtime/SQL regressions. Shared
runtime checks on 1.18.1 pass 54 cases with one existing unsupported SESSION table-function skip;
those checks do not qualify the temporal canonical adapter on 1.18. Finish retains final native
cleanup clocks without trying to schedule on an already stopped processing-time service.

This qualifies representative operator conversion using BIGINT keys/payloads and compact time
fixtures. Automatic temporal compiled-plan binding and released upstream restore programs are
qualified below, followed by representative complete-job switches and rescaling. Wider schemas,
additional rescale shapes and the full failure/version/connector matrix remain open in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
Every one of the 15 operator families still has partial coverage. Benchmarking remains deferred.


## Automatic compiled temporal binding

The released 2.2.1 stock compiled-plan loader now binds event-time temporal joins automatically.
It retains portable stock JSON, the original operator identity and key serializer, time indices,
join kind, residual predicate and retention configuration. Both columnar exchanges and the native
state owner use the original job's resolved maximum parallelism. Both inputs preserve row kinds.
The rowtime temporal-function form uses the same verified stock operator/state layout and is
bound too. Unaligned exchanges, changelog backends and asynchronous state retain stock owners.

Twenty-three planner controls cover both join kinds, TIMESTAMP/TIMESTAMP_LTZ(3), residuals,
retention (including root configuration), portable reloads, disabled/repeated explains and
unsupported modes. Two independent boundary controls override the environment's maximum on
both exchanges and the state owner. Ninety-one neighboring planner/key/SQL controls pass too.

All four unchanged released `TemporalJoinRestoreTest` programs pass on both stock and native:
plain temporal table joins, keys derived from nested rows, keys derived from maps and a rowtime
temporal function. Each native program imports four retained rows and consumes four new rows in
the actual native temporal operator. Derived keys are scalar at the temporal state boundary.
Eighty-eight canonical execution-witness controls and 48 independent summary/audit controls pass.
Explicit program contracts reject unknown fixtures and unrelated/zero-input native work. The
stock test runtime excludes StreamFusion. Representative complete-job switches and rescaling
are qualified below; wider failure/connector matrices remain open
in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.

## Complete temporal job handoffs

Twenty-six isolated complete-job scenarios pass on released Flink 2.2.1: 16 continued-input
round trips across INNER/LEFT, both native stores, both engine orders and residual predicates;
four LEFT no-input re-export round trips; two INNER native checkpoint/cancel/restore handoffs;
and four LEFT rescale round trips. Rescale distributes 64 actual temporal keys through 1 → 3 → 2
tasks at maximum parallelism 128 while the two custom sources and transactional sink remain at
one task. Every advancing native leg requires actual temporal input; each claimed rescale task
must process input. The portable stock plan stays unchanged and stock processes exclude
StreamFusion. All canonical metadata artifacts, including no-input returns, are checked for
StreamFusion class names. External committed rows match both an uninterrupted stock run and an
independent version-history lookup oracle.

These cases retain BIGINT payloads, TIMESTAMP_LTZ(3), both transposes, duplicate/NULL probe keys,
version history, residual rejection and unmatched LEFT output. Watermarks keep probes pending
until final drain so every handoff must preserve their state. They do not qualify intermediate
temporal-output commit failures, full-job TTL expiry, sink-writer rescaling, wider schemas or
cross-version handoffs. The clean combined selection passes 240 checks with zero failures,
errors or skips: these 26 jobs, four interval job controls and 210 temporal/stock/planner controls.
Component and upstream cohorts overlap this selection and are not added cumulatively. Every
operator family remains partial under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.

## Generated operator IDs in qualified compiled plans

Temporal, interval and window join bindings also work with `table.exec.uid.generation=DISABLED`.
State contracts are registered against the actual primary IDs of the completed native runtime
graph, after any explicit UIDs have been applied. They no longer infer the runtime ID by hashing
a required UID string. Canonical metadata still projects the complete source/operator/sink graph
to the original stock primary IDs in both directions. No invented UID, manual mapping or
unmatched-state allowance is needed. Native checkpoints retain their native IDs and state paths.

The independent clean selection passes 373 checks with no failures, errors or skips: 124
planner/identity checks and 249 job/runtime/metadata checks. Thirty-four complete jobs disable
explicit SQL UIDs: the full 26-case temporal matrix above; four native-first INNER interval
round trips across both clocks and stores; and four native-first INNER TUMBLE window round trips
across keyed/keyless variants and both stores. Ten complete jobs retain explicit UIDs as controls.
The temporal cases include both initial engine orders, no-input returns, native checkpoint
recovery and 64-key rescaling at 1 → 3 → 2/maxP128. Generated-ID rescale cases disable chaining
from the initial job on both engines so the original stock IDs stay stable. With default
chaining, changing these parallelisms changes chain boundaries and generated IDs: the first four
rescale cases failed, and released stock also rejects its own original stock savepoint in that
configuration. Two stock hash controls verify the distinction. Do not change chaining only at
restore or drop the resulting unmatched state. The six identity controls verify that the
registered contract key equals the actual runtime primary ID, with and without explicit UIDs.
The original three disabled-UID join controls failed before this change; explicit-UID controls
already passed.

All 15 unchanged released temporal/interval/window restore programs pass on each engine with the
same production change. Twelve native-side programs require positive state import and continued
input in the actual native join; two verify stock window SEMI/ANTI owners and one verifies a join
Flink elides. These upstream programs use their original UID settings. Their strict audit exposed
a false
fallback diagnostic for a join already elided by stock. The final source passes the complete
373-check selection as well as all native programs; initial failed reports and stock-only
negative evidence are retained. Cohorts overlap earlier qualification and are not added cumulatively.
Wider operator, connector, type/version and failure coverage remain open in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Every family stays partial;
benchmarking is deferred.


## Ordinary SQL canonical handoffs

The deployed Flink 2.2 planner now captures the original stock logical roles after physical
optimization and carries those origins through native substitution. Normal `executeSql` submission
can bind temporal, interval, window and regular joins without compiling or loading a plan. The state schema,
key serializer, resolved maximum parallelism and complete source/operator/sink identities come
from the original stock graph. Native input operators and both perimeter transposes remain in the
actual execution graph. Connector-created writer names retain their original logical sink role.
No global execution-node counter is reset and no operator is matched by list position or count.

Contracts use actual native primary IDs with `PLAN_ONLY`, `DISABLED` and `ALWAYS` SQL UID settings.
A later query resets the previous contracts and projection. Post-optimization origin metadata is
not registered with Calcite's optimizer, preserving subsequent queries in the same session.
Unknown native families/connectors or ambiguous/missing logical owners keep their existing native
execution and checkpoint route while their canonical adapter remains unqualified. Ordinary joins
with DECIMAL precision above 18 or timestamp precision above 3 in the key retain that route too:
NULL variable slots differ between their existing key layout and Flink's generated projection.
This boundary preserves existing native checkpoint routing pending complete key-layout migration.

The clean regression selection passes 331 checks, including 21 compiled/ordinary identity and
key-layout controls, eight deployed ordinary native-family parity controls and existing stock
state/metadata/SQL controls. All 26 ordinary temporal, 84 ordinary interval and 46 ordinary window-join
handoffs pass across their compiled-plan scenario matrices above, using default SQL UIDs.
The final clean cohort passes all 271 checks, including the eight emitted-output failure cases,
78 direct regular-state controls, 21 generated-identity controls and eight native parity cases. The
rescale fixtures disable chaining from the initial job to preserve original generated stock IDs.
Two additional ordinary temporal `ALWAYS` UID controls pass on the native stores. These jobs
submit SQL without a compiled artifact, retain two custom sources, a transactional sink and both
transposes, verify positive native input and stock-only metadata, and compare external committed
effects with stock. Temporal results also match an independent version-history oracle.

Eight further ordinary temporal cases emit duplicate probe results before the first boundary,
while other probes remain pending. They fail immediately before or after the external atomic sink
write, on either initial engine and both native stores. Each restores a real ordinary checkpoint,
then makes a canonical round trip through both engines. The failure is verified, the initial
external result is exactly zero or two rows, and final committed effects match stock and the
independent oracle with no duplication. Native progress is required after every advancing native
leg. This qualifies the fixture's idempotent transaction protocol; full-job TTL expiry, other sink
protocols/writer rescaling and broader type/version/failure matrices remain open.
All 15 unchanged released temporal/interval/window restore programs still pass on
each engine: 12 actual native imports with continued input, two verified stock window owners and
one join elided by Flink. Every family remains partial under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking stays deferred.


The complete-job runner observes terminal job completion while waiting for source readiness.
Existing readiness and case deadlines remain in force. Stock/native state, execution and external
sink-effect assertions do not depend on a separate job-status RPC latency threshold.


## Direct regular-join state adapter

An explicitly bound Flink 2.2.1 regular-join adapter transfers both live native input sides into
released `left-records`/`right-records` state. Each side independently uses a scalar row, a map by
another unique key, or a full-row occurrence map. Outer sides preserve association degrees;
TTL descriptors preserve original per-row write timestamps. Keys, rows, tuples and TTL envelopes
use stock serializers. Native export visits live state directly without a native snapshot
intermediate. Indexed RocksDB scans records and legacy buckets without hydrating entire keys;
imports merge chunks without expiring or refreshing retained rows.

Before import, both complete input sides are validated in bounded Arrow batches using the same
native row/key decoder as insertion. Only after every batch passes may either side mutate. This
includes a malformed right side after valid left data or a malformed row beyond the first 1,024
rows. Validation drops Arrow ownership before installing a JVM exception. Ordinary native row
processing, checkpoint layouts and TTL read behavior are unchanged.

The independent component passes 78 Java controls: six join kinds, three stock side shapes,
keyed/keyless continued execution, both native stores, multi-batch import/export and original TTL
ages, and malformed state. Eight new complete-validation cases first failed with partially
imported left state and now pass, including a malformed row forced after 1,025 valid rows. This
qualifies direct operator conversion using BIGINT fixtures and stock heap projections, not
arbitrary schemas or the complete backend/version/rescale/
connector/failure matrix. Those remain required by
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking stays deferred.

The same isolated native source passes 1,187 tests with two existing ignores. Shared Flink 1.18
compilation and runtime regressions pass 44 checks, with one existing skip because that release
has no `STATE_TTL` hint. These shared controls do not qualify stock canonical regular-join
interchange on Flink 1.18.


## Automatic regular-join binding

The Flink 2.2.1 planner now binds the direct regular-join adapter during stock compiled-plan
execution and ordinary SQL submission. It reads actual stock side layouts, unique-key selectors,
serializers and independent retention settings, and uses the original resolved maximum parallelism
on both exchanges and the native state owner. When Flink defers an unset maximum, it uses Flink's
released default for the original parallelism. Explicit UIDs are optional: state registration uses
the actual native primary ID and metadata retains the complete original stock topology.

Ordinary SQL binds the existing native join, retaining its input flags and mini-batch mode.
Physical changes apply only after the complete graph qualifies. Native filters and mini-batch
assigners keep explicit original logical identities; they are not elided as transpose helpers.
When rewriting duplicates a shared stock calculation, filter or watermark role, equivalent copies
with the same original input role collapse to one execution owner. Arrow sharing is then rebuilt
for the actual fan-out. Missing or incompatible roles still reject canonical binding.
Constructed payloads, unaligned exchange reassembly, asynchronous/changelog state backends,
ambiguous rewrites and unqualified ordinary wide-key layouts retain their existing owners/routes.

Fifty-six planner controls pass across six join kinds, three stock side layouts, both submission
forms, generated IDs, mini-batch binding, explicit maximum 257 and unset maxima at parallelism
1/172. Four initial native-first INNER multiset round trips pass across both stores and submission
forms, with two custom changelog sources, a transactional sink, both transposes and clean stock
classpath/metadata checks. Materialized external results match stock and an independent join oracle;
concurrent input interleavings need not produce identical intermediate changelog order.

All 15 unchanged released `JoinRestoreTest` programs pass on both engines. Every native program
imports actual regular-join state; 14 also consume new join input, while the released literal-aggregate
fixture requires restored native execution without new join input. The combined unchanged join
selection passes all 30 programs per engine, including existing temporal/interval/window contracts.
Eight additional shared-calculation round trips pass across both engine orders, native stores and
submission forms. These compare materialized external effects with stock and an independent oracle;
they reproduce the duplicated-owner gap that previously left a native self-join unbound.
The frozen regular-binding candidate passes all 164 complete-job scenarios: 144 continuation
cases across six kinds, three stock side layouts, both engine orders, native stores and submission
forms; eight no-input re-exports; four native checkpoint recoveries; and eight 64-key rescalings at
1→3→2 with maximum parallelism 128. Its combined cohort passes 216 checks. The later shared-role
candidate separately passes 179 planner/regression checks and the eight shared-input jobs above.
Wider schema/backend/version, mini-batch jobs, connector protocols, TTL expiry and failure/rescale
qualification remains required by
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). All families stay partial.
Benchmarking remains deferred.


### Keyless regular-join admission

A stock empty-row key is a singleton state boundary. Its released maximum parallelism is one,
even when the surrounding pipeline configures a larger maximum. Keyless INNER binding preserves
that stock context. The compiled-plan path checks the original source changelog through known
row-preserving stages before selecting INNER or ANTI: both inputs must be insert-only and retain
a payload column. This retains the already verified null-aware `NOT IN` ANTI restore path;
requiring INNER alone incorrectly removed that native owner. Other outer/updating keyless inputs
keep their stock join owner. Ordinary native admission continues to require keyless INNER.


Eight keyless INNER clean-process round trips pass across both engine orders, native stores and
submission forms. They retain duplicate rows and NULL payload keys, and check the stock singleton
state, external committed effects and an independent nested-loop oracle. The planner selection
passes 56 controls, including keyless INNER binding and preserved stock owners for unsupported
keyless outer/updating inputs.

Mini-batch FULL joins with a changing residual predicate remain unqualified. In a new eight-case
job cohort, every switched result matches the independent oracle, but three uninterrupted stock
baselines omit an unmatched opposite row. A deterministic released-stock-only control reproduces
this: after a matched left value changes from 10 to 15 under a `MOD(left,3)<>0` residual,
immediate FULL join emits both unmatched rows, while mini-batch suppression omits the right row.
Both stock behavior controls pass; they provide no native interoperability credit. The discrepant
job evidence is retained, and stock-baseline assertions have not been relaxed. Complete mini-batch
qualification remains open in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).


Four additional compiled keyless ANTI process round trips verify nullable-left `NOT IN`, including
right arrivals that retract unmatched left rows retained before the savepoint. Both native stores
and engine orders retain custom sources, the transactional sink, both transposes and stock-only
classpath/metadata checks. The unchanged released ANTI restore program remains a required native
execution contract rather than a permitted fallback.


## Stock-compatible first-N counters

Flink 2.2.1 arrival-ordered first-N binds the released `counterState` VALUE descriptor, stock INT
serializer and optional raw TTL envelope. Native memory and typed RocksDB export actual keys,
counters and original write timestamps directly through bounded Arrow batches into the stock
canonical writer. No retained input rows or intermediate native snapshot are needed. The RocksDB
visitor reads self-contained persisted values without applying TTL read expiry or refreshing ages;
ordinary state reads retain their expiry behavior.

Whole-view import requires an empty native target and rejects pre-existing counters before any
insertion. It then validates the complete stock view before inserting counters. The same native decoder
checks transfer schemas, fixed key width/row kind, duplicate keys and counter values. A malformed
counter after the first 1,024 rows cannot leave earlier valid state imported. RocksDB hydration
preserves raw ages and detects duplicates across persisted chunks. Arrow buffers are released
before JNI errors are thrown, and closing a failed target releases its existing memory reservation.

Both stock compiled-plan execution and ordinary SQL submission capture the actual released
first-N implementation, constant start-one range, output-rank mode, key serializer, retention and
resolved maximum parallelism. Generated IDs work without manual mappings. The existing `topN`
operator switch still controls admission. The portable compiled JSON remains stock-only.
Ordinary keys with non-compact decimal or high-precision timestamp root layouts retain their
existing native route pending key-layout migration. Async/changelog backends, unaligned exchange
state and non-first-N rank variants require their separate contracts. Existing native checkpoint
encodings and processing remain unchanged; checkpoint upgrades still require compatible graph
identities and maxima. Older native jobs whose maximum differs from the stock reference are not
qualified by these new-job controls.

The independent native suite passes 1,195 tests with two existing ignores, including four newly
reproduced first-N validation failures and resource-limit cleanup. Twenty direct operator controls
cover both stores, keyless and nullable keys, rank-column modes, raw TTL ages/expiry, 2,049-key
transfers, late malformed input and receiver-failure retry. Thirty-two planner controls cover both
submission forms, limits 2/5, keyless state, projected/hidden rank, retention and maximum 257.

All 60 complete-job scenarios pass: 32 ordinary/compiled continuation cases across both native
stores/orders and projected, hidden, keyless and long-retention modes; eight no-input re-exports;
four native checkpoint recoveries; eight empty-state handoffs; and eight 64-key rescalings at
1→3→2/maxP128. They retain custom sources, the transactional sink and both transposes, use clean
stock classpaths/metadata, require actual native input and compare external effects with stock and
an independent arrival-count oracle. Expiry is exercised by clocked direct controls; the one-hour
job fixtures do not establish downtime expiry.

A thin suite class selects the released `RankTestPrograms.RANK_N_TEST` and inherits the original
restore implementation and assertions. The original fixture imports two native counters and
processes four new native rows. Combined with existing joins, all 31 selected released programs
pass per engine. The other three rank programs and their original contracts remain unchanged;
this selector supplies no qualification for their adapters. Wider schemas, backend/version and
connector/failure/delivery matrices remain open under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). All families remain partial;
benchmarking stays deferred.


The final whole-view import check additionally rejects nonempty native targets, preserving their
existing counters. A late persisted target duplicate first reproduced partial insertion of 1,024
new rows and is now rejected before any are inserted. The final combined cohort passes all 129 checks, including all 60 complete-job scenarios
with this stricter initialization check.
Shared Flink 1.18 processing/checkpoint controls pass 17 cases without claiming stock canonical
interchange on that line.


## Shared first-N state owners

A reused first-N subplan feeding both sides of a join retains one counter owner, matching the
stock graph. Ordinary lowering consolidates clones only when they refer to the same captured
stock operator and agree on rank limit, rank projection, partition/key layout, persisted
configuration, descriptions and input/output roles. Different limits retain distinct owners.
Canonical binding then captures the released state contract once and rebuilds Arrow sharing
before execution; savepoint export does not combine independently evolving counters.

Planner controls cover compiled and ordinary shared/distinct counter roles. All 32 complete-job scenarios pass across both submission forms/stores/orders: 16 continuation
cases with shared or distinct limits, eight no-input re-exports and eight rescalings at 1→3→2/maxP128.
They preserve custom source/transactional sink state, both transposes, clean stock metadata/classpaths
and independent retained-first-N Cartesian-product results. Both native families prove new input
after the barrier; a partial transpose may retain downstream join input until that barrier.
The combined planner/job cohort passes all 124 checks. These
new-job controls do not qualify upgrades from older native graphs with multiple real owners.
Other duplicated stateful roles remain part of [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).


First-N configuration equivalence is inspected in the Flink 2.2 lowering layer; the shared
node compares only its rank/key definition and remains compilable on Flink 1.18. The API
placement correction passes 100 current 2.2 controls (including eight ordinary process jobs)
and 31 shared 1.18 processing/checkpoint controls. These cohorts overlap earlier runs and
do not establish Flink 1.18 canonical interchange.


## Direct changelog-normalizer state projection

The direct Flink 2.2.1 adapter projects the existing native normalizer's full retained row into
released `deduplicate-state` VALUE state with its stock row/key serializers and optional original
TTL write timestamp. It reads live memory or bounded raw typed-RocksDB rows, with no serialized
native snapshot and no replay. Canonical iteration keeps expired raw ages; processing still
applies the existing expiry rules. Ordinary native checkpoint codecs remain unchanged.

Restore requires an empty native target. The complete stock view is validated before insertion;
each transfer also checks row kinds, reserved metadata, schemas, duplicates and agreement between
the actual key and the retained row's key columns. Persisted duplicates are hydrated without TTL
expiry before detection. Invalid later chunks cannot leave earlier valid rows imported.
Moved Arrow buffers close before JNI errors, and export can retry after receiver failure.

The isolated native suite passes 1,204 tests with two existing ignores, including four new
red-to-green import reproductions and continuation, TTL, mini-batch-drain and raw-scan controls.
All 30 Java checks pass: 16 direct controls across both native stores and 14 existing processing/
checkpoint controls. They cover repeated stock/native handoffs, full tombstone payloads, null keys,
late row-kind/key mismatches, nonempty targets, 2,049-row transfers, receiver retry and exact raw
write ages with expiry at the original boundary. Both normal submission forms now bind the released normalizer contract when the complete
graph's native state owners have qualified bindings. Wider schema/backend/version/failure
matrices remain required by [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
Benchmarking remains deferred.

Shared Flink 1.18 first-N/normalizer processing and checkpoint controls pass 31 checks;
this does not qualify canonical interchange on that line.


## Automatic normalizer binding and continued jobs

Ordinary SQL and stock compiled plans capture the actual released synchronous or mini-batch
normalizer function, its full-row/key serializers, update-before mode, TTL and resolved maximum
parallelism. The portable stock JSON remains unchanged. Generated identities work without manual
mappings. Capturing the loaded function preserves saved retention even if the submitting session
sets another default. Other real stock owners retain their state and identities.

All 52 normalizer/join job scenarios are qualified across both submission forms, native stores
and switch orders: 24 immediate/mini-batch/long-retention continuations, eight no-input returns,
four native checkpoint recoveries, eight empty-state handoffs and eight rescalings at fixed
maxP128, 1→3→2 tasks. Rescaling keeps sources/sink at one task and disables chaining from the
initial graph. A custom upsert source changes a join value independently of its primary key;
key-only deletes therefore require the saved full row to retract old join matches. An insert-only
source, transactional sink, both transposes, clean stock metadata/classpaths and external effects
are checked against stock and an independent last-row/join oracle.

Mini-batch cases suppress clock markers and require actual pending native normalization work to
flush at the savepoint barrier. A downstream partial transpose can retain join input until that
barrier; both families must prove new input afterward. Initial qualification passed 44 non-rescale
jobs; eight rescale witness checks mistakenly read a comma-separated family list as one name.
After applying the existing per-family check, all eight rescalings and 126 accompanying controls
pass (134 checks). The failing reports and source hashes are retained; overlapping cohorts are
not summed. Long-retention jobs do not establish downtime expiry; direct clocked controls do.

All 36 selected released first-N/join/normalizer programs pass per engine with original fixtures
and assertions. Three normalizer programs import two rows each and process five/five/six native
rows. Two pushed-filter programs retain their stock normalizer and receive no native-state credit.
The other 31 programs retain their prior strict route contracts. Shared 1.18 processing/checkpoint
controls pass 31 checks without a canonical-interchange claim on that line.

ROW-bearing keys, ordinary wide root-key layouts, async/changelog backends, unaligned exchange
state, other shared stateful roles and unqualified native consumers remain separate work. Older
native graph/maxP upgrades are not established by these new-job tests. All families remain partial
under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569); benchmarking is deferred.


## Direct eager-dedup state projection

The released Flink 2.2.1 eager contracts use `deduplicate-state` VALUE state: Boolean presence
for processing-time keep-first, or the full retained row for processing-time keep-last and
eager event-time keep modes. The adapter retains actual key bytes, the stock Boolean/row
serializer, original TTL write age and the proctime row-kind bit used by duplicate suppression.
The event-time comparator is checked against the retained row's timestamp during import.

Memory and typed RocksDB project live rows directly in bounded batches without a native snapshot
intermediate or input replay. Presence-marker transport fields are nullable even when the original
input was NOT NULL; they are placeholders in the bridge, while the actual stock value is Boolean.
Full-row state retains its declared schema. Invalid NONNULL primitive data is rejected rather
than exported as a fabricated valid value. Physical NULL PROCTIME placeholders pass explicit
controls with the existing timestamp representation; no epoch-zero substitution is introduced.

Restore requires a fresh target and validates the complete stock view before native insertion.
It checks keys against payloads, row kinds, timestamps, frame bounds and duplicates, including
persisted earlier chunks. Late corruptions cannot leave earlier valid chunks imported. Resource
exhaustion aborts initialization; closing the failed target releases its existing reservation.
Moved Arrow buffers close before JNI errors. Ordinary processing and raw native checkpoint
formats keep their existing paths; imported presence markers survive native memory checkpoints.

Direct qualification contains 66 Java controls: 48 continued mode/store/TTL/key-layout cases,
eight exact-age/expiry cases, eight forced late corruptions and two PROCTIME placeholder cases.
The key cases are nullable, NOT NULL and keyless; payloads are BIGINT and compact timestamps.
Thirty-two existing processing/checkpoint controls pass on both Flink lines. The final native suite passes 1,216 tests with two existing ignores, including
resource-failure cleanup and invalid-data controls. These cohorts
overlap and are not cumulative counts. There is no Flink 1.18 canonical-interchange claim.

## Automatic eager-dedup binding

Flink 2.2.1 ordinary SQL and portable stock compiled plans bind the eager adapter during normal
submission. The actual released synchronous or bundled function supplies keep mode, time column,
emission flags and retention. This preserves persisted TTL despite a changed loading-session TTL.
Original stock key serializers, generated/user identities and resolved maxima are retained; no
manual mapping or conversion command is required. The runtime explicitly selects eager state when
that is the selected stock function. Buffered rowtime-first has a separate candidate/timer contract;
processing-time last-row with both emission flags off has no retained stock rows. Its empty-state
projection keeps the released row schema while omitting unused native rows, and rejects unexpected
saved rows before native insertion. Buffered rowtime-first uses the distinct candidate/timer
contract described below.

The frozen eager binding at `1ec00607` passes all 374 checks, including 152 complete-job scenarios
(76 per submission form) and 222 direct/plan/processing controls. Twelve continuation variants
cover proctime first/last, rowtime last, mini-batch proctime modes and rowtime first/last,
full-chain/compact rowtime bundles, one-hour TTL and keyless state. Additional marker/row controls
cover empty state, repeated no-input returns, native checkpoint recovery and rescaling through
1→3→2 tasks at unchanged maxP128. The rescale fixture uses 128 keys over six rounds, fixed source/
sink parallelism one and chaining disabled from the initial graph. Long-TTL jobs do not establish
downtime expiry; direct clocked controls cover age preservation and exact expiry.

Custom source/transactional-sink state, both transposes, actual native input, pending bundle flushes
at canonical barriers, independent final-winner results, uninterrupted stock effects and clean
stock metadata/classpaths are verified. Two original released eager restore programs each import
two saved rows and process four new native rows. Validation-only visits receive no import credit.
The selected first-N and five normalizer programs also pass, eight programs per engine in that
cohort. Shared 1.18 processing/checkpoint controls pass 32 checks without a canonical claim.

After the empty-state/helper changes below, sixteen eager-switch regressions also pass with the
new default-preserving connector fixture. These overlapping cohorts are not cumulative counts.
Remaining buffered qualification, emission/type variants, old graph/backend/version upgrades
and the full connector/failure matrix remain required by
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking remains deferred.


## Bare-update state and sink helper identities

Proctime last-row with both insert sensitivity and update-before emission disabled takes Flink's
stateless helper branch: every input produces a bare update-after. Native retained rows do not
control future outputs in this mode and must not be invented as stock state during export. The
adapter registers the normal stock row descriptor, clears its temporary projection, and emits no
rows. Import materializes and checks that descriptor, rejects unexpected saved rows and requires a
fresh native target through the existing eager validator. Rejection leaves both engines unchanged.

Sixteen direct checks cover stores, TTL and nullable/NOT NULL/keyless keys plus four unexpected-row
rejections. Eight plan checks capture the actual empty-state contract and false/false emission
flags with persisted TTL, generated identities and maxP257. Thirty-two complete jobs cover both
submission forms, stores, switch orders, mini-batch and TTL settings, with custom source/upsert sink,
transactional effects, both transposes and clean stock classpaths/metadata. Immediate jobs require
one update-after per input, including identical duplicates. Bundled jobs compare final per-key
upserts with an independent oracle and uninterrupted stock; stop barriers may change intermediate
bundle netting. Pending native bundles must flush at those barriers. Sixteen eager-switch and 32
processing/checkpoint regressions pass in the same cohort. Reports overlap earlier cohorts.

Ordinary SQL assigns fresh display IDs to equivalent stock sink constraint helpers. Identity
binding now compares the complete serialized immutable executor for the exact released helper
class, while preserving factory, topology, neighbour and uniqueness checks. Comparing labels alone
would miss differences such as ERROR versus DROP. Fourteen topology tests include five new
positive/negative definition cases, and all eight original bare-update plans pass after the fix.
The original four ordinary-plan errors and sixteen ordinary-job failures are retained as evidence;
inputs and assertions were not weakened. All 132 focused controls pass after the fix.

Remaining buffered qualification, wider types, backend/version upgrades and the full connector/failure matrix remain
required under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.


After adding the empty projection and semantic helper binding, all 32 shared Flink 1.18 processing/
checkpoint controls still pass, without a canonical interchange claim on that line. Changing the
loading session's insert-sensitivity option also leaves all eight bare-update plan checks passing:
the persisted plan and actual released function determine emission behavior. The selected eight
released first-N/normalizer/eager programs pass again per engine; each eager program still proves
two imported rows and four new native rows, with strict route contracts intact.


## Buffered event-time first-row state

Flink 2.2.1 watermark-buffered keep-first has three separate durable components: full rows in
`waiting-to-emit-on-timer`, true Boolean markers in `already-emitted-state-boolean`, and one
`event_user-timers` callback per waiting key. The pending row has no TTL. Only emitted markers
carry the original processing-time write age. Processing-time timer queues must be empty.
Ordinary SQL and portable compiled plans select this contract from the actual released
`RowTimeDeduplicateKeepFirstRowFunction`, retaining its keys, serializers, identity, maximum
parallelism and persisted TTL. An ascending comparator alone does not select this representation.

Memory and typed RocksDB export the live candidate/marker view directly in bounded chunks into
stock state. The canonical Flink writer persists those stock values and timer serializers; no
native snapshot or replay is used as an intermediate. Restore requires an empty native target.
A complete validation pass checks key/payload agreement, nullability, row kinds, timer ownership,
missing/orphan/duplicate callbacks and disjoint pending/marker domains before importing any row.
Consumed stock values and callbacks clear only after successful import. Pending rows preserve
compact/component timestamps, and emitted markers need no non-key payload placeholders.

Input transport row-kind and Java string-cache companions never become durable payload fields.
The same insert-only normalization applies before ordinary memory/RocksDB processing and after
canonical import. Invalid changelog kinds fail before state mutation. Keyless markers retain their
one-row cardinality in raw, legacy memory and backend-independent native partitions, including
when their key schema has zero columns and TTL is disabled.

Thirty direct matrix cases cover both stores, TTL on/off, nullable/NOT NULL/keyless keys, actual
Flink timestamp payloads, and raw-partition or SST recovery. Twenty-five further controls include
original marker ages/expiry, late corruption after more than 1,024 valid rows, timer consistency
and rejection without partial insertion. Twelve submission checks cover both ordinary and
compiled forms with persisted TTL and stock identities.

Across retained qualification cohorts, 96 isolated-process buffered jobs cover both submission
forms, stores and switch orders: 48 continued-input cases, 16 empty-state cases, 16 repeated
no-input cases, eight native-checkpoint recovery cases and eight rescaling cases. The latter
preserve maxP128 through 1→3→2 tasks, with connectors fixed at one task and chaining disabled from
the initial graph. Custom source offsets, transactional sink effects, both transposes and clean
stock classpaths/metadata are checked. Stage output and final effects match an independent
watermark/winner oracle and uninterrupted stock. Retained-winner cases additionally cover late
records and equality at the watermark. Their final eight cases retain an original winning row
in a partial transpose batch at the barrier; newer arrivals must lose after both switches.
Long-TTL process cases do not establish downtime expiry;
direct clocked controls verify original ages and exact expiry.

The first 335-check cohort passes 319 checks; its sixteen empty-job errors were missing output
directories in the test fixture before any row was emitted. Corrected targeted cases represent
that empty output explicitly and continue all handoffs. The subsequent 24-job/48-type cohort
passes in full. Sixteen eager-switch jobs and all 231 direct/planner/processing controls also pass
on the corrected runtime. Counts across these cohorts overlap; no failing invocation is credited.
A separate initial compiled-job failure exposed transport companions entering buffered state;
its unchanged inputs pass after normalization. A stale isolated planner build was rejected by
native-execution assertions and receives no coverage credit.

Remaining schema/backend/version/connector/failure coverage continues under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking remains deferred.


A further 48 actual-operator controls cover twelve scalar, nested and composite key shapes,
including non-compact DECIMAL, compact/high-precision timestamps, nullable ROW/ARRAY keys and
out-of-order non-leading root key columns. Payloads contain nested rows, arrays, non-compact
decimals, UTF-16 string cases and nanosecond timestamps. Both TTL settings match stock future
serialized output byte for byte in both directions. An original pending NULL-key candidate must
survive both switches and beat newer arrivals; this supplies preservation evidence beyond
replacing every pending candidate with new input. These nested schemas exercise memory and the
existing RocksDB snapshot-backed native path, not direct typed RocksDB. Canonical exports are
guarded against calling any native snapshot/checkpoint entry point. The current full native
suite passes 1,230 tests with two existing ignores. These are separate, overlapping cohorts.


## Automatic append Top-N projection

Normal SQL and portable compiled-plan submission capture the released Flink 2.2.1 append contract
without user mappings for AppendFast, INT/BIGINT/VARCHAR fields, one sort column and start-one
ranges. `data-state-with-append` MAP holds actual sort keys and retained row lists;
`Top1-Rank-State` VALUE holds the fast Top-1 winner. Variable ranges additionally retain independent
`rankEnd` LONG values. All use stock serializers and original TTL envelopes. Stock identities and
the resolved maximum parallelism also key the native columnar exchange.

Canonical export visits live memory or raw persisted partitions through bounded Arrow batches;
no intermediate native snapshot is written. Import validates the whole view before mutation and
consumes stock state after success. Stock-bound rank preserves per-record changes under table
mini-batching and the requested update-before policy. Runtime modes are installed before import,
including first-observed bounds. These configuration fixes do not change native state encodings.

Qualification covers 160 distinct BIGINT complete-job scenarios across both submission forms,
stores and switch orders, including no-input/empty handoffs, native checkpoints and maxP128
rescaling. Exact external changes match stock and final/stage tables match an independent oracle.
The initial cohort's two RocksDB initialization deadlines remain documented; eight unchanged
concurrent reruns/controls pass. Ninety-six typed actual-operator cases and fifty-two planner
controls supplement the process scope. The released append fixture restores three INT/STRING
rows and consumes two new native rows. See [the full append scope and limitations](../operators/top-n.md#value-ordered-append-canonical-state-partial).

Async/unaligned state, OFFSET, additional sort columns, broader schemas/graphs and the other rank
strategies require their own qualification. Full TTL timelines and the broader startup, failure,
connector and version matrices remain open. Benchmarking is deferred.
