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

This is a StreamFusion compatibility format, not stock Flink operator state. A future translation
API can read these versioned logical sections and emit the exact managed-state descriptors and
serializer snapshots expected by a target Flink operator. Until such a translator exists, restoring
the savepoint requires the corresponding StreamFusion operator. Stateful job upgrades are safe only
when the new StreamFusion version still supports the saved format and the operator definition is
compatible; changing keys, aggregate definitions, join shape, or other state schema is not implied
safe merely because the savepoint is canonical.

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
can bind temporal, interval and window joins without compiling or loading a plan. The state schema,
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
state/metadata/SQL controls. Four isolated native-first INNER temporal round trips pass across
both native stores: two with ordinary default SQL UIDs and two with `ALWAYS`. They submit SQL
without a compiled artifact, retain two custom sources, a transactional sink and both transposes,
verify positive native input and stock-only metadata, and compare external committed effects with
stock and an independent temporal oracle. These initial jobs do not establish the complete ordinary
SQL variant/rescale/failure matrix. All 15 unchanged released join restore programs still pass on
each engine: 12 actual native imports with continued input, two verified stock window owners and
one join elided by Flink. Every family remains partial under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking stays deferred.
