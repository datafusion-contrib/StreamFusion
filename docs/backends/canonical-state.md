# Canonical state format

StreamFusion uses Flink's canonical savepoint container and full-snapshot writer. Operators
bound to a verified stock contract project directly into the corresponding released Flink state
schema. Their qualified scope is documented below. Unbound operators retain reserved managed
state containing StreamFusion's logical representation, which still requires StreamFusion on restore.
Flink owns state metadata, key-group indexes, stream handles, redistribution and integrity checks.

Automatic representative canonical conversion paths now exist in all **15 operator families**.
Every family remains partial; this count excludes connector protocols and is not a percentage of
overall completion or individual state variants.

## Operator coverage inventory

Every row below remains partial. A qualified subset has actual stock/native continuation evidence;
that does not qualify all variants of its family. The remaining schemas, algorithm variants and
connector/failure combinations still require qualification. Detailed scopes and exclusions belong to the linked operator
pages. Historical cohorts are not silently requalified against each newer library.

| Family | Current stock-interchange scope | Main remaining work |
| --- | --- | --- |
| [GROUP BY](../operators/group-by.md#stock-arithmetic-accumulator-projection-partial) | Arithmetic, typed scalar/shared COUNT DISTINCT and append numeric SUM DISTINCT, append extrema, integral/date/time/boolean retracting MIN/MAX, append scalar and local/global DISTINCT mini-batches, and captured immediate/mini-batch/local-global managed UDFs | Wider types/keys, retracting mini-batches, wider UDF contracts, other views/phases and failures |
| [Deduplication](../operators/deduplication.md#canonical-state-adapter) | Automatic eager and buffered contracts, selected mini-batch and TTL modes | Remaining schemas/emission variants and full failure matrix |
| [Top-N / First-N / LIMIT](../operators/top-n.md#first-n-canonical-handoffs) | Automatic first-N, append, updating and retracting rank subsets | Algorithm-specific bounds, TTL, sort and retained-type gaps |
| [Regular joins](../operators/joins/regular-join.md) | Automatic qualified stock layouts and complete-job handoffs | Wider uniqueness, mini-batch, schema and TTL combinations |
| [Fixed windows](../operators/window-aggregate.md#stock-canonical-savepoint-interoperability) | Automatic binding, namespaced/DISTINCT state and selected complete-job handoffs | Complete phase/type/view and failure matrix |
| [Sessions](../operators/window-aggregate.md) | Automatic partitioned-session binding, operator recovery and selected complete-job handoffs | Native unpartitioned session assignment and wider merging/timer/recovery matrix |
| [Window rank](../operators/top-n.md#window-rank-canonical-interchange-partial) | Automatic event-time retained sort-list, timer and watermark binding | Wider bounds/types, processing-time projection and failure/delivery matrix |
| [Window deduplication](../operators/deduplication.md#window-deduplication-canonical-interchange-partial) | Automatic event-time winner, timer and watermark binding | Wider winner/types, processing-time projection and failure/delivery matrix |
| [OVER](../operators/over.md#automatic-bounded-arithmetic-state-binding-partial) | Bounded arithmetic, processing-time unbounded scalar and event-time unbounded V2 scalar contracts with automatic binding | Legacy event-time V1, wider keys/types, DISTINCT/views and bounded event-ROWS cleanup |
| [Changelog normalization](../operators/changelog-normalize.md#canonical-savepoint-adapter) | Automatic retained-row/TTL contract and complete jobs | Wider keys, mini-batches, consumers and failure matrix |
| [Interval joins](../operators/joins/interval-join.md#complete-job-handoff-matrix) | Event/proctime state, four join kinds and selected complete-job/rescale matrices | Wider schemas, cleanup edges and remaining ordinary-SQL combinations |
| [Temporal joins](../operators/joins/temporal-join.md#complete-job-handoff-matrix) | Qualified INNER/LEFT history/probe/timer state and complete jobs | Legacy proctime variant, wider schemas and commit-failure matrix |
| [Event-time sort](../operators/event-time-sort.md#stock-compatible-canonical-interchange) | Automatic single ascending-rowtime contract and complete-job handoffs | Secondary sorting and wider schema/connector/failure/version matrices |
| [Sink materialization](../operators/sink-upsert-materialize.md#canonical-interchange-qualification) | All five stock layouts; qualified BIGINT compiled-plan jobs | Ordinary SQL, accelerated connector binding, wider types and writer rescaling |
| [Window joins](../operators/joins/window-join.md#stock-canonical-savepoint-interoperability) | Qualified event-time INNER/outer window layouts and complete jobs | Wider schemas/residuals and additional exchange/rescale combinations |

Stock-compatible canonical qualification is on released Flink 2.2.1. Shared Flink 1.18 tests establish
processing/native-checkpoint regressions only. Custom stock source/committer fixtures retain their
state and external effects in the qualified jobs. Native connector protocols have separate
qualification: the [Kafka handoff matrices](../connectors/kafka.md#canonical-sink-handoffs-partial)
include real-broker exactly-once sink and both-native pipelines, writer rescaling, transaction
ownership and selected process/commit-failure cases. Combining partition discovery with joint reader/writer rescaling, wider schemas
and broker/coordinator/publication failures remains open; those historical cohorts are not
implicitly requalified by newer operator libraries. Benchmarking stays deferred under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).

## Private native envelope

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

## Automatic updating Top-N projection (partial)

Flink 2.2.1 normal SQL and portable stock compiled plans bind TTL-disabled, start-one constant
UpdateFast ranges with one/two sort fields and INT/BIGINT/VARCHAR payloads. Capture actual stock
selectors, serializers, identities, maximum parallelism and before-image policy. Ordinary graphs
with unqualified native state owners use the captured stock template: supported roles are native,
while other state owners, including upstream grouped aggregation, keep stock layouts.

`data-state-with-update` contains unique-key tuples; fast Top-1 uses `Top1-Rank-State` winners.
Export visits live memory/raw persisted state directly. Import validates the whole view, requires
an empty target and consumes the source only after success. Failed validation retains source state;
close a resource-denied target before retrying on a fresh one. No intermediate native snapshot is
written. Both direct and real restore-lifecycle tests cover ties, hidden overflow, stored kinds,
composite keys, both stores, both before-image policies, receiver cleanup and late error/retry.

The combined controls pass 484 cases, supplemented by the original released updating-rank fixture
(six restored rows, two new native rows). All 120 complete-process scenarios pass on the final
native library, including memory checkpoint recovery followed by canonical export. That projection
omits raw restore's enable-TTL migration timestamp because TTL-disabled stock state has no age. See the [updating scope](../operators/top-n.md#updating-canonical-state-partial).
TTL/cache-age/gap metadata, independent bounds, OFFSET and the broader native graph/version/connector/
failure/delivery matrix remain in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
Benchmarking is deferred.


## Automatic retracting Top-N projection (partial)

Normal Flink 2.2.1 SQL and portable stock compiled plans bind the released general retracting
rank owner with its actual primary identity, selectors, key serializer, serializable comparator,
maximum parallelism and before-image policy. Qualified retained types are INT/BIGINT/VARCHAR
and processing-time indicators retained as payload, with one/two sort fields, constant ranges including OFFSET/large limits, or non-null integral
first-observed bounds. Per-record state semantics apply under table mini-batching too. Unknown
upstream native state owners keep their stock template. A rejected binding also keeps stock rank
state when a direct changelog source has no upstream aggregate to trigger that fallback.

Direct export preserves independent `data-state` lists, `sorted-map` counters and optional `rankEnd`,
including presence, row kinds and independent write ages. It visits live native state into released
Flink serializers without an intermediate native checkpoint. Whole-source validation precedes
import; source state remains available after validation/resource failure. Typed state and planner
checks cover the qualified shapes and rejected contracts. The complete-process matrix verifies
168 distinct scenarios across both submission forms/backends/orders, custom sources/sinks and both
transposes, no-input/empty returns, native checkpoint recovery and 64-key rescaling 1→3→2. The
initial eight ideal-table oracle failures are corrected against twelve isolated stock reference
checks; the other 160 pass unchanged and all eight targeted reruns pass on identical production
code. Exact stock changelog assertions remain. See [retracting rank coverage and its
remaining limits](../operators/top-n.md#retracting-canonical-state-partial).

Unsupported schemas, additional sort fields, nullable variable bounds, broader native graphs and
full cross-version/failure/connector/delivery coverage remain in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking remains deferred.


The retained-indicator correction keeps PROCTIME physical NULLs while validating ordinary
required fields. Native Calc also preserves the lazy marker until PROCTIME_MATERIALIZE observes
the clock, so retractions match saved rows after an engine switch. This changes no serializer or
checkpoint format and adds no native snapshot intermediate. The corrected component passes
1,032 rank/clock checks, all 64 complete jobs, four unchanged released restore programs per engine
and 199 audit controls. The jobs retain custom connectors, both transposes, generated identities,
clean stock workers and exact external effects, including no-input returns, native checkpoints
and selected 1→3→2 rescaling. All 37 shared Flink 1.18 controls pass without canonical qualification
there. See the [exact retained-indicator scope](../operators/top-n.md#retained-processing-time-indicators-in-retracting-rank).

## Arithmetic grouped accumulator projection (partial)

The Flink 2.2.1 grouped-arithmetic adapter writes the released generated `accState` row directly
with its actual RowData serializer, key serializer and TTL envelope. Physical slot maps preserve
raw SUM/AVG accumulators, nullable SUM and append-only BIGINT MIN/MAX initialization, shared COUNT/SUM0 fields, hidden input-row
counts and write ages. Continued integer AVG uses Java's wrapping division at
`Long.MIN_VALUE / -1`, including for restored negative counts; the canonical accumulator stays raw.
The native protocol visits live state through bounded Arrow batches; it
does not encode and reread a native snapshot. Both memory and raw native RocksDB use the same
transfer semantics. Whole-source validation precedes mutation, and a failed resource allocation
retains the source for a fresh target. Receiver failures preserve native state for another export.

The qualified single-phase BIGINT arithmetic contract binds append-only and retracting input,
the same scalar projection binds append-only BIGINT MIN/MAX, and a structured adapter binds
retracting BIGINT MIN/MAX,
automatically in ordinary SQL and portable compiled plans with one INT/BIGINT grouping key or a
keyless global group, using the original generated accumulator layout, key serializer, identity,
TTL and maximum parallelism. Global operators keep the released zero-field RowData key serializer,
eight-byte empty BinaryRow key and singleton parallelism/maximum parallelism of one.
The native graph remains a private runtime projection; the compiled plan stays portable stock
Flink. Append-only state without an input counter retains existence and nullable SUM initialization
through private presence markers, without adding fields to the stock accumulator. Nullable MIN/MAX
use the same initialization policy and may share a physical row with COUNT/SUM/AVG. Aggregate counts
retain their original values even when zero or wrapped. Initialization reapplies the captured
append liveness policy after ordinary native checkpoints; the private snapshot format is unchanged.
Additional aggregate schemas/phases/other views and composite keys remain outside this binding. Compiled
grouping without any actual aggregate calls retains its stock owner.
Retracting MIN/MAX use a separate structured header and map-entry protocol. Each cached selection,
map size, frequency count and original write age survives independently, including expired maps
with no accumulator header and cached values hidden by zero map size. Generated RAW MapView
placeholders remain present and carry Flink's one-byte NullSerializer payload. The binding captures
actual MapView IDs and serializers, including ExternalSerializer snapshots; equivalent data bytes
alone do not establish serializer compatibility. A fresh raw projection backend avoids TTL wrapper
filtering or clock refresh during import. Header and map validation finish before any apply, and
source entries are cleared only after success. Export visits live native state directly in bounded
frames. Mapped extrema use memory aggregation with raw RocksDB snapshot backing when RocksDB is
configured. Their ordinary native checkpoints add an optional absent-header frame only when
orphaned views exist; the default and append-only formats are unchanged.
Unqualified upstream grouped owners in the rank-template path remain stock-owned. Qualified isolated
arithmetic jobs verify custom source and committing-sink state, both transposes, immediate re-export,
native checkpoint recovery and rescaling through one, three and two tasks. They compare full
committed changelogs against stock controls and exclude StreamFusion from stock worker classpaths.
Global source state rescales through one, three and two tasks while the aggregate and committing
sink remain singleton. Each worker verifies the actual topology; the sink stays at one task for
ordered changelog materialization. Broader sink-rescale and connector matrices remain open.
See [arithmetic aggregate scope and pending coverage](../operators/group-by.md#stock-arithmetic-accumulator-projection-partial).
All 15 families and the complete acceptance matrix remain partial/open in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking remains deferred.


## Bounded OVER submission binding (partial)

The existing released bounded arithmetic adapters bind during ordinary SQL and portable-plan
submission for processing-time ROWS, event-time RANGE and event-time ROWS without processing
cleanup. Their state schemas, raw accumulator fields, retained input buckets, counters, timers
and callback bookkeeping retain the released stock contract. Captured serializers and stock
operator identities accompany the runtime owner; canonical export visits live state directly.
Constants become internal native columns rather than extra durable state or external conversion
steps. Ordinary SQL starts this binding from the original stock template because the native
physical rewrite can introduce multiple constant helpers for one original OVER role.

The initial numeric, key, retention and state-layout limits are listed with the
[bounded OVER coverage](../operators/over.md#automatic-bounded-arithmetic-state-binding-partial).
Unsupported compiled contracts keep stock owners. Additional unbounded modes,
other views, complete connector delivery/failure guarantees and cross-version migration remain open.
Benchmarking remains deferred under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).


The current bounded binding passes one 1,834-check regression cohort and a separate 84-job
handoff/recovery/rescale cohort on Flink 2.2.1. See the operator page for the exact type, key and
scenario split; earlier overlapping cohorts are not cumulative coverage. Original released OVER
restore programs remain stock controls where their UDF/key layouts are outside this binding.
The shared 1.18 processing tests do not establish stock-compatible canonical restore there.


## Unbounded processing-time OVER binding (partial)

The released ROWS and RANGE unbounded processing-time scalar functions now bind automatically
for the exact numeric and key layouts in [unbounded OVER coverage](../operators/over.md#automatic-unbounded-processing-time-scalar-binding-partial).
Their durable stock state is the raw generated accumulator and its optional TTL envelope.
Canonical export visits live native folds directly; it does not reconstruct an accumulator from
SQL output or retained inputs and does not first serialize a native snapshot. Whole-source import
validation precedes installation, and all source entries remain available until import succeeds.
Native checkpoints remain a separate recovery path. A subsequent canonical savepoint performs
backend or engine changes. Wider unbounded contracts and connector delivery guarantees remain
unfinished under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).


The consolidated unbounded/bounded regression cohort passes 2,377 checks, including direct raw
accumulator/age comparisons, whole-source corruption and resource failures, empty returns,
native-checkpoint recovery before canonical backend changes, and automatic planner admission.
The operator page separates that evidence from complete-job qualification and remaining gaps.


Separate complete-job cohorts pass 112 unbounded arithmetic cases and sixteen COUNT(*)/literal
cases, retaining custom stock source/committer state, both transposes, clean stock workers and
external committed effects. The arithmetic matrix includes immediate returns, native-checkpoint
recovery and keyed/global rescaling with the original singleton/global constraints. Another
32 harness-regression jobs pass. These are separate cohorts, and their exact type/mode limits
remain on the operator page; none establishes complete connector or cross-version coverage.


## Event-time sort submission binding (partial)

Normal SQL and portable stock plans bind the released single ascending-rowtime sort contract,
retaining its actual empty-key serializer, row schema, emission cursor, timer membership and
maximum parallelism. The raw native buffer remains singleton; source tasks can rescale while the
sort owner keeps the stock maximum. Canonical export visits live rows directly, and import rejects
a populated target before installation. Allocation failure retains source entries for retry with
a fresh native target and preserves the original memory-limit exception.

The consolidated 1,533-control and separate 28-job cohorts qualify the exact shapes described in
[event-time sort coverage](../operators/event-time-sort.md#stock-compatible-canonical-interchange).
Both boundaries and custom stock source/committer effects remain in the complete jobs, with
strict global output order. Native connector transaction/writer failures and wider schema/version
matrices remain unfinished. All fifteen operator families remain partial under #569; benchmarking
is deferred.


## Automatic window rank and deduplication binding (partial)

Ordinary SQL and portable stock Flink 2.2.1 plans capture the actual synchronous event-time
window processor, key/sort serializers, schema, time zone, stock identities and maximum
parallelism. TUMBLE/HOP/CUMULATE use Long window-end namespaces, released event timers,
`window_rank` sort-key lists or `window_deduplicate` winner rows, and the UNION input watermark.
The direct live projection does not first serialize a native snapshot. Calendar and tie policy
configuration precede import, and native checkpoints retain managed watermark bookkeeping.
The stock plan remains portable, and generated identities work without explicit UIDs.

The paired 425-check state/bridge cohort and separate 384-check planner cohort qualify their
listed subsets. The unchanged released pair passes fourteen invocations on each engine, with
twelve positive canonical-import/new-input witnesses and two rank-after-aggregate processing
controls without rank-state import credit. Twenty admission checks preserve stock owners for
async/unaligned/OFFSET contracts. See the operator pages for complete-job evidence and limitations.
All fifteen families remain partial and all ten full acceptance criteria remain open under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.


The separate 56-job window rank/dedup cohort passes both submission forms, stores and engine orders
for keyed TUMBLE general rank/first-row dedup. It covers 16 continued-input, 16 no-input, eight
native-checkpoint and sixteen 64-key owner-rescale cases; source/sink remain singleton. The combined
2,374-check regression selection also passes. Together with the separate fixed-window/session jobs, this establishes representative automatic
handoffs in all fifteen families. Every family remains partial; the count does not qualify all variants. Native processing-time window rank
and dedup have no released stock counterpart and receive no stock canonical qualification.


## Typed grouped scalar and shared DISTINCT projection (partial)

The additional Flink 2.2.1 binding captures actual generated numeric accumulator types, composite
BinaryRow keys and shared filtered COUNT DISTINCT view serializers. Bounded live header/companion
transfer supports memory and typed RocksDB without an intermediate native snapshot. It preserves
signed membership counters, the independent visible scalar, separate NULL markers, and the
non-null generated RAW placeholder. Native checkpoint recovery preserves the scalar independently
and restores NULL counters before continued updates.

The corrected source passes 1,345 grouped-state regressions, including 170 typed state/lifecycle/
planner controls; the complete native suite passes 1,313 tests with two existing ignores. A separate corrected-source 28-process cohort
qualifies its narrower nonempty-state query, including no-input returns, native checkpoints and
rescaling. Another 372 global/processing/SQL checks pass on Flink 2.2.1. See the
[typed GROUP BY scope and remaining gaps](../operators/group-by.md#typed-scalar-and-shared-distinct-canonical-binding-partial).
The unchanged released window suite passes all 24 programs on each engine on the corrected build.
Twenty programs prove native state import with new input. Three newly passing unpartitioned
SESSION programs execute native GROUP BY after stock session assignment, but restore empty grouped
state and receive no grouped-state import credit. Split-phase Final import remains unproven where
only Slice state was populated. Wider operator/connector/failure/version qualification and all ten
full acceptance criteria remain open; benchmarking is deferred.


## Stock-owned Kafka source state (partial)

An admitted native Kafka decoder delegates split and enumerator serialization to the released
stock Kafka source. Its portable state needs original reader/coordinator and downstream host
identities, rather than a native payload converter. The source-state-preserving extension contract
allows ordinary SQL to install the same identity projection already used for portable stock plans.
Unqualified optional source implementations remain outside that contract.

The separate real-broker tests cover the source handoff subset documented on the
[Kafka page](../connectors/kafka.md#canonical-source-handoffs-partial), with a custom committing sink,
actual native output and isolated stock worker classpaths. This is not Kafka sink transaction or
complete connector delivery qualification. Ordinary native checkpoints retain their existing graph
identities, and canonical metadata exports the stock identities. Wider operator/connector/failure/
version work and all ten full acceptance criteria remain open in #569; benchmarking is deferred.


## Stock-owned Kafka sink state (partial)

A native Kafka encoder feeds the unchanged released Flink writer and committer. Their original
identities and transaction state survive canonical handoff, while the synchronous encoder is an
explicitly declared empty helper. Export rejects helpers that unexpectedly own durable state.
Portable plans reuse the same connector and constraint admission policy as ordinary SQL and
retain their stock JSON. The 36-case custom-source/sink matrix checks actual submitted native execution, saved transaction
ownership, no-input/checkpoint returns and writer redistribution. The [Kafka sink coverage](../connectors/kafka.md#canonical-sink-handoffs-partial)
separates that broker continuation evidence from planner checks and unfinished transaction,
rescaling, failure, schema and version combinations. Native checkpoints retain their ordinary
recovery path. The full #569 acceptance criteria remain open; benchmarking is deferred.


The separate 36-case native Kafka source/sink cohort now passes the same handoff/recovery/writer-
rescale modes with actual native decoding and encoding and no row/Arrow transposes. It verifies
released split/coordinator state and transaction ownership alongside exact external committed
records. The source remains singleton; the [connector scope](../connectors/kafka.md#canonical-native-source-and-sink-pipelines-partial)
keeps wider source redistribution, failure, schema and version guarantees separate. The full
fifteen-family and ten-criterion acceptance matrix remains unfinished in #569.

The separate Kafka failure lane verifies recovery from the last valid canonical savepoint after
terminating the only TaskManager before the next savepoint. Sixteen custom-source/fully-native
pipeline scenarios preserve exact external committed effects across retry and another engine
switch. Conflicting transaction owners are checked separately: the stock committer can report
failed commits while savepoint creation succeeds, so those artifacts receive no handoff credit.
See [Kafka recovery and ownership scope](../connectors/kafka.md#canonical-task-manager-recovery-partial).
Normal TaskManager shutdown is distinct from abrupt process death, and broader failure and
delivery guarantees remain unfinished under #569.

Kafka's additional controlled-boundary and abrupt-process lane qualifies forty new complete-job
scenarios. Explicitly disabling the fixture's latency timer and comparing transpose input with
encoded rows proves empty, partial and full batches at canonical barriers. Separate hard JVM
termination cases leave real Kafka transactions open for restore-time cleanup, including a row
still pending in the transpose. The same 70-check cohort includes eight existing grouped-state
rescale jobs and 22 admission controls. See the [exact Kafka scope](../connectors/kafka.md#canonical-batch-boundaries-and-abrupt-process-recovery-partial).
This adds no native snapshot intermediate, changes no production flush setting and leaves broader
operator, connector and failure qualification open under #569.

Sixteen additional Kafka complete-job cases qualify client death after canonical completion but
before the location is reported. They verify committed effects, recover the completed artifact
through the ordinary restore path on the other engine without new input, and switch back with
more input. Recovery never rolls back to an older state after new effects committed. The full
86-check cohort also reruns the preceding 70 checks on the updated fixture sources. See the
[completion-failure scope](../connectors/kafka.md#client-failure-after-canonical-completion-partial).
This does not qualify every failure during capture, publication, commit or broker outages.

The grouped scalar contract also captures DATE/TIME/BOOLEAN append MIN/MAX without changing the
native fold or transfer format. Exact released accumulator serializers and logical precision
remain in the stock artifact; values move directly through the existing bounded Arrow frames.
Its separate 194-control and 32-job cohorts qualify the precise subset on the
[GROUP BY page](../operators/group-by.md#canonical-date-time-and-boolean-extrema-partial), with
32 rejection/plan controls and 34 shared Flink 1.18 processing regressions kept separate.
Retraction/view, type/precision and broader failure/version coverage remain open under #569.


The structured retracting MIN/MAX adapter additionally binds integral, date, time and boolean
cached selections and retained map keys. Its bounded transport uses a lossless internal long
carrier while stock state retains the exact captured logical serializers and boxed values.
Independent cached selections, map-size counters, signed frequencies and original write ages
survive canonical and native-checkpoint recovery. The consolidated 1,232-check cohort, seven
initial compiled/memory/stock-first complete jobs and 34 shared processing regressions pass;
the full 196-case complete-job matrix passes all 28 modes for each of the seven types across
SQL/compiled submission, stores/orders, no-input returns, native checkpoint recovery and selected
64-key rescaling at 1 → 3 → 2/maxP128. These are the original typed-extrema library qualifications,
not reruns against later string-library builds. See the [typed retracting scope and test
boundaries](../operators/group-by.md#typed-retracting-canonical-extrema-partial). Wider types,
keys, views/phases and full connector/failure/version coverage remain unfinished under #569.


## Append-only string aggregate handoffs

The released VARCHAR accumulator for STRING/VARCHAR/CHAR MIN/MAX inputs uses the existing direct
stock-schema projection. Native memory retains the winning optional Java string representation,
while persistent state matches stock RocksDB's per-record deserialization. Canonical files carry
only the original stock serializer and encoded value. Restored caches are cold, as on stock Flink.
Captured stock input producers and cache-preserving RowData/Arrow boundaries retain live comparison
semantics; native grouped state still executes on Arrow. The 296-case cohort includes Unicode,
mixed Java/binary representations, multirow batches, malformed state, repeated switches and backend
changes. Two unchanged released GROUP BY programs also prove imported groups and resumed input; seven
others remain unqualified. Twenty-eight ASCII-string complete-job switches additionally cover both
submission forms, stores/orders, no-input returns, native checkpoint recovery and selected rescaling,
with custom source/sink state, transposes, clean stock classpaths and exact external effects.
See [the exact string scope and restore
oracle](../operators/group-by.md#canonical-string-extrema-partial). Retracting string views, wider
phases and the full connector/failure/version matrix remain unfinished; benchmarking is deferred.


## Scalar mini-batch canonical boundary

Selected append-only scalar bundles use the original released `accState` schema and serializers
in both phases. The local stage owns only temporary partial accumulators and drains before
barriers; the global stage retains its stock operator identity and key-group space. AVG exports
the widened sum and independent count directly. The global bundle combines incoming partials
before merging them once into retained state, rather than reconstructing accumulators from
emitted averages or extrema.

Once local/global buffers drain, canonical export projects the consistent live state into the
stock writer in one capture. It does not first write a native snapshot. Ordinary native
checkpoints retain their existing representation and restore the configured merge mode during
operator initialization. Optional Java-string caches remain internal execution transport and
do not add StreamFusion serializer classes to the savepoint.

The [GROUP BY coverage page](../operators/group-by.md#canonical-scalar-mini-batches-partial)
records the 390 scalar/planner/state cases, 40 corruption/retry controls, 56 distinct complete-job
scenarios and exact released restore witnesses. Selected 64-key rescalings retain the global
key-group space and preserve custom connector state. Wider phase/type/view/TTL/failure matrices
remain under qualification in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
Benchmarking remains deferred.


Shared append COUNT/SUM DISTINCT views retain each call's membership bit and NULL marker while
exporting the independently stored raw sum. A SUM is not a membership cardinality; the COUNT
consistency guard applies only to COUNT slots. Direct memory/RocksDB transfer and ordinary
native recovery reuse the existing state format and stock serializers. The
[SUM DISTINCT scope](../operators/group-by.md#canonical-shared-sum-distinct-partial) records the
124 state/recovery controls, 52 planner/admission controls and remaining local/global, retraction,
TTL, schema and full-job matrices. Released 2.2.1 qualification remains separate from shared
1.18 processing checks. Benchmarking remains deferred.


The append local/global DISTINCT extension additionally binds shared COUNT/SUM memberships while
retaining independent raw stock scalars. Private local Arrow views are transient and drain into
the global owner before canonical barriers. Its released restore fixture, 120 state/recovery
cases and 28 complete-job handoffs are qualified; see the [exact operator scope and remaining matrix](../operators/group-by.md#canonical-localglobal-distinct-mini-batches-partial).
All fifteen families remain partial and benchmarking remains deferred.


Canonical DISTINCT view bindings validate native companion indices before initialization and
use stock fallback in SQL/compiled plans when those indices cannot fit the current encoding.
The [operator admission boundary](../operators/group-by.md#native-companion-table-index-admission)
distinguishes this table-ID limit from scalar-only row width; wider native companion layouts
remain open under #569.


Captured one-phase JVM aggregate handlers retain released managed accumulator/data-view state
behind Arrow boundaries. They use normal stock restore/export and do not introduce native
snapshot intermediates or StreamFusion serializers. The [managed UDF qualification](../operators/group-by.md#captured-managed-udf-canonical-state-partial) records exact released restore
witnesses, planner and 56 complete-job scenarios across cached/uncached execution. Mini-batch UDF and wider type/TTL/failure/version coverage remain open under #569.


Captured one-phase mini-batch UDFs use their actual released bundle function and managed state.
The [qualified one-phase scope](../operators/group-by.md#captured-one-phase-udf-mini-batches-partial) records 56 complete-job scenarios, 88 state/TTL/failure cases, 32 planner controls and
eight released native aggregate resumes. These overlapping cohorts do not qualify the two-stage
UDF contract or the wider connector/version matrix. All bundles and optional cached writes drain
before the direct stock canonical writer runs; ordinary checkpoints retain their existing path.


Captured local/global UDFs retain stock managed global state while original local serializers
carry private Arrow partial events between the two captured handlers. The [two-stage scope](../operators/group-by.md#captured-localglobal-udf-canonical-handoffs-partial) records all nine released aggregate resumes, strict stage ownership, 144 state cases,
56 complete jobs and regression results. This does not qualify all user-function
types, keys or connector failure combinations.


## Event-time OVER import preflight

Read-only pending-row and cleanup-deadline validation shares the eventual mutation path's
schema/key checks without installing state. This permits complete-source validation before
import and preserves retryable source ownership. See the [OVER transfer scope](../operators/over.md#event-time-pending-state-preflight);
the released event-time stock-state adapter, timer projection, planner binding and complete-job
qualification remain pending. All fifteen families stay partial; benchmarking remains deferred.


## Event-time OVER cleanup callback foundation

Retained unbounded event-time OVER can bind actual processing callbacks independently of its
watermark-driven pending rows. Idle/late-only keys clear without new input, pending keys defer
from the actual callback clock, and ordinary native recovery rebinds pending/overdue deadlines.
Cleanup VALUE state and armed timer ownership remain separate, including released signed-overflow
hysteresis. Native memory partitions and persistent stamps preserve unarmed values; historical
stamps remain readable as armed. The internal ownership column is excluded from key-group hashing
and is not a stock managed descriptor or serializer.

The independent native suite passes 1,360 checks with two existing ignores. Its exact library
passes 206 Java transfer/callback/recovery controls, including 16 actual released 2.2.1 callback
comparisons. This is component evidence; the [remaining OVER adapter/planner/job work](../operators/over.md#event-time-cleanup-callback-component)
remains part of #569. Every family stays partial and benchmarking remains deferred.



## Captured managed UDF key schemas (partial)

Managed UDF bindings retain the actual stock key selector and serializer rather than applying
native scalar-state precision restrictions. The exchange uses the existing recursive stock key
projection. Nested ROW keys preserve their original owned binary payload across Arrow and
local/global partial boundaries, including reserved NULL decimal/timestamp slots. No new native
state codec, canonical serializer or checkpoint format is added.

The combined released Flink 2.2.1 selection passes 1,190 checks and all nine unchanged released
aggregate restore programs on each engine. Its full 168-job boundary/structured-key matrix passes
continuation, no-input re-export, ordinary native recovery and selected 1→3→2 rescaling with
maxP128. Both transposes, custom source/coordinator and transactional sink effects remain in the
jobs; stock workers exclude StreamFusion. See the [exact managed UDF key scope and remaining
requirements](../operators/group-by.md#captured-managed-udf-key-layouts-partial). Wider serializer,
view/type/TTL/failure/version matrices remain open; all fifteen families stay partial.
Benchmarking is deferred.

## Ordinary-SQL generated identities

If an ordinary physical plan initially retains stock owners, subsequent lowering uses the
original captured stock graph. Creating a second execution graph changes generated UIDs and can
make original source, aggregate, writer or committer state unresolvable on the other engine.
The corrected path passes 108 complete-identity planner cases and the combined 1,190-check
managed-UDF selection. All 168 complete jobs retain generated UIDs, including ordinary SQL and
compiled-plan continuations, native checkpoints and rescaling. Tests preserve native execution
and complete stock metadata checks; production counters are unchanged.
## Unbounded event-time V2 OVER state projection

The candidate V2 scalar binding projects the released stock plain accumulator VALUE, timestamp
input MAP, Void event callbacks, optional cleanup VALUE and independent processing callbacks.
Both native stores use direct live-state visits for canonical export. Complete read-only inventory
and chunk preflight precedes native installation, and original stock values and queues clear only
after successful import. An unarmed cleanup VALUE is preserved without inventing a callback.
Original operator IDs, key-group configuration and key/input/accumulator serializers are retained;
helper constants are transient native arguments. Native checkpoints keep their existing path.

PROCTIME indicators can occupy physical NULL input slots despite their logical NOT NULL type.
Arrow transport schemas represent those slots as nullable without altering the stock serializer
snapshot or weakening checks for ordinary required fields. This matters when persistent pending
rows are reconstructed for export, as well as when stock input rows enter canonical import.
The dedicated correction passes 32 actual SQL-derived bidirectional handoff cases and 44 malformed
state controls. The expanded component regression passes all 896 selected checks, including native
checkpoint recovery and actual SQL/compiled transpose-schema controls. The merged component passes
2,086 checks, all nine unchanged released aggregate programs per engine and all 192 event-time
OVER full jobs on the same source/library. The latter retain both native stores, ROWS/RANGE,
keyed/global state, cleanup on/off, custom connectors and transpose boundaries, no-input returns,
ordinary native recovery and selected 1→3→2 keyed rescaling. The separate 212-check Flink 1.18 run
is shared-code regression coverage, not canonical qualification. See [exact OVER coverage](../operators/over.md#released-v2-event-time-unbounded-scalar-binding-partial).
Every operator family remains partial and benchmarking remains deferred under #569.


### Mixed consumers of a ROW-key normalizer

A qualified native normalizer can share a job with a stock consumer whose native canonical
contract is not admitted. Ordinary SQL reuses the original stock template to bind the normalizer
and preserve that consumer's original identities and state. The normalizer's ROW-key input
edge is rebuilt from the same template, preserving its nested representation after restore. It does not continue with unbound
native identities or drop unmatched savepoint state. ROW keys use owned binary companions to
preserve the original serialized key bytes; canonical export still writes directly from live
state into stock schemas without an intermediate native snapshot.

### Portable plans and existing compiled execution

Newly compiled SQL keeps the stock JSON/Smile plan and a separate private execution graph,
just as ordinary SQL retains its original stock identities. Serializing a compiled plan must
not replace its in-memory native graph. Qualified state owners bind their stock contracts;
connector paths without a qualified state contract retain their existing native execution and
native checkpoint requirements. In particular, Paimon compiled source-sharing jobs continue to
execute their native scans and shared Arrow branches. This does not qualify Paimon state for
cross-engine canonical restore. Loading a serialized stock plan still uses the documented
restore-time admission policy.

ROW-key normalization also accepts Arrow Java's network-only rename of the internal non-null
UTF-16 list child (`unit` to `$data$`). Decoded retained rows are reconciled with their emitted
schema, including update-before rows after a rescale. This does not relax logical field types,
nullability or representation metadata, and it does not change stock key or state bytes.

A private graph containing a native connector without a qualified stock state contract keeps
its complete native execution route. It must not be partially rebuilt from the stock template
for a neighboring window or other state owner: that would discard the admitted connector and
its shared Arrow edges. Such graphs keep their existing native checkpoint/savepoint dependency.

Representation-only helpers are excluded when identifying native connector state. A transpose
may inherit its input's captured source identity without becoming a source-state owner.

Qualified compiled plans retain the unchanged stock execution template and lower it afresh for
each explanation or submission. Rebinding an already modified private graph can otherwise leave
a previously attached state contract without its matching canonical backend. Private native
graphs are retained only where execution requires the existing native-only route. Repeated
`explain()`/`execute()` therefore preserves canonical backend installation and original identities.

The stock-template path requires Flink's own JSON-serde support for every execution node.
DataStream-backed table scans, legacy sources/sinks and table-aggregate nodes that Flink marks
nonserializable keep their existing physical execution and native checkpoint route. They are
not forced through portable-plan serialization or claimed as cross-engine canonical coverage.
Serializable custom SQL connector factories remain covered by the isolated handoff tests.

External inline tables used by `fromDataStream` and `TableResult.collect()` also retain their
physical execution route: Flink cannot serialize their catalog-table origins. Capturing a
serializable stock template copies connector instances before runtime-provider translation,
so planning the template cannot mutate the source or sink used for actual execution.

Connector factories that depend on a custom in-memory catalog-table implementation also retain
the physical route. Stock plan serialization reconstructs a standard catalog table and cannot
preserve arbitrary subclass fields; such factories are not admitted to portable-template lowering.
