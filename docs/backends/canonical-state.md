# Canonical state format

StreamFusion uses Flink's canonical savepoint container and full-snapshot writer. Native operator
state is represented as reserved managed keyed state, so Flink owns the state metadata, key-group
index, stream handles, redistribution, and integrity checks. The payload is StreamFusion's logical
operator state; it is not yet the managed-state schema of the equivalent stock Flink operator.

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
envelope are the stable reader boundary for a future translation tool.

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
