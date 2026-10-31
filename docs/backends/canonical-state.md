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
uses a read-only inventory validator. It checks declared descriptor names and state kinds, the
current adapters' stock void namespaces, and timer key/namespace serializers. Unknown descriptors
or timer services are errors even when empty. Pending timers require explicit adapter ownership;
only Flink's known empty user-timer queues may otherwise be omitted. Aggregate declarations include
their DISTINCT/null-marker and extrema data views.

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
