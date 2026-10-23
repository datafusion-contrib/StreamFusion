# Sink upsert materialization

**Status:** native for admitted Doris Arrow sinks. Other connector matchers retain their existing
materialization fallbacks. Flink 2.2 changelog state retains the shared fallback for native keyed state.

When Flink marks an updating Doris sink as requiring upsert materialization, StreamFusion inserts
a keyed Rust operator before the released Doris writer. Both boundaries carry Arrow batches;
there is no RowData-to-Arrow or Arrow-to-RowData transpose inside this path.

For each sink primary key the operator retains ordered candidates. An INSERT or
UPDATE_AFTER emits INSERT for an empty list and UPDATE_AFTER otherwise. With an inferred input
upsert key, a matching candidate is replaced in place; without one, each addition is appended,
including duplicates. UPDATE_BEFORE and DELETE remove the first matching candidate. Removing an
older candidate emits nothing, removing the visible candidate restores its predecessor with
UPDATE_AFTER, and removing the final candidate emits DELETE using the incoming retract payload.
An unmatched retract emits nothing and does not refresh state TTL.

Matching follows the released Flink implementation after its required keyed exchange. Full rows
use binary equality; projected input upsert keys use generated field equality. MAP also uses the
generated hash on heap, while RocksDB compares serialized user-key bytes. These differences are
observable for signed zero, NaNs, map ordering, NULL values and nested row kinds.
The required keyed exchange canonicalizes NaN payloads in generic arrays and map key/value
arrays, while explicit binary-array carriers retain their raw bits. Native materialization
performs the same normalization and copies only affected columns; scalar floats and floats
inside ROW fields retain their binary payloads. Projected MAP-value comparison also preserves
Flink's generated boxed-primitive reference equality: separately boxed floating values never
match, and integer matching observes the running JVM's integer-cache range.

LEGACY and VALUE retain a candidate vector. MAP uses linked sequence nodes and a matching-key
index. ADAPTIVE migrates between these representations at its configured thresholds. The linked
representation preserves Flink's sequence cursor, replacement and duplicate chains, including
replacement of an older slot before a subsequent append. It does not sort candidates by event time.

The ordered candidate semantics follow the released Flink sink materializers. Native state owns
compact row payloads and matching identities, so it does not pin entire source batches. State
encoding and output gathering allocate native memory; the columnar boundary does not promise
zero allocation or zero state copies. Ordinary upserts forward their incoming value buffers
when all input rows are emitted; a filtered selection gathers columns, and predecessor restoration
decodes retained payloads. For positive batches with identical scalar input/sink keys,
[final replacement retention](../optimizations/sink-materializer-state.md) avoids encoding
superseded state while still emitting every change.

Materialization always routes by sink key, including with shuffle policy `NONE` or a single
writer. Recoverable columnar exchange reassembly remains active for unaligned checkpoints.
State is charged to the task off-heap budget and checkpointed as versioned payloads per Flink
key group through the shared native state wrapper. On the native RocksDB backend this operator
uses the existing snapshot store around its Rust hot state, rather than a separate typed RocksDB
data plane. Snapshots retain payloads, matching identities, linked topology and key-index entries. Restore
retains ordering and permits key-group redistribution on rescale. Snapshot version 2 is specific
to this native operator; it does not import a stock Flink materializer's savepoint state.

Legacy materialization applies `table.exec.state.ttl` to the whole candidate list, refreshed on
successful additions and removals. Expiry is silent. Enabling TTL when restoring a TTL-disabled
snapshot grants existing candidates a full retention from restore time. Flink 2.2 disables TTL
for the VALUE, MAP and ADAPTIVE materializer strategies; the native path also disables it for
those strategies.
The strategy option is `table.exec.sink.upsert-materialize-strategy.type`. ADAPTIVE high/low
threshold validation retains Flink 2.2's backend defaults (400/300 for heap, 50/40 otherwise).

Insert-only sinks do not need this stage, matching Flink's runtime decision even when
`table.exec.sink.upsert-materialize=FORCE` marks the physical sink.

Validation compares emitted row kinds and payloads against the released LEGACY and V2 operators,
including heap/RocksDB matching, checkpoint restore, TTL, key-group rescaling and off-heap limits.
The Doris SQL matrix exercises each strategy with and without inferred input keys through both
released writer modes and checks positive native materializer work. Stock SQL uses JSON for its
retracting reference because the released stock Arrow serializer rejects deletable sinks; the
native writer uses Arrow. Updating benchmark queries use Arrow on all three paths.
