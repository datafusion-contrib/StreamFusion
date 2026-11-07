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

The ordered candidate semantics follow the released Flink sink materializers. Native state retains compact row payloads and matching identities. The RocksDB data plane
persists these records instead of retaining source batches in memory. State
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
stores active candidates and matching indexes as individual RocksDB records. Select the
[native RocksDB backend](../backends/rocksdb.md) using its existing configuration; Flink 2.2
requires the explicit StreamFusion backend factory. LEGACY/VALUE scan
ordered records for matching; MAP point-reads its serialized identity index and affected linked
neighbours. ADAPTIVE changes these on-disk layouts at the same thresholds as Flink. The working
set is bounded to the current batch and affected records, including for one key with many candidates;
RocksDB cache and write buffers use the shared slot pool. Native checkpoints retain the database
files without decoding all candidates. Portable snapshots retain payloads, matching identities,
linked topology, key-index entries and sampled per-candidate timestamps. Restore
retains ordering and permits key-group redistribution on rescale. Snapshot version 3 is specific
to this native operator; it does not import a stock Flink materializer's savepoint state. Portable
exports materialize one key's candidates at a time and return serialized key-group buffers. Existing
version-2 native payloads remain readable, including automatic import of the former RocksDB
snapshot-store checkpoints into the record-level tables. New native input samples the batch
processing clock for each retained candidate. Replacing a candidate replaces its sample; successful
removals and unmatched retracts do not refresh the surviving candidates. Native snapshots, direct
RocksDB records and adaptive layout changes retain these samples independently of whole-list TTL.
Historical version-2 payloads and disk records did not record candidate timestamps. Recovery keeps
that history explicitly unknown instead of assigning the restore clock. Frozen bytes written by the
preceding writer qualify both historical readers; new timestamps coexist with unknown historical
ones. JNI clock/recovery controls run on released Flink 1.18.1 and 2.2.1. This native metadata
retention is groundwork for canonical interchange, not a stock adapter.

Legacy materialization applies `table.exec.state.ttl` to the whole candidate list, refreshed on
successful additions and removals. Expiry is silent. Enabling TTL when restoring a TTL-disabled
snapshot grants existing candidates a full retention from restore time. RocksDB also reclaims
expired keys with bounded metadata-only sweeps during ingestion; cleanup seeks past candidate
tables so a large candidate list does not enlarge the cleanup working set. Flink 2.2 disables TTL
for the VALUE, MAP and ADAPTIVE materializer strategies; the native path also disables it for
those strategies.
The strategy option is `table.exec.sink.upsert-materialize-strategy.type`. ADAPTIVE high/low
threshold validation retains Flink 2.2's backend defaults (400/300 for heap, 50/40 otherwise).
Native sink translation shares these strategy and backend-dependent matching rules, including after
ordinary native checkpoint recovery. The released comparison controls cover scalar and nested
matching values on both stores; sharing strategy construction keeps sink integrations consistent.

Insert-only sinks do not need this stage, matching Flink's runtime decision even when
`table.exec.sink.upsert-materialize=FORCE` marks the physical sink.

Validation compares emitted row kinds and payloads against the released LEGACY and V2 operators,
including heap/RocksDB matching, checkpoint restore, TTL, key-group rescaling and off-heap limits.
The Doris SQL matrix exercises each strategy with and without inferred input keys through both
released writer modes and checks positive native materializer work. Stock SQL uses JSON for its
retracting reference because the released stock Arrow serializer rejects deletable sinks; the
native writer uses Arrow. Updating benchmark queries use Arrow on all three paths.

[Disk-backed measurements](../benchmarks/doris-constraints-rocks-2026-10-23/README.md) retain
stock, actual previous RAM-backed candidate state and current RocksDB trials. The measured
one-million-row MAP jobs improve stock medians by 2.29–3.75×, while costing 1.35–2.28× the
previous RAM-backed implementation. The capacity regression retains 10,000 candidates for one
key under a 16 KiB operator budget and recovers them from native database files.

## Canonical interchange qualification

The released Flink 2.2.1 adapter imports and exports stock LEGACY, VALUE, MAP and ADAPTIVE state.
The unmerged stock compiled-plan path now attaches that contract automatically for BIGINT fields
and aligned exchanges. It replaces the inner materializer while retaining Flink's source/sink
implementations, constraints, rowtime handling, original materializer UID and maximum parallelism.
The stock compiled plan remains unchanged; no separate state conversion or manual ID mapping is
required on this qualified path. Broader types, ordinary SQL submission and accelerated Doris
submission still need binding and qualification in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Native-format portable
snapshots remain separate from stock-compatible canonical savepoints.

Seventy complete-job cases cover both engine orders and native stores across all five layouts:
20 continued-input round trips, 20 repeated handoffs without new input, 10 ordinary native
checkpoint recoveries followed by canonical export, and 20 rescale cases. The rescale matrix uses
64 actual sink keys and a distinct inferred input key through 1 → 3 → 2 tasks with maximum
parallelism fixed at 128. Every requested native materializer task must process input. Source
splits/coordinator state also rescale; the transactional sink retains one writer, so these cases do
not claim sink-writer rescaling. Per-key committed changelogs match stock without requiring a
cross-key output order. Independent custom sources and transactional
sinks retain their stock implementations. Clean stock workers reject StreamFusion on their
classpath, native legs require materializer input/backend witnesses, and external committed
changelogs match an uninterrupted stock baseline. Partial, full and empty transpose input cases
are exercised. Fifteen planner controls additionally check strategy selection, unchanged portable
JSON, stock loading, typed/operator-disabled fallback and insert-only runtime elision.

Released 2.2.1 stock protocol controls cover LEGACY, VALUE, MAP and both ADAPTIVE representations
across heap/RocksDB canonical handoffs, including a second handoff without input. They compare
actual serialized keys/values and descriptor serializers, then check continued changelog output.
LEGACY uses `values` with a list serializer and optional whole-list TTL age. VALUE uses `list`
with row/timestamp tuples. MAP uses `rowToSqnState`, `sqnToNodeState` and `highestSqnState` for
matching indexes, linked candidates and the sequence cursor/size. ADAPTIVE registers both layouts.
The adapter reads live memory candidates or persistent records directly into bounded Arrow
transport chunks and Flink's stock serializers; it does not first create or decode a native snapshot.
Separate streams carry group metadata, complete original rows and matching keys. Restore installs
one group at a time without replaying input or rebuilding linked order. Actual binary keys, the
sequence cursor/size, independent index entries, links, sampled timestamps and original LEGACY
TTL ages survive handoff. The Arrow transport is internal, not the canonical file format.

Managed values and serializer snapshots use released Flink classes, including its sequenced-multiset
serializers and generated matching-key equaliser/hash functions. Their factories are generated at
planning time, so task restore does not require planner implementation classes. No StreamFusion serializer or
private replacement for a stock state object is written. A serializer-reader test blocks loading
StreamFusion and planner implementation classes. Complete-job stock workers also restore without
StreamFusion JARs. Accelerated connector replacements and their delivery guarantees still require
separate qualification.

Internal candidate row kinds are normalized to INSERT. LEGACY explicitly ignores the stored flags
when matching, and V2 normalizes incoming flags before multiset operations. Live heap candidates
can acquire output flags when stock emits them; those mutable flags are not a byte-identity promise
across continued input. Emitted INSERT/UPDATE/DELETE kinds and future stock behavior remain strict
continuation checks. V2's
sampled timestamps are preserved even with TTL disabled. Historical native V2 candidates whose
timestamps were never recorded fail canonical export explicitly; the adapter does not assign the
restore clock. Such native snapshots remain usable for ordinary native recovery, and LEGACY has
no stock per-candidate timestamp field.

Operator controls cover both stock and native stores, repeated no-input handoffs, native ingress
and resumed processing, empty savepoints, nested ARRAY matching keys with released Java import
buffers, LEGACY TTL expiry across ordinary native checkpoints, unknown state and mismatched keys.
The native visitor preserves live state on receiver failure and exports large persistent groups
in chunks.
Forty-eight JNI capture-failure controls throw before Arrow import and after importing the batch.
They require the original JVM exception, released buffers, unchanged canonical metadata/entry bytes
on retry, and correct native and stock continuation through later retractions. Broader logical types,
key layouts, other parallelism/version combinations, ordinary SQL and accelerated-connector
workflows remain qualification work;
the qualified compiled-plan path does not establish those other submission forms.

The controls retain a released MAP/linked-ADAPTIVE failure boundary: replacing an older input-key
candidate moves the cursor; appending can overwrite a live sequence, and a later removal throws
from the stock `REMOVAL_ALL` invariant. They assert the same failure before and after handoffs,
alongside successful continuation sequences. Canonical conversion must preserve the stock cursor
and indexes rather than rebuild an apparently equivalent ordered list. The original stock protocol
controls establish this failure boundary; no successful continuation is claimed for that failing
released sequence. Benchmarking remains deferred for #569.
