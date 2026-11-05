# Preserve generated arithmetic accumulators directly

Arroyo's incremental aggregator retains accumulator state separately from batch membership state,
and flushes pending work at checkpoint barriers. StreamFusion keeps that structure in its existing
incremental grouped operator. The Flink interchange adapter copies raw native accumulators into
Flink's generated `accState` layout, retaining independent record/non-null counts and TTL ages.

The physical layout follows released Flink, rather than the SQL output schema or an Arroyo state
schema: the optimizer can share COUNT(v) and SUM0(v) across SUM/AVG results, while another plan
retains nullable SUM, AVG sum/count and an additional hidden input count. Native SUM already stores
an optional raw value; copying it preserves NULL versus cancelled zero without a format change.
AVG transfers its raw sum, never a value reconstructed from its displayed average.
Continued integer AVG uses wrapping division, as Java does for `Long.MIN_VALUE / -1` and as the
existing native integer AVG accumulator already does. A restored negative count must retain that
behavior; rejecting or normalizing it would lose reachable overflowed state.

The JNI boundary reuses Comet's moved Arrow ownership and the existing exception guard. Bounded
callbacks and raw RocksDB iteration avoid an intermediate native snapshot, reserve transient
memory, and release owners before propagating failures. Inbound Arrow owners are released before
raising a Java exception because their JVM release callbacks can otherwise clear that exception.
Import validates the full source
and empty target before applying frames; source state is cleared only after complete success.
A resource-denied target is closed before a fresh retry.

Automatic binding recovers the released physical aggregate information from the original stock
plan and checks its generated accumulator types against the captured running function. Hidden
COUNT offsets are independent of displayed aggregate order; Count1 ignores argument nullability
because its released accumulator advances unconditionally. The native state owner retains the
original serializer, TTL policy, identity and maximum parallelism. Unsupported contracts retain
the stock owner in compiled lowering and the existing ordinary graph behavior elsewhere.

Flink's append-only RecordCounter tests whether an accumulator exists, rather than reading an
input count. Append-only SUM, MIN and MAX each retain a nullable scalar without a physical non-null count, while
AVG retains its actual sum/count pair. Native execution uses existence/initialization markers for
those absent fields and projects only actual generated fields into Flink state. Markers do not
replace aggregate counts: a zero COUNT/AVG count cannot delete an append group. The captured
contract reinstalls this policy after native checkpoint restoration without a state-format flag.

Released append-only BIGINT MIN/MAX reuse this scalar adapter and initialization marker, including
when their generated row shares COUNT/SUM0 slots with SUM/AVG. Retracting MIN/MAX cannot use that
mapping: released Flink stores structured cached selections/map sizes and separate MapViews. Stock
TTL can remove the accumulator while retaining expired view entries after a late retraction.
Their independent lifetimes, clocks and negative frequencies must survive conversion; deriving a
view from a cached result or deleting it with the header would lose stock state. A separate structured adapter now preserves those values directly. It follows Arroyo's separation
of retained membership from accumulator state, while preserving Flink's independently expiring
headers and view entries. An optional private native-checkpoint frame records absent headers;
canonical export visits live state without an intermediate checkpoint. The generated RAW view
placeholder retains its one-byte NullSerializer payload. Actual captured MapView serializers and
their snapshots remain authoritative, including ExternalSerializer rather than an inferred long
serializer. These differ in restore compatibility even when their value bytes match.

This is the single-phase BIGINT arithmetic contract for append-only and retracting input, plus
append-only/retracting BIGINT MIN/MAX, with
one INT/BIGINT grouping key or a global group. Global state keeps Flink's eight-byte empty BinaryRow
key, zero-field RowData key serializer and singleton parallelism/maximum parallelism. The adapter
does not invent a grouping key or an initial accumulator/result. Compiled grouping without actual
aggregate calls retains its stock owner. Additional schemas/keys/phases,
other state-backed data views and full version/connector/failure/delivery coverage
remain in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is
explicitly deferred by the maintainer.


The structured extrema projection also captures INT/SMALLINT/TINYINT, DATE/TIME and BOOLEAN
value contracts. This extends the existing operator and transfer instead of introducing a second
snapshot format. Existing bounded long fields are lossless carriers for these values; Java
reconstructs their exact boxed stock values and uses the original serializers for both headers
and MapViews. Import checks each carrier's range and BOOLEAN domain before mutation. No type,
serializer or bookkeeping is inferred from an emitted SQL result. Flink 2.2.1's resolved TIME
precision is zero even for declared TIME(3), and that stock metadata remains authoritative.

Arroyo's sliding accumulator and retained membership separation remains the reference, while
Flink's signed counts, independently expiring views and hidden cache semantics define the state
contract. Comet's moved C Data ownership and exception propagation remain unchanged. Memory and
raw keyed RocksDB visit live state directly; existing ordinary checkpoint formats are unchanged.
This wider extrema binding still excludes composite keys, other retained types/views/phases and
the full connector/failure/version matrix. Benchmarking stays deferred under #569.
