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
input count. Append-only SUM likewise has a nullable sum with no physical non-null count, while
AVG retains its actual sum/count pair. Native execution uses existence/initialization markers for
those absent fields and projects only actual generated fields into Flink state. Markers do not
replace aggregate counts: a zero COUNT/AVG count cannot delete an append group. The captured
contract reinstalls this policy after native checkpoint restoration without a state-format flag.

This is the single-phase BIGINT arithmetic contract for append-only and retracting input, with
one INT/BIGINT grouping key or a global group. Global state keeps Flink's eight-byte empty BinaryRow
key, zero-field RowData key serializer and singleton parallelism/maximum parallelism. The adapter
does not invent a grouping key or an initial accumulator/result. Compiled grouping without actual
aggregate calls retains its stock owner. Additional schemas/keys/phases,
state-backed data views and full version/connector/failure/delivery coverage
remain in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is
explicitly deferred by the maintainer.
