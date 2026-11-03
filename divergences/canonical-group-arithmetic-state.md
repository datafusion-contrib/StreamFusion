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

The JNI boundary reuses Comet's moved Arrow ownership and the existing exception guard. Bounded
callbacks and raw RocksDB iteration avoid an intermediate native snapshot, reserve transient
memory, and release owners before propagating failures. Inbound Arrow owners are released before
raising a Java exception because their JVM release callbacks can otherwise clear that exception.
Import validates the full source
and empty target before applying frames; source state is cleared only after complete success.
A resource-denied target is closed before a fresh retry.

This is the explicit single-phase BIGINT arithmetic contract. Automatic planner binding, additional
schemas/keys/phases, state-backed data views and full version/connector/failure/delivery coverage
remain in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is
explicitly deferred by the maintainer.
