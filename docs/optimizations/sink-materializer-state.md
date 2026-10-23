# Retain final sink candidate replacements per batch

The Rust sink materializer emits every arrival required by Flink while retaining owned candidate
state. Ordinary upserts share incoming Arrow value buffers; only restored predecessor rows need
payload decoding. State encoding remains necessary so a small candidate cannot pin a large input
batch indefinitely. Generic array/map NaN payloads are normalized as in Flink's required keyed
exchange. Only affected columns are copied; ordinary columns and explicit binary-array carriers
keep their shared buffers.

For positive-only batches whose inferred input upsert key equals the sink primary key, each key
has one candidate. Replacing that candidate repeatedly within one call has no intermediate state
observer. The operator therefore emits every row with the required INSERT/UPDATE_AFTER kind but
encodes and stores only each key's final replacement. A vectorized key encoding groups repeated
identities; Flink binary routing keys are encoded only once per distinct identity in the batch. TTL refreshes use the same processing-time
sample for the whole call, memory accounting covers retained state, and checkpoints observe the
completed call. The same optimization applies to disk-backed candidates, avoiding repeated writes
of superseded replacements. The optimization requires scalar byte equality, at least 64 rows and an initial
repeated key; complex matching and retracting batches keep the ordered candidate path.

This reduces payload encoding, matching-key allocation and repeated state replacement without
coalescing the outgoing changelog or adding a Java row boundary. Differential tests compare the
emitted sequence against Flink at several batch sizes, then exercise snapshot recovery and both
state layouts.

At the Doris writer boundary, represented collection columns are normalized individually using
borrowed row views. Ordinary sibling columns retain Arrow buffer transfers, so a represented ROW
does not force scalar strings, decimals and every other column through a row conversion.

The [Doris materializer measurements](../benchmarks/doris-materializer-2026-10-22/README.md)
retain stock, previous and native complete-job trials, earlier implementations and profiling.
The CPU profile includes substantial JVM compilation/GC and upstream aggregation work; it does
not identify materialization as the only end-to-end bottleneck.
