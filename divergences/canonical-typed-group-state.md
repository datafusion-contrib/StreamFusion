# Preserve typed grouped accumulators and shared DISTINCT views

Arroyo separates incremental accumulator state from retained membership in its
`crates/arroyo-worker/src/arrow/incremental_aggregator.rs`. StreamFusion keeps that separation,
but canonical interchange must preserve the released Flink generated layout. Its membership
counters are signed Java longs with wrapping arithmetic, whereas Arroyo's batch membership count
uses saturating unsigned decrement. Stock-bound COUNT DISTINCT uses Flink's exact zero/one
transition rules; the ordinary native membership policy remains separate.

The binding derives physical schemas from released aggregate information and verifies the actual
running function's accumulator types. It uses original aggregate calls for DISTINCT/filter policy
because Flink's derived inner calls strip those flags. Actual key/view serializers and stock
identities accompany the private native execution graph; the portable plan stays stock Flink.

A shared stock view becomes one native membership table per logical count, preserving shared bit
or signed-counter slots on export. COUNT's scalar remains independent of membership. A NULL view
key is retained as a membership counter and exported to the released null-marker state; it does
not increment COUNT. Native checkpoints use the existing preserved-scalar representation so
recovery neither counts null markers nor reconstructs an independent scalar from cardinality.
The generated RAW placeholder remains a non-null value with Flink's one-byte NullSerializer payload.

The JNI handoff follows Comet's moved Arrow C Data ownership and the existing guarded callback
pattern. Direct raw RocksDB header/companion visitors and memory visitors emit bounded frames,
without writing an intermediate native snapshot. Full preflight precedes mutation and source
entries remain available until import succeeds. Entry TTL for DISTINCT is deliberately unbound
until independent view ages have a complete projection.

The exact supported and tested subsets remain on [the operator coverage page](../docs/operators/group-by.md).
Other views, phases, schemas, connector guarantees and failure/version matrices remain in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.
