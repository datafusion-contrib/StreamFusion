# Bind bounded OVER state from the released physical contract

Arroyo's window-function operator retains timestamp buckets separately from its active execution
instances and reconstructs pending execution from those buckets. StreamFusion retains that
separation between input and computation. Its existing incremental OVER implementation additionally
preserves Flink's raw accumulators, counters and callback bookkeeping, because replaying retained
rows cannot reconstruct overflowed arithmetic or already processed state exactly.

The submission binding captures the released stock function and its generated accumulator types,
physical aggregate widths, hidden count, constant arguments, serializers and identity before native
lowering. It installs the existing version-qualified bounded contract; it does not change the
native arithmetic or JNI transfer protocol. That protocol follows Comet's moved Arrow ownership
and exception handling patterns.

Ordinary physical OVER lowering materializes bound and aggregate constants with helper projections.
Those helpers can share one original OVER role, so treating them as separate state owners would
make operator identity assignment ambiguous. Qualified bounded arithmetic instead lowers a private
runtime projection of the original stock template, as rank binding already does. Internal native
constant columns retain the actual argument indices without adding durable helper state. The
original SQL or compiled stock submission artifact remains the user's restore input.

The first binding is deliberately limited to existing qualified processing-time ROWS, event-time
RANGE and event-time ROWS without processing cleanup contracts, known numeric schemas and keys.
Additional types, unbounded modes, state-backed views and the full connector/version/failure matrix
remain in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is
explicitly deferred while functionality and correctness qualification continue.
