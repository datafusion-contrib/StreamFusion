# Keep Hudi payload batches across unobserved watermarks

The independent watermark assigner preserves Flink's eager per-row boundaries.
With the Nexmark corpus, a four-way source can split a 4,096-row Arrow batch into
roughly six-row pieces. A two-million-event Q0 CPU profile attributed 83.5% of
samples to work under this slicing path, including 31.5% in append batch flushing.
Repeated C Data exports and tiny Rust Parquet writes dominated the connector.
These are inclusive sample shares, not independent costs or throughput gains.

The Hudi sink does not consume watermarks. Its planner now retains whole batches
through a terminal chain of native Calcs when their original programs contain no
`CURRENT_WATERMARK` call. Candidate evaluation, NULL rowtime failure, periodic
progress and final maximum watermark remain. Append sinks can also cross native
exchanges, mini-batch assigners and inner joins with two insert-only inputs.
For MOR sinks explicitly configured with commit-time ordering and precombine, the proof also crosses native exchanges,
mini-batch assigners, regular joins, non-windowed aggregates/ranking and
deduplication that does not observe watermarks. Rowtime keep-first retains fine
boundaries. These sinks retain the final upsert per key, so unobserved flush
positions do not select event-time winners. User-function calls stop this broader rewrite. Event-time ordering, precombine-disabled writes,
windows, unknown operators/programs and observable watermark expressions retain
the original eager boundaries. A Q4 diagnostic profile attributed 35.6% of CPU
samples to regular-join watermark flushing. A Q9 CPU capture likewise attributed
49.7% of samples to batch flushing, including 27.8% under join watermark flushing.
These inclusive CPU shares identify work performed while the process was active;
they are not elapsed-time speedups.

Earlier Q0/Q1/Q4 full-value comparisons passed, but macOS power logs later showed
repeated system suspension during the campaign. Their timing qualification has
been withdrawn. The [benchmark method](../benchmarks/hudi-nexmark.md) now requires
an awake host and records both clocks. Controlled release comparisons are pending.

An awake Q9 CPU capture then identified MOR log serialization (32.9% of samples)
and its encoder handoff (25.7%, inclusive). Java previously filled metadata for
every parent row whenever the iterator changed source batches, even when only
one selected row was encoded. Bounded reference chunks now share the reader
interleave kernel, gather only selected payloads in Rust and create metadata
only for dense output rows. Iterator order and repeated positions are preserved.
Controlled timing qualification of this change is pending.
