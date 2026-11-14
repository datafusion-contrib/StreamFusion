# Keep Hudi payload batches across unobserved watermarks

The independent watermark assigner preserves Flink's eager per-row boundaries.
With the Nexmark corpus, a four-way source can split a 4,096-row Arrow batch into
roughly six-row pieces. A two-million-event Q0 CPU profile attributed 83.5% of
samples to work under this slicing path, including 31.5% in append batch flushing.
Repeated C Data exports and tiny Rust Parquet writes dominated the connector.
These are inclusive sample shares, not independent costs or throughput gains.

The Hudi sink does not consume watermarks. Its planner now retains whole batches
through a terminal chain of native Calcs without user functions when their
original programs contain no `CURRENT_WATERMARK` call. Candidate evaluation, NULL rowtime failure, periodic
progress and final maximum watermark remain. Append sinks can also cross native
exchanges, mini-batch assigners and inner joins with two insert-only inputs.
Append and terminal commit-time upsert paths can also cross single-phase
processing-time fixed windows: these fire from the processing-time service and
do not consume event-time watermarks. Tumble, hop and cumulative plans are
covered; a `CURRENT_WATERMARK` expression below the window still stops the proof.
For MOR sinks explicitly configured with commit-time ordering and precombine, the proof also crosses native exchanges,
mini-batch assigners, regular joins, non-windowed aggregates/ranking and
deduplication that does not observe watermarks. Rowtime keep-first retains fine
boundaries. These sinks retain the final upsert per key, so unobserved flush
positions do not select event-time winners. User-function calls stop this broader rewrite. Event-time ordering, precombine-disabled writes,
event-time windows, unknown operators/programs and observable watermark expressions retain
the original eager boundaries. A Q4 diagnostic profile attributed 35.6% of CPU
samples to regular-join watermark flushing. A Q9 CPU capture likewise attributed
49.7% of samples to batch flushing, including 27.8% under join watermark flushing.
These inclusive CPU shares identify work performed while the process was active;
they are not elapsed-time speedups.

Earlier Q0/Q1/Q4 full-value comparisons passed, but macOS power logs later showed
repeated system suspension during the campaign. Their timing qualification has
been withdrawn. The [benchmark method](../benchmarks/hudi-nexmark.md) now requires
an awake host and records both clocks. The completed heap-state,
mini-batching-off comparison uses two million events, four source/sink tasks,
one warmup and three rotating release trials per engine. Q0 medians are
986.1 ms stock, 7,143.7 ms previous StreamFusion and 529.9 ms native; Q4 is
2,039.6 / 85,742.2 / 1,323.7 ms respectively. The benchmark page retains all
trial ranges and losses. The heap/mini-batching-on baseline is also recorded there.
Persistent-window tuning and full-matrix requalification remain in progress.

A separate two-million-event Q12 diagnostic collected 6,711 CPU samples. Work
under the watermark assigner/Calc accounted for 52.8% and key-group splitting
for 36.7%; empty window emission accounted for 8.1%. These inclusive shares
overlap. This motivated extending the existing proof across processing-time
fixed windows rather than changing their timer implementation. The
[structured observations](../benchmarks/hudi-processing-time-batches-20261114.json)
retain the diagnostic and every attempted comparison. Local after-change timings
overlapped another session's builds/benchmarks and remain diagnostic. The first
isolated release comparison combined this proof with an allocation-owner candidate:
Q12 native medians were 6,121 -> 400 ms at 200k events, versus 634 ms stock after
the change (15.3× over baseline, 1.59× over stock). Q8 did not establish a benefit
from the allocation candidate, which was removed. The retained configuration is
measured in the repeat below; [all 72 isolated observations](../benchmarks/hudi-java-comparison-20261114.json)
remain available. Processing-time count checks permit empty output when a
bounded job completes before its first timer; they are not full-value parity.

The [retained-configuration repeat](../benchmarks/hudi-calc-transport-20261114.json)
removes the owner candidate and combines the proof with Calc transport pruning and
empty-result suppression. Q12 native medians are 8,138 -> 529 ms (15.39×), versus
809 ms stock, in that isolated 200k-event run. Q8 improves 1.13× but still loses
to stock by 3.66×. The full two-million-event matrix remains pending.

An awake Q9 CPU capture then identified MOR log serialization (32.9% of samples)
and its encoder handoff (25.7%, inclusive). Java previously filled metadata for
every parent row whenever the iterator changed source batches, even when only
one selected row was encoded. Bounded reference chunks now share the reader
interleave kernel, gather only selected payloads in Rust and create metadata
only for dense output rows. Iterator order and repeated positions are preserved.
In that same completed matrix, Q9 medians are 2,741.7 ms stock,
166,255.0 ms previous StreamFusion and 2,804.8 ms native. This is near stock
parity (0.98×) and 59.3× faster than the previous boundaries. These measurements
include the combined watermark and writer changes; they do not isolate a gain
from log encoding alone. Windows retain their observable boundaries and remain
substantially slower than stock.
[Issue #663](https://github.com/datafusion-contrib/StreamFusion/issues/663)
tracks the remaining boundary and allocation costs without weakening watermark
semantics.
