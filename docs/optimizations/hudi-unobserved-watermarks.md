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
For MOR sinks explicitly configured
with commit-time ordering and precombine, the proof also crosses native exchanges,
mini-batch assigners, regular joins and non-windowed aggregates. These sinks retain the final upsert per
key, so unobserved flush positions do not select event-time winners. User-function
calls stop this broader rewrite. Event-time ordering, precombine-disabled writes,
windows, unknown operators/programs and observable watermark expressions retain
the original eager boundaries. A Q4 diagnostic profile attributed 35.6% of CPU
samples to regular-join watermark flushing. The broader rewrite reduced the
two-million-event Q4 heap/no-mini-batch native diagnostic from 82.2 seconds to a
2.68-second median over three measured trials. The matching stock median was
4.82 seconds and the previous path was 142.39 seconds. Native medians beat stock
in all four heap/RocksDB and mini-batch configurations, by 1.55×–6.82×. Full
values, ranges and resources are recorded with the Q4 observations linked below.
The rewrite copies only the terminal branch, preserving shared producers.

On the two-million-event, four-partition Q0 job, one warmup and three rotating
measured trials gave medians of 1,080.5 ms stock, 7,884.6 ms previous StreamFusion
and 626.6 ms with native Hudi boundaries. Ranges were 1,007.0–2,011.6,
7,873.4–9,970.4 and 589.5–639.3 ms respectively. Every trial checked all
1,840,000 bid outputs and all four sink partitions. Timing includes final commit
visibility; fixture creation and full stock readback are outside timing.
This is a 1.72× median gain over stock for the measured bounded Q0 configuration,
not a steady-state throughput claim or a result for other queries. The
[Hudi Nexmark method and observations](../benchmarks/hudi-nexmark.md) record the
resource settings, all trials and the remaining qualification work.
