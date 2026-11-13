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
progress and final maximum watermark remain. Unknown Calc programs, other operators
and observable watermark expressions preserve the original eager boundaries.
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
