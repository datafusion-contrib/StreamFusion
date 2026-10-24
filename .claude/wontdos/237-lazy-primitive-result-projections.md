# Lazy primitive-result projection experiment (#237, 2026-10-24)

Do not retain the validity-scanning fast path and lazy projection initialization tested with
primitive table-function results. Correctness requires Flink's generated scalar evaluation when
an emitted NULL contradicts non-null result metadata; ordinary values appeared eligible for
an existing column view.

A bounded JFR pass observed 40 table-function stacks, including four projection-application
frames, too sparse for precise attribution. One measured implementation passed all 368 focused
checks on each supported Flink line. Release complete jobs used 262,144 rows, parallelism one,
2 GiB heap, both transposes, two warmup pairs and five alternating measured pairs.

Compared with the correctness-enabling eager projection, control medians fell 6.3% on 2.2.1
and 2.4% on 1.18.1, but the 1.18 Row INT/ARRAY and atomic INT cases rose 28.0% and 8.6%.
Startup/JIT variability and separate-JVM drift limit attribution. Revert the extra path and
stop after that single attempt. Slower correctness-verified UDF coverage retains the accepted
performance exception; no acceleration is claimed.

[The result-conversion report](../../docs/benchmarks/results/jvm-table-function-primitive-results-2026-10-24.json)
retains all 588 trials, ranges, stock/prior controls, source hashes and sampled host activity.
Reopen only with representative longer-job evidence of repeatable benefits across both Flink
lines without regressions and with all NULL/default, lifecycle and ownership contracts preserved.

This rejects one optimization experiment. Issue #237 remains open for its remaining function contracts.
