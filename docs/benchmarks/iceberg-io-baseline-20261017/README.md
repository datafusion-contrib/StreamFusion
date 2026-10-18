# Comparison with previous StreamFusion Iceberg I/O

Both legs execute native query operators. The baseline uses stock Iceberg I/O,
including row/Arrow transposes around native query islands; the native leg keeps
data columnar through Rust Parquet reads and data-file writes. q0 has no query
island, so its baseline is the stock identity pipeline rather than invented
transpose operators. Graph assertions cover the other three queries.

This separate release+mimalloc cohort uses the final Nexmark completion protocol,
2M events, four input/output buckets, parallelism four, 4 GiB job heaps, one
warmup and three measured pairs for q0/q3/q4/q9 in memory/off and disk/off.
All **32 paired validations pass**. It is excluded from the stock-Flink matrix.

[Every median/range and raw observation](results.md) is retained. q0 improves
1.80× memory/off and 1.78× disk/off. The four-query geometric means are 1.90×
memory/off and 0.95× disk/off. Slower results remain visible: q4 memory/off is
0.79×, q4 disk/off 0.95×, and q9 disk/off 0.49×.

[Phase metrics](metrics.csv) separate corpus progress, checkpoint and drain. In
q4 memory/off, native median corpus progress is faster (1.573 versus 2.373 s),
but checkpoint/drain waits erase that gain. In q9 disk/off, native corpus progress
is slower (29.560 versus 13.139 s), as well as its settling checkpoint; this needs
state/I/O investigation rather than claiming every regression is final commit.
The [additional q9 profile](../iceberg-profile-20261017/README.md) keeps query
execution fixed and retains the much slower profiled native observation.

The I/O paths therefore do not establish a universal end-to-end speedup over
previous StreamFusion behavior. Supported contracts now accelerate by default when the optional module is installed. Further state/checkpoint and input
scheduling work is tracked in [issue #391](https://github.com/datafusion-contrib/StreamFusion/issues/391).
