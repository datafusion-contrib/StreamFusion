# Flink 1.18 Nexmark compatibility baseline

This development baseline compares the Flink 1.18.1 port with stock Flink 1.18.1 using the
unchanged Nexmark generator and blackhole sink. It records compatibility and cost for this
row-fed shape; it does not establish production support or replace the Kafka headline table.

The run used Apple M1 Max, JDK 17, Rust 1.94 release with mimalloc, 2,000,000 events,
parallelism 1, one warmup and the best of two measured trials per engine. For each query the
existing harness runs the Flink trials first, then the StreamFusion trials, in the same JVM.
Local shuffle handles remain disabled by the benchmark profile. No other local tests or
benchmarks ran concurrently. The existing schemas, queries, watermarks and wire encodings
were unchanged. Whole-job elapsed time includes planning and both row/Arrow conversions.

A separate planning check verifies all ten selected query plans contain native substitutions,
RowDataToArrow and ArrowToRowData. The benchmark also rejects silent planner fallback. The
blackhole run does not compare output values; released-Flink parity tests and unchanged
upstream integration tests provide the independent correctness checks.

| Query | Stock Flink (s) | StreamFusion (s) | Flink / StreamFusion |
|---|---:|---:|---:|
| q0 | 1.041 | 2.004 | 0.52x |
| q1 | 1.093 | 1.974 | 0.55x |
| q2 | 0.986 | 1.262 | 0.78x |
| q3 | 0.810 | 2.107 | 0.38x |
| q5 | 1.873 | 3.015 | 0.62x |
| q7 | 2.435 | 3.948 | 0.62x |
| q8 | 0.754 | 2.677 | 0.28x |
| q19 | 3.961 | 10.200 | 0.39x |
| q20 | 1.983 | 3.656 | 0.54x |
| q21 | 2.135 | 2.436 | 0.88x |

Every measured query is slower in this row-fed baseline. This result motivates measuring
and improving the native boundaries before making a performance recommendation for 1.18.
It does not estimate performance for Arrow sources, native format decoding or a production
Kafka pipeline. The existing harness also printed opt-in incompatible q1 and q21 variants;
those are excluded from the parity table above.

[Recorded harness output](flink118-nexmark-2026-09-19.txt). The harness reports the best times,
not individual trial timings. This run used Java sources at `3edc6bae` and the optimized
native libraries from the same port's release build; no native changes intervene between
that build and the measured revision.

Build the optimized library and run the unchanged harness:

```sh
SF_BENCHMARK=true SF_ROWS=2000000 SF_MATRIX_GENERATOR=true \
SF_MATRIX_PARQUET=false SF_MATRIX_KAFKA=false \
SF_MATRIX_QUERIES=q0,q1,q2,q3,q5,q7,q8,q19,q20,q21 \
mvn -B -ntp -Pbench,flink-1.18 -pl :streamfusion-runtime-flink1.18 -am test \
  '-Dtest=NexmarkMatrixBenchmark#matrix' -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.testForks=1
```

For an already-built `bin/build-release.sh --host-only --flink-line 1.18` payload, add
`-Dnative.build.skip=true -Dnative.profile=release-staging/release`, as used for this run.
The profile and artifact suffix must match the native library's Java ABI.
