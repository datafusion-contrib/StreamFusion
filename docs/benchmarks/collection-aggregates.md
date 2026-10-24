# Collection aggregates

## Integer COLLECT

COLLECT(INT/BIGINT) reuses the released Flink handler and managed MapViews inside the existing
columnar JVM grouped operator. Scalar inputs and non-character output fields are required;
other COLLECT types and ARRAY_AGG remain on fallback. Results require a verified preserving Calc
or final row sink. [Coverage](../operators/group-by.md#integer-collect) gives the exact gates.

Release/mimalloc measurements on 2026-10-24 use Flink 2.2.1, OpenJDK 17.0.20.1 and Linux
x86-64/WSL2 on an Intel Core i7-12650H. Each job has 200,000 runtime-source rows, parallelism 1,
an 8 GiB test heap, two warmups and three alternating trials per engine. INT keys have 64/4096
groups; BIGINT values cycle through 8 values per group. Every eighth input value is NULL, making
keys congruent to 7 modulo 8 all-NULL groups with empty multiset results. Accumulator caching uses
its default 0 entries. This source uses Java Row records and a rowwise blackhole sink.

Timings include planning, submission, processing, keyed state and both row/Arrow transposes;
cluster setup and initial explain are excluded. No tests, builds or other benchmarks run
concurrently. Previous code at `bbde17c4` uses identical benchmark/input code and the same frozen
native DSO; COLLECT must fall back there with no native operators. Candidate runs require
consumed NativeJvmGroup work. [All trials and profiles](results/integer-collect-2026-10-24.json)
include each JVM's paired stock controls and the full benchmark configuration.

Median seconds (minimum–maximum), retaining complete-job startup costs:

| Groups | Paired stock | Previous fallback | Final native route |
| --- | ---: | ---: | ---: |
| 64 | 0.428995 (0.311924–0.440618) | 0.333300 (0.325388–0.412596) | 0.344033 (0.329837–0.415788) |
| 4096 | 0.395371 (0.386500–0.422889) | 0.337087 (0.323164–0.383963) | 0.382120 (0.374743–0.472422) |

Native medians beat paired stock and exceed previous fallback. All ranges overlap, including
a slower wide-group native trial, so these short jobs do not establish sustained acceleration.
This ships under the accepted bounded collection tradeoff without a performance-only opt-in.

Optimization stops after two profiling/optimization passes. Native-only complete jobs have one
unprofiled warmup before each eight-second target capture; profiling stops after the current job
finishes. The 64-group capture has 19,173 samples, including 4,027 inclusive samples in released
group processing, 636 in COLLECT result snapshots and 1,264 in output append. The 4096-group
capture has 16,940 samples, including 4,034 in grouped processing, 913 in result snapshots and 1,071
in output append. Arrow map writing accounts for 460/366 inclusive samples. Counts overlap and
include planning, compilation and all JVM threads; they do not measure allocation or copied bytes.
No safe small shortcut was identified. Snapshot materialization, managed-state/output work and
startup remain performance opportunities in the [native SQL backlog](https://github.com/datafusion-contrib/StreamFusion/issues/239).
The production grouped operator, state codecs and JNI ownership protocol are unchanged.

Reproduce after building a release native library:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=JvmAggregateBenchmark#completeJobsRetainKeyedStateAndBothTransposes' \
  -Dsf.testForks=1 -Djvmagg.cases=collect -Djvmagg.rows=200000 \
  -Djvmagg.keys=64 -Djvmagg.values=8 -Djvmagg.warmup=2 -Djvmagg.runs=3 \
  -Djvmagg.expectNative=true -Djvmagg.expectCache=false
```

Also run with `-Djvmagg.keys=4096`. The previous checkout copies the same benchmark source and
sets `-Djvmagg.expectNative=false`. The optional `jvmagg.values` bounds the per-key input domain;
zero retains the existing monotonically increasing fixture used by the other aggregate cases.
The diagnostic result parser records this domain size and accepts older summaries without
`values`, treating them as the original unbounded domain (`0`). Trial-count, positivity and
reported-median checks apply to both formats.
For each CPU capture, set `SF_PROFILE_JVM_AGG=true`, select `#profileCollect`, keep the same
case/input/route settings and pass `-Djvmagg.seconds=8 -Dprofile.asprof=/path/to/asprof`.
`jvmagg.engine=flink|native|both` selects a timing engine; profile mode temporarily selects native
and restores its properties afterward. Profiled timings do not enter the trial table.
