# Collection aggregates

## Integer COLLECT

COLLECT(INT/BIGINT) reuses the released Flink handler and managed MapViews inside the existing
columnar JVM grouped operator. Scalar inputs and non-character output fields are required;
other COLLECT types remain on fallback; integer ARRAY_AGG is covered below. Results require a verified preserving Calc
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

## Integer ARRAY_AGG

On Flink 2.2.1, ARRAY_AGG(INT/BIGINT) uses the same generated JVM grouped runtime with released
RAW LinkedList accumulators. Scalar inputs and non-character output fields are required.
Flink 1.18.1 has no released ARRAY_AGG catalog entry and is explicitly N/A. Native work, resolved
types, arrival order, NULL treatment, DISTINCT/FILTER, retractions and repeated heap/RocksDB
recovery are verified. Nullable primitive arrays additionally feed native Top-N/UNNEST;
[coverage](../operators/group-by.md#integer-array_agg) describes sensitive output restrictions.

Measurements use the same release/mimalloc DSO, CPU, OS, JDK, 8 GiB heap, parallelism 1,
stock/native alternation and timing boundaries as integer COLLECT above. Each complete job has
50,000 runtime-source rows, 64/4096 INT groups, two warmups and three measured trials per engine.
BIGINT input values cycle through 8 values per group, with NULL every eighth input. INT jobs add
CAST(v AS INT) before grouping in both engines. The value domain does not bound ARRAY length:
default RESPECT NULLS arrays grow to 781/782 elements at 64 groups and 12/13 at 4096 groups. All-NULL groups
also grow under RESPECT NULLS. Accumulator caching remains at its default 0 entries.

Runtime Row sources, output-array materialization, keyed state, both transposes, a blackhole
row sink and planning/submission remain timed. Initial explain and cluster setup are excluded;
no other local tests/builds or benchmarks run concurrently with these measurements. Previous
code at `9ee3080f` uses identical benchmark/input source and the same frozen native DSO; both
ARRAY_AGG cases require fallback there. [All trials and profiles](results/integer-array-agg-2026-10-24.json)
retain both paired stock controls, input metadata and diagnostic boundaries.

Median seconds (minimum–maximum), retaining complete-job startup costs:

| Element type / groups | Paired stock | Previous fallback | Final native route |
| --- | ---: | ---: | ---: |
| BIGINT / 64 | 0.590129 (0.530637–0.612707) | 0.557446 (0.517953–0.656765) | 0.682652 (0.615160–0.695289) |
| INT / 64 | 0.530445 (0.526918–0.631507) | 0.654646 (0.535394–0.671151) | 0.630828 (0.575247–0.659267) |
| BIGINT / 4096 | 0.177614 (0.170333–0.179082) | 0.114341 (0.098210–0.180525) | 0.187168 (0.109108–0.223839) |
| INT / 4096 | 0.099210 (0.094892–0.128949) | 0.179145 (0.154373–0.208797) | 0.112457 (0.104145–0.200835) |

All native medians exceed paired stock. BIGINT medians also exceed previous fallback, while
INT medians improve it; ranges overlap. Small-array jobs are dominated by startup and do not
establish sustained throughput. This is accepted bounded collection groundwork without a
standalone acceleration claim or performance-only opt-in.

Optimization stops after two profiling/optimization passes. The wide BIGINT capture at 64 groups
has 15,403 samples: 3,999 inclusive samples in ARRAY_AGG getValue, 3,940 self samples in
LinkedList.toArray, 1,288 in array copying and 569 in Arrow array writing. Those snapshots retain
released ownership semantics; no safe small shortcut was identified. The second capture,
INT at 4096 groups, has 25,319 samples, including 902 in grouped processing, 158 in getValue,
114 in array copying and 55 in Arrow array writing. All JVM threads, planning and compilation
are included, and counts overlap. These are not allocation counts or copied-byte measurements.
Snapshot materialization, boxing/copying and startup remain work for the
[native SQL performance backlog](https://github.com/datafusion-contrib/StreamFusion/issues/239).
No production Rust, aggregate runtime or state/ownership protocol changes.

Reproduce both types and cardinalities after building the release library:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=JvmAggregateBenchmark#completeJobsRetainKeyedStateAndBothTransposes' \
  -Dsf.testForks=1 -Djvmagg.cases=array_agg,array_agg_int -Djvmagg.rows=50000 \
  -Djvmagg.keys=64 -Djvmagg.values=8 -Djvmagg.warmup=2 -Djvmagg.runs=3 \
  -Djvmagg.expectNative=true -Djvmagg.expectCache=false
```

Also run with `-Djvmagg.keys=4096`. Previous-code controls copy the same benchmark source and
set `-Djvmagg.expectNative=false`. For CPU captures set `SF_PROFILE_JVM_AGG=true`, select
`#profileArrayAgg`, use one case at a time, and pass `-Djvmagg.seconds=8 -Dprofile.asprof=/path/to/asprof`.
The two captures choose `array_agg`/64 groups and `array_agg_int`/4096 groups. Profile timings
remain separate from the unprofiled trial table.
