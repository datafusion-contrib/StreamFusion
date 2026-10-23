# Java operator wrapper audit, 2026-10-19

This historical audit timed the Java operator wrapper, including its JNI/C Data handoffs.
The maintainer subsequently required small-functionality clocks to start inside Rust on
prepared Arrow; see the [current methodology](../benchmarks.md#methodology). These timings
remain wrapper diagnostics. The decisions below describe the measured revision; shared and
temporal UDF coverage now defaults on under the accepted UDF performance exception.

This audit starts from canonical main `adbdafe4d125b40eeea3b995c0eb40591615e0bc`.
It compares production operators using **prebuilt BinaryRowData for Flink and prebuilt Arrow
buffers for StreamFusion**, with the same logical rows. Perimeter transposes are excluded.
The [result artifact](results/feature-admission-2026-10-19.json) retains every warmup and measured
trial, configurations, release-library hash, parity checks and CPU-profile summaries.
Earlier conversion-inclusive complete-job results remain valid at their recorded boundary.

## Decisions at the measured revision

| Performance admission switch | Result |
| --- | --- |
| `streamfusion.dropUpdateBefore.standalone.enabled` | Remains off: mixed-kind Arrow filtering still loses to Flink's row predicate. |
| `streamfusion.calc.sharedStatefulUdf.enabled` | Remains off: INT/BIGINT/STRING do not establish a repeatable advantage. |
| `streamfusion.calc.temporalUdf.enabled` | Remains off: identity, text and epoch cases still lose; a faster direct-evaluation candidate also loses. |
| `streamfusion.window.string-extrema.single-phase.enabled` | Optional override: event-time TVF TUMBLE/CUMULATE now default native; other single-phase shapes require `true`. Explicit `false` retains stock. |
| `streamfusion.window.attached.single-phase.enabled` | Supported layouts now default native after optimization. Explicit `false` retains stock; `true` matches the default. The old COUNT-only, five-slide and fixed-zone performance restrictions are removed. |

Collection and metadata expression switches already default on, so they do not restrict feature
coverage. Master/operator switches, semantic `allowIncompatible` flags, module installation and
platform-dependent I/O options retain their contracts. The direct-temporal, temporal-alias and
table-function projection experiments select alternative implementations; they do not independently
admit extra functionality and are not enabled by this audit. Existing correctness/type/connector
whitelists remain in force. Open unmerged coverage branches are outside this main-branch audit.

## Measured boundary

`FeatureAdmissionBenchmark` extracts actual generated stock and native operator factories from the
same SQL, serializes them and creates fresh instances per trial. Input preparation, operator
setup/open/close and planning are outside timing. A matching first prefix initializes each
instance before timing; for windows its equal state contribution remains in both engines.
The timed section includes `processElement`, final watermark firing for windows, intrinsic
conversions, JNI, state and synchronous consumption of all output fields in the engine's format.
The first warmup checks complete logical values and RowKinds as multisets; subsequent trials retain
counts and checksums. The benchmark requires the intended native operator in the extracted plan.

Keyed input is grouped by the stock key selector's actual key group. Both engines receive that
same order. Native batches stop at key-group boundaries, so the configured batch size is a maximum.
Attached cases measure the **outer re-aggregation** on prebuilt inner-window results; they do not
include the inner aggregate, exchange, checkpoint or full cascade. Attached fixtures cycle through
25 distinct assigned windows; `keys` and string `width` affect raw string fixtures, not these
outer results. The 30s/2s case carries wider bounds but does not execute source-side fanout
inside timing. RocksDB uses the native backend
wrapper for both engines, delegating stock state to Flink, with a fixed 64 MiB pool per slot.

Environment: Intel Core i7-12650H, 16 logical CPUs, x86-64 WSL2 Linux, JDK 17.0.20.1,
released Flink 2.2.1 and 1.18.1, release Rust with mimalloc. Test JVM heap is 2 GiB and task
off-heap budget 1 GiB. Benchmark jobs execute sequentially without concurrent compilation/tests.
Engine order alternates each trial. The broad initial matrix uses three warmups/seven samples;
confirmation uses five warmups/seven samples (first optimization pass: nine samples).
No unfavorable measured sample is discarded. These are local operator results, not Nexmark or
whole-pipeline acceleration claims. Very short scalar cases show JIT drift and overlapping ranges.

## Baseline and remaining slow paths

Baseline high-cardinality windows use 262,144 rows, 4,096 keys, 128-byte strings and maximum
1,024-row batches. Representative medians on Flink 2.2 are:

| Operation | Flink (ms) | StreamFusion (ms) | Decision |
| --- | ---: | ---: | --- |
| String TUMBLE | 98.68 | 60.50 | Admit |
| String HOP, five overlaps | 180.61 | 229.28 | Keep opt-in |
| String CUMULATE | 146.76 | 133.14 | Admit, modest margin |
| Attached SUM input, before optimization | 44.96 | 41.81 | Inconclusive before optimization |
| Attached COUNT input, fifteen overlaps, before optimization | 42.32 | 44.81 | Loses before optimization |
| Attached regional COUNT input | 57.02 | 19.43 | Admit |

String TUMBLE/CUMULATE also win at 64 keys/64-byte strings and at maximum 4,096-row batches.
On Flink 1.18 memory, their high-cardinality medians are 101.35/68.16 ms and 153.38/138.52 ms.
On Flink 2.2 RocksDB they are 123.13/105.35 ms and 268.17/255.13 ms. CUMULATE's latter
advantage is only about 5%, with overlapping ranges; this is not a uniform speedup guarantee.
The Flink 1.18 RocksDB CUMULATE control is especially variable (3.09–16.08 seconds versus
0.252–0.262 seconds native); all trials are retained and no general 18× claim is inferred.

The first optimization cohort also measures unchanged scalar/drop paths at 262,144 rows:

| Operation | Flink (ms) | StreamFusion (ms) |
| --- | ---: | ---: |
| Mixed-kind DropUpdateBefore | 8.80 | 17.01 |
| Shared stateful INT | 14.83 | 20.56 |
| Shared stateful BIGINT | 13.10 | 18.81 |
| Shared stateful STRING | 24.07 | 25.57 |

The lower-cardinality matrix's small shared INT/STRING apparent wins have substantial overlap
and drift; they do not override these losses.

## Profiles and two optimization passes

Async-profiler CPU captures use 524,288 rows, 4,096 keys, width 128 and 1 ms sampling.
Profiles cover whole test JVMs, including setup, warmup and first-warmup full-value capture.
Counts below are overlapping inclusive subtrees, **not percentages of timed execution**.
Instrumented durations are excluded from admission claims.

- DropUpdateBefore: 682 operator samples include 118 in its JNI call, 87 in Arrow export,
  85 in import and 64 in Arrow filtering. Output consumption is another 307 inclusive samples.
  The native path must gather retained variable-width values and manage Arrow ownership; Flink
  forwards accepted rows with a predicate. Excluding perimeter transposes does not remove this work.
- Scalar Calc: 2,192 pooled operator samples include 829 in the native Calc call and 694 in
  the JVM UDF callback, plus output consumption. Shared primitive evaluators already bypass the
  callback, but still read/write Arrow for each row. These simple generated row operations leave
  little compute for batching to amortize.
- Windows: 4,811 pooled samples include 2,150 in native window updates and 765 in Arrow `take`;
  variable-width `take_bytes` accounts for 624 inclusive samples. High-cardinality HOP repeatedly
  gathers noncontiguous values across overlapping groups. Fixing this requires changing aggregate
  selection/execution rather than merely removing a planner toggle.
- Attached windows: 1,328 samples include 555 in repeated local-to-epoch boundary conversion.
  The fixed-zone path needlessly constructs date/time objects even in UTC.

Pass one replaces fixed-zone boundary conversion with checked offset arithmetic. It reduces
native attached SUM-input/fifteen-overlap COUNT-input medians to 18.51/16.99 ms. A diagnostic
extension of existing direct-temporal evaluation to mixed INT/timestamp inputs also reduces
native identity/text/epoch medians from 53.72/77.94/44.17 ms to 28.32/56.80/22.44 ms in
separate JVM cohorts. Its matched stock medians are still only 15.48/43.34/14.46 ms.
That diagnostic expansion is rejected, and the existing temporal contracts remain unchanged.

Pass two retains canonical BIGINT Arrow input buffers within one allocator root instead of
copying them rowwise for attached aggregation. Different allocator roots still copy. Fixed-zone
boundary arithmetic accounts for most of the combined improvement; the additional small retention
change is not claimed as an isolated causal speedup. NULLs, duplicated aggregate arguments,
retained aliases, cross-root copies, fixed/regional zones and checkpoint/recovery are checked.

Final attached medians for 262,144 prebuilt outer-input rows and 25 assigned windows:

| Outer input | Flink 2.2 memory (ms) | Native memory (ms) | Flink 2.2 RocksDB (ms) | Native RocksDB (ms) | Flink 1.18 RocksDB (ms) | Native RocksDB (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SUM | 42.32 | 17.55 | 41.50 | 35.69 | 40.59 | 29.83 |
| COUNT, 30s/2s bounds | 37.01 | 16.50 | 38.38 | 30.32 | 40.27 | 29.65 |
| Regional COUNT | 45.72 | 19.42 | 43.53 | 32.30 | 47.37 | 33.49 |

This removes the attached performance whitelist. It does not invalidate prior slower complete
cascades or close their remaining routing/state costs in [#394](https://github.com/datafusion-contrib/StreamFusion/issues/394)
and [#350](https://github.com/datafusion-contrib/StreamFusion/issues/350).
Shared stateful and temporal investigations shipped through [#398](https://github.com/datafusion-contrib/StreamFusion/issues/398)
and [#389](https://github.com/datafusion-contrib/StreamFusion/issues/389); their measured limits remain documented. Repeated C Data/ownership
work was investigated in [#248](https://github.com/datafusion-contrib/StreamFusion/issues/248);
its [output-schema optimization and deferred scope](../optimizations/calc-schema-once-c-data.md#cache-calc-output-schemas)
do not establish a new result for the historical workloads above.

## Reproduction

Build the release native library and Java tests using JDK 17, then run from the repository root:

```sh
mvn -pl streamfusion-runtime -am test-compile -Pbench -DskipTests
SF_BENCHMARK=true mvn -pl streamfusion-runtime surefire:test -Pbench \
  -Dtest=FeatureAdmissionBenchmark -Dsf.testForks=1 \
  -Dsf.extraJvmArgs=-Xmx2g \
  -Dadmission.rows=262144 -Dadmission.batchRows=1024 \
  -Dadmission.keys=4096 -Dadmission.width=128 \
  -Dadmission.warmup=5 -Dadmission.runs=7
```

Use `-Pbench,flink-1.18` on both commands for Flink 1.18 and
`-Dadmission.backend=rocksdb` for persistent state. `-Dadmission.cases=string-tumble,string-hop`
selects cases. All five original opt-ins are deliberately enabled in fixture plans so protected
paths can be measured; default admission and explicit overrides are checked in the SQL suites.


## Validation

The affected SQL, operator ownership, fixed-offset, checkpoint/recovery, temporal precision,
shared AVG and DISTINCT/split-DISTINCT selections pass 280 checks on Flink 2.2.1.
Flink 1.18.1 passes 273 with seven existing release-capability skips. Earlier assertions that
expected the removed performance fallback were updated to require native execution and exact
host results. Explicit false overrides and remaining HOP/filter fallback checks remain tested.
The strict documentation build and `git diff --check` also pass.
