# TPC-H batch workload

All 22 queries execute through native compute and the native Parquet source with
exact stock-Flink results and types. The full SF1 release comparison has a **2.814×
geometric-mean speedup**, with all 22 query medians faster than stock. Both supported
Flink lines pass all-22 source/result validation at SF0.01. The new comparison uses
the same files, SQL and resources and retains complete-job timing. Earlier cohorts,
including regressions and memory failures, remain below. These are local shared-host,
warm-cache measurements, not official TPC-H throughput results.

## Fixed workload

The opt-in runtime `tpch` profile uses released `io.airlift.tpch:tpch:0.10`, the
same generator used by Flink's TPC-H end-to-end test. Queries come directly from
that JAR except Q6/Q11/Q15/Q20, which use the unchanged SQL adaptations shipped in
Flink 2.2.1. [Query provenance](results/tpch-query-provenance-20261109.json)
records their exact source URLs and hashes. Both engines receive identical SQL;
there are no engine-specific query rewrites.

Identifiers use BIGINT, integer fields INT, dates DATE and strings STRING.
Numeric fields use the standard DECIMAL(15,2) representation for exact financial
arithmetic, rather than the DOUBLE mapping in Flink's small e2e CSV example.
Both engines use this same schema. The generator streams all eight tables into
pipe-delimited CSV, removing only the final field separator. A manifest records
each file's row count, byte length and SHA-256, checked before reuse.

The CSV cohorts retain the same stock Flink reader and collecting sink, including
both perimeter conversions. The initial Parquet cohorts below also retain the stock
reader. The native-Parquet follow-up instead uses each engine's reader on the same
files: stock Flink's ordinary Parquet source versus StreamFusion's existing native
decoder under Flink's file-source machinery. It retains the same collecting sink
and Arrow-to-row exit; internal task transport remains Arrow IPC. Source decoding,
I/O and any conversions required by that engine remain inside timing.

## Modes and timing

`TpchBenchmark` is opt-in through `SF_TPCH=true` and `-Ptpch`:

- `sf.tpch.mode=audit` executes every selected stock query and captures stock/native
  plans and admission reasons. Its single cold timings are diagnostics only.
- `verify` executes both engines once, requires a native plan and positive native
  consumption, and checks exact result rows and declared result types. Debug native
  builds are suitable for this correctness mode.
- `compare` additionally requires Maven's `bench` profile and a release native DSO.
  It alternates engine order, defaults to three warmups and seven retained trials,
  and refuses silent fallback or result differences.

Timing begins before `executeSql` and ends after draining the result iterator and
fetching the completed `JobExecutionResult`. Planning, submission, filesystem
scans and decoding, intrinsic JNI, required perimeter conversions, shuffles, native operators and
sink/job completion remain inside timing. Both engines use the same MiniCluster,
parallelism, native off-heap authority and 1 GiB managed-memory setting. Adaptive
automatic parallelism and range sorting are disabled for both. Native work is
joined to the actual job ID, and stock runs must report zero native work.

## Running

Choose a task-scoped working directory and pass it as `sf.tpch.directory`.
For example, use `mvn -pl streamfusion-runtime -am test -Ptpch -Dtest=TpchBenchmark`
with `SF_TPCH=true`, `sf.tpch.mode=audit` and `sf.tpch.scale=0.01`.
Supply the native build directory through `native.resource.directory` and through
`java.library.path` in `sf.extraJvmArgs`. Point `java.io.tmpdir` there as well.
Use `sf.tpch.queries=1,6` to select queries; omit it for all 22. Other parameters are
`.parallelism` (2), `.nativeBytes` (536870912), `.warmups` (3) and `.trials` (7).
Parquet comparisons enable `.fileSource=true` and require positive job-specific
`sourceArrowRows` by default. The optional Parquet native library must be on
`java.library.path`. Set `.fileSource=false` to reproduce the stock-reader compute
baseline; no native-source credit is then claimed.
Add `bench` and supply a release DSO for comparisons; never derive speedups from
an audit or debug run. Reports are written after every query so a later failure
does not erase earlier observations. Generated data and raw plans belong in the
task directory and should be removed after concise evidence has been extracted.

A complete comparison using the repository's ordinary release build can be run
with the following shell command. It saves the report and removes generated data
on exit; use the project's supported JDK/Maven toolchain.

```sh
sf_tpch_work=$(mktemp -d "${TMPDIR:-/tmp}/streamfusion-tpch.XXXXXX")
trap 'rm -rf "$sf_tpch_work"' EXIT
SF_TPCH=true SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test \
  -Ptpch,bench -Dtest=TpchBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  "-Dsf.extraJvmArgs=-Dsf.tpch.directory=$sf_tpch_work -Djava.io.tmpdir=$sf_tpch_work -Dsf.tpch.mode=compare -Dsf.tpch.scale=1 -Dsf.tpch.format=parquet"
cp "$sf_tpch_work/report-parquet-compare.json" tpch-report.json
```

## Implementation gaps

The initial plans required decimal SUM/AVG, string/date/decimal ordering and grouping,
hash and nested-loop joins including semi/anti/outer forms, broadcast exchanges,
auxiliary grouping, reusable subplans and grouped SortAgg. A first fallback reason
can hide additional unsupported nodes; the full stock plans remain the implementation
checklist. Existing Flink-compatible decimal accumulators can be adapted into
DataFusion aggregation rather than introducing another state store.

## First native query: Q6

Q6 now has a verified native Calc, local/final decimal SUM and blocking singleton
Arrow exchange, with identical declared DECIMAL(38,4) output and exact rows at
scale factors 0.01 and 1. The adapter uses the existing Flink-compatible decimal
accumulator inside DataFusion's aggregate operator. It preserves NULL defaults,
overflow-to-NULL followed by reset on the next value, and host partial-state fields;
a native control checks empty/all-NULL input and overflow/merge behavior.

The scale-factor-one CSV comparison uses six million lineitems, three warmups and
seven alternating retained trials. Stock median is **4,838.59 ms**
[4,831.10–5,034.56]; native is **4,831.75 ms** [4,825.53–4,946.13], a neutral
1.001× ratio. This does not establish a complete-job acceleration or complete the
22-query goal. [Raw trials, data identities and native work](results/tpch-q6-20261109.json)
retain this unfavorable/neutral result rather than dropping the source/conversion costs.

Separate [JFR observations](results/tpch-q6-profile-20261109.json) show prominent
CSV/date parsing and row-to-Arrow writing; they do not measure isolated Rust
subroutine time. Verification mode accepts `sf.tpch.profile=true` to write stock
and native recordings in the task directory. Comparison mode rejects profiling,
so instrumented timings cannot silently enter the speedup table. Raw recordings
are removed after retaining their concise observations.

## Q1 coverage

Q1 now also verifies exact stock results and positive native execution at scale
factor 0.01, using the original query and the same CSV tables. It uses typed hash
grouping, existing decimal SUM/AVG accumulators, blocking Arrow exchanges and
DataFusion sorting. Focused SQL covers ONE_PHASE/TWO_PHASE, NULL groups,
Unicode keys, decimal/date/boolean composite keys and optimizer-shared decimal
SUM/AVG overflow. Q1 performance measurement is separate from this correctness
evidence. Joins and the other remaining operators still prevent full-workload
admission; the 22-query goal remains incomplete.

Q1's scale-factor-one release comparison uses the same six-million-row CSV
lineitem source, three warmups and seven alternating retained trials: stock median
**5,089.59 ms** [5,076.79–5,114.66], native **4,819.07 ms**
[4,725.65–4,849.68], **1.056×**. This is a modest complete-job gain; it does
not establish full TPC-H acceleration. [Trials and execution witnesses](results/tpch-q1-20261109.json).
The separate [typed validation evidence](results/tpch-typed-validation-20261109.json)
retains both-line SQL and unchanged upstream execution contracts.

## Intermediate join coverage

After adding the bounded DataFusion hash-join candidate, the all-22-query
scale-factor-0.01 verification passes exact results, declared schemas and positive
native work for **Q1, Q3, Q4, Q6, Q10, Q12, Q13, Q14 and Q18**. The other
queries still fail the required native-coverage assertion and remain explicit
gaps. [Coverage evidence](results/tpch-join-coverage-20261109.json) keeps each
query's first fallback; it includes no performance ratios from these debug runs.
Grouped SortAgg now passes the released upstream controls; Q5/Q7/Q8/Q22 then
reach a later nested-loop join. Remaining blockers include nested-loop joins and
join residual predicates, with additional gaps potentially hidden behind the first fallback.

## Matched Parquet cohort

`sf.tpch.format=parquet` uses stock Flink to write the generated logical tables
once as SNAPPY Parquet before query timing. Both engines then scan those same
files through Flink's ordinary filesystem/Parquet source and use the same collecting
sink. SQL, resources, warmups, alternating trials and complete-job timing remain
identical. Row/Arrow perimeter conversions remain in StreamFusion's measured path.
The manifest retains the source CSV identity, preparation job IDs, writer parallelism
and every Parquet file's size and SHA-256; cache reuse verifies the file identities.
This is an additional storage cohort, with the CSV results retained separately.
Format comparisons may also reflect the stock readers' different split/parallelism
behavior; compare engines within a cohort. Parquet reports use a separate filename.

Q3's scale-factor-one CSV release median is **5,486.57 ms** stock versus
**5,468.48 ms** native (1.003×), another neutral complete-job result.
[Retained trials and job-ID native work](results/tpch-q3-20261109.json) preserve
this result alongside Q1 and Q6; coverage alone does not establish acceleration.

The first Parquet release comparison at 1,024-row Arrow batches gave Q1
**4,308.51 / 1,868.38 ms** stock/native (**2.306×**), while Q3 regressed:
**1,152.00 / 1,351.58 ms** (**0.852×**).
[All retained trials](results/tpch-parquet-20261109.json) include the regression.
A separate [JFR observation](results/tpch-q3-profile-20261109.json) led to a
batch-size pass. With 8,192-row batches Q3 measured **1,254.80 / 1,029.47 ms**
(**1.219×**) with the same query/files/resources and both transposes.
[Batch-size trials](results/tpch-q3-batch8192-20261109.json) preserve both engines'
variability. Bounded pipelines now prefer 8,192 rows, matching Comet's default;
explicit table/JVM batch settings remain authoritative. These are per-query
comparisons, with no complete-TPC-H aggregate ratio yet.

The subsequent default-setting confirmation, with no table/JVM batch override,
measured Q1 **5,581.40 / 1,932.03 ms** (**2.889×**) and Q3
**1,569.09 / 1,465.17 ms** (**1.071×**).
[Default-setting raw trials](results/tpch-parquet-default8192-20261109.json)
retain wide shared-host variability: Q1 stock 4,905.01–7,493.17 ms/native
1,754.95–2,536.44; Q3 stock 1,477.97–1,696.51/native 1,321.17–1,548.11.
These cohorts should be assessed separately rather than combining their medians.
[Both-line SortAgg and memory controls](results/tpch-sortagg-validation-20261109.json)
record the released fixture contracts and typed/native operator checks.

## Complete compute coverage

The scale-factor-0.01 Flink 2.2 run now verifies **all 22 queries** with exact
rows, declared output types and positive native work.
[Complete correctness evidence](results/tpch-all22-validation-20261109.json)
contains no performance ratios from its debug timings. Nested-loop joins and
join residuals reuse DataFusion's existing operators and expression compiler.
Q11's scalar guard uses a planner proof that its global-aggregate input has at
most one row; unproven cardinalities retain Flink's ordinary error.

Result validation retains exact row values and multiplicities and preserves the
ORDER BY key sequence. Rows within equal-key groups may appear in either order,
as SQL allows; normalization occurs after timing. Query SQL is unchanged.
LIMIT results still require the same exact selected rows.

## Scale-factor-one full-workload attempt

The first release run attempted all 22 Parquet queries with 8,192-row batches,
three warmups, seven alternating retained trials and the original 512 MiB native
limit. Twenty-one queries passed exact results and positive native work. Q9 failed
its first native warmup on a build-side allocation after the host plan introduced
a Cartesian intermediate; this incomplete run has no full-workload speedup.
[Retained trials and failure](results/tpch-full-parquet-512m-20261109.json).

| Query | Stock median ms | Native median ms | Stock/native |
| --- | ---: | ---: | ---: |
| Q1 | 4,084.16 | 1,629.22 | 2.507× |
| Q2 | 3,273.34 | 3,874.62 | 0.845× |
| Q3 | 1,148.69 | 1,118.17 | 1.027× |
| Q4 | 927.63 | 694.26 | 1.336× |
| Q5 | 1,902.45 | 1,343.84 | 1.416× |
| Q6 | 683.20 | 782.14 | 0.874× |
| Q7 | 1,930.98 | 1,567.77 | 1.232× |
| Q8 | 5,235.89 | 2,024.78 | 2.586× |
| Q9 | — | memory limit | — |
| Q10 | 1,048.35 | 1,134.41 | 0.924× |
| Q11 | 613.49 | 490.82 | 1.250× |
| Q12 | 873.50 | 993.98 | 0.879× |
| Q13 | 908.07 | 567.98 | 1.599× |
| Q14 | 565.46 | 780.54 | 0.724× |
| Q15 | 587.24 | 1,393.73 | 0.421× |
| Q16 | 630.28 | 405.91 | 1.553× |
| Q17 | 1,602.09 | 1,561.19 | 1.026× |
| Q18 | 3,614.82 | 2,294.57 | 1.575× |
| Q19 | 1,677.83 | 1,555.65 | 1.079× |
| Q20 | 1,501.40 | 1,460.38 | 1.028× |
| Q21 | 5,593.99 | 3,011.80 | 1.857× |
| Q22 | 2,345.58 | 1,250.98 | 1.875× |

Ratios below one are regressions, not acceleration. These remain open optimization
work, including Q2, Q6, Q10, Q12, Q14 and Q15. All timings include stock readers,
both transposes, JNI, blocking Arrow shuffles, collecting sinks and job completion.
The candidate remains local and the acceleration goal remains incomplete.

## Cartesian elimination

Following DataFusion’s `eliminate_cross_join`, the native planner can reassociate
a verified inner `(A × B) JOIN C` into linked `C JOIN A JOIN B` joins when
both dimensions have equality links to C and their estimated sizes do not exceed C.
It restores the original column order and distribution and declines explicit join
hints, residuals, null-safe keys and ordered joins. SQL text stays unchanged.
This avoids materializing Q9’s Cartesian intermediate without increasing memory.
The stock comparison keeps its ordinary Flink plan.

The focused Q9 rerun passed exact results and native-work checks on every run:
stock median **27,230.35 ms**, native **1,919.77 ms**, **14.184×**. Variability was
wide (stock 25,045.78–87,225.29 ms; native 1,806.83–8,598.27 ms) on the shared
development host. [Raw trials](results/tpch-q9-reordered-20261110.json) are a
separate cohort after the planner change; they are not substituted into a claimed
single complete-workload measurement.

## Repeated regression checks after planner reuse

A separate six-query release cohort repeats the previously regressing queries
after Cartesian elimination and preserving host subplan reuse. Q2's native plan
no longer materializes a Cartesian join. Q15 now computes its revenue aggregate
once and uses the existing retained-view Arrow share operator for its two consumers.
Both paths retain identical SQL, files, resource limits and full job boundaries.

| Query | Stock median ms | Native median ms | Stock/native |
| --- | ---: | ---: | ---: |
| Q2 | 3,427.85 | 535.95 | 6.396× |
| Q6 | 571.23 | 777.76 | 0.734× |
| Q10 | 1,153.29 | 1,134.64 | 1.016× |
| Q12 | 906.49 | 1,005.63 | 0.901× |
| Q14 | 582.63 | 923.92 | 0.631× |
| Q15 | 596.21 | 794.67 | 0.750× |

[All raw trials and native work](results/tpch-subplan-reuse-20261110.json) retain
these remaining regressions. Q15 improves relative to its previous 1,393.73 ms
native median, but still loses to stock. Q10 is approximately neutral in this
cohort. Q6/Q12/Q14/Q15 remain open performance work; no full-workload aggregate
ratio is inferred by combining these separate cohorts.

## Final compatibility checkpoint

After Cartesian elimination and aggregate sharing, fresh Parquet scale-factor-0.01
runs pass **all 22 queries on both Flink 2.2 and 1.18**, requiring exact rows,
declared result types and positive native work for every query.
[2.2 evidence](results/tpch-final-validation-22-20261110.json) and
[1.18 evidence](results/tpch-final-validation-118-20261110.json) retain each job's
identity and consumption. Additional controls pass on both lines: all four
unchanged upstream outer-join variants, duplicate/NULL Cartesian reassociation,
all four reuse-switch settings, SQL ordering ties and join-task replay. The
packaged isolated planner also passes grouped aggregation and two-input join
controls on both lines.

Separate [release JFR observations](results/tpch-remaining-profiles-20261110.json)
show row-to-Arrow writing/decimal conversion and some JVM expression callbacks in
the remaining slow cases. The recordings are short, include blocked/background
threads and cannot resolve Rust-internal hotspots; these observations are not CPU
percentages or performance ratios. Raw recordings were removed after summarization.

## Complete release comparison

The final single-cohort scale-factor-one run passes all 22 queries, with exact
stock results/types and positive native work on every warmup and retained trial.
It uses the same SNAPPY Parquet files, parallelism two, 8 GiB JVM heap, 1 GiB
managed memory, 512 MiB native authority, 8,192-row native batches, three warmups
and seven alternating retained trials. All production boundaries remain timed.

The **geometric mean of per-query median speedups is 1.666×**. The sum of stock
query medians is **97,516.93 ms**, versus **32,740.18 ms** native (**2.979×**).
The latter is a sum of query medians, not a separately timed serial suite; Q9
accounts for about half the stock total. Nineteen query medians favor native,
while **Q6, Q14 and Q15 remain slower**. Ratios near one should be treated as
neutral given shared-host variability. Neither aggregate is official TPC-H QphH.
[All trials, variability, data identities, build identity and job witnesses](results/tpch-full-parquet-final-20261110.json).

| Query | Stock median ms | Native median ms | Stock/native |
| --- | ---: | ---: | ---: |
| Q1 | 4,194.20 | 1,638.52 | 2.560× |
| Q2 | 3,298.42 | 519.52 | 6.349× |
| Q3 | 1,232.70 | 1,118.90 | 1.102× |
| Q4 | 1,014.50 | 790.96 | 1.283× |
| Q5 | 2,031.42 | 1,342.16 | 1.514× |
| Q6 | 777.21 | 880.26 | 0.883× |
| Q7 | 1,900.84 | 1,554.83 | 1.223× |
| Q8 | 5,292.75 | 1,478.78 | 3.579× |
| Q9 | 49,516.42 | 2,557.19 | 19.364× |
| Q10 | 1,824.13 | 1,567.88 | 1.163× |
| Q11 | 851.19 | 611.68 | 1.392× |
| Q12 | 4,361.65 | 4,119.05 | 1.059× |
| Q13 | 4,076.56 | 2,403.20 | 1.696× |
| Q14 | 564.43 | 873.93 | 0.646× |
| Q15 | 689.23 | 876.54 | 0.786× |
| Q16 | 641.96 | 436.50 | 1.471× |
| Q17 | 2,738.19 | 1,695.91 | 1.615× |
| Q18 | 3,957.70 | 2,344.33 | 1.688× |
| Q19 | 1,683.29 | 1,653.36 | 1.018× |
| Q20 | 1,449.38 | 1,407.10 | 1.030× |
| Q21 | 4,434.76 | 2,491.39 | 1.780× |
| Q22 | 985.99 | 378.18 | 2.607× |

The developer host had background builds and changing load. Q1 includes a slow
initial retained trial; it was not dropped. CPU frequency and competing workloads
were not pinned, so this is provisional local complete-job evidence, not an
isolated throughput claim. Earlier cohorts and their failures/regressions remain
above. Q12's median favors native in this run but lost in the prior focused
cohort; that inconsistency is retained rather than treated as a resolved optimization.

Compute compatibility and the complete comparison are established. The goal of
accelerating every query remains open: Q6/Q14/Q15 still require optimization
before this candidate can be treated as completed acceleration or shipped as a
known-slower default. All work remains local.

## Native DATE predicate follow-up

EXPLAIN now distinguishes registered generated-expression evaluators from named
UDF calls. It exposed two scalar Java bridges in each of Q6/Q14/Q15's date-range
filters despite an empty `jvmFunctions` list. Direct DATE column/literal comparisons
now reuse DataFusion's existing Date32 comparison expressions. The resulting
plans contain no registered JVM evaluators for these three queries; memory
authority, Arrow shuffles, source/sink formats and query text are unchanged.

A separate SF1 release cohort with the original transpose sampler, three warmups
and seven alternating trials measured:

| Query | Stock median ms | Native median ms | Stock/native |
| --- | ---: | ---: | ---: |
| Q6 | 584.19 | 684.65 | 0.853× |
| Q14 | 685.04 | 868.95 | 0.788× |
| Q15 | 607.20 | 691.03 | 0.879× |

[Trials and witnesses](results/tpch-date-comparison-20261110.json) retain these
regressions. Removing callbacks does not establish complete-job acceleration;
the remaining row-to-Arrow boundary still needs work. This focused cohort does
not replace any query in the earlier complete-cohort aggregate.

A block-based conversion-timing sampler was tested and reverted after inconsistent
results across three cohorts. [All sampler trials](results/tpch-rejected-timing-sampler-20261110.json)
remain available; it is not enabled or described as an optimization.

Both-line all-22 validation after the DATE change is retained separately:
[2.2](results/tpch-date-validation-22-20261110.json),
[1.18](results/tpch-date-validation-118-20261110.json). The
[unchanged upstream date oracles](results/tpch-date-upstream-validation-20261110.json)
pass their expected stock-fallback contracts; the native DATE control tests its
own admitted pipeline and does not borrow native credit from these fixtures.

## Native Parquet source follow-up

The native source reuses the released Arrow/parquet-rs decoder already used by
StreamFusion's Paimon path. It reads through Flink's filesystem API and emits Arrow
batches directly, eliminating the source-side row materialization and transpose.
The stock engine reads the identical SNAPPY Parquet files through its ordinary
reader. SQL, output collection, resources, warmups and completed-job timing match.
Both engines retain their normal projection behavior. Native file splits are
whole-file splits; stock Flink retains its own splitting behavior. The local OS
cache is not flushed between trials, so these are warm-cache local-file jobs,
not remote-storage or cold-read throughput results.

A focused SF1 release cohort (three warmups, seven alternating trials) measured:

| Query | Stock median ms | Native median ms | Stock/native |
| --- | ---: | ---: | ---: |
| Q6 | 689.94 | 334.08 | 2.065× |
| Q14 | 601.05 | 339.47 | 1.771× |
| Q15 | 584.67 | 345.59 | 1.692× |

[All trials and source witnesses](results/tpch-native-parquet-focused-20261110.json).
This focused cohort does not replace queries in the earlier complete comparison.
The two decoded-row batching prototypes were removed after failing to establish
acceleration; their [negative evidence](results/tpch-rejected-row-source-batching-20261110.json)
remains separate from native decoding.

The [first native-source full SF1 attempt](results/tpch-native-parquet-first-full-20261110.json)
is incomplete: multiple narrow scans each reserved the full row-group width,
including unprojected column chunks, and exceeded the shared 512 MiB authority.
The decoder now reserves projected chunks only; the memory budget is unchanged.
That failure is retained and contributes no complete-workload aggregate.

Both-line all-22 source validation at SF0.01
([2.2](results/tpch-native-parquet-validation-22-20261110.json),
[1.18](results/tpch-native-parquet-validation-118-20261110.json))
requires exact stock result values/types and positive native-source work for every
query. Source unit controls cover projection and UTF-8/decimal NULLs, empty files,
logical-row checkpoint restoration with a changed batch size, memory denial and
prompt release of canceled queued batches.

## Complete native-Parquet SF1 release comparison

After projected workspace accounting and queued-buffer cleanup, a fresh complete
cohort passed all 22 queries. Every warmup and retained trial checks exact stock
results/types, positive native compute and positive job-specific native source rows.
The geometric mean of per-query median stock/native ratios is **3.0278×**.
All **22 of 22** query medians favor StreamFusion. The sum of stock query medians
is **69,827.69 ms**, versus **14,744.03 ms** native (**4.7360×**); this ratio is not
a separately timed serial-suite measurement, and Q9 dominates the stock total.

| Query | Stock median ms | Native median ms | Stock/native |
| --- | ---: | ---: | ---: |
| Q1 | 3,990.42 | 762.73 | 5.232× |
| Q2 | 3,251.18 | 387.06 | 8.400× |
| Q3 | 1,096.52 | 446.54 | 2.456× |
| Q4 | 875.28 | 337.66 | 2.592× |
| Q5 | 1,859.33 | 775.59 | 2.397× |
| Q6 | 550.48 | 324.60 | 1.696× |
| Q7 | 1,849.33 | 980.69 | 1.886× |
| Q8 | 5,118.05 | 888.86 | 5.758× |
| Q9 | 27,580.24 | 1,228.01 | 22.459× |
| Q10 | 1,246.16 | 469.65 | 2.653× |
| Q11 | 748.59 | 369.33 | 2.027× |
| Q12 | 838.07 | 345.99 | 2.422× |
| Q13 | 888.47 | 347.28 | 2.558× |
| Q14 | 476.87 | 337.38 | 1.413× |
| Q15 | 594.00 | 343.46 | 1.729× |
| Q16 | 622.47 | 266.49 | 2.336× |
| Q17 | 3,237.21 | 1,235.75 | 2.620× |
| Q18 | 4,525.11 | 1,440.12 | 3.142× |
| Q19 | 2,140.39 | 581.39 | 3.682× |
| Q20 | 1,784.70 | 689.67 | 2.588× |
| Q21 | 5,760.59 | 1,930.69 | 2.984× |
| Q22 | 794.21 | 255.11 | 3.113× |

[Complete trials, variability, input hashes and native-source/operator witnesses](results/tpch-native-parquet-final-20261110.json)
retain every observation, including Q11's slower native outlier. Both engines read
the same SF1 SNAPPY Parquet files at parallelism two, with 8 GiB heap, 1 GiB
managed memory and the same 512 MiB native authority. Native batches use 8,192 rows;
stock keeps its ordinary reader settings. There are three warmups and seven
alternating retained trials per engine. All scanning/decoding, required conversions,
JNI, blocking Arrow transport, collection and completed-job retrieval remain timed.
No source transpose is charged to the native path because the reader already emits
Arrow; its decoding and C Data import are timed instead.

The shared macOS developer host had changing load and intermittent pauses; CPU and
storage caches were not isolated. Q19–Q22 stock timings vary more than earlier
queries. File count bounds native source parallelism because its splits currently
cover whole files. This comparison establishes a local complete-job gain for this
cohort; it is not a remote-storage, cold-cache or official TPC-H QphH claim.

A later post-rebase system-allocator diagnostic was interrupted after finding that
its manual release build omitted the mimalloc feature selected by Maven's `bench`
profile. Its [completed query trials](results/tpch-native-parquet-system-allocator-diagnostic-20261110.json)
remain a separate partial diagnostic and contribute no complete-workload aggregate.
The production-profile follow-up explicitly builds both core and Parquet with
`--release --features mimalloc` and records both library hashes.

## Review-head production-profile comparison

The final comparison uses the rebased source at `9d25c8047` with **both core and
Parquet built using `--release --features mimalloc`**, matching Maven's `bench`
profile. All 22 queries pass exact stock values/types and positive native compute
and source work on every warmup and retained trial. A direct, job-specific
`RowDataToArrow.numInputRows` observer additionally requires **zero entry-transpose
rows**. Its stock-reader sensitivity control observes all 2,051 converted rows,
so zero is not inferred from the native-only operator list.

The geometric mean of per-query median speedups is **2.8137×**, with **22/22 median
wins**. Summed query medians are **64,430.12 ms stock / 13,820.95 ms native**,
a **4.6618×** ratio. This is a sum of medians, not a separately timed serial suite.

| Query | Stock median ms | Native median ms | Stock/native |
| --- | ---: | ---: | ---: |
| Q1 | 3,986.68 | 688.34 | 5.792× |
| Q2 | 3,244.40 | 392.36 | 8.269× |
| Q3 | 1,153.21 | 466.48 | 2.472× |
| Q4 | 929.01 | 352.83 | 2.633× |
| Q5 | 2,018.87 | 803.98 | 2.511× |
| Q6 | 666.60 | 337.58 | 1.975× |
| Q7 | 2,054.21 | 1,028.37 | 1.998× |
| Q8 | 5,163.44 | 915.98 | 5.637× |
| Q9 | 28,433.32 | 1,216.67 | 23.370× |
| Q10 | 1,117.23 | 448.45 | 2.491× |
| Q11 | 591.32 | 352.03 | 1.680× |
| Q12 | 879.34 | 332.37 | 2.646× |
| Q13 | 881.93 | 337.69 | 2.612× |
| Q14 | 559.21 | 327.98 | 1.705× |
| Q15 | 571.88 | 336.87 | 1.698× |
| Q16 | 586.95 | 246.51 | 2.381× |
| Q17 | 1,622.90 | 1,176.36 | 1.380× |
| Q18 | 2,997.45 | 1,390.20 | 2.156× |
| Q19 | 1,207.95 | 545.67 | 2.214× |
| Q20 | 1,020.39 | 558.73 | 1.826× |
| Q21 | 4,135.64 | 1,323.53 | 3.125× |
| Q22 | 608.20 | 241.96 | 2.514× |

[Full production-profile trials, source/transpose witnesses and library identities](results/tpch-native-parquet-mimalloc-final-20261110.json)
retain all variability. The earlier 3.0278× complete cohort predates the upstream
rebase and has its own identities; it is not combined with this result. Neither
cohort supports attributing a difference between them solely to the allocator.
Both are local, warm-cache, shared-host jobs with the same SF1 files, unchanged
SQL, parallelism two, 8 GiB heap, 1 GiB managed memory, 512 MiB native authority,
three warmups and seven alternating trials per engine. All I/O/decoding, intrinsic
conversions/JNI, blocking transport, result collection and completed-job retrieval
remain timed. These are not isolated cold/remote-storage or official TPC-H results.

The same mimalloc release libraries also pass fresh all-22 exact/native-source/
zero-entry controls at SF0.01 on [Flink 2.2](results/tpch-native-parquet-mimalloc-validation-22-20261110.json)
and [Flink 1.18](results/tpch-native-parquet-mimalloc-validation-118-20261110.json),
including source queue/restore/memory tests and option/metadata/retry controls.

## Validation after batch boundary fixes (November 10)

The complete SF1 native-Parquet comparison was repeated on source equivalent to `996e2d65f` after preserving zero-column Calc cardinality and keeping host multiple-input fusion outside all Arrow batch tasks. The same unchanged 22 queries, stock-written SNAPPY files, parallelism 2, one TaskManager with eight slots, 8 GiB heap, 1 GiB managed memory, 512 MiB native authority, 8,192-row batches, three warmups and seven alternating retained trials were used. The production native libraries use mimalloc and retain default RocksDB support.

All **22/22** queries passed exact declared-type and result comparisons, with **22/22 median wins** and a **2.768× geometric-mean speedup**. All 440 observations were retained. Every native run recorded source Arrow rows and zero direct entry-transpose rows; stock runs recorded zero native work. Sums of individual query medians were 65,151.125 ms for stock and 14,138.005 ms for StreamFusion (4.608×); these sums are not a timed serial-suite measurement.

The [complete report](results/tpch-native-parquet-post-ci-20261110.json) includes every observation, result/schema checks, plans, input hashes and native library identities. This is a new complete cohort; the earlier 2.814× cohort remains attached to its measured build.

| Query | Stock median ms | StreamFusion median ms | Speedup |
|---|---:|---:|---:|
| Q1 | 4505.665 | 778.667 | 5.786× |
| Q2 | 3105.616 | 391.684 | 7.929× |
| Q3 | 1132.748 | 462.062 | 2.452× |
| Q4 | 910.487 | 350.072 | 2.601× |
| Q5 | 2002.892 | 800.211 | 2.503× |
| Q6 | 671.468 | 336.954 | 1.993× |
| Q7 | 1894.468 | 1010.596 | 1.875× |
| Q8 | 6223.237 | 923.943 | 6.736× |
| Q9 | 27849.047 | 1355.310 | 20.548× |
| Q10 | 1119.608 | 459.200 | 2.438× |
| Q11 | 602.135 | 356.450 | 1.689× |
| Q12 | 790.352 | 345.881 | 2.285× |
| Q13 | 798.139 | 346.646 | 2.302× |
| Q14 | 567.835 | 338.781 | 1.676× |
| Q15 | 583.124 | 344.870 | 1.691× |
| Q16 | 492.787 | 246.628 | 1.998× |
| Q17 | 1574.815 | 1198.477 | 1.314× |
| Q18 | 3604.844 | 1548.761 | 2.328× |
| Q19 | 1210.187 | 550.678 | 2.198× |
| Q20 | 1138.039 | 556.070 | 2.047× |
| Q21 | 3735.138 | 1197.102 | 3.120× |
| Q22 | 638.492 | 238.961 | 2.672× |
