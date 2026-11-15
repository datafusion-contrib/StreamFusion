# Collection aggregates

The sections before [Rust collection state](#rust-collection-state) retain the measured history
of the preceding generated JVM collection handler. The default verified COLLECT/ARRAY_AGG route now uses
[Rust collection state](../operators/group-by.md#integer-collect); those historical timings
must not be read as measurements of the replacement kernel.

## Integer COLLECT

COLLECT(INT/BIGINT) reuses the released Flink handler and managed MapViews inside the existing
columnar JVM grouped operator. Scalar inputs and non-character aggregate result fields are
required; character grouping keys and the TINYINT/SMALLINT extension are covered below. Other
COLLECT types retain fallback. Results require a verified preserving Calc
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
RAW LinkedList accumulators. Scalar inputs and non-character aggregate result fields are required; character grouping keys
are covered below.
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

## Character grouping keys

Integer COLLECT and, on released Flink 2.2, integer ARRAY_AGG now admit STRING/VARCHAR and
CHAR grouping keys through the existing serialized-key boundary. Character aggregate results
and unverified collection consumers retain fallback. [Coverage](../operators/group-by.md#character-grouping-keys)
records the runtime parity, Unicode/collision, parallel and repeated-recovery checks. The broad
regression matrix passes 233 checks on Flink 2.2.1 and 192 with 41 version skips on 1.18.1.

The same release library, host and complete-job boundary above are retained. COLLECT uses
200,000 rows and ARRAY_AGG 50,000; both use BIGINT values with the same eight-value domain and
every-eighth-value NULL. STRING keys have 16 prefix characters at 64 groups and 264 at 4096,
starting with `é`, padded with `x`, then decimal key digits. Each engine has two warmups and
three alternating trials per fresh JVM, parallelism one, an 8 GiB heap and no accumulator cache.
Previous candidate `1296aa62` uses identical benchmark source and verified stock fallback;
current jobs require consumed NativeJvmGroup work. Retained revision `435a8922` differs from
the measured prior candidate only in diagnostic parser code/tests and benchmark documentation;
its production and benchmark Java/native code is identical. No local tests/builds overlap timings.

Median seconds (minimum–maximum), including planning, source/sink, keyed state and both transposes:

| Shape | Groups | Paired stock | Previous fallback | Native route |
| --- | ---: | ---: | ---: | ---: |
| COLLECT | 64 | 0.418451 (0.343207–0.439635) | 0.470467 (0.332164–0.592387) | 0.442228 (0.369217–0.507034) |
| COLLECT | 4096 | 0.697764 (0.557432–0.710529) | 0.530568 (0.529844–0.619316) | 0.675497 (0.658833–0.731337) |
| ARRAY_AGG | 64 | 0.617899 (0.531926–0.694289) | 0.620905 (0.534293–0.760524) | 0.807764 (0.652013–0.808651) |
| ARRAY_AGG | 4096 | 0.210483 (0.130218–0.237499) | 0.194822 (0.190731–0.233336) | 0.261850 (0.240825–0.297522) |

These short jobs show substantial startup/JIT variability. Native results do not establish
acceleration; slower results remain visible under the accepted collection tradeoff. The
[artifact](results/collection-string-keys-2026-10-24.json) retains all trials, including each
previous JVM's paired stock controls and exact configuration.

Optimization closes after two captures using the same eight-second native-only profiling
protocol above. COLLECT at 4096 wide-string groups has 17,078 samples: 4,045 inclusive in
released group processing, 962 in result snapshots and 903 in output append. String-key hashing
and equality account for 1,253 and 659 self samples respectively. ARRAY_AGG at 64 groups has
16,893 samples: 6,023 inclusive in group processing, 3,981 in result snapshots and 1,495 in append;
LinkedList.toArray accounts for 3,925 self samples. Counts overlap and cover all JVM threads,
planning and compilation; they do not count allocations or copied bytes. No safe small shortcut
was identified within the two passes. Managed-state key work, growing result snapshots and
startup remain follow-up work in [#239](https://github.com/datafusion-contrib/StreamFusion/issues/239).
The grouped operator, key/state codecs and JNI ownership protocol are unchanged.

For reproduction, use the COLLECT/ARRAY_AGG commands above with `-Djvmagg.keyType=string`,
`-Djvmagg.keyPrefixChars=16 -Djvmagg.keys=64`, then prefix 264/keys 4096. Keep 200000 COLLECT
rows and 50000 ARRAY_AGG rows, `values=8`, two warmups and three trials. Compare the same
benchmark source against `1296aa62` with `expectNative=false`; current requires `true`.
For profiles select `#profileCollect` at 4096 keys or `#profileArrayAgg` at 64 keys with the same
STRING inputs, `SF_PROFILE_JVM_AGG=true` and `jvmagg.seconds=8`.

## Small integer elements

TINYINT/SMALLINT COLLECT and Flink 2.2 ARRAY_AGG reuse the same released handlers, state
and columnar grouped operator. [Coverage](../operators/group-by.md#small-integer-collection-elements)
records typed-source limits, NULLs, changelog histories, consumer work and repeated recovery.
The broad matrix passes 253 checks on Flink 2.2.1 and 202 with 51 version skips on 1.18.1.

The release library, host and complete-job boundary above are retained. COLLECT uses 200,000
rows and ARRAY_AGG 50,000; INT keys have 64/4096 groups. Runtime BIGINT values cycle through
eight values per group, with every eighth value NULL, then pass through a TINYINT/SMALLINT CAST
before grouping on both engines. Both perimeter transposes, the native Calc cast, managed
state, source/sink and planning/submission remain in the native timing. Arrays still grow to
781/782 or 12/13 elements; the eight-value domain does not cap their length. Parallelism is one,
heap 8 GiB, accumulator cache 0, with two warmups and three alternating trials per engine.
No local tests/builds/other benchmarks overlap measurements.

Prior candidate `083ee5cf` uses identical benchmark source and the same frozen native DSO,
with verified whole-query stock fallback. Candidate runs require consumed NativeJvmGroup work.
The [artifact](results/small-integer-collections-2026-10-24.json) retains all trials, including
paired stock controls for each prior JVM, configuration and benchmark-source checksum.

Median seconds (minimum–maximum):

| Shape | Element | Groups | Paired stock | Previous fallback | Native route |
| --- | --- | ---: | ---: | ---: | ---: |
| COLLECT | TINYINT | 64 | 0.384040 (0.377551–0.426808) | 0.409516 (0.378629–0.477743) | 0.450483 (0.395221–0.462711) |
| COLLECT | SMALLINT | 64 | 0.334492 (0.303697–0.373923) | 0.308490 (0.304946–0.390241) | 0.316711 (0.316301–0.317838) |
| COLLECT | TINYINT | 4096 | 0.419148 (0.395668–0.439195) | 0.473465 (0.460230–0.479073) | 0.444146 (0.427199–0.493480) |
| COLLECT | SMALLINT | 4096 | 0.359677 (0.332119–0.360388) | 0.353041 (0.351100–0.427021) | 0.369802 (0.351690–0.467924) |
| ARRAY_AGG | TINYINT | 64 | 0.607137 (0.556293–0.666531) | 0.643704 (0.602796–0.664181) | 0.722009 (0.658594–0.729609) |
| ARRAY_AGG | SMALLINT | 64 | 0.685984 (0.533384–0.710872) | 0.566705 (0.546968–0.574309) | 0.725947 (0.638138–0.746778) |
| ARRAY_AGG | TINYINT | 4096 | 0.188956 (0.185946–0.216100) | 0.194958 (0.167390–0.200184) | 0.192006 (0.182821–0.213037) |
| ARRAY_AGG | SMALLINT | 4096 | 0.097794 (0.095426–0.193721) | 0.091185 (0.091072–0.178126) | 0.100762 (0.091968–0.173542) |

Most native medians exceed paired stock. Comparisons with prior fallback vary, and overlapping
ranges/startup costs prevent a sustained acceleration claim. These costs ship under the
accepted collection tradeoff; unfavorable results remain in the table.

Optimization stops after two eight-second target CPU captures using the native-only protocol
above. SMALLINT COLLECT at 4096 groups has 19,872 samples: 3,855 inclusive in released group
processing, 876 in result snapshots, 1,017 in output append and 356 in Arrow map writing.
TINYINT ARRAY_AGG at 64 groups has 15,295 samples: 5,153 inclusive in group processing, 3,441
in result snapshots, 1,121 in append and 483 in array writing; LinkedList.toArray contributes
3,408 self samples. These overlapping counts cover all JVM threads, planning and compilation;
they are not allocation or copied-byte counts. No safe small shortcut was identified.
Managed state, growing result snapshots, output work and startup remain performance follow-ups
in [#239](https://github.com/datafusion-contrib/StreamFusion/issues/239).

For reproduction, select `jvmagg.cases=collect_tinyint,collect_smallint` with `rows=200000`,
then `array_agg_tinyint,array_agg_smallint` with `rows=50000`; run each at `keys=64` and `4096`.
Keep `values=8`, two warmups, three trials and all other commands/settings above. Compare the
same benchmark source against `083ee5cf` with `expectNative=false`; current requires `true`.
The two captures select `#profileCollect` with
`-Djvmagg.cases=collect_smallint -Djvmagg.keys=4096`, and `#profileArrayAgg` with
`-Djvmagg.cases=array_agg_tinyint -Djvmagg.keys=64`. Keep `SF_PROFILE_JVM_AGG=true`,
`jvmagg.seconds=8` and an async-profiler path as above.

## Legacy row consumers

Flink 2.2 legacy row exits now admit direct verified integer collection groups through the
prepared-sink metadata boundary. [Coverage](../operators/group-by.md#legacy-row-consumers)
retains the exact gates; this adds no native connector or new state/ownership protocol. The
local broad matrix passes 267 checks on 2.2.1 and 208 with 59 version skips on 1.18.1. Agent
checks pass 56 with one skip, plus six scope and 25 summary checks. The unchanged upstream
COLLECT fixture passes seven/six cases with explicit contracts: two native backend variants
on each line consume 21 JVM-group rows, and all other variants keep their expected fallback.

These release/mimalloc measurements retain the host/library above, INT keys at 64/4096 groups,
parallelism one, an 8 GiB heap and no accumulator cache. COLLECT uses 200,000 runtime rows;
NOT NULL BIGINT ARRAY_AGG uses 50,000 and `COALESCE(v,CAST(0 AS BIGINT))`. Values cycle
through an eight-value per-key domain, with every eighth source value NULL. COLLECT ignores
NULL elements; ARRAY replaces them with zero. Arrays still grow to 781/782 or 12/13 elements.
Two warmups precede three alternating trials per engine in each fresh cohort JVM.

The timed legacy path calls `toRetractStream(query,Row.class).executeAndCollect()` and drains
every external insert/retract record. Planning/legacy translation, submission, state, both
row/Arrow transposes, external conversion and client collect transport remain inside timing;
cluster boot/schema setup stay outside. These results specifically describe the external
client consumer, not every possible legacy sink. Candidate jobs require consumed JVM-group
work and both transposes. Prior candidate `1ed09143`, using identical benchmark source and
the same native DSO, must have no native work for these guard-sensitive cases. Nullable
primitive arrays are not used as a fallback control; the ARRAY case has NOT NULL elements.

A two-second process sampler observed no overlapping external Java/Cargo workloads during
trials or profiles. Two raw matrix observations were its own Maven startup shells before
any warmup/timing. [All trials, configuration and profile counts](results/legacy-collection-consumers-2026-10-24.json)
also retain the prior JVMs' independent stock controls and source checksum.

Median seconds (minimum–maximum):

| Shape | Groups | Paired stock | Previous fallback | Native route |
| --- | ---: | ---: | ---: | ---: |
| COLLECT | 64 | 1.376737 (1.367508–1.413311) | 1.378298 (1.375485–1.384469) | 1.347112 (1.333860–1.347182) |
| COLLECT | 4096 | 1.260053 (1.243484–1.363648) | 1.359904 (1.346665–1.366877) | 1.243732 (1.210967–1.307879) |
| ARRAY_AGG NOT NULL | 64 | 2.103232 (2.099214–2.204880) | 2.189479 (2.183062–2.201101) | 2.619672 (2.444055–3.043646) |
| ARRAY_AGG NOT NULL | 4096 | 0.549519 (0.537020–0.573537) | 0.554805 (0.553129–0.591117) | 0.554460 (0.552484–0.556155) |

COLLECT medians improve these paired controls, while larger-cardinality ranges overlap and
separate-process controls drift. Growing arrays at 64 groups are slower; at 4096, medians
are close and ranges overlap. These short jobs do not establish sustained acceleration.
All unfavorable trials remain visible under the accepted collection tradeoff.

Optimization closes after two eight-second target CPU captures with the native-only warmup
protocol above. COLLECT at 4096 groups has 21,768 samples: 4,012 inclusive in group processing,
389 in result snapshots, 2,702 in append and 2,184 in client collection. Map serialization has
775 self samples and ByteArrayInputStream.read 1,537. ARRAY at 64 groups has 24,805 samples:
7,990 inclusive in group processing, 733 in result snapshots, 7,030 in append and 5,666 in client
collection. DataOutputStream.writeLong has 5,433 self samples, readUnsignedByte 2,714 and
ByteArrayInputStream.read 2,406; LinkedList.toArray has 731. Inclusive counts overlap, and
append includes chained downstream work. These are CPU samples, not allocation/copied-byte
counts. Serialization/client materialization dominates much of this exit; state snapshots
and Arrow output remain costs too. No safe small change inside the planner/runtime was
identified within the two passes. Further performance remains in
[#239](https://github.com/datafusion-contrib/StreamFusion/issues/239).

Reproduce after a release build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=JvmAggregateBenchmark#completeJobsRetainKeyedStateAndBothTransposes' \
  -Dsf.testForks=1 -Djvmagg.cases=legacy_collect -Djvmagg.rows=200000 \
  -Djvmagg.keys=64 -Djvmagg.values=8 -Djvmagg.warmup=2 -Djvmagg.runs=3 \
  -Djvmagg.expectNative=true -Djvmagg.expectCache=false
```

Also run keys 4096, then `cases=legacy_array_agg_not_null` with 50000 rows at both cardinalities.
The prior checkout copies the same benchmark source and sets `expectNative=false`. For the two
profiles set `SF_BENCHMARK=true SF_PROFILE_JVM_AGG=true`, use `#profileCollect` with
`-Djvmagg.cases=legacy_collect -Djvmagg.keys=4096`, then `#profileArrayAgg` with
`-Djvmagg.cases=legacy_array_agg_not_null -Djvmagg.keys=64`. Keep the same row/domain/library settings and
`jvmagg.seconds=8`, passing an async-profiler path with `profile.asprof`. Profiled timings
do not enter the comparison table.

## BOOLEAN elements

BOOLEAN collection elements use the existing released handlers, managed state and Arrow
batch boundaries. [Coverage](../operators/group-by.md#boolean-collection-elements) retains
the exact gates. The broad local matrix passes 279 checks on Flink 2.2.1 and 214 with 65
version skips on 1.18.1; agent checks pass 56 with one skip. ARRAY_AGG is absent on 1.18.

These release/mimalloc comparisons retain the host/library and complete-job boundary above.
INT keys use 64/4096 groups, parallelism one, an 8 GiB heap and no accumulator cache.
COLLECT uses 200,000 rows and ARRAY_AGG 50,000. Runtime BIGINT values cycle through eight
values per key and become BOOLEAN through `(v>3)`, retaining NULL every eighth row.
Non-NULL groups have two possible BOOLEAN values; key7mod8 groups contain only NULLs.
Array lengths still grow to 781/782 or 12/13 elements. Planning, submission, generated
comparison, keyed state, output, source/sink and both row/Arrow conversions remain timed.
Two warmups precede three alternating trials per engine, in fresh cohort JVMs.

Prior candidate `e3fd2423` uses identical benchmark source and the same DSO. Its BOOLEAN
queries require no native work; current jobs require consumed JVM-group work and both
transposes. A two-second sampler observes no overlapping external Java/Cargo workload.
The matrix and profile samplers each see one own Maven startup shell before its first
warmup/job; both are classified in the [retained evidence](results/boolean-collections-2026-10-24.json),
alongside every trial, separate prior stock controls, configuration and profile counts.

Median seconds (minimum–maximum):

| Shape | Groups | Paired stock | Previous fallback | Native route |
| --- | ---: | ---: | ---: | ---: |
| COLLECT | 64 | 0.326653 (0.319330–0.338287) | 0.363584 (0.304675–0.405923) | 0.393280 (0.351090–0.430312) |
| COLLECT | 4096 | 0.345625 (0.338376–0.410258) | 0.356046 (0.349711–0.364927) | 0.368659 (0.366698–0.395571) |
| ARRAY_AGG | 64 | 0.756527 (0.667153–0.859359) | 0.660542 (0.656398–0.720487) | 0.849646 (0.771984–0.894879) |
| ARRAY_AGG | 4096 | 0.187237 (0.185224–0.190696) | 0.208227 (0.207688–0.212281) | 0.215862 (0.172185–0.228775) |

All four native medians exceed paired stock; previous-fallback comparisons vary and ranges
overlap. Short jobs and startup costs do not establish sustained throughput. These measured
costs ship under the accepted collection tradeoff, without an acceleration claim.

Optimization closes after exactly two eight-second target CPU captures, each after an
unprofiled native-only job. BOOLEAN COLLECT at 4096 groups has 22,403 samples: 3,109
inclusive in group processing, 595 in result snapshots and 822 in append. BOOLEAN ARRAY_AGG
at 64 groups has 14,669 samples: 5,274 inclusive in group processing, 3,666 in snapshots and
1,062 in append; LinkedList.toArray contributes 3,631 self samples. State/key operations,
growing list snapshots, output and compilation remain visible costs. Inclusive counts
overlap and include all JVM threads, planning and compilation; they are not allocation
or copied-byte counts and do not causally isolate the regression against stock. No safe
small shortcut was identified inside the two passes. Further optimization remains in
[#239](https://github.com/datafusion-contrib/StreamFusion/issues/239).

Reproduce using the complete-job command above with `jvmagg.cases=collect_boolean`,
`rows=200000`, then `array_agg_boolean`, `rows=50000`, each at `keys=64` and `4096`.
Keep `values=8`, two warmups, three trials, `expectCache=false` and `expectNative=true`;
the prior checkout copies the same benchmark source and requires `expectNative=false`.
For the two captures set `SF_BENCHMARK=true SF_PROFILE_JVM_AGG=true`, select
`#profileCollect` with `cases=collect_boolean`, `keys=4096`, then `#profileArrayAgg` with
`cases=array_agg_boolean`, `keys=64`. Retain the same rows/domain/library and pass
`jvmagg.seconds=8` plus an async-profiler executable through `profile.asprof`. Profiled
timings do not enter the comparison table.

## Rust collection state

Verified primitive COLLECT and Flink 2.2 ARRAY_AGG now use Rust state rather than the generated
JVM handler. These release/mimalloc comparisons use Flink 2.2.1, JDK 17, parallelism one, an
8 GiB heap, INT grouping keys and no JVM accumulator cache. COLLECT uses 200,000 runtime rows;
ARRAY_AGG uses 50,000. Values cycle through an eight-value per-key domain with every eighth
source value NULL. BOOLEAN uses `(v>3)`; narrow integers use the corresponding CAST. Two
warmups precede three alternating stock/native trials per fresh cohort JVM. The ordinary
row-to-Arrow batch preference is 1,024 rows; operator coalescing retains its production defaults.

Each complete-job timer includes planning/submission, source and blackhole sink, keyed state,
result materialization and both perimeter transposes. The plan and consumed-work checks require
the intended Rust or JVM aggregate and both conversions; stock controls require no native work.
The preceding implementation is selected by `rustCollectionAggregates=false` in the same
candidate/library, retaining its unchanged released JVM handler and managed state. Each prior
cohort also has independent stock controls, retained in the artifact. This isolates the state
replacement instead of comparing against an unrelated older fallback.

A two-second process sampler excludes external Java/Cargo/Rust/C++ workloads from all eight
final cohorts. The driver waits for external work to finish; an earlier overlapping cohort is
excluded from the comparison. [All 320 trials, configuration, source/library hashes and profile
counts](results/native-collection-state-2026-10-27.json) retain warmups and every unfavorable
measurement. Median seconds (minimum–maximum):

| Function / element | Groups | Paired stock | Previous JVM handler | Rust state |
| --- | ---: | ---: | ---: | ---: |
| COLLECT / BIGINT | 64 | 0.410145 (0.401101–0.419614) | 0.393537 (0.381584–0.423012) | 0.393378 (0.383773–0.404893) |
| COLLECT / BOOLEAN | 64 | 0.284471 (0.237033–0.301163) | 0.330348 (0.246450–0.333776) | 0.364429 (0.362557–0.367449) |
| COLLECT / TINYINT | 64 | 0.328230 (0.315705–0.346342) | 0.337080 (0.327306–0.346345) | 0.365508 (0.364999–0.368424) |
| COLLECT / SMALLINT | 64 | 0.325115 (0.318091–0.332782) | 0.333629 (0.330462–0.340949) | 0.367524 (0.366840–0.372131) |
| COLLECT / BIGINT | 4096 | 0.460723 (0.421718–0.490280) | 0.435095 (0.375358–0.460587) | 0.401804 (0.390525–0.457847) |
| COLLECT / BOOLEAN | 4096 | 0.358457 (0.294082–0.364548) | 0.356140 (0.272621–0.356972) | 0.399951 (0.381539–0.406156) |
| COLLECT / TINYINT | 4096 | 0.387977 (0.368634–0.399182) | 0.378671 (0.361047–0.379734) | 0.376801 (0.369625–0.380383) |
| COLLECT / SMALLINT | 4096 | 0.367354 (0.366178–0.402927) | 0.363072 (0.350699–0.370132) | 0.377457 (0.375738–0.383737) |
| ARRAY_AGG / BIGINT | 64 | 0.664409 (0.570414–0.683745) | 0.776970 (0.732698–0.982627) | 0.264972 (0.247862–0.326301) |
| ARRAY_AGG / BOOLEAN | 64 | 0.630213 (0.617130–0.732652) | 0.907002 (0.689615–0.959422) | 0.218179 (0.217967–0.232675) |
| ARRAY_AGG / TINYINT | 64 | 0.617490 (0.579958–0.646764) | 0.857952 (0.739006–1.040668) | 0.298172 (0.296794–0.303482) |
| ARRAY_AGG / SMALLINT | 64 | 0.671095 (0.557645–0.689471) | 0.933262 (0.775930–1.092321) | 0.328728 (0.325788–0.334471) |
| ARRAY_AGG / BIGINT | 4096 | 0.214582 (0.192095–0.241505) | 0.188545 (0.180697–0.228420) | 0.205435 (0.097222–0.214132) |
| ARRAY_AGG / BOOLEAN | 4096 | 0.168819 (0.168175–0.203160) | 0.182682 (0.173632–0.211900) | 0.109156 (0.105359–0.169975) |
| ARRAY_AGG / TINYINT | 4096 | 0.105090 (0.087479–0.109616) | 0.094128 (0.085395–0.102327) | 0.099306 (0.093339–0.108791) |
| ARRAY_AGG / SMALLINT | 4096 | 0.094673 (0.094157–0.113881) | 0.103676 (0.100000–0.104341) | 0.102178 (0.095571–0.113202) |

ARRAY_AGG at 64 groups improves paired stock by 2.04–2.89× and the preceding handler by
2.84–4.16× in these jobs. At 4,096 groups the arrays contain only 12/13 elements and timings
are much shorter; medians/ranges overlap in several cells. COLLECT is mixed: BIGINT improves
paired stock, while BOOLEAN and several narrow-integer shapes remain slower. These short,
startup/JIT-sensitive jobs do not establish sustained throughput or a universal acceleration
claim. Slower cases ship as correctness-verified native-state groundwork under the maintainer's
explicit two-pass agreement, with no performance-only opt-in.

Optimization stops after two eight-second CPU captures, sampled only in native jobs after two
unprofiled warmups. The first, BIGINT COLLECT at 64 groups, has 14,552 samples: 4,425 inclusive
in Rust group update, 2,089 in result-column finishing, 1,974 in generic scalar-to-array conversion
and 272 in collection result-type construction. Reusing the operator's immutable result type
removes that per-row schema construction. The initial 64-group BIGINT native median was
0.455002 s and the final median is 0.393378 s; separate cohort/JIT variation prevents attributing
the entire difference to that small change.

The second and final capture, BIGINT ARRAY_AGG at 64 groups after that change, has 15,396 samples:
1,519 inclusive in Rust group update and 525 in result-column finishing; boxed Long construction
and RowData copying have 1,489 and 578 self samples. The Arrow-to-row exit and array
reader have 2,341 and 1,582 inclusive samples. JVM compiler threads account for 6,223/7,799
inclusive samples respectively. Counts overlap and include all JVM threads; they are CPU samples,
not allocation requests or copied-byte measurements. The remaining output materialization,
boxed-array reconstruction, equality work and startup need broader changes; they remain in
[#239](https://github.com/datafusion-contrib/StreamFusion/issues/239). No further tuning pass is used.

Reproduce after building the release library:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=JvmAggregateBenchmark#completeJobsRetainKeyedStateAndBothTransposes' \
  -Dsf.testForks=1 -Djvmagg.cases=collect,collect_boolean,collect_tinyint,collect_smallint \
  -Djvmagg.rows=200000 -Djvmagg.keys=64 -Djvmagg.values=8 \
  -Djvmagg.warmup=2 -Djvmagg.runs=3 -Djvmagg.expectNative=true \
  -Djvmagg.expectCache=false -Dstreamfusion.experimental.jvmAccumulatorCacheEntries=0 \
  -Dstreamfusion.experimental.rustCollectionAggregates=true
```

Repeat at 4,096 keys, then select the analogous `array_agg` cases with 50,000 rows. Repeat each
cohort with `rustCollectionAggregates=false` for the preceding handler. The CPU captures use
async-profiler 4.6, `-e cpu --cstack dwarf -i 1ms`, and an eight-second recording after two
unprofiled native-only jobs. Profiled timings are excluded from the comparison table.


## DATE and TIME elements

The temporal extension uses Rust collection state, typed DATE/TIME runtime Row sources and
rowwise blackhole sinks. Input logical values, NULL frequency, group counts and value domains
match across stock, previous-code and candidate jobs. The complete-job timer retains planning,
submission, keyed state, output materialization and both row/Arrow transposes. Cluster setup and
initial explain remain excluded. Native jobs require consumed native grouped-aggregate work;
previous code retains its planning fallback for these element types.

The benchmark accepts `collect_date`, `collect_time`, `array_agg_date` and `array_agg_time`.
DATE values are epoch-day offsets minus three; TIME values are one millisecond plus whole
seconds from the same bounded value domain. Every eighth input remains NULL. ARRAY_AGG is
N/A on 1.18. The two-pass optimization limit applies to the temporal increment as a whole.

Release/mimalloc measurements on 2026-10-27 use Flink 2.2.1, OpenJDK 17.0.20.1, Linux
x86-64/WSL2 and an Intel Core i7-12650H, parallelism one and an 8 GiB heap. COLLECT has
200,000 source rows; ARRAY_AGG has 50,000. Each engine has two warmups and three alternating
measurements at 64/4096 INT groups, with the default zero-entry accumulator cache and local
Arrow IPC enabled. The paired previous-code JVM runs canonical `2b199b0e` with the identical
benchmark source and frozen release library; its temporal aggregates retain whole-query fallback.
No competing Java/native builds or tests were observed during any measured cohort. Measurements
predate the final NOT NULL-argument admission guard; all measured inputs are nullable and
retain the identical native operator/state path. A final fixture smoke verifies that routing.

[All trials, native-work witnesses, provenance and profile counts](results/temporal-collections-2026-10-27.json)
retain both revisions' paired stock controls. Median seconds (minimum–maximum):

| Function / element / groups | Paired stock | Previous fallback | Rust collection state |
| --- | ---: | ---: | ---: |
| collect_date / 64 | 0.428242 (0.304938–0.438641) | 0.302913 (0.298356–0.430385) | 0.380309 (0.373960–0.380745) |
| collect_time / 64 | 0.303027 (0.294204–0.384323) | 0.311299 (0.296475–0.320968) | 0.368347 (0.359526–0.393264) |
| collect_date / 4096 | 0.448308 (0.337164–0.499592) | 0.387096 (0.325800–0.388156) | 0.397779 (0.384294–0.414325) |
| collect_time / 4096 | 0.378311 (0.356739–0.408316) | 0.334876 (0.328102–0.434807) | 0.397740 (0.386408–0.399494) |
| array_agg_date / 64 | 0.536786 (0.517886–0.570472) | 0.572140 (0.528304–0.652226) | 0.266243 (0.247365–0.306507) |
| array_agg_time / 64 | 0.738173 (0.733791–0.801741) | 0.610721 (0.543917–0.819238) | 0.282258 (0.280377–0.282639) |
| array_agg_date / 4096 | 0.191791 (0.186951–0.223859) | 0.165888 (0.089291–0.181733) | 0.110610 (0.100197–0.115949) |
| array_agg_time / 4096 | 0.099233 (0.098050–0.103836) | 0.097837 (0.083647–0.105374) | 0.111883 (0.106399–0.118756) |

At 64 groups, DATE/TIME ARRAY_AGG improve paired stock medians by 2.02×/2.62×, including both
transposes and materialization. DATE COLLECT improves paired stock medians at both cardinalities
but exceeds previous fallback. TIME COLLECT exceeds stock and previous fallback; the small
4096-group TIME ARRAY_AGG also exceeds both controls. Ranges and stock controls vary across JVMs.
These startup-inclusive complete jobs establish workload-specific costs, not universal sustained
throughput. Slower cases remain bounded collection groundwork under the existing collection
tradeoff, without an acceleration claim.

The investigation stops after two eight-second CPU captures, with one native warmup before each.
TIME COLLECT/64 groups has 16,800 samples: grouped processing has 3,778 inclusive samples,
scalar-to-Arrow output assembly 1,730, result emission 858 and nested equality 450. The collection
counter update itself has only 39. TIME ARRAY_AGG/4096 groups has 20,327 samples, including 991
in grouped processing, 126 in scalar-to-Arrow assembly and 214 in result emission; compilation,
planning and other JVM work dominate this short-job capture. Counts overlap and include all
threads; no allocation count or copied-byte measurement is claimed. No safe small state shortcut
was identified. Generic output assembly, equality and startup remain work in
[#239](https://github.com/datafusion-contrib/StreamFusion/issues/239). No third optimization pass.

Reproduce after building the release library and compiling the benchmark with `-Pbench`:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime surefire:test@default-test -Pbench \
  '-Dtest=JvmAggregateBenchmark#completeJobsRetainKeyedStateAndBothTransposes' \
  -Djvmagg.cases=collect_date,collect_time -Djvmagg.rows=200000 \
  -Djvmagg.keys=64 -Djvmagg.values=8 -Djvmagg.warmup=2 -Djvmagg.runs=3 \
  -Djvmagg.expectNative=true -Djvmagg.expectCache=false
```

Repeat at 4096 groups; use `array_agg_date,array_agg_time` and 50,000 rows for arrays.
Previous-code controls use the same benchmark source and release payload with
`jvmagg.expectNative=false`. For the two CPU captures set `SF_PROFILE_JVM_AGG=true`, select
`#profileCollect` / `#profileArrayAgg`, choose the corresponding single case and cardinality,
and pass `-Djvmagg.seconds=8 -Dprofile.asprof=/path/to/asprof`. Profile timings are excluded
from the trial table.


### NOT NULL temporal inputs and array copy boundaries

`-Djvmagg.notNull=true` declares DATE/TIME source values NOT NULL while retaining actual
NULL slots every eighth input, as accepted by released Flink. This measures the newly admitted
hint and array-output signatures against stock Flink and the actual previous `c5053af5`
whole-query fallback. The previous planner and frozen release library use the identical new
benchmark source; the candidate uses release+mimalloc. The method and sizes above remain:
200,000 COLLECT rows / 50,000 ARRAY_AGG rows, eight values, 64/4096 INT groups, one task,
an 8 GiB heap, two warmups and three alternating measurements per engine. Both transposes,
JNI, state and rowwise blackhole sinks remain in the complete-job timer.

[Raw trials, source/library hashes, paired previous stock controls and native-work evidence](results/not-null-temporal-collections-2026-10-28.json)
retain 160 complete jobs. All 40 candidate group observations consume their configured row
count; previous runs require fallback. No competing Java/native build or test was observed.
Median seconds (minimum–maximum):

| Function / element / groups | Paired stock | Previous fallback | Rust collection state |
| --- | ---: | ---: | ---: |
| collect_date / 64 | 0.380618 (0.328244–0.433813) | 0.313670 (0.291502–0.387066) | 0.376697 (0.363646–0.381369) |
| collect_time / 64 | 0.306514 (0.301279–0.308044) | 0.298521 (0.296325–0.316540) | 0.365302 (0.364272–0.368782) |
| collect_date / 4096 | 0.404228 (0.393031–0.456042) | 0.379955 (0.345099–0.416522) | 0.396124 (0.385839–0.461975) |
| collect_time / 4096 | 0.378920 (0.358894–0.379612) | 0.348410 (0.342260–0.365321) | 0.392731 (0.382941–0.404248) |
| array_agg_date / 64 | 0.625983 (0.578292–0.627684) | 0.704084 (0.557210–0.738284) | 0.275298 (0.264657–0.293576) |
| array_agg_time / 64 | 0.662617 (0.661261–0.670525) | 0.819756 (0.697039–0.863708) | 0.310644 (0.300989–0.324547) |
| array_agg_date / 4096 | 0.210439 (0.195651–0.231240) | 0.168335 (0.159814–0.199816) | 0.175561 (0.096228–0.223750) |
| array_agg_time / 4096 | 0.112775 (0.097534–0.124491) | 0.100451 (0.090856–0.125149) | 0.100338 (0.099196–0.107628) |

At 64 groups DATE/TIME ARRAY_AGG improve paired stock medians by 2.27×/2.13× and previous
fallback by 2.56×/2.64×. At 4096 groups the DATE array job beats its paired stock control but
slightly exceeds the previous fallback; TIME is effectively tied with previous fallback.
TIME COLLECT exceeds both controls at both cardinalities. DATE COLLECT is close to its paired
stock controls and slower than previous fallback. These short-job measurements include startup
and planner variability and do not establish universal sustained throughput. Slower cases ship
as bounded correctness groundwork under the accepted collection tradeoff, with no acceleration
claim. The two temporal CPU profiles above already closed the optimization timebox; generic
output assembly and startup remain in [#239](https://github.com/datafusion-contrib/StreamFusion/issues/239).
No additional optimization or profiling pass was made for these representation fixes.

The same benchmark command above reproduces this matrix with `-Djvmagg.notNull=true`; use
50,000 rows and the two `array_agg_*` cases for arrays, and repeat with 4096 keys. Independent
SQL checks verify key-only filtered input, parallel exchange, actual NULL slots beneath the hint,
mixed/repeated consumers and retained/shared Java callbacks. Heap/native RocksDB each restore
twice. The explicit JVM aggregate handler also retains the represented array copy category.

The final planner additionally guards NOT NULL temporal arguments whose input Calc materializes
a hidden NULL-slot payload. That guard does not affect this direct-source benchmark signature;
the artifact retains the measured source hashes separately from the final guarded planner.
Predicates reading those hints and non-identity input projections retain stock values/failures.


## Shared typed collection state

FLOAT, DOUBLE and DECIMAL use the same native counted/sequence state algorithm as the verified
primitive families. This is a coverage and state-reuse change; type-specific codecs supply
Arrow conversion and Java boxed hash/comparison rules. Existing BIGINT cases remain controls
for the primitive path. Floating snapshots preserve original bits separately from canonical
NaN membership, and decimal snapshots preserve unscaled i128 values plus precision/scale.
Six actual previous-version snapshots round-trip byte for byte through the primitive codec.

[All 480 complete jobs, paired controls, ranges, source/library hashes and native witnesses](results/shared-collection-state-2026-10-28.json)
compare stock Flink, actual previous `08df65ab`, and the candidate. The previous production
code remains unchanged; only the identical new benchmark fixture is copied. New DOUBLE and
decimal cases require previous fallback; BIGINT controls require native work in both revisions.
All 150 expected native grouped observations consume the configured input row count. No
competing Java/native build or test was observed during any measured cohort.

Runs use release+mimalloc, OpenJDK 17.0.20.1, an 8 GiB heap, one task, 1,024-row perimeter
batches, Arrow IPC exchange, zero accumulator-cache entries, two warmups and three alternating
measurements per engine. COLLECT uses 200,000 rows; ARRAY_AGG uses 50,000 and is N/A on 1.18.
Source Rows retain a NULL every eighth input and eight seed values. DOUBLE includes both signed
zeros and fractional finite values. Decimal values use the declared scale; wide values cross
64-bit range rather than exercising only compact numbers. Timers retain submission/planning,
JNI, keyed state, both row/Arrow transposes and a blackhole row sink; initial explain and cluster
setup remain outside. Shapes are grouped in one JVM to reduce repeated harness startup.

Median seconds (minimum–maximum). Previous values are fallback for typed suffixes and native
for the unsuffixed BIGINT controls. Both revisions' separately paired stock controls remain in
the artifact.

| Release / shape / groups | Paired stock | Previous StreamFusion | Shared state |
| --- | ---: | ---: | ---: |
| 2.2 / collect_double / 64 | 0.447284 (0.401610–0.525167) | 0.392560 (0.325728–0.464752) | 0.523337 (0.519383–0.583228) |
| 2.2 / collect_decimal8 / 64 | 0.344394 (0.321062–0.349272) | 0.323729 (0.323534–0.341190) | 0.398548 (0.398037–0.416726) |
| 2.2 / collect_decimal38 / 64 | 0.391792 (0.385648–0.393729) | 0.381659 (0.375550–0.386933) | 0.464878 (0.454744–0.471193) |
| 2.2 / collect / 64 | 0.322664 (0.322370–0.327527) | 0.388467 (0.386504–0.395182) | 0.374418 (0.369746–0.386592) |
| 2.2 / collect_double / 4096 | 0.433333 (0.361008–0.554675) | 0.434747 (0.392506–0.445260) | 0.580404 (0.523591–0.604660) |
| 2.2 / collect_decimal8 / 4096 | 0.381987 (0.380420–0.395484) | 0.430005 (0.372494–0.432357) | 0.433432 (0.432452–0.434152) |
| 2.2 / collect_decimal38 / 4096 | 0.449747 (0.436651–0.492483) | 0.437825 (0.429764–0.449290) | 0.494580 (0.486635–0.509529) |
| 2.2 / collect / 4096 | 0.364951 (0.352917–0.369280) | 0.411887 (0.395651–0.417218) | 0.382462 (0.376194–0.390176) |
| 2.2 / array_agg_double / 64 | 0.856105 (0.675581–1.018688) | 0.705018 (0.639266–0.816004) | 0.292919 (0.272674–0.307491) |
| 2.2 / array_agg_decimal8 / 64 | 0.652435 (0.577167–0.683721) | 0.688930 (0.652835–0.721043) | 0.376501 (0.373664–0.410876) |
| 2.2 / array_agg_decimal38 / 64 | 0.771371 (0.745136–0.892780) | 0.669858 (0.663811–0.677173) | 0.885451 (0.878665–0.903415) |
| 2.2 / array_agg / 64 | 0.893083 (0.676079–0.937198) | 0.275803 (0.270813–0.281863) | 0.332886 (0.332700–0.356742) |
| 2.2 / array_agg_double / 4096 | 0.184740 (0.114726–0.229193) | 0.159584 (0.101255–0.211135) | 0.196963 (0.126161–0.208190) |
| 2.2 / array_agg_decimal8 / 4096 | 0.100048 (0.099886–0.103969) | 0.102902 (0.099556–0.105887) | 0.118528 (0.108007–0.132616) |
| 2.2 / array_agg_decimal38 / 4096 | 0.122930 (0.109792–0.154237) | 0.107983 (0.103341–0.132501) | 0.115434 (0.105437–0.118485) |
| 2.2 / array_agg / 4096 | 0.106525 (0.105036–0.114197) | 0.110360 (0.104355–0.114469) | 0.099312 (0.096592–0.106316) |
| 1.18 / collect_double / 64 | 0.396160 (0.347136–0.478559) | 0.332004 (0.295293–0.372231) | 0.512784 (0.493059–0.514627) |
| 1.18 / collect_decimal8 / 64 | 0.337501 (0.317636–0.344608) | 0.313152 (0.312919–0.318677) | 0.390574 (0.387210–0.393046) |
| 1.18 / collect_decimal38 / 64 | 0.400177 (0.391385–0.463686) | 0.412225 (0.405946–0.548915) | 0.477503 (0.471246–0.493955) |
| 1.18 / collect / 64 | 0.317300 (0.312208–0.322978) | 0.381509 (0.373063–0.382106) | 0.369054 (0.365454–0.374415) |
| 1.18 / collect_double / 4096 | 0.408462 (0.354855–0.513331) | 0.378956 (0.376450–0.381669) | 0.539213 (0.525692–0.591038) |
| 1.18 / collect_decimal8 / 4096 | 0.382804 (0.382686–0.394632) | 0.388842 (0.377629–0.400135) | 0.459443 (0.419943–0.461260) |
| 1.18 / collect_decimal38 / 4096 | 0.489093 (0.480898–0.499109) | 0.460348 (0.458972–0.466923) | 0.494902 (0.485132–0.506136) |
| 1.18 / collect / 4096 | 0.361231 (0.344852–0.373138) | 0.389040 (0.381889–0.409086) | 0.381120 (0.372878–0.392389) |

At 64 groups DOUBLE ARRAY_AGG improves paired stock by 2.92× and compact decimal arrays by
1.73×. Wide decimal arrays take 0.885 s versus stock's 0.771 s. COLLECT remains slower than
stock in the representative numeric cohorts. At 4096 groups, wide decimal COLLECT is close to
stock on 1.18, while short-array medians vary around parity. These complete jobs establish
workload costs, not universal sustained throughput. BIGINT ARRAY_AGG/64 takes 0.333 s versus
previous native 0.276 s; its paired stock also moves from 0.707 to 0.893 s, so retain both
absolute results rather than attributing the entire cross-JVM difference to codec dispatch.
The other BIGINT controls are close to or faster than preceding native medians.

The bounded investigation ends after two eight-second CPU captures, each preceded by one
native warmup. DOUBLE COLLECT/4096 has 15,056 samples: grouped processing appears in 1,756,
collection emission in 772 and typed collection updates in 72. Wide decimal ARRAY_AGG/64 has
13,059 samples: grouped processing has 528, collection emission 480 and typed updates seven;
decimal conversion appears in 1,729. `DecimalAccessor.fromInternalValue` has 1,677 inclusive
samples, `DecimalData.fromBigDecimal` 1,681 and `DecimalData.copy` 518 self samples. BigInteger
construction/precision checks and Arrow-array reconstruction are material Java boundary costs.
Counts overlap, include all threads and include untimed cluster setup between jobs; they are
CPU diagnosis, not isolated operation timings or allocation/copy-byte counts. No third capture
or per-type tuning pass follows. Native emission and Java decimal reconstruction/copying remain
in [#239](https://github.com/datafusion-contrib/StreamFusion/issues/239). Slower profiles ship as
correctness-verified groundwork under the accepted collection timebox, without an acceleration
claim.

Reproduce with the complete-job command above, `-Djvmagg.cases=collect_double,collect_decimal8,collect_decimal38,collect`,
200,000 rows, 64/4096 keys and eight values; use the `array_agg*` names and 50,000 rows for
arrays. `collect_float`/`array_agg_float` and the `decimal20` suffix additionally support route
smokes and focused checks. Previous runs set `-Djvmagg.expectNative=false` and
`-Djvmagg.expectedNativeCases=collect` (or `array_agg`) to require fallback for new types while
retaining native control evidence. Flink 1.18 adds its Maven profile and runs COLLECT only.


## Scalar array expansion boundary

A shared structural producer check admits verified NOT NULL-element ARRAY_AGG results into
the existing generated UNNEST consumer. All scalar families reuse the same array-copy metadata;
there is no DATE/TIME-specific rule or new operator. Native Rust retains collection state,
while expansion uses the released Java correlate generator inside its Arrow-input/Arrow-output
operator. Nullable scalar arrays keep ordinary Arrow UNNEST. Coverage, representation guards
and recovery are [specified with GROUP BY](../operators/group-by.md#scalar-array-results-through-generated-unnest).

[All 120 complete jobs and paired controls](results/scalar-array-expansion-2026-10-28.json)
compare stock, actual previous `4df58b80` Java code, and the new admission. Both revisions use
identical benchmark fixtures and the frozen release+mimalloc collection DSO; there is no native
implementation change here. The preceding code must fall back for every expanded case. All
30 candidate group observations consume 32,768 source rows, and all 30 generated expansions
consume positive work. No competing Java/native build or test was observed during measurement.

Runs use Flink 2.2.1/OpenJDK 17.0.20.1, an 8 GiB heap, one task, 1,024-row perimeter batches,
IPC exchange, zero accumulator cache entries, two warmups and three alternating measurements
per engine. The append-only RowData source has eight seeds and NULL every eighth row; COALESCE
produces a typed NOT NULL element. DOUBLE retains signed-zero/fractional seeds; DECIMAL(38,18)
values cross the compact-integer range. Each intermediate growing array is expanded: 64 keys
produce 8,404,992 element rows per native trial, and 4096 keys produce 147,456. Timing includes
submission/planning, JNI, keyed state, Java expansion, both transposes and blackhole row output;
initial EXPLAIN and cluster setup remain outside. Retraction/failure/recovery parity comes from
the SQL regression matrix, separately from these append-source timings. ARRAY_AGG is N/A on 1.18.

Median seconds (minimum–maximum). Previous StreamFusion is verified fallback; its separately
paired stock controls remain in the artifact.

| Keys / element | Paired stock | Previous StreamFusion | Shared producer boundary |
| --- | ---: | ---: | ---: |
| 64 / BIGINT | 5.773431 (5.763612–5.826561) | 5.721306 (5.647072–5.754609) | 1.002027 (0.939273–1.027504) |
| 64 / DOUBLE | 7.178278 (7.159385–7.188569) | 6.749799 (6.689754–6.790683) | 0.998907 (0.992064–1.010254) |
| 64 / DECIMAL(38,18) | 14.797527 (14.770691–14.797693) | 14.440044 (14.366825–14.535495) | 2.047731 (2.022990–2.605549) |
| 4096 / BIGINT | 0.261172 (0.233455–0.319980) | 0.232991 (0.209535–0.238281) | 0.204004 (0.192032–0.277335) |
| 4096 / DOUBLE | 0.220469 (0.188099–0.225747) | 0.211191 (0.197555–0.253218) | 0.147155 (0.102450–0.191357) |
| 4096 / DECIMAL(38,18) | 0.140432 (0.133967–0.151380) | 0.153692 (0.132387–0.223562) | 0.120239 (0.111241–0.229741) |

At 64 keys the new boundary beats paired stock by 5.76×/7.19×/7.23× for BIGINT/DOUBLE/wide
DECIMAL. At 4096 keys the ratios are 1.28×/1.50×/1.17×; these shorter jobs include startup and
have overlapping ranges in some cohorts. Retain the complete timings and controls rather than
claiming universal sustained throughput. All six candidate medians improve paired stock and
previous fallback. No new optimization or profiling pass follows: this reuses the already
profiled collection state and generated representation boundaries, and the earlier two-capture
timebox remains closed. Wider result/conversion work remains in
[#239](https://github.com/datafusion-contrib/StreamFusion/issues/239).

Reproduce with the complete-job command above, `-Djvmagg.cases=expanded_array_agg,expanded_array_agg_double,expanded_array_agg_decimal38`,
`-Djvmagg.rows=32768`, `-Djvmagg.keys=64` (then 4096), `-Djvmagg.values=8`, two warmups and three
runs. Candidate uses `-Djvmagg.expectNative=true`; the preceding production revision uses
`false` and must execute no native operators. Other existing ARRAY_AGG suffixes also accept
the `expanded_` prefix for focused route checks.


## Timestamp and interval element families

TIMESTAMP/TIMESTAMP_LTZ use the shared native collection algorithm with owned full-range i128
keys, the existing component reader and TimestampData hashing. Year-month/day-time intervals
reuse the primitive month/millisecond codecs. The required native exchange copy boundary
clears compact timestamp sub-millisecond remainders before both single-channel and partitioned
exchange, matching Flink's serializer and preserving subsequent COLLECT map iteration.
[Coverage and restore limits](../operators/group-by.md#timestamp-and-interval-collection-elements)
include the released DAY(4) cast planning error and historical compact timestamp state boundary.

[All 1,080 complete jobs, sources/library hashes, paired controls and two CPU captures](results/temporal-interval-collection-state-2026-10-28.json)
retain the initial 720 jobs and one focused pass's 360 jobs. The actual preceding Java/native
code is `4fd9d15e`; only identical benchmark fixtures are copied into its isolated builds. New
temporal/interval cases require previous fallback. BIGINT collection and MIN TIMESTAMP(3)
with COUNT(*) remain native in both revisions. All 420 expected grouped observations consume
the configured row count. No competing Java/native build or test was observed in any cohort.

Runs use Flink 2.2.1/1.18.1, OpenJDK 17.0.20.1, release+mimalloc, an 8 GiB heap, one task,
1,024-row perimeter batches, IPC exchange, zero accumulator cache entries, two warmups and three
alternating measurements per engine. COLLECT uses 200,000 runtime RowData records; ARRAY_AGG
uses 50,000 and is N/A on 1.18. Both use 64/4096 INT keys, eight seeds and NULL every eighth
row. Timestamp values span years 1/1970/9999 with fractional nanoseconds. Year-month values
use I32, and day-time millisecond values exceed 32-bit range. Source schemas explicitly declare
the logical temporal type. Timers retain planning/submission, production JNI/state, both
row/Arrow transposes and a blackhole row sink; initial EXPLAIN and MiniCluster setup are outside.

Final median seconds (minimum–maximum). Previous new-type cases are fallback; BIGINT and MIN
controls are native. Each revision/pass retains its separately paired stock controls in the
artifact, so cross-JVM movement is visible.

| Line / keys / shape | Final paired stock | Previous StreamFusion | Final native |
| --- | ---: | ---: | ---: |
| 2.2 / 64 / collect_timestamp3 | 0.439916 (0.318148–0.444052) | 0.381486 (0.375575–0.388854) | 0.528791 (0.510544–0.545845) |
| 2.2 / 64 / collect_timestamp9 | 0.320575 (0.309698–0.393724) | 0.310323 (0.309086–0.312347) | 0.520272 (0.514901–0.536467) |
| 2.2 / 64 / collect_interval_month | 0.312613 (0.311028–0.313273) | 0.310072 (0.309498–0.318775) | 0.373783 (0.357564–0.374778) |
| 2.2 / 64 / collect_interval_day | 0.333642 (0.328940–0.405322) | 0.317383 (0.315968–0.318505) | 0.373285 (0.371099–0.377387) |
| 2.2 / 64 / collect | 0.313128 (0.307401–0.320929) | 0.385527 (0.376790–0.392681) | 0.371519 (0.369943–0.382803) |
| 2.2 / 64 / min_timestamp3 | 0.142045 (0.138790–0.143378) | 0.340405 (0.335723–0.342983) | 0.340779 (0.332849–0.344703) |
| 2.2 / 4096 / collect_timestamp3 | 0.477187 (0.415544–0.511665) | 0.443834 (0.384778–0.466644) | 0.589026 (0.571574–0.598551) |
| 2.2 / 4096 / collect_timestamp9 | 0.432834 (0.349591–0.459340) | 0.360420 (0.351034–0.370145) | 0.558410 (0.556847–0.564077) |
| 2.2 / 4096 / collect_interval_month | 0.343719 (0.343607–0.377173) | 0.363527 (0.341203–0.388674) | 0.370228 (0.367534–0.385022) |
| 2.2 / 4096 / collect_interval_day | 0.376071 (0.352701–0.380411) | 0.364404 (0.362571–0.368487) | 0.395917 (0.390347–0.398499) |
| 2.2 / 4096 / collect | 0.415182 (0.344293–0.449999) | 0.396470 (0.394263–0.403023) | 0.397501 (0.388666–0.422443) |
| 2.2 / 4096 / min_timestamp3 | 0.146815 (0.140490–0.173256) | 0.361824 (0.349676–0.370828) | 0.364129 (0.345921–0.405477) |
| 2.2 / 64 / array_agg_timestamp3 | 0.871428 (0.830123–0.888429) | 0.677224 (0.633716–0.880480) | 0.475057 (0.427914–0.515917) |
| 2.2 / 64 / array_agg_timestamp9 | 0.842571 (0.665472–0.860158) | 0.659099 (0.625679–0.726751) | 0.421450 (0.418763–0.432252) |
| 2.2 / 64 / array_agg_interval_month | 0.620463 (0.524524–0.680582) | 0.586492 (0.561591–0.759259) | 0.246855 (0.241229–0.248197) |
| 2.2 / 64 / array_agg_interval_day | 0.831598 (0.634520–0.923233) | 0.638915 (0.603805–0.821160) | 0.328628 (0.326650–0.335460) |
| 2.2 / 64 / array_agg | 0.584274 (0.581305–0.870570) | 0.288701 (0.281487–0.293552) | 0.327387 (0.322866–0.334375) |
| 2.2 / 64 / min_timestamp3 | 0.086566 (0.083596–0.091424) | 0.140545 (0.134305–0.140636) | 0.135706 (0.127781–0.138032) |
| 2.2 / 4096 / array_agg_timestamp3 | 0.172590 (0.113524–0.224448) | 0.173339 (0.107597–0.214507) | 0.128303 (0.125540–0.202798) |
| 2.2 / 4096 / array_agg_timestamp9 | 0.109903 (0.099430–0.116646) | 0.097178 (0.092518–0.166864) | 0.125304 (0.114516–0.127005) |
| 2.2 / 4096 / array_agg_interval_month | 0.103680 (0.095970–0.127722) | 0.102840 (0.094125–0.104824) | 0.098238 (0.094311–0.102176) |
| 2.2 / 4096 / array_agg_interval_day | 0.101791 (0.099082–0.110817) | 0.106995 (0.092067–0.109441) | 0.098068 (0.091714–0.105245) |
| 2.2 / 4096 / array_agg | 0.099093 (0.095407–0.102629) | 0.108812 (0.106727–0.116080) | 0.105680 (0.100373–0.109161) |
| 2.2 / 4096 / min_timestamp3 | 0.083553 (0.083390–0.093328) | 0.151000 (0.149721–0.154751) | 0.152761 (0.147268–0.163514) |
| 1.18 / 64 / collect_timestamp3 | 0.425261 (0.309746–0.470076) | 0.308883 (0.301271–0.335683) | 0.525663 (0.501168–0.534349) |
| 1.18 / 64 / collect_timestamp9 | 0.303264 (0.298533–0.348748) | 0.302565 (0.298001–0.308478) | 0.498279 (0.496763–0.506235) |
| 1.18 / 64 / collect_interval_month | 0.311108 (0.310690–0.318292) | 0.304061 (0.292975–0.326582) | 0.355967 (0.345618–0.372366) |
| 1.18 / 64 / collect_interval_day | 0.411792 (0.340408–0.543153) | 0.310818 (0.307397–0.314854) | 0.423095 (0.386546–0.427949) |
| 1.18 / 64 / collect | 0.322348 (0.318704–0.323611) | 0.381967 (0.376576–0.383099) | 0.359806 (0.356204–0.360685) |
| 1.18 / 64 / min_timestamp3 | 0.141385 (0.130811–0.146613) | 0.338223 (0.334853–0.348422) | 0.334806 (0.332220–0.341086) |
| 1.18 / 4096 / collect_timestamp3 | 0.424140 (0.417999–0.481334) | 0.371443 (0.355589–0.374963) | 0.570734 (0.558189–0.614636) |
| 1.18 / 4096 / collect_timestamp9 | 0.343805 (0.330647–0.398302) | 0.357641 (0.337422–0.374597) | 0.562878 (0.531065–0.572545) |
| 1.18 / 4096 / collect_interval_month | 0.371950 (0.340290–0.380317) | 0.358658 (0.351014–0.378769) | 0.365112 (0.364166–0.386349) |
| 1.18 / 4096 / collect_interval_day | 0.377785 (0.365343–0.404279) | 0.373204 (0.359755–0.380358) | 0.390133 (0.380906–0.397588) |
| 1.18 / 4096 / collect | 0.361846 (0.353103–0.363178) | 0.392311 (0.379747–0.416121) | 0.385109 (0.372683–0.394312) |
| 1.18 / 4096 / min_timestamp3 | 0.146825 (0.126530–0.148337) | 0.366146 (0.361097–0.370771) | 0.353819 (0.346692–0.376993) |

At 64 keys on 2.2, compact/full-precision timestamp arrays beat paired stock by 1.83×/2.00×,
and month/day-time interval arrays by 2.51×/2.53×. Timestamp COLLECT remains slower: 0.529/0.520 s
versus paired stock's 0.440/0.321 s. At 4096 keys the timestamp array medians are 0.128/0.125 s
versus stock's 0.173/0.110 s; ranges overlap in these short jobs. Interval arrays approach parity
there. On 1.18, temporal COLLECT still loses stock; high-cardinality month intervals are near
parity. These are complete-job costs, not universal sustained-throughput claims.

The existing MIN TIMESTAMP(3) control at 200,000 rows/64 keys takes 0.341 s final versus 0.340 s
previous native, while paired stock takes 0.142 s. Its pre-existing loss remains visible. BIGINT
ARRAY_AGG/64 takes 0.327 s final versus previous native 0.289 s, with an initial new-family
median of 0.328 s before the focused pass. All controls and ranges remain in the table/artifact;
the timestamp-specific pass does not erase that absolute difference.

The first eight-second CPU capture has 15,580 samples: collection emission appears in 1,752,
timestamp casts in 860, Arrow data reconstruction in 1,177 and typed updates in 25. The single
measured optimization returns exact-typed timestamp Arrow arrays before rebuilding component
ArrayData. TIMESTAMP(9) COLLECT/64 improves from 0.668 to 0.520 s and TIMESTAMP(3) from 0.690 to
0.529 s, about 22–23%. A second eight-second confirmation capture has 15,756 samples: timestamp
casts appear in 33, emission in 1,280, reconstruction in 610 and nested equality in 600.
Categories overlap and include all threads, JIT and untimed cluster setup between jobs; they
are diagnosis rather than isolated kernel timings or allocation/copy-byte measurements.

This closes the bounded investigation after one measured optimization and two captures.
Native result construction, nested equality and Java boundaries remain follow-up
[#239](https://github.com/datafusion-contrib/StreamFusion/issues/239). Slower collection cases
ship as accepted aggregate groundwork, without another tuning pass or an acceleration claim.

Reproduce using the complete-job command above with
`-Djvmagg.cases=collect_timestamp3,collect_timestamp9,collect_interval_month,collect_interval_day,collect,min_timestamp3`,
200,000 rows, 64/4096 keys, eight values, two warmups and three runs. ARRAY_AGG uses the matching
`array_agg*` names and 50,000 rows. The `timestamp0` and `ltz0/3/9` suffixes also support route
smokes. Prior new-type runs set `-Djvmagg.expectNative=false` and
`-Djvmagg.expectedNativeCases=collect,min_timestamp3` (or `array_agg,min_timestamp3`) to retain
native control assertions. Add the Flink 1.18 Maven profile for its COLLECT-only comparison.


## Collection aggregate array inputs

Native collection admission shares execution/aggregate-form checks with the JVM handler without
inheriting its scalar-input limit. Verified nullable-element numeric/BOOLEAN/DATE/TIME arrays,
including nested arrays, reuse the existing key encoding, columnar exchange, collection state
and snapshots. This is a planner change; the benchmark uses the **same frozen release/mimalloc
library** for candidate and actual previous code `a7a05128`. The previous planner must fall back
with no native work. [Coverage](../operators/group-by.md#collection-aggregate-array-inputs)
describes the exact input and consumer limits.

The 2026-10-28 comparison uses Flink 2.2.1/1.18.1, OpenJDK 17.0.20.1, parallelism one, 8 GiB heap,
64/4096 groups and identical source/blackhole resources. Keys are ARRAY<DOUBLE> containing
`[double(row % keys), NULL]`. BIGINT values use eight seeds per key and a NULL every eighth row.
COLLECT has 200,000 rows; ARRAY_AGG has 50,000. Each engine has two warmups and three alternating
trials. Complete-job timing retains planning/submission, state, JNI and both row/Arrow transposes;
cluster setup and initial explain remain outside timing. Local IPC caching stays disabled and
accumulator caching is zero. No competing Java/native build, test or benchmark process was
observed by the process monitor.

[All 120 jobs, 30 consumed native group witnesses and paired controls](results/collection-array-inputs-2026-10-28.json)
retain the exact benchmark source/library hashes, commands, trial arrays and ranges. Candidate
native group witnesses account for every configured source record. Medians in seconds (min–max):

| Flink / aggregate | Keys | Previous paired stock | Previous fallback | Candidate paired stock | Native |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2.2 COLLECT | 64 | 0.346071 (0.339055–0.429422) | 0.410590 (0.399666–0.436610) | 0.424615 (0.423587–0.433719) | 0.439998 (0.434324–0.459862) |
| 2.2 COLLECT | 4096 | 0.468079 (0.455963–0.524854) | 0.461275 (0.406322–0.499007) | 0.463717 (0.411232–0.493607) | 0.454986 (0.438612–0.465492) |
| 2.2 ARRAY_AGG | 64 | 0.569019 (0.565962–0.652175) | 0.625992 (0.605744–0.682339) | 0.671464 (0.556638–0.674724) | 0.265601 (0.261984–0.277028) |
| 2.2 ARRAY_AGG | 4096 | 0.190511 (0.104385–0.199529) | 0.167184 (0.102442–0.173684) | 0.192713 (0.183719–0.205197) | 0.124536 (0.107006–0.222133) |
| 1.18 COLLECT | 64 | 0.401249 (0.400550–0.440985) | 0.371207 (0.332759–0.387747) | 0.426346 (0.329719–0.522692) | 0.441383 (0.412582–0.475752) |
| 1.18 COLLECT | 4096 | 0.395199 (0.366182–0.517281) | 0.425412 (0.363912–0.437631) | 0.440229 (0.414496–0.443789) | 0.439441 (0.422337–0.469866) |

At 64 keys, native ARRAY_AGG takes 0.265601 s versus paired stock's 0.671464 s (2.53×) and previous
fallback's 0.625992 s. At 4096 keys the native/stock medians are 0.124536/0.192713 s (1.55×), but
ranges overlap and the slowest native trial loses to stock; this is not sustained-throughput
proof. COLLECT stays close to paired stock while the 64-key native medians lose to previous
fallback on both lines. Those losses remain enabled under the accepted collection-coverage
tradeoff. No new kernel optimization is claimed or attempted: the shared collection engine's
existing two-capture investigations remain closed, with emission/state/perimeter costs tracked
in [#239](https://github.com/datafusion-contrib/StreamFusion/issues/239).

Reproduce after building the release library:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=JvmAggregateBenchmark#completeJobsRetainKeyedStateAndBothTransposes' \
  -Dsf.testForks=1 -Djvmagg.keyType=array_double -Djvmagg.cases=collect \
  -Djvmagg.rows=200000 -Djvmagg.keys=64 -Djvmagg.values=8 \
  -Djvmagg.warmup=2 -Djvmagg.runs=3 -Djvmagg.expectNative=true \
  -Djvmagg.expectCache=false
```

Repeat with 4096 keys and with `array_agg`/50,000 rows on Flink 2.2. Add `flink-1.18` to the
profile list for the 1.18 COLLECT comparison. Copy the identical benchmark source into the
previous checkout, set `jvmagg.expectNative=false` and load the same release library. ARRAY_AGG
is N/A on 1.18. Correctness/restore verification is separate from these timed jobs.

## Owned string collections

STRING/VARCHAR and CHAR collection elements extend the shared collection boundary. ARRAY_AGG
on Flink 2.2 accumulates owned UTF-8 values in Rust; character COLLECT on both lines uses the
existing generated JVM handler inside the columnar group. JVM object-identity collision ties
prevent claiming that character COLLECT is Rust accumulation. This is coverage groundwork,
subject to the accepted two-pass optimization timebox, rather than a universal acceleration claim.

The release comparison uses the identical `JvmAggregateBenchmark` fixture at the previous
commit `872687f35725869fce11c0a0af72103908701216` and the candidate. Both use optimized native
builds with mimalloc and RocksDB support. STRING values are `v<value>:é🙂` followed by 32 ASCII
`x` characters; CHAR(8) uses a SQL cast of the same source. Each complete job has 50,000 rows,
64 or 4,096 INT keys, an eight-value per-key domain and NULL every eighth row. Each engine has
two warmups and three alternating timed trials. The fixture retains planning/submission,
rowwise source and blackhole sink, keyed state, JNI and both perimeter transposes, excluding
cluster setup and the initial explain. Previous string cases remain whole-plan Flink fallback;
previous native BIGINT ARRAY_AGG/COLLECT controls verify the shared backend independently.

Correctness requires 38 passing checks on Flink 2.2 and 16 passing/22 unavailable-function
skips on Flink 1.18. Existing primitive, character-key and array-input regressions pass 656
checks on 2.2 and 247 with 409 capability skips on 1.18. Unchanged released COLLECT tests pass
seven/six invocations with exact native/fallback contracts; only the fallback diagnostic text
changes in the manifests. These upstream integer tests are regression evidence; the new string
suite positively verifies Rust ARRAY_AGG and the JVM COLLECT route separately, including two
nonempty heap/RocksDB recoveries.

The comparison retains **320 complete jobs and 100 positive group witnesses**, with no
competing Java/native compilation detected in its cohorts. Seconds are medians (min–max).
Previous stock and previous StreamFusion controls are retained independently; apparent
improvements between short jobs with overlapping ranges are not sustained-throughput claims.

| Flink / shape | Keys | Previous stock | Previous StreamFusion | Current stock | Current StreamFusion |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2.2 / collect_string | 64 | 0.255555 (0.183160–0.267526) | 0.255062 (0.215379–0.265643) | 0.256177 (0.227903–0.294199) | 0.286318 (0.214592–0.288351) |
| 2.2 / collect_char | 64 | 0.227915 (0.158396–0.230799) | 0.230908 (0.222120–0.239790) | 0.225828 (0.161506–0.266311) | 0.261873 (0.189744–0.264317) |
| 2.2 / array_agg_string | 64 | 1.036930 (0.980573–1.075990) | 1.043200 (0.980509–1.060535) | 1.154967 (1.056040–1.165088) | 1.403289 (1.340063–1.439068) |
| 2.2 / array_agg_char | 64 | 0.966864 (0.932827–1.035282) | 0.925242 (0.840796–0.977501) | 0.977956 (0.938954–0.999409) | 1.086809 (1.046757–1.107717) |
| 2.2 / array_agg | 64 | 0.863246 (0.823310–0.873698) | 0.288823 (0.285065–0.292343) | 0.839750 (0.807894–0.840052) | 0.329603 (0.315933–0.331331) |
| 2.2 / collect_string | 4096 | 0.292148 (0.291102–0.292640) | 0.247239 (0.193127–0.261667) | 0.296945 (0.276053–0.310246) | 0.301686 (0.292887–0.307357) |
| 2.2 / collect_char | 4096 | 0.248638 (0.180463–0.253721) | 0.239114 (0.228935–0.255344) | 0.174250 (0.164161–0.244092) | 0.262833 (0.184091–0.274300) |
| 2.2 / array_agg_string | 4096 | 0.140592 (0.137364–0.150310) | 0.129057 (0.123342–0.131810) | 0.154541 (0.133802–0.155131) | 0.140018 (0.128899–0.141874) |
| 2.2 / array_agg_char | 4096 | 0.143365 (0.129758–0.164276) | 0.128420 (0.121408–0.131651) | 0.122926 (0.121658–0.124124) | 0.132284 (0.126754–0.146799) |
| 2.2 / array_agg | 4096 | 0.099647 (0.095412–0.100666) | 0.110310 (0.107413–0.122472) | 0.100979 (0.095464–0.105412) | 0.110004 (0.101032–0.111236) |
| 1.18 / collect_string | 64 | 0.214007 (0.186996–0.263845) | 0.275204 (0.185680–0.278105) | 0.294065 (0.255530–0.305684) | 0.213706 (0.213684–0.307122) |
| 1.18 / collect_char | 64 | 0.152819 (0.145687–0.161516) | 0.197792 (0.170522–0.272955) | 0.174236 (0.160488–0.231148) | 0.211587 (0.175240–0.253124) |
| 1.18 / collect | 64 | 0.133454 (0.132751–0.143245) | 0.157805 (0.148502–0.162804) | 0.144275 (0.139469–0.150246) | 0.147464 (0.147113–0.155306) |
| 1.18 / collect_string | 4096 | 0.214821 (0.174703–0.281765) | 0.285327 (0.189554–0.313804) | 0.204875 (0.187483–0.279155) | 0.283906 (0.212533–0.324204) |
| 1.18 / collect_char | 4096 | 0.176106 (0.153228–0.280113) | 0.179172 (0.171862–0.241693) | 0.187259 (0.176012–0.208528) | 0.189979 (0.189364–0.191800) |
| 1.18 / collect | 4096 | 0.165758 (0.152124–0.166639) | 0.159594 (0.150405–0.171786) | 0.148774 (0.139262–0.155378) | 0.149585 (0.142427–0.153750) |

At 64 keys on Flink 2.2, owned STRING ARRAY_AGG takes 1.403289 s versus paired stock's
1.154967 s and previous StreamFusion's 1.043200 s. CHAR ARRAY_AGG also loses to stock.
The BIGINT ARRAY_AGG control takes 0.329603 s versus paired stock's 0.839750 s, but loses
to its previous native median of 0.288823 s. High-cardinality ranges and character COLLECT
results vary; neither the native route nor the codec abstraction establishes a universal win.
The accepted collection-groundwork tradeoff applies; remaining aggregate emission/state costs
follow [#239](https://github.com/datafusion-contrib/StreamFusion/issues/239).

[Raw trials, source/library hashes, portable commands and execution counters](results/owned-string-collections-2026-10-28.json)
retain the complete comparison. Reproduce with `JvmAggregateBenchmark`, `jvmagg.cases` set to
`collect_string,collect_char,array_agg_string,array_agg_char,array_agg` on Flink 2.2 or
`collect_string,collect_char,collect` on Flink 1.18, `jvmagg.rows=50000`, `jvmagg.values=8`,
`jvmagg.keys=64` or `4096`, `jvmagg.warmup=2`, `jvmagg.runs=3`, `jvmagg.expectCache=false`
and `jvmagg.expectNative=true`, using the `bench` profile and a frozen release/mimalloc library.
For previous checkouts use `jvmagg.expectNative=false` and `jvmagg.expectedNativeCases=array_agg`
(2.2) or `collect` (1.18), keeping the new string cases as explicit fallback controls.

Two native-only CPU captures close this increment's investigation. Each follows one unprofiled
complete job and uses an eight-second target loop, stopping after the current job; both JFR
summaries report eight seconds. STRING ARRAY_AGG has 11,005 samples, including 6,500 unique
inclusive native-group samples, 3,914 row/Arrow and 3,012 JIT samples. Notable leaf counts are
Arrow `VarCharVector.get` (1,173), Flink generic-array copying (759), native UTF-8 builder
appends (708) and collection result emission (458). Character COLLECT has 24,156 samples:
3,665 unique inclusive JVM-handler, 1,387 row/Arrow and 16,546 JIT. Categories overlap and
include chained callers; these are not isolated-kernel timings or allocation/copy byte counts.
They identify remaining result copying/materialization and generated-handler work without
justifying further tuning in this bounded item. The optimization timebox is closed.

The raw artifact retains category patterns, leaf counts and recording hashes. Reproduce with
`SF_BENCHMARK=true SF_PROFILE_JVM_AGG=true`, the same release/profile/settings above,
`JvmAggregateBenchmark#profileArrayAgg` / `jvmagg.cases=array_agg_string` and
`JvmAggregateBenchmark#profileCollect` / `jvmagg.cases=collect_string`, `jvmagg.seconds=8`,
`profile.asprof=<async-profiler executable>` and `profile.outputDir=<recording directory>`.
The two captures include all JVM/native threads, JIT and repeated complete-job setup. No
stock jobs run in the profiled process, and no further optimization passes are claimed.

## Binary collection elements

Binary collections retain the released JVM handler inside the columnar island;
these are not Rust value-equality accumulators. The shared native projection
contract also passes collections through by reference beside unrelated numeric
expressions. [Coverage and identity gates](../operators/group-by.md#binary-collection-elements)
include the whole-stock boundary for COLLECT-to-character formatting and the
unchanged retained-MULTISET restriction.

The frozen previous revision is `ce38857f`. Both revisions use the same frozen
release/mimalloc native library; the code change is Java planning/handler admission.
Flink 1.18 padding composes existing native kernels, without a new JNI protocol.
JDK 17.0.20.1, parallelism one, 8 GiB heap, Intel i7-12650H and Linux x86-64 are
shared. Each engine/revision has two warmups and three alternating measured trials.
Timers retain execution-time planning/submission, keyed managed state, row sources,
row blackhole sinks, production JNI and both transposes on native paths. Previous
queries use their actual whole-plan fallback. Cluster setup and the initial explain
are outside timing. No competing Java/Rust process was observed in retained cohorts.

Both initial bare-result and final projection matrices contain 240 complete jobs:
480 total, with 192 warmups and 288 measured jobs. Initial queries emit the key and
collection; final queries additionally emit `SUM(k)+1`, requiring the shared pure
native projection. All admitted runs require consumed JVM group records; final
runs also require consumed NativeCalc work. There are 5,000 rows/64 keys and
50,000 rows/4,096 keys. Inputs contain eight byte payloads with NULL every eighth
input; non-NULL arrays contain a value byte, ff/00 and 37 x bytes. Fixed BINARY(8)
uses a cast of those same 40 bytes. Source rows create fresh byte arrays even when
the payload contents match. ARRAY_AGG is explicitly N/A on 1.18.

The table shows final-query median seconds and [minimum–maximum]. Every initial
and final trial, previous paired stock control, sanitized Maven arguments, native
witness, binary/source hash and exact initial-harness reconstruction is retained
in the [raw report](results/binary-collection-elements-2026-10-28.json).

| Flink / aggregate / type | Rows / keys | Current stock, s | Previous SF, s | Current SF, s |
| --- | --- | --- | --- | --- |
| 2.2 / COLLECT / VARBINARY | 5,000 / 64 | 0.196 [0.189–0.246] | 0.205 [0.193–0.213] | 0.214 [0.194–0.221] |
| 2.2 / COLLECT / BINARY(8) | 5,000 / 64 | 0.158 [0.154–0.174] | 0.171 [0.159–0.180] | 0.172 [0.093–0.215] |
| 2.2 / ARRAY_AGG / VARBINARY | 5,000 / 64 | 0.081 [0.077–0.180] | 0.076 [0.072–0.135] | 0.082 [0.077–0.157] |
| 2.2 / ARRAY_AGG / BINARY(8) | 5,000 / 64 | 0.075 [0.075–0.084] | 0.074 [0.074–0.145] | 0.074 [0.068–0.079] |
| 2.2 / COLLECT / VARBINARY | 50,000 / 4,096 | 0.288 [0.259–0.315] | 0.254 [0.249–0.295] | 0.286 [0.250–0.299] |
| 2.2 / COLLECT / BINARY(8) | 50,000 / 4,096 | 0.260 [0.230–0.261] | 0.242 [0.230–0.283] | 0.239 [0.167–0.247] |
| 2.2 / ARRAY_AGG / VARBINARY | 50,000 / 4,096 | 0.141 [0.129–0.192] | 0.124 [0.121–0.196] | 0.111 [0.105–0.126] |
| 2.2 / ARRAY_AGG / BINARY(8) | 50,000 / 4,096 | 0.127 [0.115–0.129] | 0.127 [0.124–0.134] | 0.109 [0.105–0.113] |
| 1.18 / COLLECT / VARBINARY | 5,000 / 64 | 0.191 [0.175–0.200] | 0.193 [0.191–0.231] | 0.217 [0.187–0.217] |
| 1.18 / COLLECT / BINARY(8) | 5,000 / 64 | 0.159 [0.091–0.165] | 0.162 [0.147–0.174] | 0.096 [0.091–0.181] |
| 1.18 / COLLECT / VARBINARY | 50,000 / 4,096 | 0.268 [0.247–0.320] | 0.278 [0.265–0.286] | 0.296 [0.259–0.305] |
| 1.18 / COLLECT / BINARY(8) | 50,000 / 4,096 | 0.243 [0.241–0.258] | 0.154 [0.154–0.254] | 0.166 [0.161–0.274] |

Results are mixed. At 50,000 rows the 2.2 ARRAY_AGG medians are 0.111/0.109 s
versus 0.141/0.127 s stock for variable/fixed bytes. Small COLLECT jobs remain
slower on 2.2, and variable-byte COLLECT remains slower on 1.18 at both sizes.
Fixed-byte COLLECT improves the 1.18 paired medians. The ranges and previous stock
controls expose startup/JIT drift; these short jobs do not establish sustained
throughput or a blanket binary acceleration claim.

The initial bare-result matrix is retained separately, not pooled with the final
query. Most of its native medians lose to stock, including 2.2 fixed-byte COLLECT
at 64 keys (0.170 s native / 0.094 s stock) and 1.18 variable-byte COLLECT at
4,096 keys (0.251 / 0.158 s). No unfavorable initial cohort is discarded.

The timebox closes after exactly two CPU captures, one per variable-byte aggregate
on the initial bare-result query, following an unprofiled native warmup. Each has
an eight-second target loop and finishes its current job. COLLECT has 24,295
samples: 1,923 in JVM handler scopes, 967 in row/Arrow conversion, 698 in
serialization and 17,536 in JIT. ARRAY_AGG has 23,178 samples, with
1,297 / 718 / 639 / 16,768 in those respective scopes. Inclusive counts overlap;
they do not measure copied bytes or isolated operator costs. Both include all
threads, JIT and repeated job setup, and are excluded from elapsed-time tables.
They do not isolate the final numeric projection. No extra captures or tuning
passes follow. Remaining handler/state-emission and expression costs stay with
[#239](https://github.com/datafusion-contrib/StreamFusion/issues/239),
[#392](https://github.com/datafusion-contrib/StreamFusion/issues/392) and
[#505](https://github.com/datafusion-contrib/StreamFusion/issues/505), under the
accepted collection-groundwork tradeoff.

The consolidated 2.2 suite passes 227 checks. On 1.18, 28 pass with 30 explicit
capability skips. Cases include per-key ordered changes, duplicate byte-array keys,
NULL/empty/wide bytes, fixed padding, FILTER, global/keyed and empty groups,
binary grouping keys, numeric coresults, literal/shared byte-array inputs and
heap/native RocksDB recovery across two nonempty prefixes. Source fixtures enable
object reuse and use three-row batches. Negative checks retain binary DISTINCT
and MIN/MAX planning errors, the MULTISET cardinality generator error and the
formatting/retained-state boundaries. The unchanged released COLLECT variants pass
7/6 cases with their native/fallback contracts; their existing scalar element types
complement the binary-specific runtime-source evidence. Nineteen agent checks pass.

Flink 1.18's padding makes the wide global result exceed the default 2 MiB result
collector, so correctness fixtures give both engines a 16 MiB collector. Benchmark
sinks remain blackholes. Initial failed assertions exhausted the reporter while
printing the full wide changelog; final binary checks compare every ordered row
and report the first difference compactly. Those failed runs are not timing data.

## Nested collection elements

MAP and MULTISET elements share the existing recursive preserving Arrow bridge and
released JVM aggregate handler. These are functionality-enabling JVM-state paths,
not new Rust collection accumulators. [Coverage and state gates](../operators/group-by.md#nested-collection-elements)
retain whole-stock fallback for ARRAY/ROW/STRUCTURED aggregate elements, nested
COLLECT formatting and unverified retained consumers.

The actual previous revision is `5b454fe4`; its queries use whole-plan stock fallback.
Both revisions use the same frozen release/mimalloc library, released Flink artifacts,
identical benchmark source and JDK 17.0.20.1. Each engine/revision receives two warmups
and three alternating measured trials, parallelism one and an 8 GiB heap, on an Intel
i7-12650H / Linux x86-64 host. The matrix contains 240 complete jobs: 96 warmups and
144 measured jobs. No competing Java/Rust process was observed in retained cohorts.

Queries return the key, collection and `SUM(k)+1`. Native trials require consumed work
in the JVM group and the shared pure native projection. Timers retain execution-time
planning/submission, managed keyed state, production JNI, row sources, blackhole row
sinks and both perimeter transposes. Cluster setup and initial explain are outside
timing. There are 5,000 rows/64 keys and 50,000 rows/4,096 keys, with eight values and
a NULL container every eighth input. MAP values contain `v{value}` mapped to the BIGINT
value plus a NULL key/value; STRING MULTISET values contain that key with the integer
value as count and a NULL key with count three. ARRAY_AGG is N/A on Flink 1.18.

Median seconds [minimum–maximum] follow. The [raw report](results/nested-collection-elements-2026-10-28.json)
retains all trials, both revisions' stock controls, sanitized commands, native witnesses,
source/library hashes, correctness results and the closed profiles.

| Flink / aggregate / element | Rows / keys | Current stock, s | Previous SF, s | Current SF, s |
| --- | --- | --- | --- | --- |
| 2.2 / COLLECT / MULTISET | 5,000 / 64 | 0.184 [0.173–0.205] | 0.192 [0.154–0.198] | 0.215 [0.190–0.225] |
| 2.2 / COLLECT / MAP | 5,000 / 64 | 0.149 [0.074–0.157] | 0.145 [0.077–0.166] | 0.100 [0.095–0.178] |
| 2.2 / ARRAY_AGG / MULTISET | 5,000 / 64 | 0.095 [0.090–0.187] | 0.098 [0.088–0.162] | 0.197 [0.121–0.234] |
| 2.2 / ARRAY_AGG / MAP | 5,000 / 64 | 0.094 [0.093–0.106] | 0.094 [0.088–0.121] | 0.126 [0.126–0.193] |
| 2.2 / COLLECT / MULTISET | 50,000 / 4,096 | 0.295 [0.291–0.369] | 0.326 [0.308–0.335] | 0.424 [0.414–0.434] |
| 2.2 / COLLECT / MAP | 50,000 / 4,096 | 0.285 [0.211–0.355] | 0.270 [0.217–0.306] | 0.276 [0.272–0.368] |
| 2.2 / ARRAY_AGG / MULTISET | 50,000 / 4,096 | 0.162 [0.152–0.168] | 0.173 [0.168–0.233] | 0.297 [0.219–0.343] |
| 2.2 / ARRAY_AGG / MAP | 50,000 / 4,096 | 0.169 [0.156–0.171] | 0.171 [0.164–0.171] | 0.232 [0.228–0.234] |
| 1.18 / COLLECT / MULTISET | 5,000 / 64 | 0.172 [0.167–0.180] | 0.173 [0.156–0.192] | 0.204 [0.199–0.264] |
| 1.18 / COLLECT / MAP | 5,000 / 64 | 0.147 [0.078–0.155] | 0.151 [0.078–0.157] | 0.174 [0.095–0.181] |
| 1.18 / COLLECT / MULTISET | 50,000 / 4,096 | 0.310 [0.305–0.312] | 0.294 [0.290–0.326] | 0.391 [0.370–0.448] |
| 1.18 / COLLECT / MAP | 50,000 / 4,096 | 0.220 [0.214–0.354] | 0.205 [0.201–0.216] | 0.295 [0.278–0.366] |

Results are mixed. At 5,000 rows, MAP COLLECT on 2.2 measures 0.100 s versus
0.149 s stock and 0.145 s previous SF. At 50,000 rows it measures 0.276 s versus
0.285 s stock but 0.270 s previous SF. The larger ARRAY_AGG cases lose: MULTISET
measures 0.297 s versus 0.162 s stock / 0.173 s previous, and MAP measures 0.232 s
versus 0.169 / 0.171 s. Both 1.18 element families remain slower than their stock
and previous controls at the larger size. The full ranges above show the limits
of comparing these short jobs.

These short jobs remain startup-sensitive. Slower cases ship under the accepted
collection-groundwork tradeoff; the ranges and separate previous stock controls are
retained rather than pooled. These measurements do not establish sustained throughput.

Exactly two CPU captures close the profiling timebox: MAP COLLECT and MULTISET
ARRAY_AGG on Flink 2.2, each with an eight-second target loop following one unprofiled
native warmup job. Each capture finishes its current complete job. Inclusive sample
counts overlap and include JIT, setup and production callbacks/conversions; they are
not isolated operator timings or allocation-byte measurements.

`collect_nested_map` contains 25,629 samples. Inclusive categories: native_group 0, jvm_handler 1,652, row_arrow 915, owned_string 0, allocation 221, serialization 748, jit 19,123.

`array_agg_nested_multiset` contains 22,968 samples. Inclusive categories: native_group 0, jvm_handler 2,199, row_arrow 1,722, owned_string 0, allocation 216, serialization 1,173, jit 16,313.

JIT dominates both all-thread captures (19,123 and 16,313 samples). Application
scopes still contain the released handler, row/Arrow conversions and serializers;
these captures do not isolate a sustained operator bottleneck or copied bytes.
Replacing that state/emission boundary is separate work, not a per-type tuning fix.

No extra captures or tuning passes follow. Remaining state/emission and expression
costs stay with [#239](https://github.com/datafusion-contrib/StreamFusion/issues/239),
[#392](https://github.com/datafusion-contrib/StreamFusion/issues/392) and native
replacements in [#505](https://github.com/datafusion-contrib/StreamFusion/issues/505).

The final consolidated suites pass 293 checks on 2.2 and 181 on 1.18 with 112
explicit capability skips. The current 2.2 nested fixture passes 34 checks, and the
suite-agent package passes 56 checks with one skip. Documentation builds successfully.

The consolidated suites verify the new gates beside existing binary, structured,
array-key, legacy-consumer and planner contracts. The new MAP/MULTISET fixture
requires values, resolved types, per-key ordered changes and actual JVM group
consumption, including two nonempty checkpoint recoveries on heap/native RocksDB.
Final ARRAY/ROW/STRUCTURED negative cases keep stock execution. The raw report
records rejected row/array prototypes and excludes their failed or contaminated
runs from successful evidence. The unchanged released COLLECT variants pass seven
cases on 2.2 and six on 1.18 with exact native/fallback contracts; those scalar-input
fixtures complement the new nested-element runtime sources.
