# Aggregate specialization fast paths

**Applies to:** insert-only numeric, DATE, TIME and BOOLEAN MIN/MAX, immediate grouped output,
and mini-batch group-aggregate `DISTINCT` (q15/q16/q17-shaped queries)

Two of the local aggregate's hot leaves were paying for generality their actual input doesn't need:
an insert-only MIN/MAX carrying full retraction support, and a `DISTINCT` accumulator boxing every
probe value into a `ScalarValue`. Specializing each to what its input actually requires turned into
two of the larger single-operator wins in the ledger.

## Insert-only fixed-width MIN/MAX keeps one running extreme

An insert-only MIN/MAX needs one extreme per group, not a counted tree of every distinct
value. The local aggregate uses this specialization when its input has no row-kind column.
For single-phase and global aggregates, the planner's existing insert-only proof selects
running MIN/MAX kinds 10/11. Numeric, DATE, millisecond TIME and BOOLEAN values use this
path; retracting inputs and other value types retain their existing multiset semantics.
Typed Arrow reads avoid per-row scalar construction. DATE, TIME and BOOLEAN running state
retains the declared SQL type, including all-NULL groups and checkpoint restore.

The original numeric specialization raised Criterion's 4096-row, 64-key MIN/MAX bundle from
9.50 to 33.89 M rows/s (3.57x). A contemporaneous release+mimalloc q17 mini-batch A/B rose
from 1.535 to 1.661 M events/s (+8.2%). A later DATE/TIME/BOOLEAN profile found 1,163 samples
under local update, including 177 multiset updates, 137 tree searches and 60 tree insertions.
Removing those local operations improved two-million-row native DATE/TIME/BOOLEAN jobs from
0.665/0.647/0.589 s to 0.517/0.492/0.492 s, with single-destination exchange forwarding enabled.

Extending typed running state to the insert-only single-phase path removes its remaining
multiset overhead. At twenty million rows, native DATE/TIME/BOOLEAN medians fell from
4.154/4.179/3.804 s with only the local specialization to 2.861/2.847/2.807 s, reductions of
31%, 32% and 26%. Matched final measurements follow; each cell gives median and five-trial range:

| Phase / type | Flink (s) | Native (s) |
|---|---:|---:|
| Single / DATE | 3.835 (3.783–3.959) | 2.861 (2.837–2.865) |
| Single / TIME | 3.860 (3.823–3.932) | 2.847 (2.836–2.862) |
| Single / BOOLEAN | 3.721 (3.648–3.806) | 2.807 (2.775–2.860) |
| Two / DATE | 5.070 (5.003–5.211) | 4.544 (4.508–4.747) |
| Two / TIME | 4.943 (4.831–5.034) | 4.330 (4.303–4.352) |
| Two / BOOLEAN | 4.749 (4.643–4.950) | 4.263 (4.238–4.282) |

Configuration: release+mimalloc, Intel Core i7-12650H Linux/WSL, JDK 17, Flink 2.2.1,
2026-09-27, parallelism one, 64 groups, one-eighth NULLs, 4,096 DATE/TIME values, two warmups
and five alternating measured trials. Both transposes and the rowwise sink are included;
two-phase bundles contain 1,024 rows and local zero-copy transport is disabled.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=TimestampExtremaBenchmark -Dextrema.types=DATE,TIME,BOOLEAN \
  -Dextrema.rows=20000000 -Dextrema.warmup=2 -Dextrema.runs=5
```

At two million rows, final single-phase Flink/native medians were 0.565/0.369 s (DATE),
0.500/0.345 s (TIME), and 0.487/0.407 s (BOOLEAN); two-phase medians were 0.654/0.566,
0.585/0.544 and 0.632/0.568 s. The twenty-million-row two-phase results remain broadly flat
relative to the local-only optimization; this extension primarily improves single-phase jobs.
These measurements cover MIN/MAX, not every temporal aggregate function or parallelism.

Regression tests import both typed running snapshots and previous multiset snapshots, then
continue folding lower and higher values across restored partitions. DATE and BOOLEAN use
the existing direct RocksDB row codec; TIME retains its existing raw snapshot fallback.
Retracting SQL controls retain counted state. The native suite passes 587 tests (one ignored),
and the temporal/Boolean, timestamp and grouped-value SQL suites pass 92 tests on Flink 2.2.1
and 75 on Flink 1.18.1 (17 documented host limitations skipped).

## Immediate changelog output shares the tuple allocation with its cache

The immediate aggregate constructs its current tuple to compare with the previous output.
Previously, emitting a change cloned that whole vector into the last-output cache, then moved
its cells into column output buffers. It now clones cells directly into those buffers and moves
the existing tuple vector into the cache. This removes one temporary vector allocation and
free per emitted insert/update, retaining the same scalar copies, previous-value ownership,
NULLs, update-before policy and TTL-driven emissions. Mini-batch output is unchanged.

A twenty-million-row TIME/BOOLEAN FIRST_VALUE/LAST_VALUE CPU profile found 628 leaf samples
in vector cloning under aggregate update, alongside 392 scalar reads and 442 scalar-size calls.
The optimization targets the vector allocation identified by that profile. Timings below use
unprofiled release+mimalloc jobs, both row/Arrow transposes and a rowwise sink.

Twenty-million-row FIRST_VALUE/LAST_VALUE medians and five-trial ranges:

| Type / engine | Before (s) | After (s) |
|---|---:|---:|
| TIME native | 6.243 (6.222–6.282) | 5.474 (5.437–5.582) |
| TIME Flink | 5.935 (5.863–6.116) | 5.885 (5.869–5.947) |
| BOOLEAN native | 5.869 (5.857–5.877) | 5.436 (5.386–5.461) |
| BOOLEAN Flink | 5.641 (5.600–5.710) | 5.620 (5.563–5.789) |

Native time falls 12.3% for TIME and 7.4% for BOOLEAN, while the Flink controls remain nearly
flat. An independent optimized run measured native TIME/BOOLEAN at 5.650/5.497 s versus
Flink 5.784/5.615 s. At two million rows, optimized TIME/BOOLEAN medians are 0.655/0.630 s
versus Flink 0.658/0.638 s; the smaller workloads are approximately tied.

Configuration: Intel Core i7-12650H Linux/WSL, JDK 17, Flink 2.2.1, 2026-09-27,
release+mimalloc, 64 groups, one-eighth NULLs, parallelism one, two warmups and five
alternating measured trials. The source, SQL and exchange settings are unchanged.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=GroupedValueBenchmark -Dgrouped.value.types=TIME,BOOLEAN \
  -Dgrouped.value.twoPhase=false -Dgrouped.value.rows=20000000 \
  -Dgrouped.value.warmup=2 -Dgrouped.value.runs=5
```

Existing first/last controls at two million rows show BIGINT essentially flat (native
0.682 → 0.680 s, ranges 0.670–0.700 → 0.678–0.684 s) and STRING improving
(1.073 → 1.025 s, ranges 1.058–1.078 → 1.016–1.044 s). Flink BIGINT controls are
0.599 → 0.636 s and STRING controls 0.955 → 0.930 s. These existing workloads still
trail Flink on this host; the shared change does not establish a general first/last speedup.

SINGLE_VALUE uses one key per row to obey its cardinality contract. An initial 500,000-row
five-trial check had broad overlapping ranges, including a 5% higher BOOLEAN native median.
A longer control with three warmups and nine alternating trials did not reproduce that
regression: native TIME/BOOLEAN medians changed from 0.495/0.463 to 0.422/0.416 s.
Native ranges were 0.407–0.535/0.418–0.531 s before and 0.394–0.480/0.389–0.494 s after.
Flink controls changed from 0.370/0.315 to 0.354/0.348 s (after ranges
0.302–0.530/0.324–0.371 s). Despite the native improvement, SINGLE_VALUE remains slower
than Flink and is a separate performance blocker. The first/last improvement does not
establish readiness for all temporal aggregate functions.

Validation: 587 native tests pass (one ignored). Grouped-value, temporal/Boolean, DISTINCT
average and decimal merge-order SQL checks pass 82 tests on Flink 2.2.1 and 65 on Flink 1.18.1
with 17 documented host-limit skips. These cover retracting output, typed NULLs, cardinality
errors and order-sensitive decimal behavior as well as insert-only cases.

## Group-aggregate DISTINCT folds primitives; the changelog emit reads its cache

The multi-`DISTINCT` day/channel [GROUP BY](../operators/group-by.md) aggregates (q15/q16/q17)
owned the largest native islands, and their hot leaves were `ScalarValue` construct/hash/clone/drop:
every row built a scalar per distinct agg call just to probe the distinct sets, and each emit
materialized the group's full output tuple twice — the pre-update value for the changelog `-U`, the
post-update value for the `+U`.

Distinct sets are now typed — a BIGINT distinct column keys a plain `i64` map read straight off the
array, no scalar involved — and each group caches its last-emitted tuple, so the pre-update value is
a take-from-cache (recomputed only after restore) and the `-U` moves it out instead of cloning it.
The emit protocol stays byte-identical, including the unchanged-result suppression Flink itself
applies. Measured on the generator profile loop: **q16 +17%, q17 +4%, q15 +3%** — q16, long the
floor of the Parquet/Kafka tables, gains the most. The cached tuple's own size accounting is covered
on [Memory accounting designed off the hot path](memory-accounting-off-hot-path.md).

## Typed DISTINCT multiplicities

Non-windowed DISTINCT sets specialize BIGINT, INT, SMALLINT, TINYINT, DECIMAL and STRING
keys. Integer maps retain their exact width; decimal maps store unscaled i128 keys with
precision and scale on the map. STRING probes borrow UTF-8 bytes from the Arrow array in
single-phase, local and global-merge folds. A duplicate does not allocate an owned string;
new live entries and newly journaled elements own their bytes. Equality remains byte exact,
without case folding or Unicode normalization. AHash remains the hash implementation.

All representations share multiplicity updates, last-occurrence deletion and journal handling.
Snapshots and persistent element journals still serialize typed scalar values with the same
encoding. Restore does not journal an already persisted entry; blob import does. An unexpected
value type promotes the map to the generic scalar representation, preserving live counts and
pending journal entries. FLOAT/DOUBLE and complex types retain generic scalar keys and their
existing equality rules. Admission gates are unchanged.

`typed_distinct` in the native operator benchmark measures single-phase and local COUNT DISTINCT
for all specialized types with input-domain sizes 4 and 256 per group, 16 groups and one-seventh
NULLs. The BIGINT cases are controls for the existing specialization; strings use 8- and
256-byte keys. `TypedDistinctBenchmark` measures complete Flink jobs with a row source, both
Arrow transposes and a rowwise blackhole sink. It can vary per-group cardinality, NULL frequency
and group skew through `distinct.cardinality`, `distinct.nullEvery` and `distinct.skew`.

Release+mimalloc diagnostic on an Intel Core i7-12650H under Linux/WSL, 2026-09-27:
4,096-row batches, 0.3-second warmup, one-second measurement and 10 Criterion samples per
case. A longer BIGINT control used one-second warmup, three-second measurement and 30
samples. The table reports baseline/optimized median time, so values above 1 mean higher
throughput. Single-phase includes changelog construction; local includes distinct-view output.

| Key type | Single, 4 values | Single, 256 values | Local, 4 values | Local, 256 values |
|---|---:|---:|---:|---:|
| BIGINT | 1.015x | 1.001x | 0.980x | 1.030x |
| INT | 1.126x | 1.108x | 1.195x | 1.223x |
| SMALLINT | 1.127x | 1.103x | 1.204x | 1.236x |
| TINYINT | 1.126x | 1.100x | 1.221x | 1.236x |
| DECIMAL | 1.116x | 1.110x | 1.220x | 1.269x |
| STRING8 | 1.514x | 1.209x | 2.179x | 1.389x |
| STRING256 | 1.473x | 1.110x | 1.972x | 1.093x |

The initial 10-sample BIGINT local/high-cardinality result was 2.5% slower; the longer control
did not reproduce that regression. The small-cardinality local control remained about 2% slower,
within Criterion's noise threshold. These controls do not establish a BIGINT speedup.

Run the baseline command on the pre-specialization code with the same benchmark harness:

```sh
cargo bench --manifest-path native/Cargo.toml -p streamfusion --features mimalloc \
  --bench operators -- typed_distinct --warm-up-time 0.3 --measurement-time 1 \
  --sample-size 10 --save-baseline scalar
# Run the same harness after applying the specialization, replacing
# --save-baseline scalar with --baseline scalar.
```

Complete-job measurements use Flink 2.2.1, JDK 17, two million rows, parallelism 1, two warmups
and five measured trials in alternating Flink/native order. Each query computes one
COUNT(DISTINCT) per key; the two-phase bundle size is 1,024. The high-cardinality case has
64 uniform keys, 4,096 input values per key and one-seventh NULLs. The low-cardinality case has
four input values, approximately 90% of records on key zero and one-third NULLs. STRING8 and
STRING256 append 8- and 256-character suffixes to the numeric value. Both Arrow transposes
are asserted in each measured native plan; two-phase plans also assert the native local operator.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=TypedDistinctBenchmark -Ddistinct.rows=2000000 -Ddistinct.cardinality=4096 \
  -Ddistinct.warmup=2 -Ddistinct.runs=5
# Repeat with -Ddistinct.cardinality=4 -Ddistinct.skew=true -Ddistinct.nullEvery=3.
```

**High-cardinality complete jobs** (median seconds):

| Type | Phase | Flink control, baseline → comparison | Native scalar → typed | Native throughput ratio |
|---|---|---:|---:|---:|
| BIGINT | one | 0.725 → 0.655 | 0.521 → 0.518 | 1.006x |
| BIGINT | two | 0.757 → 0.708 | 0.723 → 0.705 | 1.025x |
| INT | one | 0.660 → 0.627 | 0.641 → 0.501 | 1.280x |
| INT | two | 0.692 → 0.629 | 0.961 → 0.769 | 1.249x |
| DECIMAL | one | 1.009 → 0.910 | 0.696 → 0.593 | 1.173x |
| DECIMAL | two | 0.967 → 0.904 | 1.205 → 1.092 | 1.103x |
| STRING8 | one | 1.021 → 1.004 | 0.723 → 0.641 | 1.127x |
| STRING8 | two | 1.149 → 1.114 | 1.335 → 1.050 | 1.272x |
| STRING256 | one | 1.688 → 1.624 | 1.931 → 1.728 | 1.118x |
| STRING256 | two | 2.691 → 2.586 | 2.595 → 2.301 | 1.128x |

**Low-cardinality complete jobs** (median seconds):

| Type | Phase | Flink control, baseline → comparison | Native scalar → typed | Native throughput ratio |
|---|---|---:|---:|---:|
| BIGINT | one | 0.511 → 0.504 | 0.375 → 0.375 | 1.000x |
| BIGINT | two | 0.602 → 0.605 | 0.535 → 0.538 | 0.995x |
| INT | one | 0.501 → 0.468 | 0.375 → 0.373 | 1.005x |
| INT | two | 0.527 → 0.521 | 0.587 → 0.561 | 1.046x |
| DECIMAL | one | 0.528 → 0.516 | 0.461 → 0.465 | 0.991x |
| DECIMAL | two | 0.567 → 0.566 | 0.684 → 0.681 | 1.003x |
| STRING8 | one | 0.556 → 0.549 | 0.555 → 0.570 | 0.973x |
| STRING8 | two | 0.722 → 0.720 | 0.804 → 0.755 | 1.064x |
| STRING256 | one | 0.842 → 0.871 | 1.222 → 1.142 | 1.070x |
| STRING256 | two | 1.024 → 1.024 | 1.162 → 1.095 | 1.061x |

High-cardinality native throughput improves by 10–28% (9–22% lower elapsed time) for the new
specializations. Low-cardinality
jobs gain less: the single-phase short-string case is about 3% slower, while long strings and
some two-phase cases improve. The complete-job BIGINT controls are essentially flat or slightly
faster. Stock-Flink timings also vary between runs, so the tables include both control medians;
small differences should not be interpreted as general speedups. Several native two-phase and
long-string jobs remain slower than Flink despite improving over the previous native path.

The specialization is retained for its repeatable native-loop gains and substantial gains on
higher-cardinality complete jobs, while preserving shared state contracts and admission gates.
SQL regressions compare resolved types, complete single-phase changelogs, local/global
materialization, filtered state, duplicate retractions, group recreation and integer SUM DISTINCT
overflow on released Flink 2.2.1 and 1.18.1. Native tests cover typed snapshots, journal imports,
RocksDB checkpoint/reopen, final-occurrence deletion and exact string bytes; FLOAT/DOUBLE NaN
regressions retain their existing generic-key behavior.

## DISTINCT averages, wide decimals, and timestamp membership

The PR #272/#275 workloads were correct but slower than stock Flink. Reproducing their
unchanged release benchmark before optimizing gave native medians of 4.855 s for the
wide-decimal COUNT/SUM query, 2.265 s for split DECIMAL(19,2) AVG, and 2.500 s for split
DECIMAL(38,2) AVG. The corresponding Flink controls were 1.421, 1.565, and 1.562 s.

Async-profiler CPU sampling (`ctimer`, 1 ms, DWARF native stacks) identified several costs:

- Timestamp DISTINCT keys were one-row Arrow structs inside `ScalarValue`. Local updates
  and global merges repeatedly sliced, cloned, hashed, compared, and dropped these structs.
  The original COUNT/SUM profile contained 6,741 samples under the native JNI calls;
  its largest named leaf was `StructArray::clone` (317 samples), alongside scalar equality
  (235), datatype cloning (189), scalar construction (138), and scalar hashing (124).
- The AVG profile likewise showed scalar comparison, construction, and hashing in the
  local membership maps. This work did not require a general-purpose scalar representation.
- After specializing the keys, timestamp writing at the input transpose accounted for
  620 of 3,275 samples under native operator call stacks. It repeatedly reconstructed the
  Arrow field description and looked up the same two child vectors for every row.
- Local view serialization built temporary vectors of scalar entries. The host-order model
  also allocated two temporary index vectors for every occupied bucket during resize.

The existing typed-map optimization is now integrated with the DISTINCT coverage stack.
Typed column readers are selected once per input batch, and local/global folds share the
same multiplicity update. Boolean keys use booleans; component timestamps use full-range
`i128` nanoseconds, preserving all of Flink's `i64` milliseconds and fractional nanos.
Local view builders append directly to Arrow buffers. General or promoted key types keep
the scalar reconstruction/cast path. Checkpoints and persistent element journals still
encode the same typed scalars; no stored-state format changes.

The timestamp writer retains a validated accessor and child **vector objects**, following
Comet's fixed-vector writer pattern. It does not cache buffer addresses, so vector growth
and reset remain safe. Both rowwise transposes stay in the measured production path.
The Java HashMap ordering model partitions existing node links during resize instead of
allocating per-bucket vectors. Bucket order, collision trees, decimal copy order, and the
order-sensitive overflow semantics are preserved.

The regression suite includes full-range timestamps, one-nanosecond distinctions, NULLs,
last-occurrence retractions, generic key promotion, memory snapshots and RocksDB reopen,
writer growth/reset, and the released-host decimal/group ordering fixtures.

For profiling, build and compile the benchmark normally, then run the existing Surefire
execution directly to avoid rebuilding between measurements:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test-compile -Pbench \
  -DskipTests -Dmaven.javadoc.skip=true
SF_BENCHMARK=true mvn -pl streamfusion-runtime surefire:test@default-test -Pbench \
  -Dtest=DistinctAggregateBenchmark -Ddistinct.precision=38 \
  "-Dsf.extraJvmArgs=-agentpath:${ASYNC_PROFILER_HOME}/lib/libasyncProfiler.so=start,event=ctimer,interval=1ms,cstack=dwarf,file=distinct-%p.collapsed"
```

Use `-Ddistinct.average=true -Ddistinct.twoPhase=true` for AVG and precision 19 or 38.
Omit the profiler argument for timing runs. `%p` keeps separate JVMs from overwriting the
same profile. Sampling counts above identify bottlenecks, not benchmark speedups; the
unprofiled, alternating-engine trials establish performance.

The final unprofiled comparison uses the same Intel Core i7-12650H, Linux/WSL, JDK 17,
Flink 2.2.1, release+mimalloc build, two million runtime rows, 64 keys, 1,024-row bundles,
two warmups, and five measured runs in alternating engine order. No compiler or other
benchmark runs concurrently. The source, sink, transpose assertions, exchange configuration,
and benchmark queries are unchanged.

| Workload | Flink before → after (s) | Native before → after (s) | Native speedup | Flink / optimized native |
|---|---:|---:|---:|---:|
| COUNT/SUM, DECIMAL(38,2) | 1.421 → 1.466 | 4.855 → 1.903 | 2.552x | 0.771x |
| AVG, DECIMAL(19,2) | 1.565 → 1.653 | 2.265 → 1.680 | 1.349x | 0.984x |
| AVG, DECIMAL(38,2) | 1.562 → 1.560 | 2.500 → 1.965 | 1.272x | 0.794x |

Measured elapsed-time ranges after optimization:

| Workload | Flink range (s) | Native range (s) |
|---|---:|---:|
| COUNT/SUM, DECIMAL(38,2) | 1.414–1.593 | 1.878–1.926 |
| AVG, DECIMAL(19,2) | 1.585–1.754 | 1.655–1.705 |
| AVG, DECIMAL(38,2) | 1.558–1.805 | 1.937–2.007 |

These are substantial improvements over the previous native implementation, **not a demonstrated
win over stock Flink**. Narrow AVG is approximately tied within run-to-run variation; both wide
decimal queries still trail Flink. The final wide-AVG profile has 2,575 samples under native
operator stacks, including 1,455 under JNI. Membership updates (210 leaf samples), hash-table
growth (102), and decimal order insertion (85) lead the remaining native work. Columnar exchange,
IPC, and buffer handling also contribute. The coverage stack still needs further optimization
or an explicit maintainer decision on these measured tradeoffs before shipping it as acceleration.

These timings isolate the typed-membership changes. The subsequent
[single-destination exchange fast path](native-columnar-exchange.md) reduces the two-million-row
native medians further to 1.556 s for wide COUNT/SUM, 1.499 s for narrow AVG, and 1.673 s for wide
AVG. Narrow AVG then beats its matched Flink control; wide decimal aggregation still trails it.
At twenty million rows, wide AVG improves from 18.737 s to 15.938 s, versus a 13.683 s Flink control.

Validation: 584 native tests pass (one ignored). The selected SQL/operator suite passes all
122 cases on released Flink 2.2.1 and 120 on released Flink 1.18.1, with two existing
host-capability skips (state-TTL hints and session-window DISTINCT). After the final ordering
and generic-view changes, 36 focused checks pass on 2.2.1, including NaN payloads, decimal
and group-map ordering, typed views, and timestamp writer growth/reset. Native formatting
and workspace/JNI boundaries also pass.
