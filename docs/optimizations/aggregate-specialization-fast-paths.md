# Aggregate specialization fast paths

**Applies to:** insert-only numeric, DATE, TIME and BOOLEAN MIN/MAX; grouped-value state and
immediate changelog output; and mini-batch group-aggregate `DISTINCT` (q15/q16/q17-shaped queries)

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
Retracting SQL controls retain counted state. The port onto the temporal coverage branch passes 567 native tests (one ignored),
91 SQL/exchange checks on Flink 2.2.1, and 74 on Flink 1.18.1 (17 documented host skips).
Downstream integration also passes 587 native tests (one ignored), with 92 temporal/Boolean,
timestamp and grouped-value SQL cases on Flink 2.2.1 and 75 on Flink 1.18.1
(17 documented host limitations skipped).

The temporal branch was measured separately after porting the optimization, with the same
configuration at two million rows. Single-phase DATE/TIME/BOOLEAN Flink/native medians are
0.484/0.346, 0.482/0.335 and 0.451/0.333 s; two-phase medians are 0.589/0.507,
0.557/0.487 and 0.546/0.475 s. Native five-trial ranges are respectively 0.338–0.362,
0.325–0.339, 0.325–0.340, 0.495–0.527, 0.482–0.489 and 0.466–0.476 s.
Flink ranges are 0.470–0.608, 0.438–0.575, 0.432–0.500, 0.580–0.594,
0.546–0.586 and 0.536–0.609 s. All six measured MIN/MAX cases beat their matched controls.

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
  -Dgrouped.value.rows=20000000 \
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

Validation on the downstream integration branch: 587 native tests pass (one ignored). Grouped-value, temporal/Boolean, DISTINCT
average and decimal merge-order SQL checks pass 82 tests on Flink 2.2.1 and 65 on Flink 1.18.1
with 17 documented host-limit skips. These cover retracting output, typed NULLs, cardinality
errors and order-sensitive decimal behavior as well as insert-only cases.

The temporal-coverage branch independently passes 567 native tests (one ignored), 59 focused
SQL checks on Flink 2.2.1 and 42 on Flink 1.18.1 (17 host-limit skips). Its two-million-row
TIME/BOOLEAN first/last medians are 0.605/0.617 s versus matched Flink 0.659/0.637 s,
with the same source and measurement configuration. Native ranges are 0.593–0.675 and
0.607–0.621 s; Flink ranges are 0.639–0.768 and 0.598–0.642 s.

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

Non-windowed DISTINCT sets specialize BIGINT, INT, SMALLINT, TINYINT, DECIMAL, BOOLEAN,
TIMESTAMP and STRING keys. Integer maps retain their exact width; decimal maps store unscaled i128 keys with
precision and scale on the map. STRING probes borrow UTF-8 bytes from the Arrow array in
single-phase, local and global-merge folds. A duplicate does not allocate an owned string;
new live entries and newly journaled elements own their bytes. Equality remains byte exact,
without case folding or Unicode normalization. AHash remains the hash implementation.

CPU profiling of the downstream DISTINCT coverage stack identified repeated one-row timestamp
struct cloning, scalar comparison/construction/hashing, and repeated timestamp vector-layout
lookups at the transpose. The same paths occur in the BOOLEAN/TIMESTAMP_LTZ/DECIMAL(19)
COUNT/SUM workload. The implementation below applies those profiled optimizations to that
narrower coverage independently of wide-decimal merge ordering.

Fixed-width DISTINCT inputs are downcast once per batch and update typed multiplicities directly
in both local and global aggregate stages. Timestamp keys use full-range i128 nanoseconds;
the output remains the existing millisecond-plus-fraction Arrow struct. Local partial views
append typed keys and counts directly into Arrow builders, avoiding intermediate scalar vectors.
The transpose timestamp writer also caches its validated child vectors across rows and resets,
following Comet's per-field writer pattern. These changes preserve the existing Arrow view schema.

All representations share multiplicity updates, last-occurrence deletion and journal handling.
Snapshots and persistent element journals still serialize typed scalar values with the same
encoding. Restore does not journal an already persisted entry; blob import does. An unexpected
value type promotes the map to the generic scalar representation, preserving live counts and
pending journal entries. FLOAT/DOUBLE and complex types retain generic scalar keys and their
existing equality rules. Admission gates are unchanged.

The combined two-phase COUNT DISTINCT BOOLEAN/TIMESTAMP_LTZ(9) plus SUM DISTINCT
DECIMAL(19,2) query now beats the matched Flink control at both measured sizes. These runs
use release+mimalloc, JDK 17, Flink 2.2.1, Intel Core i7-12650H on Linux/WSL, parallelism 1,
an explicit 2 GiB heap, 64 groups, 128 timestamp/decimal values per group, NULL every seventh
row, and 1,024-row bundles. Each comparison has two warmups and five alternating measured
trials. The runtime row source, rowwise blackhole sink, both transposes, and both native
aggregate stages remain in the measured plan. The date is 2026-09-27.

| Rows / implementation | Flink median (range), s | Native median (range), s |
| --- | ---: | ---: |
| 2M, current main optimizations before column specialization | 1.401 (1.368–1.457) | 4.202 (4.096–4.247) |
| 2M, typed column readers and direct partial views | 1.416 (1.374–1.441) | 1.186 (1.179–1.227) |
| 20M, typed column readers and direct partial views | 12.235 (12.082–12.458) | 11.199 (11.156–11.611) |

The two-million-row native elapsed time falls 71.8% from the matched-resource native
baseline. Compared with Flink, native takes 16.2% less time at 2M rows and 8.5% less at 20M.
These measurements concern precision-19 SUM; wider decimals and DISTINCT AVG require their
own parity and performance validation.

Validation passes 570 native tests (one ignored), including timestamp range/nanoseconds,
multiplicity, checkpoint/journal and promoted-key view coverage. The focused DISTINCT SQL
and timestamp accessor suite passes 59 cases on Flink 2.2.1 and 58 on Flink 1.18.1, with
one documented released-host capability skip on 1.18. Cached writer tests cover vector growth
and reset with NULL and negative-epoch values.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=DistinctAggregateBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Ddistinct.rows=2000000 -Ddistinct.warmup=2 -Ddistinct.runs=5 \
  -Dsf.extraJvmArgs=-Xmx2g
# Repeat with -Ddistinct.rows=20000000 for the sustained comparison.
```

The single-phase exact AVG DISTINCT extension uses the same typed membership transitions
with the existing integer/decimal AVG accumulators. A 2M-row TINYINT/BIGINT/DECIMAL(20,2)
comparison after integration measures **2.609 s native (2.596–2.633) versus 6.574 s Flink
(6.554–6.616), 2.519x**. It uses the hardware, release builds, 2 GiB heap, parallelism, warmups,
and alternating trials above, with mini-batching disabled, 64 groups, domains 127/1,024/128,
and NULL every seventh row. Both transposes and the rowwise source/sink remain present.
The pre-feature planner routes this query to Flink. This validates the complete integrated
path; it does not isolate any one optimization's contribution.
At 5M rows with the same configuration, native measures **6.399 s (6.354–6.508) versus
Flink 16.267 s (16.054–16.322), 2.542x**. The native advantage persists as the input grows.

The split AVG DISTINCT extension also benefits from the integrated typed column path. It
uses TINYINT/BIGINT/DECIMAL(19,2), the same domains and NULL distribution, two native stages,
and 1,024-row bundles. With the same explicit 2 GiB heap and repeated-trial method, 2M rows
measure **1.275 s native (1.269–1.405) versus 1.609 s Flink (1.487–1.629)**. At 20M rows,
native measures **12.071 s (12.030–12.261) versus Flink 13.511 s (13.449–13.566)**, taking
20.8% and 10.7% less elapsed time. Both transposes and the rowwise source/sink remain measured.
This resolves the original slower split-average workload; wide-decimal sums/averages retain
their separate ordering and performance requirements. Select this workload with
`-Ddistinct.average=true -Ddistinct.twoPhase=true` on `DistinctAggregateBenchmark`.

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

## Ordered values share the aggregate's inline storage

FIRST_VALUE, LAST_VALUE and SINGLE_VALUE previously allocated a separate ordered-state
object for every aggregate in every group. That object now lives directly in the aggregate
state. On the measured 64-bit build, the enclosing state remains 144 bytes while the
separate 128-byte allocation disappears. This removes one allocation/free pair per group
and saves 64 MB of payload for 500,000 single-aggregate groups, excluding allocator overhead.
Off-heap accounting subtracts the removed allocation while retaining dynamic scalar and
retraction-queue storage. Snapshot encoding, arrival order and deletion behavior are unchanged.

A 500,000-key SINGLE_VALUE profile identified state creation/destruction and allocator work
as major costs. Release+mimalloc measurements on the temporal coverage branch use Flink 2.2.1,
JDK 17, an i7-12650H under Linux/WSL, parallelism one, one row per key, one-eighth NULLs,
both transposes and a rowwise sink. Three warmups precede nine alternating trials per type.
Boxed-state TIME/BOOLEAN native medians were 0.419/0.400 s. Two inline runs measured
0.459/0.380 s and 0.415/0.347 s respectively. The repeat is approximately flat for TIME
and 13% faster for BOOLEAN; the first TIME run was slower and included a 1.759 s outlier.
Matched repeat Flink medians were 0.363/0.332 s, so this change does not resolve the
remaining SINGLE_VALUE performance gap. These short jobs remain sensitive to runtime noise.

Nine-trial ranges for boxed/inline-repeat native TIME were 0.375–0.466/0.365–0.489 s;
BOOLEAN ranges were 0.381–0.448/0.340–0.439 s. Flink ranges for those paired runs were
0.349–0.637/0.345–0.605 s (TIME) and 0.324–0.510/0.313–0.368 s (BOOLEAN).

Two-million-row FIRST_VALUE/LAST_VALUE controls (two warmups, five alternating trials)
remain approximately flat: boxed/inline native TIME 0.613/0.603 s, BOOLEAN 0.616/0.624 s,
BIGINT 0.676/0.674 s and STRING 1.028/1.025 s. Matched inline Flink medians were
0.648, 0.618, 0.654 and 0.926 s. Native inline ranges were 0.599–0.655, 0.616–0.631,
0.671–0.688 and 1.012–1.033 s respectively. The existing BIGINT/STRING controls remain
slower than Flink. The memory-budget regression covers SINGLE_VALUE and retracting FIRST/LAST
alongside COUNT, including over-budget rejection and full release after group deletion.

Validation: 567 native tests pass (one ignored), including the expanded memory-budget case.
The grouped-value, temporal/Boolean and columnar aggregate SQL controls pass 59 cases on
Flink 2.2.1 and 42 on Flink 1.18.1 (17 documented host limitations skipped).

## Single-result SINGLE_VALUE emits directly from its accumulator

For one unfiltered SINGLE_VALUE aggregate, the accumulator already contains the complete
result. Immediate changelog output now emits that scalar directly, avoiding a temporary
one-element tuple vector and a duplicate cached result per live group. A later touch
reconstructs the preceding tuple from the accumulator before applying the row. NULL counting,
cardinality errors, deletes, TTL and snapshot encoding are unchanged. Mixed aggregates and
filtered SINGLE_VALUE keep the existing cache; filtered groups can receive many rows that
do not contribute to their result. Mini-batch handling is unchanged.

The high-cardinality CPU profile identified state creation/destruction and allocator work.
On the measured 64-bit build, removing the cached scalar avoids 64 bytes per live group
(32 MB for 500,000 groups), plus retained variable-width payloads. Output column buffers
remain in the measured path.

Measurements use release+mimalloc, Flink 2.2.1, JDK 17, i7-12650H Linux/WSL, parallelism one,
one unique key per row, one-eighth NULLs, both transposes and a rowwise sink. Five warmups
precede nine alternating trials. Both engines and both native builds use a 2 GB Java heap
on the 7.6 GB machine. Each cell gives median and trial range in seconds.

| Workload | Native before | Native after | Flink before | Flink after |
|---|---:|---:|---:|---:|
| 500k / TIME | 0.382 (0.334–0.486) | 0.362 (0.333–0.415) | 0.353 (0.310–0.513) | 0.336 (0.296–0.521) |
| 500k / BOOLEAN | 0.363 (0.343–0.463) | 0.365 (0.323–0.381) | 0.343 (0.308–0.411) | 0.337 (0.281–0.517) |
| 500k / STRING | 0.470 (0.421–0.522) | 0.393 (0.369–0.499) | 0.445 (0.387–0.553) | 0.422 (0.390–0.566) |
| 1m / TIME | 0.678 (0.620–0.761) | 0.629 (0.574–0.796) | 0.676 (0.556–0.856) | 0.673 (0.570–0.859) |
| 1m / BOOLEAN | 0.668 (0.641–0.732) | 0.628 (0.589–0.710) | 0.682 (0.550–0.744) | 0.687 (0.579–0.738) |

The one-million-key native medians improve 7.2% for TIME and 5.9% for BOOLEAN and beat their
matched Flink controls. At 500,000 keys, STRING improves 16.4% and beats Flink; TIME improves
5.2%, while BOOLEAN is flat. At this earlier stage, TIME/BOOLEAN at that smaller size trailed Flink by about 8%.
The shared-boundary measurements below revisit that gap.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=GroupedValueBenchmark -Dsf.extraJvmArgs=-Xmx2g \
  -Dgrouped.value.single=true -Dgrouped.value.types=TIME,BOOLEAN \
  -Dgrouped.value.rows=1000000 -Dgrouped.value.warmup=5 -Dgrouped.value.runs=9
```

The earlier default-heap run allowed 8 GB on this 7.6 GB machine. It completed the
one-million-key TIME trials at native 0.760 s versus Flink 0.656 s, then failed on a
TaskManager heartbeat timeout before completing BOOLEAN. No OOM was observed; memory
pressure is a suspected cause, not a confirmed diagnosis. Both comparison builds complete
with the same 2 GB cap. Earlier 500,000-key default-heap direct-emission medians were native
TIME/BOOLEAN 0.366/0.353 s versus Flink 0.395/0.334 s, retaining the unfavorable BOOLEAN result.

FIRST_VALUE/LAST_VALUE controls retain their existing cache. An initial two-warmup TIME
control appeared about 5% slower, so a matched five-warmup/nine-trial repeat was run.
At two million rows, baseline/final native TIME medians are 0.627/0.629 s (ranges
0.623–0.644/0.625–0.750 s); BOOLEAN is 0.617/0.611 s (0.607–0.628/0.606–0.613 s).
These are approximately flat. Matched final Flink medians are 0.631/0.605 s, so these
short workloads are approximately tied. The initial BIGINT/STRING native controls
were 0.668/1.021 s versus earlier baseline 0.674/1.025 s; their Flink controls were
0.619/0.914 s, retaining their existing native gap. These low-cardinality controls use
the unchanged default heap setting because their retained keyed state is small.

An independent final-build one-million-key repeat measured native TIME/BOOLEAN at
0.658/0.625 s (ranges 0.588–0.720/0.611–0.742 s), versus matched Flink
0.713/0.711 s (0.641–0.933/0.584–0.831 s). This confirms the benefit at that size
without changing the then-unfavorable 500,000-key result.

Validation: the final path passes 567 native tests (one ignored), 59 grouped-value/temporal/
columnar SQL controls on Flink 2.2.1, and 42 on Flink 1.18.1 (17 documented host skips).

## Shared boundary ownership improvements

The row/Arrow boundaries now [write directly into owned buffers and avoid a redundant exit
copy](row-major-transpose.md#direct-writes-at-the-streaming-boundary). Buffer allocation and
finalization remain timed exactly; sampled write timing removes the per-row clock bottleneck.
These changes preserve the typed accumulators, snapshots, filters and retraction rules above.

The following final-branch measurements use release+mimalloc, Flink 2.2.1/JDK 17,
i7-12650H Linux/WSL, parallelism one, a matched 2 GiB heap, both transposes and a rowwise sink.
SINGLE_VALUE uses one unique key per input row and one-eighth NULLs, with five warmups and
nine alternating trials. Each cell gives median and full trial range in seconds.

| SINGLE_VALUE workload | Native | Flink | Flink/native |
| --- | ---: | ---: | ---: |
| 500k / TIME | 0.373 (0.340–0.473) | 0.402 (0.321–0.449) | 1.077x |
| 500k / BOOLEAN | 0.328 (0.302–0.497) | 0.363 (0.333–0.451) | 1.106x |
| 500k / STRING | 0.409 (0.346–0.474) | 0.412 (0.375–0.503) | 1.007x |
| 1000k / TIME | 0.610 (0.590–0.738) | 0.719 (0.585–0.886) | 1.180x |
| 1000k / BOOLEAN | 0.632 (0.589–0.715) | 0.718 (0.611–0.795) | 1.137x |

An independent 500k-key repeat, after the MIN/MAX controls, measured:

| SINGLE_VALUE workload | Native | Flink | Flink/native |
| --- | ---: | ---: | ---: |
| 500k / TIME | 0.340 (0.321–0.407) | 0.401 (0.328–0.575) | 1.181x |
| 500k / BOOLEAN | 0.333 (0.315–0.380) | 0.365 (0.301–0.568) | 1.097x |

The new TIME/BOOLEAN medians beat their matched Flink controls at both cardinalities and in the
smaller-workload repeat. Compared with the earlier 500k direct-emission native medians
(0.362/0.365 s), the repeat reaches 0.340/0.333 s. The first TIME run was 0.373 s, and host
controls also moved, so the full ranges and both runs remain visible rather than attributing
every difference to the code change. The STRING control is effectively tied with Flink in this
run. Existing unsupported-mode gates remain unchanged.

FIRST_VALUE/LAST_VALUE at 2M rows and 64 keys use the same 2 GiB cap and five-warmup,
nine-trial method:

| FIRST/LAST workload | Native | Flink | Flink/native |
| --- | ---: | ---: | ---: |
| 2000k / TIME | 0.573 (0.564–0.589) | 0.613 (0.605–0.623) | 1.069x |
| 2000k / BOOLEAN | 0.551 (0.545–0.563) | 0.573 (0.559–0.583) | 1.039x |

Fresh MIN/MAX controls use 2M rows, 64 keys, two warmups and five alternating trials;
two-phase cases retain 1,024-row bundles:

| Phase / type | Native median (range), s | Flink median (range), s | Flink/native |
| --- | ---: | ---: | ---: |
| one / DATE | 0.352 (0.343–0.385) | 0.434 (0.421–0.444) | 1.232x |
| one / TIME | 0.346 (0.338–0.352) | 0.429 (0.425–0.440) | 1.240x |
| one / BOOLEAN | 0.340 (0.336–0.345) | 0.417 (0.402–0.431) | 1.226x |
| two / DATE | 0.462 (0.462–0.470) | 0.579 (0.566–0.583) | 1.252x |
| two / TIME | 0.441 (0.431–0.453) | 0.557 (0.548–0.559) | 1.262x |
| two / BOOLEAN | 0.437 (0.429–0.440) | 0.548 (0.534–0.561) | 1.255x |

All six current MIN/MAX controls and both FIRST/LAST controls beat their matched Flink medians.
These measurements cover the listed schemas, sizes and cardinalities, including their unfavorable
historical results above; they are not a claim about every aggregation workload.

The final merged-base tree passes 88 focused grouped-aggregate, transpose and ownership checks
on Flink 2.2.1; Flink 1.18.1 passes 71 with the same 17 documented host-capability skips.
The native source is unchanged from the 567-test validated direct-emission implementation;
a forced release rebuild reproduces its saved library hash. Hosted CI must validate the exact
PR head before merge.
