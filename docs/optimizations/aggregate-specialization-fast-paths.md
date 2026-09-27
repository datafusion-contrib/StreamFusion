# Aggregate specialization fast paths

**Applies to:** insert-only numeric, DATE, TIME and BOOLEAN MIN/MAX, and mini-batch group-aggregate
`DISTINCT` (q15/q16/q17-shaped queries)

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
