# Aggregate specialization fast paths

**Applies to:** the two-phase local aggregate's numeric MIN/MAX, and mini-batch group-aggregate
`DISTINCT` (q15/q16/q17-shaped queries)

Two of the local aggregate's hot leaves were paying for generality their actual input doesn't need:
an insert-only MIN/MAX carrying full retraction support, and a `DISTINCT` accumulator boxing every
probe value into a `ScalarValue`. Specializing each to what its input actually requires turned into
two of the larger single-operator wins in the ledger.

## Append-only local numeric MIN/MAX keeps one running extreme

The two-phase local aggregate had been giving every numeric MIN/MAX group a retractable
`BTreeMap<value, count>`, even though the local half of an insert-only plan can only ever add
values — it never needs to know what to fall back to when the current extreme is retracted.
It now uses the existing scalar running MIN/MAX state when no row-kind column is present;
retracting input, strings, decimals, and the global merge still retain the counted tree and its
delete semantics, since those genuinely need multiset bookkeeping.

Criterion's 4096-row, 64-key MIN/MAX logical bundle rose from **9.50 to 33.89 M rows/s** (**3.57x,
+258%**). A contemporaneous release+mimalloc q17 mini-batch A/B rose from 1.535 to
**1.661 M events/s (+8.2%)**; the immediate path, which does not use the local pre-aggregate,
remained approximately flat at 1.750 versus 1.745 M events/s. The matched 25-second CPU profile
completed 180 iterations versus 163 before and removed the local aggregate's 87-sample tree search,
68-sample tree destruction, and 37-sample aggregate-state destruction leaves; `GroupAggState::accumulate`
fell from 55 to 31 samples. The few remaining tree samples come from the downstream global
aggregate, whose input is retracting partial updates and so still needs the tree.

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
