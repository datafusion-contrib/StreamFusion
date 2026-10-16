# GROUP BY

**Status:** Native for non-windowed `GROUP BY`, both as the single-phase (immediate) plan and the
two-phase mini-batch plan, over the aggregate/value-type combinations in [Type
support](#type-support) below — with a real, enumerated gap list, see [Still falls
back](#still-falls-back).

Flink picks between the two plan shapes itself, based on `table.exec.mini-batch.enabled`; this page
covers both, since a query only accelerates when whichever shape Flink chose is fully native.

## Single-phase

Flink's internal `SUM0` admits nullable TINYINT, SMALLINT, INT, BIGINT and DECIMAL,
including global aggregation and per-aggregate FILTER. This lets shared `COUNT(v)`,
`AVG(v)` and `SUM(v)` reductions keep their native aggregate and outer Calc. SUM0 starts
at zero and emits zero when no non-NULL input contributes; the host-planned Calc uses
COUNT to restore NULL for AVG/SUM. Integer accumulation wraps at the declared width.
DECIMAL uses a widened `(38, input-scale)` accumulator whose overflow stays NULL;
ordinary SUM's reset-after-overflow rule is deliberately different. DISTINCT and
floating SUM0 remain unsupported. Released Flink's invalid DECIMAL(38,38) SUM0
initializer remains on the host.

COUNT/SUM DISTINCT uses Java boxed floating equality: all NaN payloads count as one value,
while positive and negative zero remain distinct. The same encoding is used for local/global
merges, retractions, and persistent distinct-element lookups. Primitive FLOAT/DOUBLE GROUP BY
keys retain Flink's raw-bit-sensitive equality; DISTINCT normalization does not change those keys.

Floating elements written inside ARRAY keys canonicalize NaN payloads, matching Flink's
BinaryArray encoding. Arrays differing only in those payload bits form one group; signed-zero
elements remain distinct. This also applies to arrays nested in composite keys.

The immediate plan applies every input row to the keyed accumulator state and emits on every
change — no batching. `SUM`/`MIN`/`MAX`/`COUNT` are native over `DECIMAL` (`SUM` →
`DECIMAL(38, s)` with overflow → NULL; `MIN`/`MAX` → `DECIMAL(p, s)`; carried as an i128 at scale
`s`, matching Flink).

Decimal SUM overflow makes the accumulator NULL. The next non-NULL input starts it again
at that value; a retraction after overflow starts it at the negated value. NULL inputs leave
the accumulator unchanged. This rule also applies after restore and to filtered SUMs.
Decimal AVG has a separate accumulator whose overflow stays NULL.

Single-phase `AVG(DISTINCT)` admits TINYINT, SMALLINT, INT, BIGINT and DECIMAL. A value folds
into the existing widened AVG accumulator only on its first occurrence and retracts only
when its last occurrence leaves the distinct set. The denominator counts distinct non-NULL
values. Integer results retain their input width and truncate toward zero; decimal results
retain Flink's DECIMAL(38, max(6, input-scale)) type and rounding. FILTER, changelog inputs,
empty/all-NULL groups, state TTL, and checkpoint restoration use the existing grouped path.
The running sum and distinct count are checkpointed directly alongside membership so restore
does not refold an overflow-sensitive decimal sum in map order. Both memory and typed RocksDB
state retain this contract, including migrations between the two. Floating AVG DISTINCT
still falls back. Insert-only two-phase AVG DISTINCT supports integers and DECIMAL precision
up to 38, as described below.

`TINYINT` and `SMALLINT` `SUM`/`MIN`/`MAX` retain their input width, including the local
partials of an insert-only two-phase plan. SUM wraps on overflow at 8 or 16 bits rather
than widening to BIGINT. Single-phase retractions subtract at the same width; MIN/MAX
retain duplicate multiplicities until the last occurrence is removed. NULL inputs are
ignored and all-NULL groups return NULL. FILTER and integer SUM(DISTINCT) preserve these
rules across batches and checkpoint restore. Retracting two-phase exact integer SUM carries a separate non-null count. Retracting
two-phase MIN/MAX and non-integer SUM retain the general fallback described below.

FLOAT SUM/MIN/MAX preserve FLOAT input, partial and result widths; FLOAT SUM rounds at
that width after every addition. Floating SUM starts from its first non-NULL input, preserving
a lone negative zero. Insert-only FLOAT/DOUBLE MIN/MAX use primitive comparisons, retaining
the first signed zero on a tie and retaining an initial NaN. Retracting extrema use Java boxed
comparison instead: all NaN payloads share a membership key, NaN sorts above infinity, and
negative zero sorts below positive zero. This distinction matches Flink's different accumulator
implementations. Persistent membership probes and writes use the same canonical NaN key.

Aggregate-result change detection uses Flink's primitive floating equality: opposite zero signs
compare equal, while NaN never compares equal to itself. Even a suppressed update retains the
new accumulator result for the next UPDATE_BEFORE or DELETE. Membership equality remains
separate from this output rule.

`MIN`/`MAX` over `TIMESTAMP(p)` and `TIMESTAMP_LTZ(p)` preserve both milliseconds and fractional
nanoseconds. The single-phase path handles insertions and retractions with a value/multiplicity
multiset: retracting one duplicate keeps the extreme, retracting the last occurrence reveals the
next value, and deleting the last record removes the group. NULL values do not contribute; an
all-NULL group reports NULL extrema. Local-zoned values compare as instants, independently of
the session zone. The declared logical type and precision are retained on output.

DATE, TIME and BOOLEAN MIN/MAX use the same retractable multiset. DATE compares epoch
days, TIME compares its internal milliseconds (including milliseconds retained under a
TIME(0) declaration), and FALSE sorts before TRUE. Duplicate counts, all-NULL groups,
FILTER and typed outputs survive checkpoint/restore. Insert-only two-phase plans carry
the same typed extrema in their partials. DATE and BOOLEAN extrema use the existing
per-element RocksDB codec when selected; TIME retains the snapshot-blob state path.

`AVG` is native: a running sum — widened to bigint for any integer input, double for float/double —
plus the non-null count, emitting `count == 0 ? NULL : sum / count` cast back to the input type,
with **integer division truncating toward zero**. This is a direct port of Flink's
`AvgAggFunction`, over bigint/int/smallint/tinyint/float/double, and is retract-aware. Decimal
`AVG` is native too: the sum uses a `DECIMAL(38, s)` accumulator, and the emit divides by
the non-null count using Flink's exact decimal division — a 38-significant-digit quotient then
**HALF_UP** rescale — reporting `DECIMAL(38, max(6, s))`, `findAvgAggType`'s result type.

Released Flink 1.18.1 and 2.2.1 cannot initialize AVG over DECIMAL(38,38):
planning fails with `IllegalArgumentException: Cannot convert 0 to DECIMAL(38, 38)`.
Native admission retains this narrow host planning failure, including shared
SUM0 reductions and local/global plans. SUM alone and lower-scale AVG retain
their existing coverage. `FlinkDecimalAverageAdmissionSqlHarnessTest` captures
independent stock/native-enabled planning outcomes and verifies scales 2/6/37
still execute natively; this is failure compatibility, not an incorrect-average
workaround or an upstream dependency fix.

The DISTINCT map representations and benchmark method are described under
[typed DISTINCT multiplicities](../optimizations/aggregate-specialization-fast-paths.md#typed-distinct-multiplicities).
Integer, decimal and string key specialization preserves the existing scalar snapshot and
persistent-element encoding, including exact decimal metadata and string bytes; it does not
expand the DISTINCT admission gates.

`FlinkSharedAggregateSqlHarnessTest` verifies shared COUNT/AVG/SUM over nullable
INT/BIGINT/DECIMAL, grouped/global/filtered/empty inputs, overflow, retractions and
group recreation. It compares resolved schemas and the emitted rows **with their
RowKinds**, in immediate and actual local/global mini-batch plans. Eight recovery
combinations cover BIGINT/DECIMAL, both phase strategies, HashMap/native RocksDB,
and two successive checkpoint recoveries, including NULL-only and overflowed
checkpoint prefixes. Recovery metrics require consumed work in each intended
native aggregate stage. See [release measurements](../benchmarks/basic-type-coverage.md)
for the performance boundary and the decimal division optimization it required.

Released Flink 1.18.1 suppresses unchanged mini-batch aggregate results even with
positive state TTL, in both single-phase and global merge plans. Flink 2.2.1 emits
those unchanged updates with TTL enabled. The native operator selects this behavior
from the Flink compatibility line for heap, raw restore and typed RocksDB state;
secondary input metadata does not determine the emission mode.

### LISTAGG

Single-phase LISTAGG runs over CHAR/VARCHAR for append and retracting input, with an
omitted comma separator or a verified non-null character literal. Empty, quoted and
multi-byte separators are supported. NULL values are skipped; empty strings and
duplicates contribute in arrival order. A retraction removes the first matching
occurrence. FILTER, numeric aggregate composition and single-phase mini-batching
retain Flink's result types and changelog behavior.

Append input retains an owned concatenated prefix. Retracting input retains ordered
owned occurrences and pending unmatched retractions, matching released Flink. Versioned
binary accumulator snapshots preserve this state across memory/native RocksDB restores.
Tests compare ordered RowKinds and schemas on Flink 2.2.1 and 1.18.1; four recovery
profiles each force two restores and verify native consumption and released memory.
Positive state TTL, DISTINCT, explicit ordering and local/global LISTAGG remain fallback.
JSON array/object coverage is described below.

Release measurements on JDK 17/Flink 2.2.1 use p=1, 2 GiB heap, production IPC,
mimalloc, 1,024-row transposes, ONE_PHASE without mini-batching, two warmups and five
alternating stock/native trials. Complete jobs retain the row source, both transposes,
JNI, state and row blackhole sink. Values are NULL every seventh input.

| Workload | Stock median | Previous StreamFusion median | Native median | Native vs stock |
|---|---:|---:|---:|---:|
| 2M rows, 65,536 keys, 16-byte strings, append | 2.414 s | 2.420 s | 1.322 s | 1.83× |
| Same, insert/insert/delete input | 3.801 s | 3.660 s | 1.336 s | 2.84× |
| 1M rows, 32,768 keys, 128-byte strings, append | 2.103 s | 2.038 s | 1.840 s | 1.14× |

The previous revision is actual main `f88dc237`, using identical harness/configuration
and asserting zero native substitutions. Its separately paired stock controls are
2.435/3.587/2.028 s; the 4–6% drift in the latter controls limits fine comparisons.
Native ranges are 1.257–1.361/1.324–1.355/1.785–1.977 s; corresponding stock ranges
are 2.347–2.483/3.690–4.109/1.959–2.206 s. These bounded cardinalities do not establish
performance for arbitrarily large per-key strings.

Mixed Java/Rust CPU and allocation profiles were captured with async-profiler 4.0,
`cpu-clock`, 2 ms sampling, DWARF native stacks and 1 MB allocation sampling.
Rust state updates, allocation and result materialization dominate the native task;
Java conversion and JNI remain visible. No speculative micro-optimization is included.
All raw trials, Criterion estimates, source/library hashes and compressed profile stacks
are retained in [the measurement artifact](../benchmarks/results/listagg-2026-10-15.json).
Reproduce complete jobs with `SF_BENCHMARK=true mvn test -Pbench
-Dtest=StringAggregateBenchmark
-Dsf.benchmarkJvmArgs="-Xmx2g -Dstreamfusion.exchange.zeroCopyLocal=false"`; workload properties are
`stringagg.rows`, `stringagg.keys`, `stringagg.width` and `stringagg.updating`.

### JSON_ARRAYAGG and JSON_OBJECTAGG

Single-phase JSON array/object aggregation accepts append and retracting input whose
arguments are serialized character values. Flink's planner inserts JSON_STRING for
value arguments; its existing native expression whitelist still applies. Arrays retain
arrival order and duplicates, and remove the first matching serialized value on
retraction. Both NULL ON NULL and ABSENT ON NULL are supported, with the released
default for each function. Objects remove by key regardless of the retracted value,
permit reinsertion after removal, and sort output keys by Java UTF-16 order.
Duplicate keys fail before ABSENT ON NULL can omit a NULL value.

The result retains VARCHAR(2000) NOT NULL. Owned per-key strings checkpoint through
the existing binary state representation; ordered arrays are not stored as unordered
multisets. Positive state TTL, DISTINCT, explicit ordering and local/global JSON
aggregation remain fallback. Both released parsers reject JSON aggregate FILTER syntax.
Flink 1.18 decimal JSON_STRING expressions use the existing generated evaluator
inside Calc because that line strips decimal scale; the grouped accumulator still runs
natively. The 2.2 expression retains scale.

NULL object keys preserve release-specific error timing: Flink 2.2 rejects them on
accumulation, before NULL omission. Flink 1.18 retains the key until serialization,
except an omitted NULL value; the cold failure delegates to the released host to retain
its exception and cause. Group output materialization is fallible so this error occurs
after all accumulators have processed the input. JNI reports structured JSON errors
after releasing the imported Arrow batch, preserving the Comet ownership pattern.

All 19 JSON SQL tests pass on each of released Flink 2.2.1 and 1.18.1/JDK 17. They
compare exact ordered RowKinds, schemas, escaping, NULLs, duplicates, retractions,
malformed UTF-16 boundaries and mini-batch error precedence. Eight recovery profiles
exercise two successive restores across memory/native RocksDB and both input changelog
modes. Three release native state tests and all 48 string/JSON Criterion fixtures pass.

Complete-job release results on Flink 2.2.1/JDK 17, p=1, 2 GiB heap,
production IPC, mimalloc, 1,024-row transposes and both row boundaries:

| Workload | Stock median | Previous StreamFusion median | Native median | Native vs stock |
|---|---:|---:|---:|---:|
| Array, 2M rows / 65,536 keys / 16 bytes | 8.939 s | 8.757 s | 1.891 s | 4.73× |
| Object, same size and width | 26.992 s | — | 4.046 s | 6.67× |
| Object, 500k rows / 16,384 keys / 16 bytes | 6.290 s | 6.173 s | 1.033 s | 6.09× |
| Array, same smaller size, retracting | 1.431 s | 1.365 s | 0.500 s | 2.86× |
| Object, same smaller size, retracting | 2.782 s | 2.747 s | 0.614 s | 4.53× |
| Array, same smaller size, 128-byte strings | 3.442 s | 3.362 s | 1.252 s | 2.75× |
| Object, same smaller size, BIGINT values | 6.309 s | 6.206 s | 0.955 s | 6.61× |

Large cases use two warmups/five alternating pairs; smaller controls use one warmup/three
pairs and preserve roughly 30 rows per key. Previous behavior is actual main `f88dc237`,
with the identical harness/data/resources and zero native substitutions asserted. The
large object case has no separate previous-revision run; the smaller object control does.
Paired stock controls differ by at most 4%. All native/stock trial ranges are disjoint;
startup remains in the measurements, with no time subtracted. These cardinalities do not
establish performance for arbitrarily large per-key strings.

Mixed Java/Rust CPU profiles use released async-profiler 4.0, `cpu-clock`, 2 ms sampling
and DWARF native stacks. They show stock JSON tree traversal/serialization and state-map
access; native costs are state update, string allocation/copying and object-key sorting.
Java transposes remain visible. Across one warmup/three pairs, task-thread Java allocation
sample weights are 67.3 GB stock versus 14.7 GB native for arrays (8M rows per engine),
and 28.3 GB versus 5.0 GB for objects (2M rows per engine). These are sampled Java heap
allocation traffic, not peak memory or Rust allocations. Profiled timings are excluded
from speed claims.

All 56 production fixtures pass with untimed Rust allocation counters and Arrow output
buffer reports; 32 JSON update fixtures and eight string/JSON snapshot/restore fixtures
also retain release Criterion timing estimates. Probes
record allocation requests/full requested bytes separately from output buffer lengths,
without equating either with copied bytes. All complete-job trials, hashes, CPU/Java
allocation stacks, native allocation probes and Criterion estimates are retained in
[the result artifact](../benchmarks/results/json-aggregates-2026-10-15.json).

Reproduce complete jobs with the LISTAGG command above and
`-Dstringagg.function=JSON_ARRAYAGG` or `JSON_OBJECTAGG`. Set `stringagg.value=bigint`
for numeric object values, `stringagg.updating=true` for retracting input, and
`stringagg.warmup`/`stringagg.runs` for the recorded trial counts. The smaller controls use
`stringagg.rows=500000` and `stringagg.keys=16384`; the wide case sets `stringagg.width=128`.

### Floating aggregate validation

`FlinkFloatingGroupAggregateSqlHarnessTest` compares released Flink values, result schemas,
ordered single-phase RowKinds and materialized local/global output. It covers rounding at
FLOAT width, NULL-only/empty/filtered groups, NaNs, infinities, both zero signs, DISTINCT
membership, group removal/recreation and SINGLE_VALUE errors in local and global paths.
Twelve profiles each force two checkpoint restores across memory/native RocksDB, reductions,
ordered values, DISTINCT SUM and single/two-phase SINGLE_VALUE. Execution metrics require
consumed rows in both intended native stages. Native tests also reopen a typed RocksDB
checkpoint and retract NaNs with different payloads.

See [floating GROUP BY release measurements](../benchmarks/floating-group-by.md) for the
stock, previous fallback and native comparison, raw trials, workload limits and reproduction.

### FIRST_VALUE, LAST_VALUE and SINGLE_VALUE

The one-argument forms run natively in the single-phase plan over TINYINT, SMALLINT,
INT, BIGINT, FLOAT, DOUBLE, DECIMAL, CHAR/VARCHAR, DATE, TIME, BOOLEAN, TIMESTAMP and
TIMESTAMP_LTZ. Each preserves the input value's type and precision. FIRST_VALUE/LAST_VALUE support per-aggregate FILTER.
Flink 1.18.1 cannot plan CHAR or temporal FIRST_VALUE/LAST_VALUE and fails a type assertion for
SINGLE_VALUE over TIME(0), including string casts to TIME(3) that resolve to TIME(0);
those host limitations remain unavailable on that release. Typed TIME(3) input is supported.

FIRST_VALUE and LAST_VALUE skip NULLs and follow arrival order within a key. Append-only
input retains one scalar. Retracting input retains the ordered non-NULL occurrences;
a retraction removes the oldest matching occurrence, including when values repeat across
Arrow batches. Floating retractions use Java boxed equality: NaN payloads match, while opposite
zero signs do not. Removing every contributing value yields NULL, and removing the last
record deletes the group. Results depend on arrival order, so SQL parity fixtures use a
controlled source rather than asserting equal results from independently reordered inputs.

`FIRST_VALUE(DISTINCT string)` and `LAST_VALUE(DISTINCT string)` also run in a single
phase over STRING/VARCHAR. They retain the arrival order of unique live values and their
multiplicities: inserting a duplicate does not move its position, and only the last
retraction removes it. Reinsertion after complete removal gives it a new position. NULLs
are ignored, and filtered instances have independent membership. Checkpoints retain both
order and multiplicities. These DISTINCT forms require zero state retention because
Flink's separate membership/order maps can expire independently.

Ordered aggregate state now resides inline in the existing per-aggregate storage, avoiding a
separate allocation for every group; dynamic strings and retraction queues remain accounted
against the task memory budget. Checkpoint representation and aggregate semantics are unchanged.



An immediate, single-phase group with one unfiltered SINGLE_VALUE emits directly from its accumulator
without a duplicate cached result or temporary tuple vector. A later touch reconstructs
the preceding result from that accumulator. Filtered and mixed aggregates retain their cache;
mini-batch emission and snapshot formats are unchanged.

SINGLE_VALUE counts every element, including NULL. Zero elements yield NULL; one element
yields that value. A second element raises Flink's `TableRuntimeException` with the same
cardinality diagnostic. Retraction clears the retained value and decrements the count;
released Flink rejects a SQL FILTER clause on SINGLE_VALUE during planning.

Checkpoints preserve the scalar/count or ordered occurrences. Append-only first/last and
SINGLE_VALUE support the enclosing group's TTL. Retracting FIRST_VALUE/LAST_VALUE with a
positive retention, including a STATE_TTL hint, fall back: Flink independently expires its
value-to-order and order-to-value map entries, which a single group lifetime does not model.
These aggregate states use the existing raw keyed snapshot path with both memory and RocksDB
backends; the direct RocksDB accumulator-row codec does not yet encode ordered occurrences.
Two-phase FIRST_VALUE/LAST_VALUE plans, SINGLE_VALUE DISTINCT, DISTINCT first/last over types other
than STRING/VARCHAR, two-argument value/order dialects, and other value types retain explicit
fallback gates.

`GroupedValueBenchmark` measures append-only FIRST_VALUE/LAST_VALUE with a release native
build (`-Pbench`, mimalloc), 2 million rows, 64 keys, one-eighth NULL values, parallelism 1,
two warmups and five interleaved measured runs. The M4 Pro/JDK 17/UTC run against Flink 2.2.1
kept the row source, both row/Arrow transposes and the rowwise blackhole sink. It asserted
the native aggregate and both transposes before measuring.

| Value type | Flink seconds | Native seconds | Flink/native |
| --- | ---: | ---: | ---: |
| BIGINT | 0.754073 | 0.711064 | 1.060x |
| STRING | 1.119623 | 1.226295 | 0.913x |

The integer case shows a small local gain; the string case is slower. This adds coverage
within columnar pipelines and does not establish a general speedup. Retracting state and
SINGLE_VALUE were validated for correctness but are not measured by this benchmark.

**Idle-state TTL.** `table.exec.state.ttl` runs natively here (and on the two-phase global merge
below — the local half is transient and holds no TTL-eligible state). Semantics match Flink
exactly: every stored value carries its last-**write** wall-clock timestamp (reads never refresh
it), expiry happens at `last_write + ttl` inclusive, and expired state reads as absent and is
deleted on read. The `STATE_TTL` hint overrides the job-wide retention on aggregates specifically.

## Two-phase / mini-batch

The mini-batch plan splits the aggregate into four cooperating operators, all of them native:

1. **`MiniBatchAssigner`** emits the batching marker.
2. **Local** — a transient in-memory bundle, flushed on that marker, on a `mini-batch.size`
   trigger, or before each checkpoint. It holds no checkpointed state, mirroring Flink's own
   `MapBundleOperator`.
3. A **keyed shuffle** — a native columnar exchange — repartitions bundled partials by key.
4. **Global** reuses the single-phase group-aggregate operator to merge partials (`COUNT` merges
   as a `SUM` over partial counts).

**Scope.** `SUM`/`MIN`/`MAX`/`COUNT` over bigint/int/smallint/tinyint/float/double value columns (Flink's `SUM` partial
keeps the value's own type, so nothing is lost to widening in the split), and `AVG` over the full
single-phase numeric set — bigint/int/smallint/tinyint/float/double. An `AVG` spans **two
positional partials**: the widened running sum (bigint for integer inputs, double for
float/double) plus the bigint non-null count. The local runs these as a widened-sum state and a
`COUNT` over the same column; the global folds the pre-summed pair into the ordinary `AVG` state
(the count partial bumps the non-null count), so the final divide/truncate/cast-back — including
the cast back to a narrow integer or float result — is byte-identical to the single-phase `AVG`.

Insert-only two-phase `MIN`/`MAX` also admit DATE, TIME, BOOLEAN, `TIMESTAMP` and
`TIMESTAMP_LTZ`. Both halves carry the complete typed value and timestamp components; a partial must retain the input's logical type and precision.
Two-phase retracting extrema retain the COUNT/AVG/SUM0/exact integer SUM gate described below.

Decimal `SUM`/`MIN`/`MAX`/`AVG` carry through the split too: `SUM`'s partial is the i128 running
sum as `DECIMAL(38, s)` (a bundle overflow emits NULL and latches the merged `AVG` NULL, skipped
by the `SUM` merge — the host's own null-propagation), `MIN`/`MAX` partials keep `DECIMAL(p, s)`
through the extremes multiset, and `AVG` merges the `(DECIMAL(38, s), bigint)` pair into the exact
division emit.

SUM0 has one typed partial: zero for an all-NULL local bundle, a wrapped integer
sum, or the widened decimal sum. A NULL decimal partial means overflow and latches
the global state, while a NULL input to the local accumulator is ignored. Both
stages support retractions; zero net count does not discard a nonzero sum adjustment.

**Both mini-batch assigner modes are native**: proc-time (markers generated from the clock) and
row-time (upstream event-time watermarks filtered to the mini-batch interval — a pure function of
the input watermarks, so results stay deterministic).

Tests that compare every intermediate update use ordered inputs and count-triggered bundles without
processing-time markers. Independent file scheduling or clock-driven flushes can change the number
of valid intermediate updates, even at parallelism one. The TTL fixtures assert the exact changelog
with retention enabled and disabled, including the unchanged `-U`/`+U` pair emitted only with TTL.

**Distinct aggregates ride the split natively** in the default (no-split) plan: the local's bundle
set travels as a trailing view column — its distinct `(value, count)` entries as a list of
structs, the Arrow form of Flink's serialized `MapView` partial — and the global folds the entries
into its per-key distinct state with multiplicities, so a value repeating across bundles counts
once. Scope: `COUNT(DISTINCT)` over bigint/int/smallint/tinyint/float/double/string/decimal,
BOOLEAN, DATE, TIME, TIMESTAMP and TIMESTAMP_LTZ; `SUM(DISTINCT)` over bigint/int/smallint/tinyint and DECIMAL
with precision up to 38.
Decimal sum partials must be DECIMAL(38, input-scale), while the distinct view preserves each
input value's original precision and scale. Boolean and timestamp views retain their Arrow
value types, including fractional timestamp nanoseconds. The merge folds in set-iteration
order, so order-sensitive float/double sums stay on the host.
DATE membership uses epoch days; TIME views carry internal milliseconds even if Flink resolves
a lower declared precision. Both timestamp families use the existing millisecond/nanosecond
component representation, retaining precision 0/3/6/9 and wide/pre-epoch values without a session-zone
conversion. Runtime SQL checks cover shared/filtered views, NULL-only groups, empty global input,
and repeated checkpoint recovery with memory and native RocksDB state.
The [release job measurements](../benchmarks/basic-type-coverage.md#temporal-two-phase-distinct-2026-10-16)
retain both transposes and IPC shuffle: 1.32-1.54x stock at 2M rows/64 keys and 1.07-1.15x
at 10M rows/1,024 keys. Persistent recovery is verified for correctness; no RocksDB speedup is claimed.

Insert-only `AVG(DISTINCT)` also rides the split for integers and DECIMAL precision up to 38.
The local keeps two distinct states: a widened sum partial and a BIGINT count partial, while
its membership view retains the original input value type. Empty/all-NULL bundles emit a
zero sum and zero count. The global merges membership entries into the same exact average
state used by single-phase AVG DISTINCT; it does not add the local sums/counts, which would
count values appearing in several bundles more than once. Decimal sum fields must retain
precision 38 and the input scale, and the result type must match Flink's AVG type.

Insert-only two-phase `SINGLE_VALUE` over STRING/VARCHAR, FLOAT and DOUBLE retains a typed
value and an INT count in each local bundle. NULL increments the count. Both local accumulation and global merging
raise Flink's cardinality exception on a second element; global checkpoints preserve that
count even when the value is NULL. The global consumes the count partial, rather than
counting bundles. The declared value partial must match the input type, and its count must
be INT. Other value types and retracting SINGLE_VALUE splits remain on Flink.
Local execution failures use the same Flink exception mapping as global aggregation, after
releasing imported Arrow input, so cardinality failures are not reported as memory limits.

Distinct views are indexed by expanded native partial positions. Each preceding AVG, plain
or DISTINCT, and each SINGLE_VALUE occupies two positions. This also fixes an existing crash
when an ordinary AVG preceded COUNT/SUM DISTINCT in a mixed local aggregate.

**Per-aggregate `FILTER (WHERE …)` rides the split too**, on the admitted SUM/MIN/MAX/COUNT/AVG
forms, plain and DISTINCT alike:
the predicate is a boolean column the local gates every fold on, so the merge stays filter-blind.
Filtered distinct instances each get their own native view/set per `(args, filter)` pair — the
same final output as Flink's shared bitmask view, since a filtered distinct is an unfiltered
distinct over the filtered row subset.

**A retracting local input** (the aggregate consumes another aggregate's changelog — Nexmark q4's
shape) is native for `COUNT`, `AVG`, exact `SUM0` and ordinary integer `SUM`.
SUM_RETRACT uses a separate `(sum, BIGINT count)` partial pair; the other admitted layouts are invariant under
retraction. The local subtracts `-U`/`-D` rows, and the appended (or reused) `count1` `COUNT(*)`
partial drives per-key liveness in the global (`-D` and state drop when the merged count reaches
zero, Flink's `RecordCounter` semantics).

Ordinary retracting SUM initially admits TINYINT, SMALLINT, INT and BIGINT with matching declared
sum partial/result widths and a BIGINT non-null contribution count. Its local sum adjustment
survives a zero-net-count bundle; the global adds the adjustment and updates the non-null count
separately. A zero non-null count emits NULL, while the separate count1 field controls group
liveness. Numeric zero is not an empty group. The record counter removes a group only at exactly
zero, matching Flink even when duplicate retractions make it negative. DECIMAL and floating
SUM_RETRACT remain on the host until their partial/overflow/order contracts are implemented.

AVG's local sum remains an accumulator even when the bundle's net count is zero: replacing `10`
with `20` emits a sum adjustment of `10` and a count adjustment of `0`. The global merge applies
both. All-null bundles emit `(0, 0)`; a NULL decimal sum means overflow and propagates regardless
of the net count.

**Checkpointing.** The durable global state stays as a Rust hot map but checkpoints through
Flink's raw keyed state: each non-empty key group gets its own snapshot payload, and a rescaled
task restores exactly the payloads assigned to its new key-group range, using the same BinaryRow
hash/key-group calculation as the native exchange. See the [RocksDB backend](../backends/rocksdb.md)
for the persistent-state-backend angle on this same raw-keyed-state layout.

Still falling back, specific to the two-phase split: the opt-in `distinct-agg.split.enabled`
incremental chain (a deliberate non-goal — see [Unsupported operators](unsupported.md)),
`MIN`/`MAX` with a retained physical `DISTINCT` flag, floating `SUM(DISTINCT)` and
`AVG(DISTINCT)`, and — under a
retracting input — any aggregate other than `COUNT`/`AVG`/exact `SUM0`/ordinary exact integer `SUM`
(non-integer SUM and MIN/MAX retract variants retain unverified layouts, and a monotonicity-exempt `MIN`/`MAX` ignores
retractions in ways the native fold would not) plus `DISTINCT` (its view value switches to
per-filter live counts under retraction).

## Type support

The matcher only accelerates `(aggregate, value-type)` pairs where DataFusion's native arithmetic
agrees byte-for-byte with Flink's — this table is that guardrail; anything marked ✗ falls back.

| value type | SUM | AVG | MIN | MAX | COUNT |
|---|---|---|---|---|---|
| BIGINT | ✓ | ✓ ¹ | ✓ | ✓ | ✓ |
| INT | ✓ ² | ✓ ¹ | ✓ | ✓ | ✓ |
| SMALLINT / TINYINT | ✓ ² | ✓ ¹ | ✓ | ✓ | ✓ |
| DOUBLE | ✓ | ✓ | ✓ | ✓ | ✓ |
| FLOAT | Yes | ✓ ³ | Yes | Yes | ✓ |
| DECIMAL | ✓ ⁴ | ✓ ⁴ | ✓ | ✓ | ✓ |
| CHAR / VARCHAR | ✗ | ✗ | ✓ ⁵ | ✓ ⁵ | ✓ |
| TIMESTAMP / TIMESTAMP_LTZ | - | - | Yes | Yes | Yes |
| DATE / TIME / BOOLEAN | - | - | Yes | Yes | Yes |

¹ **Integer `AVG`** diverges from DataFusion's native `Float64` average; a custom accumulator sums
in int64 and truncates the cast back to the input integer type, matching Flink's `AvgAggFunction`.

² **Integer `SUM`** (INT/SMALLINT/TINYINT) uses a custom wrapping accumulator that keeps the
narrow input type and wraps at that type's width on every step, instead of DataFusion's widening
sum — the host's exact "store the running sum in the input type, cast back each step" semantics,
pinned by an overflow-boundary parity test.

³ **`AVG` over FLOAT** sums in double and narrows the quotient to float, as Flink's
`FloatAvgAggFunction` does. FLOAT SUM/MIN/MAX keep FLOAT state and results instead of
using that widened AVG state. Released Flink rejects SQL `CAST(... AS REAL)` during validation
with `Type is not supported: REAL`; this spelling retains the same planning failure.

⁴ **DECIMAL** carries type-preserving `MIN`/`MAX`/`COUNT` over the column's own precision/scale,
`SUM` as an i128 running sum reported as `DECIMAL(38, s)`, and `AVG` as that sum divided by the
non-null count with Flink's exact decimal division. Overflow mirrors Flink's buffer shapes exactly:
`SUM`'s buffer is the nullable sum alone, so an overflow past `DECIMAL(38, s)` goes NULL and the
**next value resets it** (no sticky latch) and the merge **skips** a NULL partial; `AVG`'s `(sum,
count)` buffer null-propagates instead, so its overflow is sticky. Both are pinned at the overflow
boundary by parity tests.

⁵ **String `MIN`/`MAX`** compare byte-lexicographically, matching Flink's `BinaryStringData`
common binary comparison path. The one place this can differ from Flink is its separate
materialized-Java-object path for supplementary-plane characters, which this native comparison
does not replicate.

Grouping keys admit bigint/int/string/boolean/date/timestamp/decimal; multiple value columns of
different types are each read independently (e.g. `SUM(a), SUM(b)` over columns of different
types both accelerate). `COUNT(*)` reads a synthesized non-null column so it counts every row,
including alongside value aggregates.

## JVM aggregate investigation

Performance admission now requires symmetric stock/native preflight. Earlier v1 measurements
below explained only native before timing and remain historical diagnostics. The corrected
[method and first complete-job trials](../benchmarks/jvm-aggregates.md#symmetric-preflight-correction)
retain the distinct-key regression and require further comparisons.

JVM aggregate functions still fall back by default under [#237](https://github.com/datafusion-contrib/StreamFusion/issues/237).
The diagnostic system property `streamfusion.experimental.jvmGroupAggregates=true` admits
released Flink bridging aggregate functions in the synchronous single-phase plan when every
input and output field is VARCHAR, INT or BIGINT. It retains Flink's generated function,
accumulate/retract logic, managed accumulator/DataView state and lifecycle inside an
Arrow-batch input/output operator. It preserves each emitted changelog row; input coalescing
does not reduce user function invocations or intermediate results. The operator disable switch
`streamfusion.operator.jvmGroupAggregate.enabled=false` retains whole-island fallback.

The opt-in harness passes 44 checks on each released Flink line with JDK 17, at both
65,536-entry and three-entry capacities. Coverage includes the original STRING-key append/CDC
fixtures, numeric boundary/NULL keys at parallelism one and three, nonleading grouping-field
selection confirmed in the optimized plan, all RowKinds and delete/reinsert, and mixed
ListView/UDAF/COUNT aggregation. Twelve memory/native-RocksDB repeated-recovery scenarios per
line assert two forced restores, consumed native work and released native memory. Thirteen further
checks cover STRING-key NULL/empty/long Unicode ownership, all RowKinds, delete/reinsert,
ListViews, parallelism one/three and repeated recovery with both backends. The STRING cases
also use distinct keys with the same released Flink hash, including deletion, reinsertion,
recovery and cache overflow. Lifecycle
failures and excluded-mode fallback are checked; the released host's processing-time mini-batch
control compares known final results and schema because intermediate batches may differ.
A further two mixed-UDAF/ListView checkpoint-rescale scenarios per line and capacity pass
through parallelisms 1 → 3 → 2 with 128 key groups. They verify restored source offsets,
stable operator identities, consumed native work, checkpoint ownership ranges and final
materialized values against uninterrupted stock Flink. Existing builtin rescale controls
also pass. [Checkpoint rescale evidence](../benchmarks/results/jvm-aggregate-rescale-2026-10-16.json)
retains all stages, key-group ranges, source offsets, native work and cleanup observations.

Actual canonical savepoints also restore through 1 → 3 → 2 in eight scenarios: each supported
Flink line, both backends and both cache capacities. The fixture triggers
`JobClient.triggerSavepoint(..., CANONICAL)` and restores the returned savepoint path,
retaining the mixed UDAF/COUNT/managed-ListView workload. It verifies the same values, schemas,
source offsets, stable operator identities, complete key-group ownership, consumed native work
and cleanup. Both lines also pass the existing checkpoint controls through the shared harness.
[Savepoint evidence](../benchmarks/results/jvm-aggregate-savepoint-2026-10-16.json)
records every stage. Cross-version migration, stock/native state-layout migration, broader
accumulator types and two-phase merge/reset remain pending.

Mini-batching, positive idle retention or state-TTL hints, asynchronous state, specialized
functions, DISTINCT, approximate aggregates, FILTER and ordered calls are excluded from this
prototype. Two-phase merge/reset, OVER functions, nested scalar results and broader table
function types remain separate work in #237. This experiment does not claim those contracts.

`streamfusion.experimental.jvmAccumulatorCacheEntries` defaults to zero. A positive value
stages accumulators for a single INT, BIGINT or VARCHAR grouping key in an entry-bounded heap cache.
NULL has a separate entry, and full-width primitive keys preserve minimum/maximum values.
Updates flush to Flink managed state at capacity, before watermarks, completion and checkpoint
snapshots. Snapshot boundaries invalidate borrowed accumulator views before subsequent
mutation; restore starts with an empty cache. Flushing restores the operator's previous key
context and ignores an empty cache on idle subtasks. Other key shapes use managed state directly.
Cache hits also reuse their immutable, owned Flink BinaryRow key: the input grouping field
probes a primitive numeric map or STRING hash index before key construction. STRING probes borrow a mutable
Flink binary-string view over the current Arrow buffer for lookup only; the view is local to
one ingest call and never enters the map. Buffers too large for a Java NIO view retain the
released copying reader. Retained StringData keys refer only to owned BinaryRow
storage, never to the closing Arrow batch. A STRING hit passes
the selected entry to the state read without constructing a second string view. Misses use
the released selector's owned copy. A separate borrowed-string reader supplies key projection
even after cache bypass or with caching disabled, avoiding a temporary input-string copy before
that owned key is created. User aggregate callbacks still receive the ordinary reader, whose
string ownership is unchanged.
The key field comes from the optimized input projection, rather than the original source schema.
If an additional key arrives after the cache fills without any hit, pending rows first flush
to managed state and the cache is bypassed for the rest of that task attempt. Reads, writes
and clears then delegate directly; a restored attempt starts with an empty cache and evaluates
reuse again. A working set that fits the capacity stays cached. Cold-prefix tests revisit and
retract old groups after the switch, including ListView state and repeated recovery.
STRING hashes index the existing primitive map, with full binary-string equality along each
collision chain. Cache capacity counts every distinct key, not hash buckets; flushing and
removal visit all colliding entries. The bound counts entries, not accumulator bytes, and is
not a task memory budget.

The investigation has mixed Java/symbolized Rust CPU and sampled Java allocation profiles.
An initial BinaryRow-keyed write-through cache was rejected: its key comparisons outweighed
saved heap-state reads (1.395 s versus stock 1.084 s at 2M rows/65,536 keys). Primitive-key
lookups reduced that cost, after which managed-state writes and rehashing dominated.

Final release measurements use JDK17, Flink2.2.1, the unchanged release/mimalloc native library,
p=1, 2 GiB heap, production IPC, row source/blackhole sink and both 1,024-row transposes.
Each complete job owns and closes its metrics cluster; cluster boot/config is outside timing,
while planning/submission, source, sink, conversions, functions and state remain timed.
Each row below has two warmup pairs and five alternating measured pairs, with 2M input rows
and every eighth value NULL. All completed trials are retained.

| Candidate / keys | Stock median (range), s | Native median (range), s |
|---|---:|---:|
| Key reuse / 65,536 | 1.086 (0.936–1.114) | 0.672 (0.549–0.822) |
| Key reuse / 16 | 0.576 (0.544–0.577) | 0.505 (0.401–0.518) |
| Before key reuse / 2M distinct | 1.767 (1.744–1.832) | 2.165 (2.093–2.237) |

The clean distinct-key regression remains unresolved. The key-reuse distinct-key repeat
observed another build and is diagnostic only. Earlier per-pair metrics ownership retained
both completed jobs through the comparison; its distinct-key run had three full GC pauses.
Per-job ownership had none, but the regression remained. Initial unmonitored dispersed trials
also remain in the artifact and do not establish performance admission.

Matching hot-key profiles (1M rows/32,768 keys, two warmups/three pairs) sample key-selector
Java allocations at 849 MB before reuse and 25 MB after. Total allocations under the JVM
aggregate operator fall from 3.63 GB to 2.74 GB. Both profiles also contain a native builtin-SUM
Rust control. The baseline records 3.28 external CPU seconds; the candidate records none.
These are sampled allocation estimates, not peak memory, copied bytes or Rust heap totals;
profile timings are excluded from speed claims. The key-reuse release medians also span
separate JVMs with stock drift, so they do not isolate an exact percentage effect of key reuse.
See the [probe technique](../optimizations/borrowed-key-probes.md#experimental-jvm-aggregate-keys).

[All diagnostic trials and profiles](../benchmarks/results/jvm-aggregates-investigation-2026-10-15.json)
retain the variability, GC observations and interference records. There is no default admission:
Broader key/accumulator contracts and representative preceding-fallback comparisons remain
outstanding. The isolated results below now cover both Flink lines. An additional cold-key bypass run measured
1.770 s native versus 1.769 s stock but overlapped another build, so it is diagnostic only.
[Isolated release diagnostics](../benchmarks/jvm-aggregates.md) build and finish correctness
checks before measuring typed key/cardinality cases and collecting Java/Rust profiles on
separate GitHub runners. Stock Flink remains the default.

### Isolated typed-key results (2026-10-16)

The first isolated run completed all nine cases and four Java/Rust profiles on each Flink
line. It measures the numeric-cache/cold-bypass implementation before the STRING-cache
experiment. On the four-CPU GitHub hosts, medium numeric keys win on both lines; STRING
low/medium keys regress, and distinct-key ranges are broad. All medians below are seconds
for the same 2M-row complete-job boundary described above.

| Key / cardinality | Stock 2.2 | Native 2.2 | Stock 1.18 | Native 1.18 |
| --- | ---: | ---: | ---: | ---: |
| INT / 16 | 1.028 | 0.889 | 1.015 | 0.890 |
| INT / 65536 | 1.306 | 1.006 | 1.386 | 0.997 |
| INT / 2000000 | 3.022 | 3.188 | 2.740 | 2.911 |
| BIGINT / 16 | 0.980 | 0.910 | 0.980 | 0.834 |
| BIGINT / 65536 | 1.371 | 1.004 | 1.386 | 1.000 |
| BIGINT / 2000000 | 3.759 | 2.974 | 3.336 | 2.836 |
| STRING / 16 | 1.238 | 1.421 | 1.248 | 1.299 |
| STRING / 65536 | 1.693 | 1.970 | 1.746 | 1.932 |
| STRING / 2000000 | 3.574 | 3.660 | 3.311 | 3.610 |

[All trials, ranges, logs and compressed Java/Rust stacks](../benchmarks/results/jvm-aggregates-isolated-2026-10-16.json)
are retained, including every regression. For example, the 2.2 INT-distinct stock range is
2.731–4.081 s and native is 2.619–4.533 s; a median alone does not establish a stable effect.
Medium STRING profiles attribute 1.87 GB (2.2) and 1.81 GB (1.18) of sampled allocations
under JVM aggregate ingest to the released key selector. Inclusive totals overlap downstream
copying; they are sampled bytes, not copied-byte counts. Rust CPU stacks are retained from
the builtin-SUM control, while the UDAF itself executes released generated Java.

These profiles motivated the experimental STRING owned-key cache described above. Its
39-case semantic suite passes both lines at both capacities; the full 46-case rescale/recovery
selection also passes at capacity three. A subsequent local run of its copying probes still
regresses: 16-key native/stock medians are .715/.717 s, 65,536-key 1.405/1.284 s, and 2M distinct
2.082/1.988 s. The medium-key CPU profile shifts the lookup hotspot to the cache, and sampled
allocations still include temporary input-string wrappers. These are separate-host results
from the isolated baseline, not a matched causal comparison. [All trials and mixed stacks](../benchmarks/results/jvm-aggregate-copying-string-probe-2026-10-16.json)
retain the negative results.

The next experiment borrows an Arrow UTF-8 view only for lookup, following Comet's view/ownership
pattern. All 49 combined checks pass on both Flink lines at both cache capacities: the
39-case SQL harness, seven checkpoint/savepoint-rescale controls and three new slice/NULL/
empty-buffer/owned-copy checks. The final NIO-size fallback guard also compiles on both lines
and passes both three-case view checks. [Validation](../benchmarks/results/jvm-aggregate-borrowed-string-validation-2026-10-16.json)
records the source hashes and counts.

Matched local profiles retain the same 1M-row, 32,768-key UDAF plus builtin Rust control and
five native UDAF jobs. Inclusive aggregate-ingest allocation samples fall from 4.999 to 4.031 GB
(19%); cache-path samples fall from 1.060 to .086 GB (92%). CPU samples still concentrate in
map lookup. The first unprofiled repeat observed 34.53 external CPU seconds, so its
1.515/1.432 s native/stock medians are diagnostic only; remaining cases were deferred.
[Complete stacks and diagnostic trials](../benchmarks/results/jvm-aggregate-borrowed-string-probe-2026-10-16.json)
retain that limitation. This allocation reduction does not establish whole-job admission.
The feature and cache remain default off; #237 stays open for the remaining contracts.
The maintainer accepted shipping this implementation on October 16 with the measured
performance limitations retained and further optimization deferred.

The next cache experiment removes `HashMap` node lookup, the remaining hotspot in the clean
borrowed-probe CPU profile. It indexes STRING hashes in the primitive map and verifies full
string equality within each collision chain. Both Flink lines pass all 54 combined checks at
both capacities, including a verified hash-collision pair, fitting three-key sets, overflow
by a fourth distinct key, deletions/reinsertions and repeated memory/RocksDB restores.
[Validation and source hashes](../benchmarks/results/jvm-aggregate-string-hash-index-validation-2026-10-16.json)
record the matrix. A separate process-scoped Java/Rust diagnostic profile records external work and excludes
all timing claims. It confirms primitive-map lookup in place of map nodes; primitive lookup
and string equality remain hot. [Stacks and limits](../benchmarks/results/jvm-aggregate-string-hash-index-profile-2026-10-16.json)
are retained. The change has no throughput admission yet. The borrowed-probe predecessor has its own
isolated run so later revisions preserve that comparison.

The [completed symmetric-preflight matrix](../benchmarks/results/jvm-aggregates-symmetric-isolated-2026-10-16.json)
retains 18 aggregate cases and eight Java/Rust profiles across both Flink lines. Medium numeric
keys improve; STRING and distinct-key results remain mixed, with regressions and broad ranges
retained. It does not establish default admission or complete #237.

The next default-off [borrowed STRING exit experiment](../optimizations/zero-copy-exit-transpose.md#experimental-borrowed-string-exit)
shares the batch-scoped UTF-8 view helper used by cache probes, retaining owned cache keys and
mandatory output copies. Matched profiles reduce inclusive aggregate allocation samples by
22%, while the latest local distinct-key job remains 7.6% slower than stock. Normal-capacity
validation passes 73 combined cases on each Flink line; the small-capacity JSON/JVM selection
passes 108 on 2.2 and 106 with two compatibility skips on 1.18. All trials, profiles and test
logs are [retained](../benchmarks/results/jvm-aggregate-borrowed-string-exit-2026-10-16.json).
The aggregate implementation and cache remain default off.

The subsequent [owned-key projection investigation](../optimizations/borrowed-key-probes.md#borrow-before-projecting-an-owned-jvm-key)
avoids a temporary STRING copy on misses and after bypass, while callback inputs remain owned.
Its normal-capacity selection passes 74 checks per line; small-capacity aggregate/JSON checks
plus the separate nested suite pass 109 on 2.2 and 107 with two compatibility skips on 1.18.
Matched allocation profiles improve, but external work prevents timing claims; isolated
performance admission remains pending.

The [completed exit-only isolated matrix](../benchmarks/results/jvm-aggregates-string-exit-isolated-2026-10-16.json)
retains 18 aggregate cases on both lines, before the key-projection follow-up. Low and medium
cardinality medians improve, while distinct STRING keys remain slower than stock on both
lines and distinct INT regresses on 2.2. All ranges, negative results and mixed profiles are
preserved. It validates the exit-only revision, not the later key-projection candidate.

The [completed key-projection matrix](../benchmarks/results/jvm-aggregates-key-projection-isolated-2026-10-16.json)
now includes both Flink lines and actual historical fallback runtime `b965b8bf`. Most medians
improve, but medium STRING and distinct BIGINT still regress on 1.18. Distinct STRING on 2.2
beats its current stock control but remains slightly slower than the previous-runtime median.
Separate-JVM control drift and all ranges are retained. Both native-library caches hit and all
historical runtime queries verify zero native operators. This completes that measurement gap,
not performance admission or the remaining merge/reset/OVER/type contracts in #237.

The [encoding follow-up matrix](../benchmarks/results/jvm-encoding-isolated-2026-10-16.json)
retains a further 18 aggregate cases plus both historical-runtime controls and eight mixed
profiles. STRING remains slower on 1.18: low/medium stock/native medians are 1.288/1.411 and
1.898/2.142 s; distinct INT and BIGINT are 3.111/3.236 and 3.160/3.434 s. Repeated wide table
output improves separately, while distinct wide text on 2.2 remains slower than stock. The
next writer change resets its bounded miss counter on a hit; its allocation evidence and
whole-job gate are documented with the [encoding technique](../optimizations/string-copy-reduction.md#repeated-lazy-string-encodings).
These results do not admit grouped UDAFs by default or finish #237.

## Still falls back

Both the single-phase gate and, for the two-phase plan, **both halves independently** must clear
their own matcher before the query accelerates — one operator staying on the host drags the whole
query back via the [all-or-nothing island](index.md#the-all-or-nothing-island) rule.

**Single-phase / either two-phase half in common:**

- LISTAGG and JSON aggregates with DISTINCT, explicit ordering or positive state TTL.
- A UDAF under normal settings; the default-off investigation below is not admitted acceleration.
- `AVG`/`SUM`/`MIN`/`MAX` over a value type outside [Type support](#type-support)'s ✓ set.
- `AVG(DISTINCT)` over FLOAT/DOUBLE, DISTINCT SINGLE_VALUE, and DISTINCT FIRST_VALUE/LAST_VALUE
  over types other than STRING/VARCHAR.
  (`COUNT(DISTINCT x)` keeps a per-key
  value set; `SUM(DISTINCT x)` adds a running sum folded as values enter/leave it; `MIN`/`MAX
  (DISTINCT)` run as their plain, multiplicity-blind forms.)
- An approximate aggregate.
- FIRST_VALUE/LAST_VALUE/SINGLE_VALUE over a value type outside the single-phase list above,
  or with more than one argument.
- DISTINCT or retracting FIRST_VALUE/LAST_VALUE with positive state TTL.
- An unsupported grouping-key or value column type.

The released-host string aggregate baseline passes 18 checks on each of Flink 2.2.1 and
1.18.1/JDK 17. LISTAGG accepts an omitted separator (comma) or a non-null character literal;
dynamic and NULL separators fail SQL validation. Supported literals include empty, multi-byte
and quoted separators. Append and retract inputs preserve arrival order and duplicates, skip
NULL values, and honor FILTER. Retraction removes the first matching duplicate.

Plain GROUP BY JSON array/object aggregates accept the tested retracting input on both releases;
there is no blanket JSON changelog exclusion for these forms. Tests preserve array order and
duplicates, NULL/ABSENT clauses, escaping, and duplicate-object-key errors before NULL omission.
Object keys sort by Java UTF-16 order, including supplementary Unicode keys. JSON aggregate
FILTER syntax is rejected by both released parsers. Flink 2.2 preserves decimal scale in JSON
arrays (`[1.00,2.00]`); Flink 1.18 returns `[1,2]`. A NULL object key fails at accumulation on
2.2, even with ABSENT ON NULL and a NULL value. On 1.18 the non-null-value case fails at
serialization, while ABSENT ON NULL with a NULL value returns `{}`. A singleton malformed
Java UTF-16 string remains visible before aggregation but normalizes at the tested global
and keyed aggregate boundaries, with either object-reuse setting and either changelog mode.
Their resolved result is VARCHAR(2000) NOT NULL; LISTAGG retains nullable STRING. Empty bounded
input in streaming mode emits no aggregate row. Native string/JSON coverage is described above.

**Local group aggregate (two-phase local half) only:**

- Any aggregate other than SUM/MIN/MAX/COUNT/AVG/exact SUM0 or insert-only SINGLE_VALUE over STRING/VARCHAR/FLOAT/DOUBLE.
- A SUM/MIN/MAX value type outside bigint/int/smallint/tinyint/float/double/decimal (MIN/MAX also admit
  strings, DATE, TIME, BOOLEAN and timestamps), or an AVG value type outside
  bigint/int/smallint/tinyint/float/double/decimal.
- A `COUNT(DISTINCT)` value type outside bigint/int/smallint/tinyint/float/double/string/decimal/
  BOOLEAN/DATE/TIME/TIMESTAMP/TIMESTAMP_LTZ, or a `SUM(DISTINCT)` value outside bigint/int/smallint/tinyint/DECIMAL;
  DECIMAL sum partials with a different scale/precision other than 38;
  `AVG(DISTINCT)` outside integers/DECIMAL or with incorrectly widened partials;
  `MIN`/`MAX` with a retained physical `DISTINCT` flag (Flink normally removes it).
- A partial whose declared type differs from what the native side emits — defensive only, not
  reachable from Flink's own planner.
- A retracting input with any aggregate other than plain COUNT/AVG/exact SUM0/exact integer SUM.

**Global group aggregate (two-phase merge) only:**

- Any merge other than SUM/MIN/MAX/COUNT/AVG/exact SUM0 or the typed SINGLE_VALUE split described above.
- A partial column outside bigint/int/smallint/tinyint/float/double/decimal (strings, DATE, TIME, BOOLEAN and timestamps
  allowed under MIN/MAX).
- An AVG whose partial pair isn't `(bigint, bigint)` for an integer average, `(double, bigint)` for
  float/double, or `(decimal(38, s), bigint)` for decimal.
- A distinct merge outside the local half's `COUNT`/`SUM(DISTINCT)` scope.
- A retracting merge with any aggregate other than plain COUNT/AVG/exact SUM0/exact integer SUM (those merge natively, the
  `count1` partial driving per-key liveness).
- An unsupported grouping-key or output column type.

## Narrow integer validation and timing

`FlinkNarrowGroupAggregateSqlHarnessTest` compares values, resolved schemas and native plans with
released Flink 2.2.1 and 1.18.1. Cases include overflow in both directions, NULL-only and empty
inputs, FILTER, DISTINCT, multiple partial bundles, and the single-phase retracting changelog.
Native tests additionally cover memory snapshots and RocksDB checkpoint/reopen with duplicate
extrema, wrapping sums and distinct multiplicities.

`NarrowGroupAggregateBenchmark` measures SUM/MIN/MAX over both narrow integer columns, with
2,000,000 runtime rows, 64 keys, and NULLs every seventh row. A local ARM64/JDK 17 run on Flink
2.2.1 (2026-09-24) used the release native build with mimalloc, one warmup and three interleaved
measured trials per engine. Both row/Arrow transposes and the row sink are included; the
two-phase bundle size is 1024.

| Plan | Flink median (s) | Native median (s) | Flink/native |
|---|---|---|---|
| Single-phase | 0.783 | 0.914 | 0.856x |
| Two-phase | 0.767 | 0.664 | 1.156x |

The single-phase standalone query is slower in this diagnostic; two-phase benefits from local
partial aggregation. This coverage also permits narrow aggregates inside larger native pipelines;
these measurements do not establish a general speedup.

```sh
SF_BENCHMARK=true mvn test -Pbench -pl streamfusion-runtime -am \
  -Dtest=NarrowGroupAggregateBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dnarrow.warmup=1 -Dnarrow.runs=3
```

## Timestamp extrema validation and timing

SQL parity tests cover single-phase and insert-only two-phase timestamp extrema at precisions
0, 3, 6 and 9 in UTC, Asia/Shanghai and America/Los_Angeles. Inputs include years 0001 and 9999,
negative epochs, fractional ties, duplicate values, NULL/all-NULL groups, and filtered extrema.
A retracting SQL case compares the complete changelog, including duplicate deletion and empty
group removal. Native tests also cover the full i64-millisecond range plus fractional nanos,
local/global merges, memory snapshots, and RocksDB checkpoint/reopen with subsequent retractions.

Release diagnostics on Apple M4 Pro, JDK 17, Flink 2.2.1 (2026-09-16): two million generated
rows, 64 groups, timestamp values cycling over 4,096 samples, one-eighth NULLs, parallelism 1,
two warmups and five measured runs in alternating engine order. The two-phase bundle size is
1,024. Both row/Arrow transposes remain in the measured plan, with a rowwise blackhole sink.

```sh
TZ=UTC SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=TimestampExtremaBenchmark -Dextrema.rows=2000000 \
  -Dextrema.warmup=2 -Dextrema.runs=5
```

| MIN and MAX query | Flink median (s) | Native median (s) | Flink/native ratio |
|---|---:|---:|---:|
| Single-phase TIMESTAMP(9) | 0.467 | 3.037 | 0.154x |
| Single-phase TIMESTAMP_LTZ(9) | 0.469 | 3.053 | 0.153x |
| Two-phase TIMESTAMP(9) | 0.642 | 1.604 | 0.400x |
| Two-phase TIMESTAMP_LTZ(9) | 0.684 | 1.614 | 0.424x |

The standalone native aggregate is slower in all four cases. It currently retains the existing
multiset/scalar-state machinery for exact timestamp ordering and recovery. This is coverage for
larger native pipelines, not an aggregate speedup; an append-only timestamp accumulator and
reduced scalar materialization remain performance opportunities.

## DATE, TIME and BOOLEAN validation and timing

Runtime-source SQL tests check resolved output types and complete changelogs for extrema,
FIRST_VALUE/LAST_VALUE and SINGLE_VALUE. Cases include filtered state, duplicate retractions,
all-NULL and empty groups, group deletion/recreation, TIME(0)/TIME(3), and SINGLE_VALUE
cardinality errors. Released Flink 2.2.1 and 1.18.1 are covered, with the 1.18 host restrictions
above explicitly skipped. Native state tests cover snapshot recovery, exact TTL expiry,
arrival order and the DATE/BOOLEAN RocksDB element codec. Existing string and timestamp
regressions remain controls.

The following release+mimalloc diagnostic used an Intel Core i7-12650H under Linux/WSL,
JDK 17 and Flink 2.2.1 on 2026-09-26: two million runtime rows, 64 groups, one-eighth NULLs,
parallelism 1, two warmups and five measured trials in alternating engine order. DATE and
TIME cycle over 4,096 samples; BOOLEAN alternates across groups of 64 input records.
Two-phase bundles contain 1,024 rows. Both row/Arrow transposes and the rowwise blackhole
sink remain in the measured plan.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=TimestampExtremaBenchmark -Dextrema.types=DATE,TIME,BOOLEAN \
  -Dextrema.rows=2000000 -Dextrema.warmup=2 -Dextrema.runs=5
```

| MIN and MAX query | Flink median (s) | Native median (s) | Flink/native ratio |
|---|---:|---:|---:|
| Single-phase DATE | 0.484 | 0.482 | 1.003x |
| Single-phase TIME | 0.459 | 0.485 | 0.947x |
| Single-phase BOOLEAN | 0.454 | 0.453 | 1.003x |
| Two-phase DATE | 0.625 | 0.769 | 0.812x |
| Two-phase TIME | 0.552 | 0.690 | 0.800x |
| Two-phase BOOLEAN | 0.560 | 0.625 | 0.896x |

This baseline predates single-destination exchange forwarding and typed running extrema.
Insert-only DATE/TIME/BOOLEAN MIN/MAX now retains one typed extreme in both local and global
state; retracting input retains counted state. At twenty million rows, single-phase native
medians are 2.861/2.847/2.807 s versus Flink's 3.835/3.860/3.721 s. Two-phase native medians
are 4.544/4.330/4.263 s versus Flink's 5.070/4.943/4.749 s. See the
[optimization ledger](../optimizations/aggregate-specialization-fast-paths.md) for profiling,
before/after measurements, trial ranges and checkpoint compatibility. This establishes MIN/MAX
performance for the measured workloads; other aggregate functions need separate measurements.

`GroupedValueBenchmark` also accepts `-Dgrouped.value.types=TIME,BOOLEAN` and
`-Dgrouped.value.twoPhase=false` to measure the temporal/Boolean single-phase coverage.
The default types remain BIGINT/STRING for first/last and STRING for DISTINCT or SINGLE_VALUE;
SINGLE_VALUE still defaults to two phases unless overridden. SINGLE_VALUE assigns each input
its own key so the workload satisfies its cardinality contract.

The [output allocation optimization](../optimizations/aggregate-specialization-fast-paths.md)
removes a temporary tuple vector per emitted update. On the downstream integration branch,
TIME/BOOLEAN first/last now beats Flink at twenty million rows and is approximately tied at
two million. The upstream temporal branch independently measures two-million-row TIME/BOOLEAN first/last at
0.573/0.551 s versus Flink 0.613/0.573 s after the shared transpose optimization.
SINGLE_VALUE at 500,000 unique keys now measures 0.373/0.328 s for TIME/BOOLEAN versus
Flink 0.402/0.363 s; independent repeats and one-million-row trials also beat the matched
controls. The ledger records before/after controls, configurations, and trial ranges.

### Two-phase DISTINCT type coverage

Runtime-source regressions assert both native local/global operators for COUNT DISTINCT over
BOOLEAN and TIMESTAMP_LTZ(3/9), and SUM DISTINCT over DECIMAL(19,2). Five-row bundles cover
repeated values across flushes, independently filtered/shared views, all-NULL groups, empty
input, and global aggregation. Native restore tests checkpoint the merged distinct state,
then merge duplicate and new values without losing multiplicities or decimal result scale.
This extends the existing insert-only split; retracting two-phase DISTINCT still falls back.
Two-phase SUM/AVG DISTINCT supports DECIMAL precision 20–38, including the DECIMAL(20,2)
case in [#231](https://github.com/datafusion-contrib/StreamFusion/issues/231). Decimal overflow
can reset SUM to NULL, and a later addition restarts it, making map iteration order observable.
The local stage preserves Flink's group-map order and the copied/serialized membership-map
order. The global stage unions local views in a temporary map and folds it at bundle flush;
immediate per-view folding can produce a different overflow result. The
native local-view ordering model is checked against 55 shared released-host fixtures covering
resizes, collision trees, copies, NULLs and duplicate insertions. Local shared filters retain
the same union order with zero-count entries for inactive values. The native global stage now
buffers those views and folds their temporary-map order at the logical bundle boundary;
regressions cover overflow, shared filters, duplicate bundles and retained-memory release.
Wide SUM checkpoints preserve the running sum and live count independently of membership
order, including NULL after overflow. Recovery tests cover raw snapshots, RocksDB import,
checkpoint/reopen and export. Local flush retains its state reservation while constructing
output and reserves bounded scratch space for wide-decimal view copies; a rejected reservation
leaves the bundle intact. The local stage now emits wide-decimal groups in Flink's map order,
including bucket capacity retained across flushes, and carries declared timestamp-key precision
through JNI for matching binary-key hashes. The retained bucket allocation remains accounted
between bundles. All-DISTINCT local bundles also reuse empty membership-map allocations,
limited to 128 groups and a conservative 1 MiB estimate per operator. The cache retains no
keys, input batches, membership values, or decimal ordering structures. Retained map capacity
remains charged to the task budget; unused cached states are discarded under budget pressure
and before flush scratch-space reservations. Partial sums, counts and membership start empty
on reuse, and operator close releases the retained reservation.
Shuffled runtime-source probes pass with this integration; ordering values
inside each group's membership map alone was insufficient. The shared ordering core matches
18 released-host binary-group fixtures, including bucket collision trees, resize splits and
clear/reuse. With positive retention, the wide-decimal global stage retains and accounts
for incoming Arrow batches until flush, then reads and refreshes durable state using the
flush clock, matching Flink's temporary-bundle merge. Recovery regressions cover buffered
SUM, AVG and COUNT together through raw snapshots, canonical migration and RocksDB reopen.
Operator tests on both released Flink lines verify flush-time expiry and refresh across a
checkpoint with memory and direct RocksDB state.
Distinct binary keys with identical full hashes introduce
JVM identity-based ordering: overflow-sensitive stock Flink jobs can then return different final
SUMs for identical input. Tests for that case check valid host outcomes; deterministic cases
continue to require exact parity.

The DISTINCT, wide-decimal ordering, timestamp grouping and existing two-phase SQL suites
pass 58 cases on Flink 2.2.1; Flink 1.18.1 passes 57 with one released-host capability skip.
The identity-collision test checks native output against the same valid host outcomes.

For the wider-decimal release comparison, `DistinctAggregateBenchmark` accepts
`-Ddistinct.precision=38`; set `-Ddistinct.average=true -Ddistinct.twoPhase=true` for AVG.
Both forms retain the same two-million-row runtime input, 64 keys, 1,024-row bundles,
two warmups and five measured runs in alternating engine order. Native plan assertions
require both aggregate stages and both row/Arrow transposes; output uses a rowwise blackhole
sink. Profiling-driven specialization now includes primitive/boolean/timestamp membership,
direct Arrow view construction, cached timestamp writer access, and allocation-free bucket
partitioning in the host-order model. On Intel Core i7-12650H / Linux-WSL / JDK 17 / Flink 2.2.1,
release+mimalloc (2026-09-27), COUNT/SUM with DECIMAL(38,2) improved from **4.855 s to 1.903 s
native (2.552x)**; the contemporaneous Flink control was **1.466 s (0.771x Flink/native)**.
Two-phase AVG with DECIMAL(38,2) improved from **2.500 s to 1.965 s native (1.272x)** versus
**1.560 s Flink (0.794x)**. Both wide-decimal workloads still trail stock Flink. Full trial
ranges, controls, profiles, and the remaining bottlenecks are recorded in
[aggregate specialization](../optimizations/aggregate-specialization-fast-paths.md#distinct-averages-wide-decimals-and-timestamp-membership).
Correctness coverage is complete; outperforming Flink on these workloads remains unfinished.

A release+mimalloc diagnostic on Intel Core i7-12650H, Linux/WSL, JDK 17, Flink 2.2.1
(2026-09-27) combines the three newly admitted aggregate forms over two million runtime rows,
64 keys, 128 timestamp/decimal values per key, and NULLs every seventh row. It uses
1,024-row bundles, two warmups and five measured runs in alternating engine order, and asserts
both native aggregate stages and both row/Arrow transposes with a rowwise blackhole sink.
Median elapsed time was **1.400 s for Flink and 4.563 s for native (0.307x)**. Native trials
ranged from 4.449–4.625 s; Flink trials ranged from 1.379–1.526 s. This is a coverage extension
using existing state and view handling; the standalone workload is substantially slower,
and no performance improvement was established by that baseline. It predates typed maps,
column readers and direct partial-view builders. The current implementation specializes BOOLEAN
and full-range timestamp membership as well as the existing primitive maps, and caches the
transpose timestamp layout. See the [optimization ledger](../optimizations/aggregate-specialization-fast-paths.md)
for current comparisons and validation. With the same two-million-row workload and an explicit
2 GiB heap, the optimized native median is **1.186 s versus Flink 1.416 s**. At twenty million
rows, native is **11.199 s versus Flink 12.235 s**; both comparisons retain the row source/sink,
transposes, and both aggregate stages.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=DistinctAggregateBenchmark -Ddistinct.rows=2000000 \
  -Ddistinct.warmup=2 -Ddistinct.runs=5 -Dsf.extraJvmArgs=-Xmx2g
```

### Single-phase DISTINCT averages

Runtime-source checks compare resolved types and complete ordered changelogs for integer and
DECIMAL(20,2) AVG DISTINCT, including duplicate removals, UPDATE_BEFORE/UPDATE_AFTER pairs,
filtered instances, group deletion/recreation, empty/global results and all-NULL groups.
A precision-38 decimal probe verifies that overflow remains NULL after later cancellation.
The new cases and existing grouped/count-distinct suites pass on both released Flink lines:
32 cases on 2.2.1; 31 passed and one host-capability skip on 1.18.1. Native checks cover raw
sum/membership restoration, duplicate retractions, sticky decimal overflow, typed RocksDB
checkpoint/reopen, and memory↔RocksDB canonical-partition migration.
The current integrated branch passes 573 native tests (one ignored). Its focused DISTINCT SQL
suites pass 54 cases on Flink 2.2.1 and 53 on Flink 1.18.1, with one documented host-capability
skip on 1.18.

The existing `DistinctAggregateBenchmark` accepts `-Ddistinct.average=true` to compare
single-phase AVG DISTINCT over TINYINT, BIGINT and DECIMAL(20,2), retaining runtime rows,
both row/Arrow transposes and the rowwise sink. The default benchmark still measures the
previous two-phase COUNT/SUM DISTINCT type extension.

On Intel Core i7-12650H, Linux/WSL, JDK 17 and Flink 2.2.1 (2026-09-27), release+mimalloc
measured two million rows across 64 keys, with 127 TINYINT, 1,024 BIGINT and 128 decimal
values per key and NULLs every seventh row. Two warmups and five measured runs alternate
engine order. Median elapsed time was **6.421 s for Flink and 2.810 s for native (2.285x)**.
Flink trials ranged from 6.346–6.591 s; native trials ranged from 2.769–2.964 s. The baseline
routes this unsupported AVG DISTINCT query entirely to Flink. These are workload-specific
whole-job results, including the unchanged row/Arrow perimeter, and precede #246's typed maps.

After integrating the typed-column DISTINCT path and shared transpose optimizations, the
same 2M-row workload with an explicit 2 GiB heap measures **2.609 s native versus 6.574 s
Flink (2.519x)**. Native trials range 2.596–2.633 s and Flink trials 6.554–6.616 s; all other
configuration above is unchanged. The explicit heap setting makes this a fresh matched-resource
comparison rather than a claimed isolated speedup over the earlier run.
At 5M rows with the same configuration, native measures **6.399 s (6.354–6.508) versus
Flink 16.267 s (16.054–16.322), 2.542x**. The native advantage persists as the input grows.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=DistinctAggregateBenchmark -Ddistinct.average=true \
  -Ddistinct.rows=2000000 -Ddistinct.warmup=2 -Ddistinct.runs=5 \
  -Dsf.extraJvmArgs=-Xmx2g
```

### Two-phase DISTINCT averages

The released-Flink SQL checks now include split AVG DISTINCT over narrow/wide integers and
DECIMAL(19,2), repeated values across multiple bundles, shared COUNT/SUM/AVG views,
independently filtered views, empty/global results, and an ordinary AVG before a distinct
view. They also verify wider-decimal native admission and fallback for retracting distinct splits. The
combined distinct/two-phase suites pass 49 cases on Flink 2.2.1; Flink 1.18.1 passes 48 with
one host-capability skip. Native tests verify widened partial types, integer overflow,
all-NULL partials, and merging duplicate membership across checkpoint restoration; the full
native core suite passes 584 tests with one ignored.

`-Ddistinct.average=true -Ddistinct.twoPhase=true` selects the split benchmark, using
DECIMAL(19,2) alongside TINYINT and BIGINT. The profiling-driven changes measured on the same
released toolchain (2026-09-27), release+mimalloc, two million runtime rows, 64 keys, 1,024-row
bundles, two warmups and five alternating measured runs improve native elapsed time from
**2.265 s to 1.680 s (1.349x)**. The contemporaneous Flink median is **1.653 s**. Native trials
range from 1.655–1.705 s and Flink from 1.585–1.754 s: this is approximately a tie, not a
proven native win. Both native aggregate stages, both transposes, and the rowwise blackhole
sink remain in the measured plan. See the [profiling and performance analysis](../optimizations/aggregate-specialization-fast-paths.md#distinct-averages-wide-decimals-and-timestamp-membership).

The upstream narrow-decimal branch was also measured after the shared boundary changes. That implementation, with an explicit 2 GiB heap and otherwise the same 2M-row
configuration, now measures **1.275 s native (1.269–1.405) versus Flink 1.609 s (1.487–1.629)**.
At 20M rows it measures **12.071 s native (12.030–12.261) versus Flink 13.511 s
(13.449–13.566)**. Native takes 20.8% and 10.7% less elapsed time respectively. Both comparisons
use two warmups and five alternating trials, retain both transposes, and assert both native
aggregate stages. The explicit heap setting makes these fresh matched-resource comparisons;
they do not isolate any individual optimization's contribution.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=DistinctAggregateBenchmark -Ddistinct.average=true -Ddistinct.twoPhase=true \
  -Ddistinct.rows=2000000 -Ddistinct.warmup=2 -Ddistinct.runs=5 \
  -Dsf.extraJvmArgs=-Xmx2g
# Repeat with -Ddistinct.rows=20000000 for the sustained split comparison.
```

### DISTINCT first/last strings

Runtime-source tests assert the native grouped operator and compare ordered, kinded
changelogs for STRING and bounded VARCHAR inputs. They cover duplicate insertions, removing
one versus the last occurrence, update pairs, reinsertion, NULL-only groups, independent
filters, empty global results and positive-TTL fallback. The grouped-value suite passes
45 cases on Flink 2.2.1 and 31 with 14 host-capability skips on Flink 1.18.1. The full native
core suite passes 584 tests with one ignored, including a checkpoint regression that removes
duplicates and reinserts values after restore. Ordered DISTINCT state uses the existing
ordered snapshot side frames, with multiplicities in their count fields; ordinary ordered
snapshots continue writing one per occurrence.

`GroupedValueBenchmark` accepts `-Dgrouped.value.distinct=true` for the new STRING forms.
It retains two million runtime rows, 64 keys, one-eighth NULLs, parallelism one, both
row/Arrow transposes and a rowwise blackhole sink. The plan must contain the native grouped
aggregate and both transposes. The source cycles 1,024 strings across 64 keys (14 non-NULL
unique values per key); two warmups precede five alternating measured runs per engine.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=GroupedValueBenchmark -Dgrouped.value.distinct=true \
  -Dgrouped.value.rows=2000000 -Dgrouped.value.warmup=2 -Dgrouped.value.runs=5
```

On Intel Core i7-12650H, Linux/WSL, JDK 17 and Flink 2.2.1 (2026-09-27), release+mimalloc
measured medians of **0.821 s for Flink and 0.714 s for native (1.150x)**. Flink trials ranged
from 0.796–0.925 s; native trials ranged from 0.712–0.747 s. This is a workload-specific
whole-job result for duplicate-heavy append-only input; it does not establish a speedup for
high-cardinality or retracting workloads. The unsupported baseline routes this query to Flink.

### Two-phase SINGLE_VALUE strings

Runtime-source regressions require both native aggregate stages and compare values/types
for mixed AVG/SINGLE_VALUE/DISTINCT accumulators, a single NULL, and empty global input.
Error regressions use bundles of one and five rows to exercise global and local cardinality
failures, including two NULL elements. They compare the exception type, message and phase
against released Flink. A separate control preserves Flink's planning failure for SQL
FILTER on SINGLE_VALUE. Native tests verify INT partial counts, checkpoint restoration of
NULL cardinality, and the host's zero/negative-count merge expressions.

`GroupedValueBenchmark` accepts `-Dgrouped.value.single=true` for an insert-only split
SINGLE_VALUE query. Every row has a unique key, so each group has exactly one element
(including NULL); this avoids benchmarking a query that must fail cardinality validation.
The benchmark asserts both native aggregate stages and both row/Arrow transposes, retains
the rowwise source and blackhole sink, and uses parallelism one with 1,024-row bundles.
Two warmups precede five alternating measured runs per engine.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=GroupedValueBenchmark -Dgrouped.value.single=true \
  -Dgrouped.value.rows=200000 -Dgrouped.value.warmup=2 -Dgrouped.value.runs=5
```

The combined grouped-value, two-phase and DISTINCT SQL suites pass 100 tests on Flink 2.2.1
and 85 with 15 host-capability skips on Flink 1.18.1. The native core suite passes 584 tests
with one ignored. On Intel Core i7-12650H, Linux/WSL, JDK 17 and Flink 2.2.1 (2026-09-27),
release+mimalloc measured 200,000 rows/keys with one-eighth NULLs: median **0.374 s for Flink
and 0.327 s for native (1.144x)**. Flink trials ranged from 0.370–0.483 s; native trials ranged
from 0.293–0.444 s. The ranges overlap, so this short workload establishes coverage with a
modestly faster observed median, not a general speedup. The unsupported baseline uses Flink.

### Retracting SUM partial validation and release comparison

The four-width SQL matrix checks complete ordered changelogs at logical bundle sizes 1/2/4,
including mixed SUM/COUNT/AVG/FILTER, NULL keys/values, zero, negative record counts,
zero-count sum adjustments, deletion/recreation, and signed overflow. A 2,688-row single-key
case spans multiple Arrow batches with deterministic count-trigger cadence; independent keys
can alter the global count-trigger cadence when local bundles enumerate keys in a different order.
DECIMAL/FLOAT/DOUBLE SUM_RETRACT execute through explicit stock fallback. Operator tests cover
memory/RocksDB recovery, backend migration, key-group rescaling and TTL refresh/expiry.

`RetractingSumPartialsBenchmark` runs the updating MAX→SUM/COUNT/AVG/FILTER cascade from a
rowwise runtime source to a rowwise blackhole sink. Candidate plans retain both transposes;
metrics require both native local and both native global aggregations to consume rows.
The released Flink 2.2.1/JDK17 comparison uses parallelism 1, 128 inner/16 outer keys,
nullable INT/BIGINT, logical bundles of 512, memory/native RocksDB state, two warmups and
three alternating measured trials per engine, an 8g Java heap and 48g task off-heap reservation.
Planning/setup are excluded; job startup,
JNI, conversions, state and completion are included. Final five-million-row timings use
the production release build with mimalloc:

| Type / state | Stock median (range), s | Native median (range), s | Stock/native |
| --- | ---: | ---: | ---: |
| INT / memory | 1.735 (1.683–1.743) | 1.195 (1.183–1.196) | 1.45× |
| BIGINT / memory | 1.605 (1.578–1.607) | 1.101 (1.093–1.107) | 1.46× |
| INT / RocksDB | 1.814 (1.731–1.819) | 1.508 (1.506–1.568) | 1.20× |
| BIGINT / RocksDB | 1.892 (1.844–1.951) | 1.576 (1.559–1.688) | 1.20× |

Previous canonical `74822fd4` takes the explicit whole-island SUM_RETRACT fallback: its
paired enabled/stock measurements remain near parity (0.98–1.04× stock/native).
The final candidate is 1.20–1.46× its paired stock control. Independent invocation times
vary and must not be used as a direct before/after ratio. These numbers cover the stated
cardinality/type/query matrix, not every aggregate workload.
[All completed trials](../benchmarks/results/retracting-sum-partials-2026-10-14.json)
include warmups, prior fallback, initial one-million-row startup-sensitive runs, repeated
non-mimalloc release trials, and the final production build. The final release DSO SHA256 is
`205e4c00922126ec59d85ea505baed6c94e2cf0b9aa94e1eb572d12f2767a10a`.
Criterion's `group_record_counter/sum_count` fixtures separately exercise the production
partial merge at 256/4096 rows and 1/64 keys, including NULL contribution counts and negative
record counters; that native boundary does not stand in for the complete Flink comparison.
