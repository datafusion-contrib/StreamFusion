# GROUP BY

**Status:** Native for non-windowed `GROUP BY`, both as the single-phase (immediate) plan and the
two-phase mini-batch plan, over the aggregate/value-type combinations in [Type
support](#type-support) below — with a real, enumerated gap list, see [Still falls
back](#still-falls-back).

Flink picks between the two plan shapes itself, based on `table.exec.mini-batch.enabled`; this page
covers both, since a query only accelerates when whichever shape Flink chose is fully native.

## Single-phase

Flink's internal `SUM0` is also admitted when a nonempty grouping reads one unfiltered,
non-null integer column. A live group then has at least one contributing value, making SUM
and SUM0 equivalent. This includes an outer SUM over a window COUNT result. Nullable inputs,
global aggregation, filtered or DISTINCT SUM0, and non-integer values remain outside this rule.

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
at most 19, as described below.

`TINYINT` and `SMALLINT` `SUM`/`MIN`/`MAX` retain their input width, including the local
partials of an insert-only two-phase plan. SUM wraps on overflow at 8 or 16 bits rather
than widening to BIGINT. Single-phase retractions subtract at the same width; MIN/MAX
retain duplicate multiplicities until the last occurrence is removed. NULL inputs are
ignored and all-NULL groups return NULL. FILTER and integer SUM(DISTINCT) preserve these
rules across batches and checkpoint restore. Retracting two-phase SUM/MIN/MAX retain
the general fallback described below.

Insert-only floating MIN/MAX uses primitive comparisons, retaining the first signed zero or
NaN on a tie. Retracting floating extrema remain subject to the type admission below.

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

### FIRST_VALUE, LAST_VALUE and SINGLE_VALUE

The one-argument forms run natively in the single-phase plan over TINYINT, SMALLINT,
INT, BIGINT, DECIMAL, CHAR/VARCHAR, DATE, TIME, BOOLEAN, TIMESTAMP and TIMESTAMP_LTZ. Each preserves
the input value's type and precision. FIRST_VALUE/LAST_VALUE support per-aggregate FILTER.
Flink 1.18.1 cannot plan CHAR or temporal FIRST_VALUE/LAST_VALUE and fails a type assertion for
SINGLE_VALUE over TIME(0), including string casts to TIME(3) that resolve to TIME(0);
those host limitations remain unavailable on that release. Typed TIME(3) input is supported.

FIRST_VALUE and LAST_VALUE skip NULLs and follow arrival order within a key. Append-only
input retains one scalar. Retracting input retains the ordered non-NULL occurrences;
a retraction removes the oldest matching occurrence, including when values repeat across
Arrow batches. Removing every contributing value yields NULL, and removing the last
record deletes the group. Results depend on arrival order, so SQL parity fixtures use a
controlled source rather than asserting equal results from independently reordered inputs.

`FIRST_VALUE(DISTINCT string)` and `LAST_VALUE(DISTINCT string)` also run in a single
phase over STRING/VARCHAR. They retain the arrival order of unique live values and their
multiplicities: inserting a duplicate does not move its position, and only the last
retraction removes it. Reinsertion after complete removal gives it a new position. NULLs
are ignored, and filtered instances have independent membership. Checkpoints retain both
order and multiplicities. These DISTINCT forms require zero state retention because
Flink's separate membership/order maps can expire independently.

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

**Scope.** `SUM`/`MIN`/`MAX`/`COUNT` over bigint/int/double value columns (Flink's `SUM` partial
keeps the value's own type, so nothing is lost to widening in the split), and `AVG` over the full
single-phase numeric set — bigint/int/smallint/tinyint/float/double. An `AVG` spans **two
positional partials**: the widened running sum (bigint for integer inputs, double for
float/double) plus the bigint non-null count. The local runs these as a widened-sum state and a
`COUNT` over the same column; the global folds the pre-summed pair into the ordinary `AVG` state
(the count partial bumps the non-null count), so the final divide/truncate/cast-back — including
the cast back to a narrow integer or float result — is byte-identical to the single-phase `AVG`.

Insert-only two-phase `MIN`/`MAX` also admit DATE, TIME, BOOLEAN, `TIMESTAMP` and
`TIMESTAMP_LTZ`. Both halves carry the complete typed value and timestamp components; a partial must retain the input's logical type and precision.
Two-phase retracting extrema retain the shared COUNT/AVG-only gate described below.

Decimal `SUM`/`MIN`/`MAX`/`AVG` carry through the split too: `SUM`'s partial is the i128 running
sum as `DECIMAL(38, s)` (a bundle overflow emits NULL and latches the merged `AVG` NULL, skipped
by the `SUM` merge — the host's own null-propagation), `MIN`/`MAX` partials keep `DECIMAL(p, s)`
through the extremes multiset, and `AVG` merges the `(DECIMAL(38, s), bigint)` pair into the exact
division emit.

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
BOOLEAN and TIMESTAMP_LTZ; `SUM(DISTINCT)` over bigint/int/smallint/tinyint and DECIMAL
with precision at most 19.
Decimal sum partials must be DECIMAL(38, input-scale), while the distinct view preserves each
input value's original precision and scale. Boolean and timestamp views retain their Arrow
value types, including fractional timestamp nanoseconds. The merge folds in set-iteration
order, so order-sensitive float/double sums stay on the host.

Insert-only `AVG(DISTINCT)` also rides the split for integers and DECIMAL precision at most 19.
The local keeps two distinct states: a widened sum partial and a BIGINT count partial, while
its membership view retains the original input value type. Empty/all-NULL bundles emit a
zero sum and zero count. The global merges membership entries into the same exact average
state used by single-phase AVG DISTINCT; it does not add the local sums/counts, which would
count values appearing in several bundles more than once. Decimal sum fields must retain
precision 38 and the input scale, and the result type must match Flink's AVG type.

Insert-only two-phase `SINGLE_VALUE(STRING/VARCHAR)` retains a string value and an INT count
in each local bundle. NULL increments the count. Both local accumulation and global merging
raise Flink's cardinality exception on a second element; global checkpoints preserve that
count even when the value is NULL. The global consumes the count partial, rather than
counting bundles. The declared string partial must match the input type, and its count must
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
shape) is native for `COUNT` and `AVG` only — their accumulators are layout-invariant under
retraction. The local subtracts `-U`/`-D` rows, and the appended (or reused) `count1` `COUNT(*)`
partial drives per-key liveness in the global (`-D` and state drop when the merged count reaches
zero, Flink's `RecordCounter` semantics).

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
`MIN`/`MAX` over `DISTINCT`, floating/wide-decimal `AVG(DISTINCT)`, float `SUM`/`MIN`/`MAX`
partials, and — under a
retracting input — any aggregate other than `COUNT`/`AVG` (Flink's `SUM`/`MIN`/`MAX` retract
variants declare extra accumulator fields, and a monotonicity-exempt `MIN`/`MAX` ignores
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
| FLOAT (REAL) | ✗ | ✓ ³ | ✗ | ✗ | ✓ |
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
`FloatAvgAggFunction` does. Non-windowed FLOAT SUM/MIN/MAX still fail the planner's
running-value type gate; support for those types in other aggregate operators does not admit
them here.

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

## Still falls back

Both the single-phase gate and, for the two-phase plan, **both halves independently** must clear
their own matcher before the query accelerates — one operator staying on the host drags the whole
query back via the [all-or-nothing island](index.md#the-all-or-nothing-island) rule.

**Single-phase / either two-phase half in common:**

- A UDAF (no native path for arbitrary user aggregation logic).
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

**Local group aggregate (two-phase local half) only:**

- Any aggregate other than SUM/MIN/MAX/COUNT/AVG or insert-only SINGLE_VALUE over STRING/VARCHAR.
- A SUM/MIN/MAX value type outside bigint/int/smallint/tinyint/double/decimal (MIN/MAX also admit
  strings, DATE, TIME, BOOLEAN and timestamps), or an AVG value type outside
  bigint/int/smallint/tinyint/float/double/decimal.
- A `COUNT(DISTINCT)` value type outside bigint/int/smallint/tinyint/float/double/string/decimal/
  BOOLEAN/TIMESTAMP_LTZ, or a `SUM(DISTINCT)` value outside bigint/int/smallint/tinyint/DECIMAL;
  DECIMAL input precision above 19 or sum partials with a different scale/precision other than 38;
  `AVG(DISTINCT)` outside integers/DECIMAL precision ≤19 or with incorrectly widened partials;
  `MIN`/`MAX` over `DISTINCT`.
- A partial whose declared type differs from what the native side emits — defensive only, not
  reachable from Flink's own planner.
- A retracting input with any aggregate other than plain COUNT/AVG.

**Global group aggregate (two-phase merge) only:**

- Any merge other than SUM/MIN/MAX/COUNT/AVG.
- A partial column outside bigint/int/smallint/tinyint/double/decimal (strings, DATE, TIME, BOOLEAN and timestamps
  allowed under MIN/MAX).
- An AVG whose partial pair isn't `(bigint, bigint)` for an integer average, `(double, bigint)` for
  float/double, or `(decimal(38, s), bigint)` for decimal.
- A distinct merge outside the local half's `COUNT`/`SUM(DISTINCT)` scope.
- A retracting merge with any aggregate other than plain COUNT/AVG (those merge natively, the
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

Single-phase DATE and BOOLEAN are approximately tied with Flink on this workload; TIME and
all two-phase cases are slower. These additions reuse the existing typed multiset and ordered
value state to extend native pipeline coverage. They do not establish a standalone aggregate
speedup; reducing per-row scalar materialization remains a separate optimization opportunity.

### Two-phase DISTINCT type coverage

Runtime-source regressions assert both native local/global operators for COUNT DISTINCT over
BOOLEAN and TIMESTAMP_LTZ(3/9), and SUM DISTINCT over DECIMAL(19,2). Five-row bundles cover
repeated values across flushes, independently filtered/shared views, all-NULL groups, empty
input, and global aggregation. Native restore tests checkpoint the merged distinct state,
then merge duplicate and new values without losing multiplicities or decimal result scale.
This extends the existing insert-only split; retracting two-phase DISTINCT still falls back.
The remaining wider-decimal two-phase SUM/AVG DISTINCT gap is tracked in
[#231](https://github.com/datafusion-contrib/StreamFusion/issues/231).

Two-phase SUM/AVG DISTINCT over DECIMAL precision 20–38 remains on Flink. Decimal overflow can
reset SUM to NULL, and a later addition restarts it, making map iteration order observable.
A DECIMAL(38,0) runtime probe with `9e37`, `9e37 - 1`, and `-9e37` produced different host/native
results in eight of twelve trials before the wider admission was removed. For precision at
most 19, even the sum of every positive distinct unscaled value is below `5e37`; negative
values have the same bound, so all subset sums fit DECIMAL(38) regardless of iteration order.
The DECIMAL(20,2) two-phase case from #231 remains part of the open coverage gap. A released-host
regression also demonstrates that copying/serializing the same membership map can change
its iteration order and overflow result. Flink's global stage first unions local views in
a temporary map, then merges that map at bundle flush; immediate per-view folding is not
equivalent for wide decimals. Matching only the local view order is insufficient. These
ordering and buffering contracts must be preserved before widening the admission gate.

The eight new SQL cases pass on Flink 2.2.1 and 1.18.1, along with the existing two-phase
suite (28 cases on 2.2.1; 27 passed and one released-host capability skip on 1.18.1).

A release+mimalloc diagnostic on Intel Core i7-12650H, Linux/WSL, JDK 17, Flink 2.2.1
(2026-09-27) combines the three newly admitted aggregate forms over two million runtime rows,
64 keys, 128 timestamp/decimal values per key, and NULLs every seventh row. It uses
1,024-row bundles, two warmups and five measured runs in alternating engine order, and asserts
both native aggregate stages and both row/Arrow transposes with a rowwise blackhole sink.
Median elapsed time was **1.400 s for Flink and 4.563 s for native (0.307x)**. Native trials
ranged from 4.449–4.625 s; Flink trials ranged from 1.379–1.526 s. This is a coverage extension
using existing state and view handling; the standalone workload is substantially slower,
and no performance improvement is claimed. The run precedes the separate typed-DISTINCT-map
optimization in #246.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=DistinctAggregateBenchmark -Ddistinct.rows=2000000 \
  -Ddistinct.warmup=2 -Ddistinct.runs=5
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

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=DistinctAggregateBenchmark -Ddistinct.average=true \
  -Ddistinct.rows=2000000 -Ddistinct.warmup=2 -Ddistinct.runs=5
```

### Two-phase DISTINCT averages

The released-Flink SQL checks now include split AVG DISTINCT over narrow/wide integers and
DECIMAL(19,2), repeated values across multiple bundles, shared COUNT/SUM/AVG views,
independently filtered views, empty/global results, and an ordinary AVG before a distinct
view. They also verify fallback for wider decimals and retracting distinct splits. The
combined distinct/two-phase suites pass 49 cases on Flink 2.2.1; Flink 1.18.1 passes 48 with
one host-capability skip. Native tests verify widened partial types, integer overflow,
all-NULL partials, and merging duplicate membership across checkpoint restoration; the full
native core suite passes 565 tests with one ignored.

`-Ddistinct.average=true -Ddistinct.twoPhase=true` selects the split benchmark, using
DECIMAL(19,2) alongside TINYINT and BIGINT. On the same machine and released toolchain above
(2026-09-27), release+mimalloc, two million runtime rows, 64 keys, 1,024-row bundles, two
warmups and five alternating measured runs produced medians of **1.562 s for Flink and
2.200 s for native (0.710x)**. Flink trials ranged from 1.545–1.891 s; native trials ranged
from 2.194–2.356 s. The plan asserts both native aggregate stages and both row/Arrow
transposes, and retains the rowwise blackhole sink. This is a correctness/coverage extension;
the standalone split workload remains slower than Flink. It precedes #246's typed maps.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=DistinctAggregateBenchmark -Ddistinct.average=true -Ddistinct.twoPhase=true \
  -Ddistinct.rows=2000000 -Ddistinct.warmup=2 -Ddistinct.runs=5
```

### DISTINCT first/last strings

Runtime-source tests assert the native grouped operator and compare ordered, kinded
changelogs for STRING and bounded VARCHAR inputs. They cover duplicate insertions, removing
one versus the last occurrence, update pairs, reinsertion, NULL-only groups, independent
filters, empty global results and positive-TTL fallback. The grouped-value suite passes
45 cases on Flink 2.2.1 and 31 with 14 host-capability skips on Flink 1.18.1. The full native
core suite passes 566 tests with one ignored, including a checkpoint regression that removes
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
and 85 with 15 host-capability skips on Flink 1.18.1. The native core suite passes 568 tests
with one ignored. On Intel Core i7-12650H, Linux/WSL, JDK 17 and Flink 2.2.1 (2026-09-27),
release+mimalloc measured 200,000 rows/keys with one-eighth NULLs: median **0.374 s for Flink
and 0.327 s for native (1.144x)**. Flink trials ranged from 0.370–0.483 s; native trials ranged
from 0.293–0.444 s. The ranges overlap, so this short workload establishes coverage with a
modestly faster observed median, not a general speedup. The unsupported baseline uses Flink.
