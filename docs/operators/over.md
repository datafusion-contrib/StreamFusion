# OVER

**Status:** Native across all four time-based frame shapes and the verified non-time ROW_NUMBER
shape, with the gaps enumerated below.

Time-based `OVER` runs over one ascending order (rowtime or, where noted, proctime) and one window group. Each
aggregate reads its own — possibly different — value column of type
bigint/int/smallint/tinyint/double/float, exact DECIMAL, CHAR/VARCHAR, DATE, TIME, BOOLEAN,
TIMESTAMP and TIMESTAMP_LTZ. Ordinary COUNT also admits VARBINARY, day-time/year-month
intervals and ARRAY/ROW/MAP/MULTISET. DATE/TIME/BOOLEAN support ordinary
COUNT/MIN/MAX/FIRST_VALUE/LAST_VALUE; released Flink 1.18
retains its DATE/TIME FIRST/LAST factory failures. Their frames and checkpoints retain the declared logical type.
Narrow ints and 4-byte float retain the host result type. Decimal SUM/AVG use exact widened
state; temporal extrema and selections retain milliseconds and fractional nanoseconds. `FIRST_VALUE`/`LAST_VALUE` and the window functions
`ROW_NUMBER`/`RANK`/`DENSE_RANK` (no value column, unbounded frame) are admitted alongside the
aggregates below.

Constant NULL arguments are also supported with their declared type, including
`MIN(CAST(NULL AS VARCHAR))`, `MAX(CAST(NULL AS VARCHAR))`, and `COUNT(CAST(NULL AS VARCHAR))`.
They can share a frame with numeric aggregates and `COUNT(*)`; typed NULL Arrow columns are
handled without attempting a numeric conversion.
Running MIN/MAX checkpoints preserve this typed NULL state alongside other aggregates.

Event-time parity fixtures keep the watermark behind the entire input timestamp range until
end of input, so file enumeration order cannot introduce accidental late rows. Late-row tests
control the input order separately.

## Ordinary ROW_NUMBER (Flink 2.2)

An unfiltered `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY v ASC)` also runs over an ordinary
TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, CHAR/VARCHAR, DATE, TIME, BOOLEAN, TIMESTAMP or
TIMESTAMP_LTZ column. Timestamp ordering retains all declared fractional nanoseconds.
This separate matcher requires one ROW_NUMBER group with ROWS UNBOUNDED
PRECEDING through CURRENT ROW, insert-only input, supported partition/payload types and disabled
idle-state TTL. NULLS FIRST/LAST are preserved. Flink 1.18 rejects ordinary non-time ordering;
its validation is unchanged. Other non-time analytic functions and order specifications retain
fallback, including MAP/MULTISET. Descending ordinary
OVER ordering and multiple ORDER BY columns are rejected by released Flink itself,
including otherwise valid ordinary scalar keys. Sixteen released-SQL failure-parity checks
pin both planning restrictions on Flink 2.2.1 and 1.18.1; these are not acceleration gaps.
See the [released ordering audit](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/non-time-over-ordering.md).

ARRAY and ROW sort values are also admitted recursively over the non-floating scalar order
types above, or another admitted ARRAY/ROW. Floating children and
MAP/MULTISET retain fallback. Parent NULLS FIRST/LAST are independent of child ordering:
released Flink always sorts nested NULLs first. For NULLS LAST, two native projections add
and remove a derived parent-NULL flag; the existing ranker orders that flag before the
original complex value with nested NULLS FIRST. This preserves row identity and arrival-order
ties without changing the retained sort-state format. Restored list child field names are
aligned with the declared output schema using the existing recursive Arrow cast helper;
metadata alignment retains value buffers before the normal output gather.

Each new row emits INSERT at its rank, followed by UPDATE_BEFORE/UPDATE_AFTER pairs for the
same existing rows whose ranks shifted. Ties retain arrival order. Timestamp comparison
distinguishes sub-millisecond values. NULL placement, pre-epoch values and nanosecond
fractions survive physical batch splits and two memory/RocksDB restores. The implementation reuses
[Top-N](top-n.md)'s sorted Arrow-row state with an explicit emission mode; ordinary filtered
Top-N keeps its existing rank-slot update contract. Mini-batching does not collapse these
row-identity updates. The generic event-time/proctime OVER guard is unchanged.

`NonTimeRowNumberBenchmark` accepts `rank.orderTypes=TIMESTAMP(9);TIMESTAMP_LTZ(9)`.
The prior-revision control requires the rank itself to remain on Flink while preserving any
other native substitutions that revision supports. Both measured implementations retain the
runtime row source, rowwise sink and their production conversion/state paths.

The timestamp extension passes 42 SQL/operator checks on Flink 2.2.1 and all 88 native
Criterion fixtures. Flink 1.18 passes its two validation/control checks with 40 host-capability
skips. Whole-job release+mimalloc comparisons use JDK 17, parallelism 1, a 2 GiB heap, UTC,
131,072 runtime rows, 1,024 keys, NULL every 31st row, two warmups and five measured trials.
Input arrival varies; all SQL ORDER BY clauses remain ascending.

| Type / arrival | Stock median (range), s | Native median (range), s | Previous SF median (range), s |
| --- | ---: | ---: | ---: |
| TIMESTAMP(9) / ascending | 1.012 (0.997–1.447) | 0.225 (0.198–0.392) | 1.068 (1.013–1.165) |
| TIMESTAMP(9) / descending | 6.837 (6.489–7.228) | 1.888 (1.743–1.942) | 6.940 (6.933–7.766) |
| TIMESTAMP_LTZ(9) / ascending | 1.069 (0.850–1.407) | 0.277 (0.212–0.453) | 0.976 (0.825–1.039) |
| TIMESTAMP_LTZ(9) / descending | 6.131 (6.031–6.305) | 1.832 (1.746–1.837) | 6.922 (6.888–8.097) |

The measured cases improve 3.346–4.492x over paired stock and 3.521–4.744x over the actual
preceding StreamFusion revision. The [112-job artifact](../benchmarks/results/non-time-temporal-row-number-2026-10-19.json)
retains every warmup/trial, configuration, source/DSO hash and 88 allocation/buffer reports.
Exact nanoseconds and pre-epoch values are checked separately by SQL/native fixtures; the
whole-job source derives timestamp order from runtime milliseconds. These measurements do
not establish a universal speedup or bounded retained state.

Every input row remains in state, including duplicate rows; state grows with the input rather
than a configured Top-N bound. Watermarks do not retire this rank state. Native memory
accounting and configured off-heap limits apply. Updating input and positive TTL retain fallback
until their separate contracts are verified. MAP/MULTISET payloads, including nested descendants,
use the tagged map-list transport also used by Top-N. MAP/MULTISET sort or partition fields
retain their separate key-contract gates.

The released-host oracle covers ties, nullable partitions/order, nonmonotonic arrival, reversed
input and NULL placement. Native recovery compares two checkpoint restores on memory/RocksDB
with uninterrupted stock output, because the released heap host failed an injected restore with
a missing prior accumulator. See the [architecture note](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/11-over-incremental-vs-window-exec.md#non-time-row_number-uses-a-retained-sorted-buffer).

### Complex rank measurements (2026-10-19)

Release+mimalloc comparisons on Flink 2.2.1/JDK 17 use 65,536 runtime rows, 1,024 partitions,
parallelism one, a 2 GiB heap, 1,024-row batches and production IPC. Nullable parents occur every
31st input; ARRAY values have two children, nested arrays add a nullable child, and ROW values
have two fields. All SQL ordering is ascending; input arrival varies. Complete jobs include
the row source, both transposes, derived-flag projections, JNI, retained state and row blackhole
sink. Two warmups precede three alternating measured stock/native pairs. Plans and profile
counters require native consumption. Actual previous revision `a9035025` runs the identical
fixture with its preceding release library and complex-rank fallback asserted.

| Type | Arrival | Parent NULLs | Stock median (s) | Previous SF median (s) | Native median (s) | Stock/native |
|---|---|---|---:|---:|---:|---:|
| array | ascending | FIRST | 0.478 | 0.434 | 0.179 | 2.667× |
| array | ascending | LAST | 0.401 | 0.321 | 0.155 | 2.582× |
| array | descending | FIRST | 1.564 | 1.407 | 1.009 | 1.549× |
| array | descending | LAST | 1.600 | 1.523 | 1.008 | 1.587× |
| row | descending | LAST | 1.539 | 1.409 | 1.219 | 1.262× |
| nested_array | descending | LAST | 1.570 | 1.386 | 1.296 | 1.212× |
| row_array | descending | LAST | 1.515 | 1.488 | 1.436 | 1.055× |
| row_array | descending | FIRST | 1.624 | 1.608 | 1.402 | 1.158× |

Complex profiles improve 1.055–2.667× over paired stock and 1.036–2.422× over
the actual previous medians. ROW-of-ARRAY gains are modest. These results cover the declared
small container widths and do not establish a universal throughput gain. The already-native
BIGINT control is 0.503 s current versus 0.488 s previous
(3.1% higher current median); ranges overlap at
0.494–0.506 s and 0.467–0.499 s. No scalar speedup is claimed.
The [180-job artifact](../benchmarks/results/non-time-complex-rank-2026-10-19.json) retains every
warmup/trial, variability, exact source/library hashes, the scalar control, 32 allocation probes
and all released qualification summaries. Flink 2.2 passes 46 SQL/recovery checks and five additional default-NULL, global and
three-way-shuffle checks, plus 27 native
ranker unit tests; Flink 1.18 passes its two host controls with 44 capability skips. Native
fixtures include named list children and raw restore; SQL recovery also covers native RocksDB.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  -Dtest=NonTimeComplexRankBenchmark -Drank.orderTypes=array \
  -Drank.nulls=FIRST,LAST -Drank.cases=ascending,descending \
  -Drank.rows=65536 -Drank.keys=1024 -Drank.warmup=2 -Drank.runs=3 \
  -Dsf.extraJvmArgs=-Xmx2g
```

### Numeric ranking measurements

Release+mimalloc, Flink 2.2.1/JDK 17, Core i7-12650H, 2 GiB heap, parallelism one,
1,024-row batches and production IPC. A runtime row source supplies nullable sort values
(NULL every 31 rows); a blackhole row sink drains every emitted change. Complete jobs include
both transposes, JNI, retained rank state and output materialization. Two warmups precede five
alternating stock/native pairs; plans and native input counters are asserted.

| Rows / partitions | Order type | Arrival | Stock median (range), s | Native median (range), s |
| --- | --- | --- | ---: | ---: |
| 131,072 / 1,024 | BIGINT | ascending | 0.811 (0.647–0.930) | 0.229 (0.212–0.285) |
| 131,072 / 1,024 | BIGINT | descending | 4.727 (4.648–5.338) | 1.775 (1.661–1.840) |
| 262,144 / 2,048 | BIGINT | ascending | 1.649 (1.366–1.816) | 0.400 (0.365–0.426) |
| 262,144 / 2,048 | BIGINT | descending | 8.816 (8.543–9.195) | 3.535 (3.485–3.707) |
| 131,072 / 1,024 | INT | ascending | 0.922 (0.762–1.031) | 0.206 (0.199–0.246) |
| 131,072 / 1,024 | INT | descending | 7.633 (6.983–8.201) | 1.824 (1.748–1.839) |

Descending arrival changes many previously emitted ranks, so it performs much more output work
than ascending arrival. Compare engines within the same row, retaining all output costs. The
[raw trials](../benchmarks/results/non-time-row-number-2026-10-15.json) include every measured
and warmup job, the previous correct fallback, source/library hashes and variability. Separate
JVM stock controls drift; cross-run differences are descriptive rather than isolated attribution.

For the 262,144-row BIGINT fixture, the previous correct fallback measured 1.662 s ascending
and 11.264 s descending, with paired stock controls 1.479/10.431 s. Its descending range was
7.550–12.078 s; all those trials remain in the artifact. The final native medians improve
4.12×/2.49× against their paired stock controls, and 4.15×/3.19× against the earlier fallback.
At 131,072 rows, INT's previous fallback measured 0.985/6.732 s, with stock 0.851/6.732 s;
final native medians are 0.206/1.824 s. Control drift limits precision of cross-run attribution.

Each measured job has its own metrics reporter and local cluster, closed after timing and
native-work assertions. The JVM/2 GiB heap remains shared across trials; both engines have the
same configured off-heap budget from the metrics fixture (48 GiB, not reserved physical memory).
An initial reporter retained closed-job gauges across a whole case and the fallback JVM ran out
of heap after 13 jobs. All initial samples and the failure are retained as diagnostics; the
reported comparisons use the matched per-job lifetimes on both engines and complete normally.

Reproduce with `SF_BENCHMARK=true mvn test -Pbench -pl streamfusion-runtime -am
-Dtest=NonTimeRowNumberBenchmark -Dsurefire.failIfNoSpecifiedTests=false
-Dsf.extraJvmArgs=-Xmx2g`. Defaults are 131,072 rows / 1,024 partitions / BIGINT; use
`-Drank.rows=262144 -Drank.keys=2048` for the larger fixture or `-Drank.orderType=INT`
for the INT sort projection. `-Drank.expectNative=false` asserts the previous fallback.

The focused SQL, existing Top-N and time-OVER suites pass 62 checks on Flink 2.2.1;
Flink 1.18.1 passes 49 with 13 host-capability skips, including its non-time validation boundary.
Three release Rust tests cover emitted row identities, complete retained-state restore and
memory limits; 80 release Criterion fixtures validate the production kernel separately.

### Additional scalar order comparison

Eight typed SQL cases compare every rank update under NULLS FIRST/LAST. Ten more checks
restore string, decimal, DATE, TIME and BOOLEAN ranks twice on memory and RocksDB; the
existing fourteen-case rank suite also passes. Native fixtures cover ten physical sort
representations, including narrow integers, wide decimals, 8/256-byte strings and nullable
temporal/Boolean values, with an independent inversion-count oracle.

The October 18, 2026 release+mimalloc comparison uses Flink 2.2.1/JDK 17, UTC,
production IPC, parallelism one and a 2 GiB heap on Intel i7-12650H/Linux WSL2. Every
profile has 131,072 runtime rows, 1,024 partitions and a NULL order value every 31 rows.
Two warmups precede three measured jobs, alternating engine order. Complete jobs retain
both transposes, row source, blackhole sink, sort casts, JNI, full retained state and every
row-identity update; native consumption is asserted per job. Both arrival cases use
`ORDER BY ... ASC NULLS FIRST`.

| Order type | Arrival | Stock median (s) | Previous origin (s) | Native median (s) | Stock/native |
| --- | --- | ---: | ---: | ---: | ---: |
| TINYINT | Ascending | 0.902448 | 0.921784 | 0.220978 | 4.084x |
| TINYINT | Descending | 6.069253 | 6.660529 | 1.751583 | 3.465x |
| SMALLINT | Ascending | 0.771852 | 0.980990 | 0.224256 | 3.442x |
| SMALLINT | Descending | 7.593005 | 6.628773 | 1.940302 | 3.913x |
| DECIMAL(20,2) | Ascending | 1.628816 | 1.607016 | 0.216867 | 7.511x |
| DECIMAL(20,2) | Descending | 6.319737 | 7.401731 | 1.759849 | 3.591x |
| STRING | Ascending | 3.134559 | 3.501260 | 0.828730 | 3.782x |
| STRING | Descending | 4.383904 | 5.352431 | 1.232235 | 3.558x |
| CHAR(8) | Ascending | 3.249102 | 3.963801 | 0.822642 | 3.950x |
| CHAR(8) | Descending | 4.833143 | 4.922082 | 1.283715 | 3.765x |
| DATE | Ascending | 0.892963 | 1.070902 | 0.202173 | 4.417x |
| DATE | Descending | 5.992467 | 6.614214 | 1.708785 | 3.507x |
| TIME | Ascending | 0.518208 | 0.530490 | 0.189039 | 2.741x |
| TIME | Descending | 0.506685 | 0.509098 | 0.197192 | 2.570x |
| BOOLEAN | Ascending | 2.073037 | 2.199477 | 0.822275 | 2.521x |
| BOOLEAN | Descending | 1.972475 | 2.174961 | 0.590466 | 3.341x |

Previous-origin controls run identical fixtures with native planning enabled and fallback
asserted. Their separate JVM has paired stock controls; the
[raw artifact](../benchmarks/results/row-number-scalar-types-2026-10-18.json) retains every
warmup and sample, variability and fixture/library hashes, including control drift. These
are complete-job comparisons including startup, without excluding unfavorable samples.

Select these profiles with `NonTimeRowNumberBenchmark` under `-Pbench` and
`SF_BENCHMARK=true`, plus
`'-Drank.orderTypes=TINYINT;SMALLINT;DECIMAL(20,2);STRING;CHAR(8);DATE;TIME;BOOLEAN'
-Drank.rows=131072 -Drank.keys=1024 -Drank.warmup=2 -Drank.runs=3
-Dsf.extraJvmArgs=-Xmx2g`. Original origin additionally sets `-Drank.expectNative=false`.

## Frame shapes

Running checkpoints retain raw widened sums and non-null counts for AVG and decimal SUM.
The evaluated average is never restored as a sum. Bounded decimal SUM/SUM0/AVG and floating
SUM0 retract expired values before adding arrivals and retain their arithmetic history. This
preserves decimal overflow and FLOAT/DOUBLE rounding rather than refolding the retained slice. Typed
extrema/selections retain the input decimal scale or both timestamp components. Precision 0/3
values normalize to the released serializer's millisecond representation before aggregation;
precision 6/9 values retain their fractional nanoseconds.

Bounded string state keeps its selected value, duplicate membership and retractable views
independently of the physical frame. Serialized duplicate keys can collapse without discarding
the independent selection/order history. MIN/MAX update incrementally and rescan only when the
winner retires. FIRST/LAST retain their selected value and value-to-order/order-to-value views.
Heap rescans follow JDK hash buckets and tree bins; RocksDB follows serialized key order.
Numeric frames keep their existing state layout. Managed memory accounts for owned payloads,
view storage and map membership, and cleanup releases both frame and selection ownership.

Cached Flink strings compare by UTF-16 when both operands have original Java caches, and by
UTF-8 otherwise. Metadata-marked Arrow List<UInt16> companions can retain the original units,
including unpaired surrogates with identical replacement bytes. Reads reconstruct the original
representation. An input batch may omit companions when all its strings lack Java caches.
Results still emit companions for cached values retained from earlier batches, including
when the current input is NULL. Shuffle and serialized recovery clear caches; RocksDB normalizes proctime views
between individual rows, including within one Arrow batch. Explicit transport controls and full
SQL comparisons cover source/computed strings, unpartitioned input, chained OVER and window-to-OVER.
The [sparse-input regression checks](../benchmarks/results/over-sparse-cache-validation-2026-10-18.json)
include all 256 typed OVER harness cases on each released Flink line and 32 native fixtures
for cached winners followed by ordinary string/NULL batches across all four rowtime frames.

Decimal RANGE folds and cached string views also retain the backend's expired-timestamp-group
order. Heap membership includes pending groups and follows JDK insertion, tree splitting and
removal. Ordinary checkpoints reconstruct capacity from the serialized membership count;
canonical heap savepoints insert into a default-capacity map. A managed marker observes actual
heap copy-on-write while a snapshot is outstanding, so live native maps rebuild when the host
backend copies state. Aggregate values remain in Arrow-owned native state; the marker carries
only the lifecycle observation. Input and value maps have separate markers: snapshot completion
between buffering and firing must not copy a value map prematurely. Completed-snapshot controls
preserve the uninterrupted order.
Timestamp and value maps apply their respective normal/canonical reconstruction rules.

Released-stock controls establish that large malformed cached-key collisions do not define a
unique byte-for-byte oracle. FIRST_VALUE can change with its helper's implicit wall-clock groups;
restored MIN can change when identity-ordered tree entries serialize to the same binary key.
Native FIRST/LAST use relative arrival order for these inherently nondeterministic cases.
Deterministic ordering and supported string representations retain exact stock parity. The
[raw controls](../benchmarks/results/over-basic-values-2026-10-13.json) and
[architecture note](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/11-over-incremental-vs-window-exec.md)
record the released behavior and state model.

### Unbounded `RANGE … CURRENT ROW` and `ROWS … CURRENT ROW` (running folds)

Both retain incremental aggregates per partition across batches. Event-time RANGE folds all
rows tied at a timestamp before emitting their shared result. ROWS emits after each row,
including separate running results for tied rows in their arrival order. Watermarks release
buffered event-time rows in timestamp order; proctime folds eagerly in arrival order.

`COUNT(*)` counts every row, while `COUNT(value)` skips NULL values. The planner materializes
one non-null BIGINT constant for row counting and reuses the existing aggregate kernel, frame
state, TTL and checkpoint paths. This works for all four admitted frame shapes, including
mixed aggregates, nullable partition keys and unpartitioned input.

Non-distinct `COUNT(value)` also admits runtime DECIMAL, CHAR/VARCHAR, TIMESTAMP,
TIMESTAMP_LTZ, DATE, TIME, BOOLEAN, VARBINARY, day-time/year-month intervals, ARRAY, MAP,
MULTISET and ROW values. MAP/MULTISET-containing arguments retain the proctime performance gate
described below. It reads the Arrow validity bitmap, without
converting these values to numbers or inspecting decimal precision and temporal fractions.
Bounded frames retain NULL membership with the declared column type through snapshots and memory/RocksDB transitions;
running COUNT state remains a BIGINT. COUNT(DISTINCT value) also admits DECIMAL, CHAR/VARCHAR,
DATE, TIME, BOOLEAN, TIMESTAMP and TIMESTAMP_LTZ, retaining logical membership types in running
state and bounded frame snapshots.
DECIMAL also admits DISTINCT SUM/SUM0/AVG; other DISTINCT aggregates retain the numeric value whitelist.

### Bounded `ROWS BETWEEN n PRECEDING AND CURRENT ROW`

Retains the preceding row slice plus the current row. Decimal SUM/SUM0/AVG, floating SUM0
and string reductions retract expired membership; other reductions fold the retained slice.

### Bounded `RANGE BETWEEN INTERVAL n PRECEDING AND CURRENT ROW`

Retains every row within `n` of the current row's rowtime. Incremental reductions retract
expired timestamp groups in the backend's membership order, including peers.

## Late event-time rows

Unbounded frames admit only timestamps strictly greater than the current watermark.
Bounded ROWS and RANGE frames instead compare against that partition's last-fired timestamp,
initially zero. Equality is late; an untouched partition can accept a positive timestamp behind
the global watermark. Rows tied before their timer fires remain admissible together.

The newest retained frame row preserves the last-fired timestamp through memory snapshots,
RocksDB checkpoints, and backend transitions. A zero-width ROWS frame keeps one marker row,
which is excluded from the next row's aggregate. RANGE eviction uses each partition's own
last timestamp. Its event-time cleanup timer clears the whole partition and resets admission:
Flink's deadline hysteresis registers `timestamp + floor(1.5 * interval) + 1` when the existing
deadline precedes `timestamp + interval + 1`. Those deadlines are checkpointed alongside frames;
pending-only partitions reconstruct them from their buffered arrivals on canonical restore.

## Proctime order

The running and bounded-ROWS frames are native on proctime as well: arrival order, eager emit, no
wall-clock timer needed.

A **bounded-RANGE frame over proctime** falls back — with processing time materialized as a fixed
per-batch timestamp, a wall-clock-interval frame has no meaningful definition.

## Gaps

The matcher declines:

- A physical `AVG` call outside DECIMAL. This defensive matcher restriction does not exclude
  SQL primitive averages on the supported releases: Flink lowers TINYINT, SMALLINT, INT,
  BIGINT, FLOAT and DOUBLE `AVG` and `AVG(DISTINCT)` into admitted reductions. SQL execution
  contracts cover all six types together across all seven supported rowtime/proctime frames,
  require positive native OVER input, and compare resolved result widths and NULL results.
- A non-numeric value column outside DECIMAL, CHAR/VARCHAR, DATE, TIME, BOOLEAN, TIMESTAMP
  and TIMESTAMP_LTZ, except ordinary COUNT also admits VARBINARY, day-time/year-month intervals
  and ARRAY/ROW/MAP/MULTISET. Processing-time COUNT also admits MAP/MULTISET descendants
  in all existing proctime frames. Complex DISTINCT
  and complex value selections retain fallback. The scalar types admit ordinary COUNT and typed MIN/MAX/FIRST_VALUE/LAST_VALUE;
  DECIMAL also admits SUM/SUM0/AVG and their DISTINCT forms. COUNT(DISTINCT) additionally
  admits all these types; other DISTINCT aggregates retain their numeric whitelist.
- Temporal FIRST_VALUE/LAST_VALUE on released Flink 1.18, whose aggregate factory rejects them.
  Native planning retains the same failure; Flink 2.2 admits these functions.
- CHAR MIN/MAX/FIRST_VALUE/LAST_VALUE combinations rejected by the released aggregate factory.
  Flink 1.18 accepts only unbounded CHAR MIN among these selections; CHAR COUNT and
  COUNT(DISTINCT) remain admitted. Flink 2.2 accepts all four CHAR selections.
- DECIMAL scale-38 SUM/SUM0/AVG, including DISTINCT: both released Flink lines fail
  while constructing the zero accumulator, even for SUM. Scale 37 is admitted.
  COUNT and value selections retain scale-38 support.
- A `PARTITION BY` key outside the [verified scalar types](window-aggregate.md#scalar-grouping-and-partition-keys):
  integer widths, FLOAT/DOUBLE, character, BOOLEAN, DATE, TIME, TIMESTAMP/LTZ, DECIMAL, binary
  and intervals. Collection/ROW/object key columns retain fallback.
- A frame not of the form `… PRECEDING .. CURRENT ROW` (a `ROWS`/`RANGE` lower bound that isn't a
  constant preceding offset or UNBOUNDED PRECEDING).
- A bounded-RANGE frame over a proctime order.

## Parity, not gaps

Flink itself rejects or single-groups these in streaming, so not running them natively matches Flink
rather than falling short of it: more than one window group, decimal-valued frame offsets, `FOLLOWING`
frames, non-time or descending order, and `LAG`/`LEAD`.

## Idle-state TTL

`OVER` runs `table.exec.state.ttl` natively across all four frame shapes, but the mechanics differ
by shape:

- **Rowtime frames and the proctime bounded-ROWS frame** share a per-key cleanup deadline (the same
  scheme as the temporal join): registered on every element, with hysteresis and a
  `minRetentionTime > 1` enablement threshold, checked lazily and swept, clearing the key's
  accumulator and frame buffer silently. One wrinkle: at the deadline, the rowtime shapes *defer*
  while the key still has buffered rows the watermark hasn't folded (the timer re-registers and
  waits), whereas the proctime bounded-ROWS frame clears its retract frame unconditionally — so that
  frame can observably restart short.
- **The proctime unbounded fold** instead puts a per-value TTL (`> 0` enables, refreshed on last
  write) directly on its accumulator. An expired key visibly restarts its running fold — and its
  `ROW_NUMBER`/`RANK` numbering — from zero, exactly as Flink's `NeverReturnExpired` state does.
- **The bounded-RANGE rowtime frame** takes no retention at all: Flink's own function accepts none,
  since event-time frame eviction and cleanup bound state, so `table.exec.state.ttl` changes nothing there.

With that, nothing declines a nonzero retention setting. See [Configuration](../configuration.md) for
the TTL flag surface, and [window aggregate](window-aggregate.md) for the (unaffected — no idle-state
TTL applies) window operators.

## Running ROWS validation

The released-SDK operator matrix covers all supported event-time/proctime frames, heap/RocksDB,
ordinary checkpoints and canonical backend transitions. Fixtures include precision-38 decimal
arithmetic/overflow, wide temporal values and widening casts, all-NULL frames, peers, duplicate
extrema, Unicode cache representations, malformed serialized aliases, pending rows and state TTL.
Large valid-Unicode hash collisions additionally cover winner retirement, tree bins, normal and
canonical restore, and outstanding/completed heap snapshots. Controlled SQL recovery fails a job
repeatedly after completed checkpoints and compares mixed decimal/string/temporal output against
stock. Native execution counters establish actual OVER work for SQL comparisons.

The focused Flink 2.2.1 suite covers COUNT(*) versus nullable COUNT, mixed aggregates, constant
arguments, DISTINCT, empty/all-NULL input, multiple partitions, timestamp peers, late admission,
and 5,003-row multi-batch input. Operator tests cover TTL expiry and running-state continuation
across RocksDB checkpoints and memory/RocksDB backend transitions.

`RunningRowsBenchmark` measures proctime COUNT(*), COUNT(value), SUM, MIN and MAX over one
million rows and 64 keys, parallelism one, with both row/Arrow transposes and a row blackhole sink.
On an Apple M1 Max/JDK 17, the September 16, 2026 release run used two warmups and five interleaved
measurements: median Flink **0.605496 s**, native **0.591282 s** (**1.024x**). The difference is small;
this establishes comparable end-to-end performance for the new coverage, not a substantial
speedup. Keeping OVER columnar also lets it compose with adjacent native operators.

```bash
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench -Dtest=RunningRowsBenchmark
```

## DATE, TIME and BOOLEAN COUNT measurements

Release+mimalloc comparisons on Linux x86-64 (Intel i7-12650H), JDK 17 and released
Flink 2.2.1 use one million runtime rows, 64 keys, parallelism one, a 2 GiB heap and
1,024-row transposes. The row source, row blackhole sink, production IPC, JNI and state
work stay in the complete-job measurement. Two warmups precede three interleaved
measurements of stock Flink, native execution and forced OVER fallback. Every native
trial asserts both transposes and consumed native OVER records. NULLs occur every
seventh row; one partition contains only NULLs.

| Value | ROWS frame | Stock median (s) | Previous origin (s) | Native median (s) | Stock/native |
| --- | --- | ---: | ---: | ---: | ---: |
| DATE | Unbounded preceding | 1.414398 | 1.421788 | 0.540089 | 2.619x |
| DATE | 16 preceding | 5.254846 | 5.150412 | 0.639251 | 8.220x |
| TIME | Unbounded preceding | 1.424072 | 1.465953 | 0.573401 | 2.484x |
| TIME | 16 preceding | 5.050802 | 5.372921 | 0.678752 | 7.441x |
| BOOLEAN | Unbounded preceding | 1.427646 | 1.386028 | 0.539139 | 2.648x |
| BOOLEAN | 16 preceding | 5.148383 | 5.336134 | 0.640806 | 8.034x |

The unchanged-origin control runs the identical fixture with native planning enabled
and verifies its previous OVER fallback. Its separate JVM has its own paired stock
controls; cross-run comparisons must retain that control drift. The
[raw artifact](../benchmarks/results/over-count-basic-types-2026-10-18.json) retains all
warmups and measured trials, library and fixture hashes, forced fallback and original
origin controls. Startup remains included; these are complete-job comparisons.

Reproduce with `OverBasicValuesBenchmark` under `-Pbench`, `SF_BENCHMARK=true`,
`-Dover.values.types=date,time,boolean -Dover.values.frames=3,1
-Dover.values.countOnly=true -Dover.values.rows=1000000 -Dover.values.keys=64
-Dover.values.warmups=2 -Dover.values.runs=3 -Dsf.extraJvmArgs=-Xmx2g`.
The original-origin fixture additionally sets `-Dover.values.expectNative=false`.
SQL parity and memory/RocksDB restoration cover all four event-time and three
processing-time frames on both released Flink lines; Criterion adds 84 typed-count
fixtures to the existing 168 validity-count controls.

## Basic-value complete-job comparisons

The October 13, 2026 Linux/JDK 17 comparison uses released Flink 2.2.1, UTC, parallelism one,
release+mimalloc, IPC exchange, a runtime row source and row blackhole sink. Both transposes,
JNI, buffering, state and output conversion remain inside each job. Two warmups precede three
measured trials per engine; stock, native and forced-OVER-fallback jobs rotate execution order.
Current-job metrics must establish actual native OVER consumption. SQL parity is checked
separately through collecting queries, including decimal overflow, all-NULL partitions, peers,
broad years, Unicode, unpartitioned/computed strings and two native-island pipeline controls.

At 500,000 rows and 64 keys, all sixteen type/frame profiles beat both stock and previous
fallback; stock/native medians span 1.853–4.806×. Decimal profiles cover SUM, AVG, extrema,
FIRST/LAST and COUNT together; string and temporal profiles cover extrema, FIRST/LAST and COUNT.
The input is globally ordered by event time. Watermarks run every 50 ms with no out-of-order
delay, retaining all peers at the current maximum timestamp until it closes.

At one million rows and 4096 keys, all eight running/bounded ROWS profiles also beat stock
and previous fallback, spanning 2.335–4.761× stock. All samples, including the
11.008-second LTZ stock observation, are retained.

The [raw artifact](../benchmarks/results/over-basic-values-2026-10-13.json) retains every
warmup and observation. An earlier diagnostic withheld watermarks for a year and therefore
buffered the entire bounded input until EOF: its running decimal median was 2.429× stock,
while its first bounded stock/fallback warmups took 48.991/48.740 seconds versus native
0.952 seconds. That run was stopped during warmup to correct the streaming fixture; no
completed bounded estimate is claimed. The observations remain visible.

After the final string map/copy/restore corrections, all six affected string comparisons were
rerun with the same fixture and controls. They beat stock by 2.176–3.913× and also beat the
previous fallback. These supersede the earlier string profiles; unchanged decimal and precision-9
temporal paths retain their original comparisons. All 90 final string trials, including warmups
and the 11.805-second stock bounded-ROWS observation, remain in the artifact.

| String profile | Stock median (s) | Native median (s) | Previous fallback (s) | Stock/native |
|---|---:|---:|---:|---:|
| 500k rows, 64 keys, running RANGE | 1.370 | 0.599 | 1.390 | 2.286× |
| 500k rows, 64 keys, bounded ROWS | 3.559 | 0.910 | 3.537 | 3.913× |
| 500k rows, 64 keys, bounded RANGE | 3.533 | 1.034 | 3.510 | 3.416× |
| 500k rows, 64 keys, running ROWS | 1.236 | 0.568 | 1.267 | 2.176× |
| 1M rows, 4096 keys, bounded ROWS | 10.008 | 3.089 | 9.764 | 3.240× |
| 1M rows, 4096 keys, running ROWS | 3.170 | 1.095 | 3.118 | 2.894× |

```sh
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=OverBasicValuesBenchmark -Dsf.testForks=1 -Dsurefire.failIfNoSpecifiedTests=false \
  -Dover.values.rows=500000 -Dover.values.keys=64
```

Properties select `over.values.types`, `over.values.frames` (0 running RANGE, 1 bounded ROWS,
2 bounded RANGE, 3 running ROWS), rows, keys, warmups, runs and watermark delay in milliseconds.

## DATE, TIME and BOOLEAN value selection

Ordinary MIN/MAX/FIRST_VALUE/LAST_VALUE now use a typed 32-bit selected value with its logical
DATE/TIME/BOOLEAN type. Frame rows, output and checkpoints retain the declared type; they
do not expose an integer carrier. NULL-only groups remain NULL, boolean false remains a
value, and bounded frames evict NULL rows as well as contributing rows. DISTINCT value
selection remains gated; the order and input gates remain unchanged. Flink 1.18 keeps its temporal FIRST/LAST factory
failures; COUNT/MIN/MAX and boolean FIRST/LAST run natively where that release accepts them.

The focused selection has 96 passing checks and eight released-host skips on Flink 2.2.1,
and 104 passing checks on 1.18.1. It includes same-key peers, physical splits, exact result
types, two memory/RocksDB restores and host-failure parity. All 107 native OVER regressions
and 2,656 release Criterion fixtures pass, including 336 new primitive cases with independent
frame oracles, repeated snapshots and allocation/buffer reports.

Whole-job release+mimalloc measurements use JDK 17, parallelism 1, a 2 GiB heap, UTC, one
million runtime rows, 64 keys, two warmups and three measured trials. Values vary within
keys, NULLs occur every seventh row and key zero has no contributing values. ROWS uses
16 preceding rows; bounded RANGE uses one second. All twelve Flink 2.2 type/frame cases
improve **2.508–6.669x** over paired stock. Both unbounded shapes and both bounded shapes
retain the runtime row source, rowwise sink, production transposes/JNI/state and native
execution assertions.

The actual preceding revision is sampled on bounded ROWS for all three types, improving
5.058–5.452x. Its controls use the identical fixture and resources; disabled-OVER controls
are additionally retained for the full current matrix. These are separate comparisons.

| Bounded ROWS type | Stock median (range), s | Native median (range), s | Actual previous median (range), s |
| --- | ---: | ---: | ---: |
| DATE | 6.385 (6.224–6.490) | 1.200 (1.199–1.206) | 6.068 (6.045–6.271) |
| TIME | 6.530 (6.448–6.540) | 1.189 (1.189–1.235) | 6.484 (6.324–6.503) |
| BOOLEAN | 6.388 (6.274–6.650) | 1.168 (1.137–1.176) | 6.321 (6.298–6.388) |

Released Flink 1.18 bounded-ROWS profiles improve 5.218–6.030x over paired stock. The
[270-job artifact](../benchmarks/results/over-primitive-values-2026-10-19.json) retains every
warmup/trial, all controls, configurations, source/DSO hashes and 336 allocation records.
Correctness also covers proctime; these whole-job timings are rowtime workloads. Results
are representative rather than a universal speed guarantee.

## DATE, TIME and BOOLEAN distinct counts

COUNT(DISTINCT value) uses the existing OVER membership set and BIGINT result state for
these three logical types. NULL does not enter the set; false remains a distinct value.
Running frames preserve membership across inputs, while bounded frames count only the
retained rows, including duplicate removal and later reinsertion. Logical DATE/TIME/BOOLEAN
membership survives both memory and native RocksDB recovery. No new native production path,
JNI entry point, checkpoint layout, or frame shape is introduced.

The expanded SQL suite has 29 passing checks and four released-host skips on Flink 2.2.1,
and 33 passing checks on 1.18.1. Seven event/processing-time shapes per type run alongside
ordinary reductions; eight mixed profiles exercise two successive restores on both state
backends. The independent native set oracle covers each type, empty/all-NULL partitions,
duplicates, expiry and physical chunks with repeated snapshots.

Whole-job release+mimalloc measurements retain the runtime row source, rowwise sink,
both transposes, JNI/state costs and native execution metrics. They use JDK 17,
parallelism one, a 2 GiB heap, UTC, two warmups and three interleaved measured trials
per engine. The three engines are stock, native and same-revision OVER-disabled.

| Type / frame / rows / keys | Stock median (range), s | Native median (range), s | Actual previous SF median (range), s |
| --- | ---: | ---: | ---: |
| DATE / running RANGE / 1,000,000 / 64 | 1.589 (1.440–1.665) | 0.566 (0.540–0.572) | — |
| DATE / bounded ROWS / 1,000,000 / 64 | 5.293 (5.036–5.406) | 1.235 (1.220–1.236) | 5.869 (5.689–6.479) |
| TIME / running RANGE / 1,000,000 / 64 | 1.521 (1.487–1.577) | 0.583 (0.581–0.586) | — |
| TIME / bounded ROWS / 1,000,000 / 64 | 5.265 (5.227–5.338) | 1.249 (1.228–1.252) | 5.781 (5.329–5.851) |
| BOOLEAN / running RANGE / 1,000,000 / 64 | 1.483 (1.457–1.498) | 0.531 (0.530–0.545) | — |
| BOOLEAN / bounded ROWS / 1,000,000 / 64 | 5.188 (5.143–5.394) | 0.811 (0.809–0.843) | 5.807 (5.495–5.868) |
| DATE / bounded RANGE / 1,000,000 / 64 | 5.970 (5.730–6.410) | 0.862 (0.797–0.878) | — |
| DATE / running ROWS / 500,000 / 1 | 0.781 (0.772–0.830) | 0.316 (0.307–0.344) | — |
| BOOLEAN / bounded RANGE / 500,000 / 256 | 2.083 (1.972–2.159) | 0.389 (0.380–0.477) | — |

These nine profiles improve 2.468–6.928x over paired stock.
Actual previous revision `a3181672` is checked separately for bounded ROWS on all three
types and improves 4.629–7.164x; its rank-independent native library is identical.
The earlier revision's paired stock controls are retained to expose cross-process drift.
Released Flink 1.18 bounded-ROWS profiles improve 4.562–7.025x over stock:
DATE 5.718s stock / 1.253s native; TIME 5.768s stock / 1.253s native; BOOLEAN 5.649s stock / 0.804s native.

The [225-job artifact](../benchmarks/results/over-primitive-distinct-count-2026-10-19.json)
retains all warmups, repeated samples, disabled controls, source/library identities and
420 allocation/buffer records, including 84 new distinct-count fixtures. These are
representative rowtime workloads; proctime is covered by SQL/native correctness checks.
Actual-prior and 1.18 comparisons sample bounded ROWS, rather than every current frame.
Reproduce with `OverBasicValuesBenchmark -Dover.values.distinctCount=true`, selecting
types, frames, rows, keys, warmups and runs using the properties above.

## Exact DECIMAL distinct counts

COUNT(DISTINCT) also uses the existing typed membership/count state for DECIMAL values,
including precision 38 and scale 38. Membership retains the declared precision and scale;
NULL does not enter the set. Running and bounded frames preserve that logical type across
physical batches and memory/native RocksDB snapshots. Decimal DISTINCT SUM/AVG have the
separate arithmetic coverage below; other order and input restrictions retain their existing gates.

Twenty-nine SQL checks pass on each released Flink line. They cover three decimal layouts
across four event-time and three admitted processing-time frames, duplicate pairs,
precision limits, all-NULL partitions and same-key timestamp peers. Eight mixed-scale
profiles inject two checkpoint restores on each state backend. The existing 57 typed
OVER controls also pass: the combined selection has 82 passes/four host skips on Flink
2.2.1 and 86 passes on 1.18.1.

Complete-job release+mimalloc measurements use JDK 17, parallelism one, a 2 GiB heap,
UTC, two warmups and three measured trials. Paired stock/current jobs and a separately
compiled actual-prior revision retain the runtime row source, rowwise sink, both
transposes, JNI/state costs and actual native execution metrics.

| Layout / frame / rows / keys | Stock median (range), s | Native median (range), s | Actual previous SF median (range), s |
| --- | ---: | ---: | ---: |
| DECIMAL(12,2) / running RANGE / 500,000 / 64 | 0.872 (0.862–0.974) | 0.370 (0.332–0.410) | — |
| DECIMAL(12,2) / bounded ROWS / 500,000 / 64 | 3.066 (3.058–3.129) | 0.601 (0.594–0.660) | 3.161 (2.852–3.234) |
| DECIMAL(38,0) / running RANGE / 500,000 / 64 | 0.844 (0.828–1.024) | 0.332 (0.324–0.333) | — |
| DECIMAL(38,0) / bounded ROWS / 500,000 / 64 | 3.086 (2.904–3.203) | 0.591 (0.587–0.610) | 3.364 (3.006–3.491) |
| DECIMAL(12,2) / running ROWS / 1,000,000 / 1 | 1.545 (1.399–1.579) | 0.545 (0.516–0.554) | — |

These five profiles improve 2.359–5.223x over stock.
Actual previous `4f87826e` bounded-ROWS controls improve 5.256–5.692x;
their separately paired stock samples expose cross-process drift. Released Flink 1.18's
wide bounded-ROWS sample measures 3.253s stock / 0.609s native (5.343x).

The [80-job artifact](../benchmarks/results/over-decimal-distinct-count-2026-10-19.json)
retains every warmup and trial, source/library identities, paired baseline drift and 84
new allocation/buffer reports. Precision/scale 38/38 and proctime are correctness-covered;
these timings sample rowtime, compact decimals and 38-digit noncompact decimals.
Actual-prior timings sample bounded ROWS, and the 1.18 timing samples the wide layout.
These representative results do not establish a universal speedup.

`OverBasicValuesBenchmark -Dover.values.distinctCount=true -Dover.values.engines=2`
selects the paired method; types include `decimal` and `decimal_wide`. The default remains
three engines. A missing disabled-OVER control is printed as `NaN` with empty samples
and recorded as null in the artifact, rather than as a measured zero.


## STRING and timestamp distinct counts

COUNT(DISTINCT) admits CHAR/VARCHAR, TIMESTAMP and TIMESTAMP_LTZ. String membership uses
binary string bytes, independently of a cached Java UTF-16 representation used by selections.
Running state owns a string only when it first enters the set. Timestamp membership retains
all fractional nanoseconds; compact declared precisions still observe the released Flink
serializer's millisecond representation. NULL never enters a membership set.

Running membership accounts for hash-table capacity and owned string bytes in the native
memory budget. Restores derive this accounting from existing typed checkpoint fields, without
a checkpoint format change. Native RocksDB running frames probe and persist text membership
as well as numeric membership, so repeated values across physical batches and checkpoints
are counted once. Bounded frames retain their typed frame rows and recompute membership.

Fifty-four new SQL checks run on both released Flink lines: seven event/proctime frame shapes
for four column expressions, mixed text/temporal aggregates with two restores on memory and
native RocksDB backends, declared timestamp precisions 0/3/6/9, mixed cached-string selections,
and eight CHAR factory contracts. Together with 119 existing controls, Flink 2.2.1 has 165 passes
and eight host capability skips; Flink 1.18.1 passes all 173 checks, including seven expected
CHAR factory failures. Native validation adds 79 OVER unit checks, five membership checks,
140 typed distinct-count fixtures (84 decimal controls and 56 string/timestamp cases), and 16
native RocksDB fixtures with independent continued-arrival/checkpoint oracles.

Release+mimalloc complete jobs use JDK 17, parallelism one, UTC and a 2 GiB heap, with two
warmups and three measured trials. Paired engines retain the runtime row source, rowwise sink,
both transposes, JNI/state costs and actual native execution metrics. Local builds and other
heavy work were excluded from the timing interval.

| Type / frame / rows / keys | Stock median (range), s | Native median (range), s | Actual previous SF median (range), s |
| --- | ---: | ---: | ---: |
| string / running RANGE / 500,000 / 64 | 0.992 (0.931–0.995) | 0.360 (0.327–0.398) | — |
| string / bounded ROWS / 500,000 / 64 | 2.792 (2.714–2.848) | 0.601 (0.589–0.627) | 2.858 (2.647–2.877) |
| timestamp / running RANGE / 500,000 / 64 | 0.820 (0.804–0.826) | 0.341 (0.328–0.350) | — |
| timestamp / bounded ROWS / 500,000 / 64 | 2.833 (2.738–2.907) | 0.632 (0.599–0.654) | 2.921 (2.901–2.956) |
| ltz / running RANGE / 500,000 / 64 | 0.875 (0.834–0.875) | 0.331 (0.330–0.337) | — |
| ltz / bounded ROWS / 500,000 / 64 | 2.892 (2.883–3.035) | 0.604 (0.603–0.620) | 3.050 (2.560–3.057) |
| string_wide / bounded ROWS / 500,000 / 16 | 5.638 (5.393–5.659) | 1.043 (1.018–1.069) | 5.268 (4.804–5.438) |
| timestamp / running ROWS / 1,000,000 / 1 | 1.515 (1.394–1.541) | 0.574 (0.545–0.609) | — |
| decimal / running RANGE / 500,000 / 64 | 0.922 (0.878–0.961) | 0.379 (0.306–0.387) | 0.379 (0.310–0.402) |

The eight new-path profiles improve 2.402–5.407x over stock. Actual previous revision
`c92cb5ae` bounded-ROWS comparisons improve 4.618–5.053x. Its separately paired stock controls
remain in the artifact to expose process-to-process drift. The existing running decimal
COUNT(DISTINCT) control measures 0.379s on both versions (1.002x previous/current); paired
stock medians are 0.922s and 0.924s. Released Flink 1.18's wide-string profile measures
5.990s stock / 1.036s native (5.782x).

The [150-job artifact](../benchmarks/results/over-text-temporal-distinct-count-2026-10-19.json)
retains every warmup/trial, source and native-library hashes, 140 typed-count allocation
records and 16 RocksDB allocation records. Timings sample rowtime, two string widths,
64/16/one partition keys and STRING/TIMESTAMP/LTZ; CHAR, other declared timestamp precisions
and proctime are correctness-covered. Previous-version comparisons sample bounded ROWS
plus the decimal control; the 1.18 timing samples wide strings. These measurements do not
establish a universal speedup. Reproduce using `OverBasicValuesBenchmark` with
`-Dover.values.distinctCount=true -Dover.values.engines=2` and types
`string,timestamp,ltz,string_wide`; frame, row-count, key-count, warmup and trial properties
are unchanged. The default three-engine benchmark remains available.


## Exact DECIMAL DISTINCT SUM and AVG

SUM(DISTINCT) and AVG(DISTINCT), including the planner's internal DISTINCT SUM0, admit exact
DECIMAL values through precision 38 and scale 37. NULL and duplicate memberships do not update the
arithmetic. Bounded frames retract only when the last occurrence expires, preserving Flink's
arithmetic history: SUM can resume after an overflowed accumulator, while SUM0/AVG retain their
sticky overflow. Recomputing a sum from the current unique values would lose this history.

The bounded fold saves the existing sum/count fields and reconstructs multiplicities from
retained frame rows when continuing a physical batch or restoring a checkpoint. It never
replays arithmetic while rebuilding membership. Running frames retain the existing typed
DISTINCT checkpoint representation. Memory and native RocksDB use the same fold and frame
contracts, without a new JNI boundary or a separate persistent membership format for bounded
frames. Existing frame, ordering, input and non-decimal DISTINCT gates are unchanged.


Released Flink 2.2 and 1.18 reject SUM/AVG over DECIMAL(38,38) during planning, with or
without DISTINCT: constructing the accumulator fails with `Cannot convert 0 to DECIMAL(38, 38)
due to overflow`. Sixteen factory-boundary checks compare scale 37/38, SUM/AVG, ordinary/DISTINCT
and running/bounded ROWS. Native admission preserves the same failure class, message and phase
for the eight scale-38 cases; scale 37 runs natively. This also corrects the previous matcher
which could bypass this released failure for ordinary SUM/AVG. Scale-38 COUNT and selections
remain admitted.


Forty-five new SQL checks pass on each released Flink line. Three decimal layouts
(12,2; 38,0; 38,37) cover seven event/proctime frames, precision-limit overflow, duplicate
pairs, NULL-only partitions and same-key peers. Eight mixed-scale profiles inject two
restores on memory/native RocksDB; sixteen checks pin the factory boundary above.
Together with 173 existing controls, Flink 2.2.1 has 210 passes/eight host capability skips
and Flink 1.18.1 passes all 218. Eighty native OVER unit checks and 252 release fixtures
separately validate arithmetic, membership expiry, physical chunks and repeated snapshots.


Complete-job release+mimalloc comparisons use JDK 17, parallelism one, UTC, a 2 GiB heap,
two warmups and three measured trials. The measured query combines DISTINCT SUM/AVG/COUNT;
individual native fixtures cover SUM/SUM0/AVG separately. Runtime row sources, rowwise sinks,
both transposes, JNI/state costs and execution metrics remain in the paired stock/current
and separately compiled actual-prior jobs. Other local heavy work was excluded.

| Type / frame / rows / keys | Stock median (range), s | Native median (range), s | Actual previous SF median (range), s |
| --- | ---: | ---: | ---: |
| DECIMAL(12,2) / running RANGE / 500,000 / 64 | 1.593 (1.522–1.618) | 0.510 (0.489–0.518) | — |
| DECIMAL(12,2) / bounded ROWS / 500,000 / 64 | 3.206 (3.184–3.403) | 0.765 (0.755–0.772) | 3.240 (3.071–3.344) |
| DECIMAL(38,0), balanced signs / running RANGE / 500,000 / 64 | 1.044 (1.020–1.064) | 0.426 (0.425–0.428) | — |
| DECIMAL(38,0), balanced signs / bounded ROWS / 500,000 / 64 | 3.293 (3.225–3.370) | 0.743 (0.742–0.743) | 3.336 (3.262–3.383) |
| DECIMAL(12,2) / running ROWS / 1,000,000 / 1 | 3.015 (2.971–3.028) | 0.804 (0.737–0.817) | — |

These five profiles improve 2.448–4.433x over stock. Actual previous revision `e675f7af`
bounded-ROWS comparisons improve 4.235–4.490x. Separately paired previous-version stock
controls are retained to expose process-to-process drift. Flink 1.18's wide bounded-ROWS
profile measures 3.420s stock / 0.839s native (4.077x).

The wide source alternates positive/negative 38-digit values over 514 memberships per
partition, avoiding permanently overflowed sum state. AVG can still overflow its result
precision on unbalanced frames; those results and costs remain measured. Compact decimals
use 1,000 unscaled slots. Both layouts include NULLs and a NULL-only partition at 64 keys.
The single-key profile measures one million rows. Timings sample rowtime, compact decimals
and noncompact scale-zero values; admitted scale 37 and proctime are correctness-covered.
Actual-prior controls sample bounded ROWS, as does the 1.18 check. These representative
results do not establish a universal speedup.

The [80-job artifact](../benchmarks/results/over-decimal-distinct-reductions-2026-10-19.json)
retains every warmup/trial, baseline drift, source/native-library hashes and all 252 native
allocation/buffer reports. Reproduce with `OverBasicValuesBenchmark
-Dover.values.distinctReductions=true -Dover.values.engines=2`, selecting `decimal` or
`decimal_wide_balanced` plus the frame/row/key properties above. Default three-engine
benchmark behavior is unchanged.


## Complex ordinary COUNT

Ordinary `COUNT` over ARRAY, MAP and ROW reads only the parent validity bitmap. Empty containers
and a non-NULL ROW with NULL children count as one; NULL parents count as zero. DISTINCT and
other complex-valued reductions retain fallback. All four event-time frames admit these three types; running ROWS/RANGE and bounded ROWS
over proctime admit all three types, including nested MAP/MULTISET descendants. They use the
existing BIGINT membership/frame state. The former conversion-performance refusal was
evaluated under the old complete-job gate; the small-functionality gate now compares Flink
on prepared RowData with StreamFusion inside Rust on prepared Arrow. Conversion-inclusive
job results remain separate diagnostics, including earlier slower MAP profiles.

Flink permits NULL map keys. Arrow Java declares map keys non-nullable, and Rust IPC enforces
that declaration during restore. Newly admitted MAP-count inputs therefore use a tagged
List-of-key/value-struct schema with nullable keys, borrowing the identical offsets, validity
and value buffers. Existing C export/import retains buffer references. This transport is scoped
to newly admitted complex COUNT with MAP anywhere in its input schema; merely passing MAP payloads through an
already-admitted scalar query does not change its checkpoint format. Logical MAP/MULTISET readers
recover the declared value after emission and restore. Numeric results retain native columnar
sinks; MAP-valued outputs reaching an unverified native sink require the existing row-sink route.
See the [representation decision](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/48-flink-collection-representation.md).

Proctime complex COUNT prefers 4,096-row entry batches to amortize Calc, Arrow C Data and JNI
setup. The preference follows its columnar input island, including an already-pruned entry
transpose, and stops at host boundaries. Explicit table/JVM `streamfusion.transpose.batchRows`
settings take precedence; other queries retain the 1,024-row default. Timer, watermark,
end-input and checkpoint flushes remain active, including the default 50 ms latency backstop.

Seventy primary checks and four nested recovery cases pass on each released Flink line. SQL comparisons cover all seven frame/order contexts, nullable containers/children/map keys,
NULL-only partitions, empty values, returned original payloads, a three-way shuffle, both sink
routes and two successive memory/native RocksDB restores. Default proctime batches and explicit
seven-row batches have independent parity checks. Release native coverage adds 196 fixtures
with 64/4,096 rows, one/64 keys and 4/64-entry containers. Setup stays outside native timing;
frame output, pending peers and checkpoint continuation are independently checked.

Complete jobs use released Flink, JDK 17, a 2 GiB heap, parallelism two, UTC, release+mimalloc,
production IPC, runtime nullable containers and a row blackhole sink. Both transposes, JNI,
state and startup stay measured; every native job must report consumed OVER records. Two
warmups precede three alternating measured trials. Event-time cases use 500,000 running rows
or 50,000 bounded rows; the wide MAP case uses 4,096 keys/32 entries instead of 64 keys/four
entries. Bounded stock execution at 500,000 rows took roughly 34 seconds per job; that
interrupted diagnostic is retained separately rather than extrapolated into final ratios.

| Event-time type / frame | Stock median (range), s | Native median (range), s | Actual previous SF median (range), s |
| --- | ---: | ---: | ---: |
| ARRAY / running | 0.941 (0.900–1.048) | 0.471 (0.311–0.489) | 0.876 (0.822–0.931) |
| MAP / running | 1.162 (1.113–1.230) | 0.587 (0.586–0.619) | 1.114 (1.083–1.164) |
| ROW / running | 0.874 (0.823–0.888) | 0.324 (0.318–0.329) | 0.853 (0.748–0.947) |
| ARRAY / bounded | 0.310 (0.305–0.403) | 0.173 (0.171–0.194) | 0.328 (0.307–0.331) |
| MAP / bounded | 0.309 (0.250–0.336) | 0.134 (0.126–0.162) | 0.250 (0.250–0.316) |
| ROW / bounded | 0.210 (0.209–0.210) | 0.102 (0.095–0.105) | 0.248 (0.226–0.255) |
| ARRAY / range | 0.326 (0.295–0.506) | 0.213 (0.169–0.222) | 0.325 (0.306–0.340) |
| MAP / wide-map | 0.393 (0.372–0.472) | 0.334 (0.244–0.354) | 0.362 (0.356–0.390) |

These event-time profiles improve 1.178–2.699x over paired stock. Flink 1.18's bounded ARRAY,
MAP and ROW profiles improve 2.052–3.025x. Measurements include startup and allocation;
the shorter bounded cases are not sustained-throughput claims or universal guarantees.
The [complete evidence](../benchmarks/results/over-complex-count-2026-10-19.json) retains
warmups, trials, actual-prior controls, allocation reports, source/library hashes and rejected
proctime diagnostics. Reproduce with `OverComplexCountBenchmark`; `over.cases`, `over.frames`,
`over.proctime`, `over.rows`, `over.keys` and `over.width` select the profiles above.


The final 500,000-row proctime ARRAY/ROW matrix measures 1.271–1.728x over stock and
1.204–1.967x over actual previous StreamFusion. Full medians and ranges are in the
[entry batching ledger](../optimizations/row-major-transpose.md#batch-preference-for-proctime-complex-count).
Its 80 additional complete jobs include matching previous-version bounded-ROWS controls;
previous running controls remain in the earlier 180-job batching matrix. Earlier 50,000-row
bounded and rejected MAP proctime candidates remain available, including unfavorable samples.


## MULTISET COUNT

Event-time ordinary COUNT also admits standalone MULTISET values across all four existing
frames. It counts parent validity, independently of element occurrences, empty bags and NULL
elements. The existing nullable-key List-of-entry transport carries element-to-INT pairs,
so original MULTISET payloads remain readable after emission and repeated IPC restore. No
new frame, membership, checkpoint or JNI format is introduced beyond that existing transport.

Eight released-SQL cases combine four event-time frames and two restores on each memory/native
RocksDB backend. They return the original bags as well as counts, covering NULL parents,
empty bags, NULL elements, varying occurrence counts and NULL-only partitions. Three more
checks exercise native proctime COUNT with original bags. Six additional proctime heap/RocksDB
recovery cases preserve those bags through two restores, for seventeen checks on each
released Flink line. DISTINCT retains fallback.

The existing release OVER fixture suite includes 112 MULTISET cases over 64/4,096 rows, one/64 keys,
4/64 elements, four event-time frames, three proctime frames and both regular MAP-of-UTF8-to-INT
and IPC-safe nullable-element List carriers. Independent frame/peer/checkpoint oracles
validate the 308-case suite. Native setup stays outside timing; allocation requests and buffer sharing
are recorded without equating either with copied bytes.


Complete-job release+mimalloc timings use JDK 17, UTC, parallelism two, a 2 GiB heap,
1,024-row entry batches and production IPC. Nullable runtime bags contain empty values, a NULL
element and positive occurrence counts. Both row/Arrow transposes, JNI, state, startup and a
row blackhole sink remain measured. Two warmups precede three alternating trials. Native
plans/counters verify execution; separately compiled actual previous revision `a038f539`
asserts its window/OVER fallback. Other local heavy work was excluded.

| Profile / rows / keys / elements | Stock median (range), s | Native median (range), s | Actual previous SF median (range), s |
| --- | ---: | ---: | ---: |
| running ROWS / 500,000 / 64 / 4 | 1.188 (1.164–1.270) | 0.650 (0.631–0.659) | 1.128 (1.104–1.163) |
| running RANGE / 500,000 / 64 / 4 | 1.175 (1.120–1.296) | 0.641 (0.563–0.795) | 1.044 (0.979–1.082) |
| 3 preceding ROWS / 50,000 / 64 / 4 | 0.389 (0.328–0.473) | 0.203 (0.115–0.215) | 0.346 (0.336–0.358) |
| 300 ms RANGE / 50,000 / 64 / 4 | 0.316 (0.258–0.437) | 0.127 (0.125–0.136) | 0.248 (0.248–0.274) |
| 3 preceding ROWS / 50,000 / 4,096 / 32 | 0.364 (0.364–0.455) | 0.357 (0.307–0.359) | 0.370 (0.349–0.373) |

These profiles improve 1.021–2.495x over paired stock and 1.036–1.959x
over actual previous StreamFusion. The Flink 1.18 bounded ROWS control improves
1.853x over stock (0.411/0.222 s). Wide-bag gains are modest and
ranges overlap previous execution; startup/source allocation remain measured. These are
representative complete jobs, not universal or sustained-throughput claims. The
[260-job artifact](../benchmarks/results/multiset-count-2026-10-19.json) retains every warmup,
trial, prior stock control, source/library hash and all 308 native allocation/buffer reports.
Reproduce with `OverComplexCountBenchmark -Dover.cases=multiset`
and the row/key/width/phase/frame properties shown above.


## Ordinary FLOAT/DOUBLE ROW_NUMBER (Flink 2.2)

Scalar FLOAT/DOUBLE keys use the released ordinary non-time ROW_NUMBER contract. Signed zeros
compare equal. A NaN compares equal to any non-NULL key in Flink's generated numeric comparator;
this differs from the boxed-value equality used by DISTINCT. The host scans groups from the
start and replaces a matched group's comparison key with the latest row's key. Consequently,
NaNs can make the retained group order nonmonotonic. A total floating sort or one permanently
canonical NaN key would produce different rank updates.

The existing ranker preserves that group order, scans actual last-member keys using the host
comparison, and emits the inserted row before each shifted row's UPDATE_BEFORE/UPDATE_AFTER.
Every row keeps its original key/payload bytes, including NaN payloads and signed zeros.
Tagged group lengths use the existing optional count metadata: `-(length + 2)` identifies a
floating OVER group header; `-1` remains absent and nonnegative OFFSET counts retain their
existing meaning. Raw/canonical restore preserves these headers and group order rather than
sorting them again. Existing scalar/complex non-floating ranks and filtered Top-N keep their
previous contracts. See the [rank-state decision](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/11-over-incremental-vs-window-exec.md).

Admission still requires insert-only input, one ascending key, ROWS UNBOUNDED PRECEDING through
CURRENT ROW and disabled idle-state TTL. Floating ARRAY/ROW children retain fallback. Released
Flink 1.18's ordinary non-time factory rejection is preserved for every type; explicit FLOAT,
DOUBLE and existing BIGINT controls verify failure class, message and phase with native planning.

The combined released SQL/context suite has 79 passes and three host-capability skips on Flink
2.2.1, and five passes/77 skips on Flink 1.18.1. New cases compare every changelog event and raw
floating payload bits under both NULL placements and physical batch sizes one/seven. Eight
memory/RocksDB cases inject two restores. Four more contexts check mini-batches and a three-way
shuffle. Existing scalar/complex ranking controls run with the same library. All 28 native
Top-N unit checks pass, including an independent linear-group model with changing representative
keys and repeated snapshots.

The existing `non_time_row_number` release fixture suite has 120 cases, including 32 new
FLOAT/DOUBLE cases over 16/256 rows, one/64 partitions, both NULL placements and initial/later
NaNs. Independent group-list oracles validate every row identity, rank and changelog kind,
including seven-row physical chunks and two restores. Fixture construction stays outside the
measured production update/output path; allocation and buffer-sharing reports are retained.

### Floating rank performance

Release+mimalloc complete-job measurements use Flink 2.2.1, JDK 17, UTC, a 2 GiB heap,
parallelism one, 131,072 runtime rows, 1,024 partitions and 1,024-row entry batches. Both
row/Arrow transposes, production IPC, JNI, all rank changes, startup and the row blackhole sink
remain measured. Each engine has two warmups and three alternating trials. Plans and per-job
counters verify native execution; actual previous revision `bd87a085` runs the identical fixture
with its preceding library and floating fallback asserted. SQL ordering is always ascending;
arrival labels describe only the finite component of the input. Other heavy local work was excluded.

| Type / arrival / corpus | Stock median (range), s | Native median (range), s | Actual previous SF median (range), s |
| --- | ---: | ---: | ---: |
| FLOAT / ascending / mixed | 2.897 (2.790–2.999) | 1.098 (1.068–1.123) | 2.626 (2.620–2.742) |
| FLOAT / descending / mixed | 4.594 (4.517–4.662) | 1.668 (1.586–1.677) | 5.297 (5.258–5.527) |
| DOUBLE / ascending / mixed | 2.722 (2.621–2.730) | 1.155 (1.114–1.168) | 2.543 (2.541–2.575) |
| DOUBLE / descending / mixed | 4.534 (4.433–4.656) | 1.781 (1.751–1.859) | 5.326 (4.305–6.042) |
| DOUBLE / ascending / all_nan | 0.360 (0.230–0.415) | 0.175 (0.155–0.188) | 0.234 (0.210–0.238) |
| BIGINT / ascending / finite | 0.772 (0.640–0.866) | 0.253 (0.252–0.271) | 0.243 (0.209–0.287) |

The five floating profiles improve 2.056–2.754x over paired stock and 1.340–3.176x over
actual previous StreamFusion. The unchanged BIGINT control is 4.1% slower by median than
the preceding native ranker, with overlapping ranges; no scalar speedup is claimed. These
complete-job observations include startup and do not establish universal or sustained throughput.
The [120-job artifact](../benchmarks/results/floating-ordinary-rank-2026-10-19.json) retains all
warmups/trials, paired previous stock controls, source/library hashes, 120 native allocation
reports, initial NaN-parity failures and the rejected fixed-representative prototype.

Reproduce with `SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench` and
`-Dtest=NonTimeFloatingRankBenchmark -Drank.float.types=FLOAT,DOUBLE`
`-Drank.float.arrivals=ascending,descending -Drank.float.corpus=mixed`
`-Drank.float.rows=131072 -Drank.float.keys=1024 -Drank.float.warmup=2 -Drank.float.runs=3`
and `-Dsf.extraJvmArgs="-Xmx2g -Duser.timezone=UTC"`. The all-NaN control uses
`-Drank.float.types=DOUBLE -Drank.float.arrivals=ascending -Drank.float.corpus=all_nan`
and `-Drank.float.nullEvery=0`; the scalar control uses BIGINT and `finite`.

## Primitive zero-initialized OVER sums

The planner preserves SUM0 as its own aggregate for TINYINT, SMALLINT, INT, BIGINT, FLOAT
and DOUBLE, including DISTINCT. Its accumulator starts at positive zero and returns zero
for NULL-only frames. Replacing it with ordinary SUM would emit NULL for a NULL-only prefix
and lose the declared non-null result. Table API `.sum0().over(...)`, including result aliases,
uses this path on Flink 2.2. Flink 1.18 rejects ordinary Table API SUM0 OVER when a SQL
view round trip exposes its inconsistent nullable/NOT NULL result metadata; that stock
planning assertion is preserved and tested. DISTINCT SUM0 runs on both released lines.
FLOAT additions and retractions round at FLOAT width; DOUBLE retains DOUBLE
width. Bounded floating SUM0 stores its running sum beside the final retained frame row, so
cancellation followed by eviction follows Flink's subtraction history across batches and
checkpoints. Heap RANGE arithmetic uses the existing keyed-state copy observer to retain
Flink's timestamp-group retraction order through asynchronous snapshots, including DISTINCT.

Legacy primitive SUM checkpoints restore NULL and negative zero as SUM0's positive zero.
For old bounded frames without saved arithmetic fields, restoration reconstructs the newly
required accumulator from retained rows. Arithmetic history already discarded by an old
writer cannot be recovered; upgrading cannot repair its earlier rounding errors. Existing
decimal overflow fields and string views keep their own positions during this migration.
Native RocksDB uses a version prefix only for extended frame layouts and decodes older
unmarked rows with their original codec; both layouts can coexist until eviction.

The release Criterion `over_primitive_zero_sum` group exercises all six numeric types, ordinary
and DISTINCT sums, 64/4096 rows, one/64 partitions, four event-time and three processing-time
frame shapes. Fixtures check non-null results and checkpoint continuation before timing.
The small accumulator admission measurements remain documented with
[floating SUM0](group-by.md#floating-sum0-admission); they are not whole-OVER job speed measurements.

[Correctness and compatibility evidence](../benchmarks/results/over-zero-initialization-2026-10-19.json)
retains the failing intermediate reproductions as well as final validation results.

Final validation passes 108 selected checks on Flink 2.2 and 100 on Flink 1.18. These include
16/8 new heap/RocksDB two-restore cases, eight existing decimal recovery controls on each
line, and 42 checks of Flink 1.18's upstream planning rejection. Native release validation
passes 803 tests with two existing ignores; all 336 new SUM0 Criterion fixtures pass.

## Processing-time MAP COUNT admission

Ordinary COUNT of MAP, MULTISET and containers containing them uses the existing parent-validity
operation in running ROWS/RANGE and bounded ROWS. There is no new feature toggle or runtime
operator. Validation covers 64 selected SQL/recovery checks on each released Flink line, including
original payloads, NULL map/multiset keys, nested descendants, partition shuffles and two
restores with heap/native RocksDB state. The native suite adds 48 proctime MULTISET fixtures,
for 308 complex COUNT fixtures in total.

`FeatureAdmissionBenchmark -Dadmission.cases=over-map-rows,over-map-range,over-map-bounded`
measures Flink's released production OVER operator on prebuilt BinaryRowData. Its matching
`over_map_count` release Rust driver starts on Arrow buffers and retains frame/state work,
allocation, a one-GiB native budget and equivalent output consumption inside Rust. Both
engines prewarm the identical first input batch before timing. Arrow fixture preparation and
perimeter transposes/JNI stay outside this small-functionality gate. Full-value/RowKind
validation uses separately exported native output; checksum-only equality is insufficient.
The preceding StreamFusion refusal used the same stock operator as Flink at this boundary.

Five warmups and nine measured trials, batch size 1,024, NULL parents every seventh row and
empty maps every third row produce these medians (ranges retain all measured samples):

| Flink / rows / keys / width / frame | Flink ms (range) | Rust ms (range) | Flink/Rust |
| --- | ---: | ---: | ---: |
| 2.2.1 / 16,384 / 64 / 4 / rows | 8.666 (4.421–15.876) | 0.923 (0.881–1.211) | 9.38× |
| 2.2.1 / 16,384 / 64 / 4 / range | 3.066 (2.823–7.880) | 0.911 (0.889–1.008) | 3.36× |
| 2.2.1 / 16,384 / 64 / 4 / bounded | 7.174 (6.639–17.837) | 2.005 (1.994–2.088) | 3.58× |
| 2.2.1 / 262,144 / 4,096 / 16 / rows | 56.153 (46.406–70.790) | 15.837 (15.412–17.361) | 3.55× |
| 2.2.1 / 262,144 / 4,096 / 16 / range | 54.039 (48.298–80.821) | 16.838 (15.953–17.518) | 3.21× |
| 2.2.1 / 262,144 / 4,096 / 16 / bounded | 125.653 (107.488–196.120) | 37.974 (37.283–40.261) | 3.31× |
| 2.2.1 / 131,072 / 64 / 64 / rows | 31.873 (30.764–49.977) | 6.635 (6.438–7.192) | 4.80× |
| 2.2.1 / 131,072 / 64 / 64 / range | 37.919 (31.274–47.297) | 6.941 (6.537–8.944) | 5.46× |
| 2.2.1 / 131,072 / 64 / 64 / bounded | 73.297 (64.934–76.214) | 15.988 (15.540–16.972) | 4.58× |
| 1.18.1 / 262,144 / 4,096 / 16 / rows | 55.830 (47.093–68.679) | 14.912 (14.494–16.820) | 3.74× |
| 1.18.1 / 262,144 / 4,096 / 16 / range | 59.547 (48.847–70.437) | 15.822 (15.674–18.238) | 3.76× |
| 1.18.1 / 262,144 / 4,096 / 16 / bounded | 123.823 (117.574–131.749) | 37.749 (37.176–43.265) | 3.28× |

All twelve profiles pass the functionality gate at 3.21–9.38× stock. No new kernel optimization
was needed under this boundary. These are operation timings, not complete-job speedups.
[Raw trials and verification evidence](../benchmarks/results/proctime-map-count-2026-10-19.json)
retain every warmup/sample, fixture/driver hashes, full-value oracle checks and preliminary
diagnostics. Earlier conversion-inclusive MAP regressions remain available above.

Export the Arrow fixture with `-Dadmission.exportArrow=fixture.arrow`, then run the compiled
`over_map_count` release example with `fixture.arrow rows 5 9 output.arrow` (or `range` /
`bounded`). `-Dadmission.rustOutput.over-map-rows=output.arrow` verifies the matching stock
profile against full native results; use the corresponding property for the other frames.


## Binary and interval COUNT validation and measurements

Ordinary COUNT of VARBINARY and both interval families reuses the existing parent-validity
aggregate. It counts empty binary values and zero/negative intervals as non-NULL, without
reinterpreting their payload. Binary DISTINCT/value selection and fixed BINARY remain outside
this expansion. SQL comparisons return the original binary and interval columns alongside four
counts over seven event/proctime frame contexts, checking values and declared types. Twenty-eight
operator cases verify all frame/time combinations through checkpoints on both memory and native
RocksDB, including canonical state transitions and retained payloads. The native Criterion
suite adds 112 binary/interval oracle and snapshot fixtures to the existing OVER COUNT target;
all 420 release fixtures pass after integration with the Rust-side admission changes.

Complete-job measurements use released Flink 2.2.1/JDK17, fresh release+mimalloc, parallelism
one, 2 GiB heap, 1,024-row batches, 500,000 runtime rows, 64 keys, two warmups and five
alternating measured pairs. Binary width is eight bytes, with empty values and high bytes;
each argument is NULL every seventh row. Both transposes, production IPC/JNI/state and the
blackhole row sink remain inside timing, and runtime metrics prove native OVER consumption.

**Measured boundary:** running event-time RANGE, with periodic source watermarks disabled and
all pending rows released by the final end-of-input watermark. These results measure that
batch-at-end workload, not sustained periodic-watermark streaming throughput.

| Argument | Stock median (range), s | Native median (range), s | Actual previous fallback median (range), s |
| --- | ---: | ---: | ---: |
| VARBINARY | 1.521270 (1.304641–1.529008) | 0.489119 (0.423386–0.535428) | 1.557590 (1.435662–1.654291) |
| Day-time interval | 1.453400 (1.372810–1.718790) | 0.432224 (0.411476–0.459116) | 1.427665 (1.334343–1.495268) |
| Year-month interval | 1.495918 (1.478797–1.631447) | 0.440533 (0.419294–0.451289) | 1.384305 (1.289410–1.475339) |

Native medians are 67.8–70.6% below paired stock and 68.1–69.8% below preceding
controls, with separated ranges. Previous controls use unchanged production revision
`adbdafe4` and the same new fixtures, verifying zero native substitutions. Their stock
medians drift between JVMs; do not attribute every difference to the admission change.

The initial binary running comparison also completes and is retained. Its next bounded-ROWS
stock warmup is stopped under the time box: a thread dump records 31.609 CPU seconds in the
stock task at `RowTimeRowsBoundedPrecedingFunction.onTimer`. No bounded-frame speed claim
is made. All completed warmups/trials, the partial run, that stack, source/library hashes
and exact configuration remain in the
[raw evidence](../benchmarks/results/over-count-binary-interval-2026-10-19.json).

Bounded RocksDB COUNT frames retain membership in their declared frame-codec types.
When an argument's physical Arrow type differs from that declaration (for example binary
or year-month interval arguments using the numeric membership carrier), validity becomes a
typed default-or-NULL value before persistence. The original payload columns remain intact.
Matching physical types retain the existing raw-column path. Checkpoint encoding is unchanged;
canonical memory/RocksDB transitions and aligned RocksDB recovery preserve these counts.
