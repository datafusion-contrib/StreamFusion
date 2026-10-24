# GROUP BY

**Status:** Native for non-windowed `GROUP BY`, both as the single-phase (immediate) plan and the
two-phase mini-batch plan, over the aggregate/value-type combinations in [Type
support](#type-support) below — with a real, enumerated gap list, see [Still falls
back](#still-falls-back).

Flink picks between the two plan shapes itself, based on `table.exec.mini-batch.enabled`; this page
covers both, since a query only accelerates when whichever shape Flink chose is fully native.

Integer COLLECT uses the verified single-phase generated JVM handler behind Arrow batch
boundaries, with released managed MapViews; see [Integer COLLECT](#integer-collect) for its
narrow type and representation gates. ARRAY_AGG and richer COLLECT results remain pending.

When the selected aggregate uses a mini-batch trigger, translation uses the released host's
size validation. Unset/default `-1`, zero and negative `table.exec.mini-batch.size` values
fail during planning with the same exception and message as Flink, for single-phase and
local/global plans. Mini-batch-disabled aggregation does not validate an unused size.

## Single-phase

Flink's internal `SUM0` admits nullable TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE and DECIMAL,
including global aggregation and per-aggregate FILTER. This lets shared `COUNT(v)`,
`AVG(v)` and `SUM(v)` reductions keep their native aggregate and outer Calc. SUM0 starts
at zero and emits zero when no non-NULL input contributes; the host-planned Calc uses
COUNT to restore NULL for AVG/SUM. Integer accumulation wraps at the declared width.
DECIMAL uses a widened `(38, input-scale)` accumulator whose overflow stays NULL;
ordinary SUM's reset-after-overflow rule is deliberately different. DISTINCT SUM0 uses the
same accumulator with distinct membership, including the shared SUM0/COUNT lowering of
DISTINCT averages. Floating SUM0 starts from positive zero and rounds each addition/subtraction at
the declared FLOAT/DOUBLE width; ordinary floating SUM still starts at the first non-NULL value.
Released Flink's invalid DECIMAL(38,38) SUM0 initializer remains on the host.

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

Single-phase `AVG(DISTINCT)` admits TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE and DECIMAL. A value folds
into the existing widened AVG accumulator only on its first occurrence and retracts only
when its last occurrence leaves the distinct set. The denominator counts distinct non-NULL
values. Integer results retain their input width and truncate toward zero; decimal results
retain Flink's DECIMAL(38, max(6, input-scale)) type and rounding. FILTER, changelog inputs,
empty/all-NULL groups, state TTL, and checkpoint restoration use the existing grouped path.
The running sum and distinct count are checkpointed directly alongside membership so restore
does not refold an overflow-sensitive decimal sum in map order. Both memory and typed RocksDB
state retain this contract, including migrations between the two. Floating averages accumulate
in DOUBLE and retain the input result width; DISTINCT uses Java boxed equality, canonicalizing
NaNs and distinguishing opposite zero signs. Insert-only two-phase SUM/AVG DISTINCT supports
integers, FLOAT, DOUBLE and DECIMAL precision up to 38, as described below.

`TINYINT` and `SMALLINT` `SUM`/`MIN`/`MAX` retain their input width, including the local
partials of an insert-only two-phase plan. SUM wraps on overflow at 8 or 16 bits rather
than widening to BIGINT. Single-phase retractions subtract at the same width; MIN/MAX
retain duplicate multiplicities until the last occurrence is removed. NULL inputs are
ignored and all-NULL groups return NULL. FILTER and integer SUM(DISTINCT) preserve these
rules across batches and checkpoint restore. Retracting two-phase numeric SUM carries a separate non-null count. Decimal overflow and
typed mapped MIN/MAX are described below.

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

Single-phase LISTAGG runs over CHAR/VARCHAR for append and retracting input, and
ordinary and DISTINCT LISTAGG also admit local/global plans for both input modes. Both forms use an
omitted comma separator or a verified non-null character literal. Empty, quoted and
multi-byte separators are supported. NULL values are skipped. In ordinary LISTAGG, empty strings
and duplicates contribute in arrival order. A retraction removes the first matching
occurrence. FILTER, numeric aggregate composition and single-phase mini-batching
retain Flink's result types and changelog behavior.

Append input retains an owned concatenated prefix. Retracting input retains ordered
owned occurrences and pending unmatched retractions, matching released Flink. Versioned
binary accumulator snapshots preserve this state across memory/native RocksDB restores.
Tests compare ordered RowKinds and schemas on Flink 2.2.1 and 1.18.1; four recovery
profiles each force two restores and verify native consumption and released memory.
Single-phase DISTINCT uses string membership around the same ordered accumulator. Duplicate
inserts do not concatenate again; retracting input decrements multiplicity and removes a
value only when its last occurrence leaves. Reinsertion appends it at the new arrival position.
NULLs never enter membership; separator literals, FILTER, CHAR casts, mixed aggregates and
mini-batches use the same contract. Checkpoints preserve membership and ordered values, and
native retained-memory accounting includes owned membership keys and table capacity.
Ordinary append LISTAGG admits positive state TTL in single-phase, single-phase mini-batch
and local/global plans. Its separator/prefix accumulator is one TTL value state, so existing
whole-group expiry and write refresh preserve released behavior, including NULL writes.
Ordinary retracting LISTAGG also admits positive retention. Its value list and pending
retraction list retain per-entry write clocks independently of the enclosing group. A successful
removal rewrites surviving value timestamps; an absent removal only appends a separately timed
pending retraction. NULL rows refresh the group without reviving old list entries. Both lists
and their original deadlines survive memory and direct-RocksDB snapshots. DISTINCT LISTAGG
also admits positive retention. Membership expires independently of the cached prefix or
inner value/pending lists. Append duplicates do not refresh the shared membership clock;
retracting counters refresh on each admitted non-NULL update. Filtered aggregates with the
same argument list share this clock, including one-argument COUNT DISTINCT. An explicit
separator makes a two-argument DISTINCT key and therefore a separate membership view.
Restoring a snapshot without membership deadlines seeds each argument view from the saved
group-write time, or the restore time when the snapshot had no TTL clock.
DISTINCT local/global merges replay the ordered shared membership view, including filtered
consumers and the full value/separator argument identity.
Released Flink rejects LISTAGG WITHIN GROUP ordering. Both releases produce real
local/global plans under TWO_PHASE. For ordinary append input, the local
emits the declared pair of STRING fields (separator, prefix), and the global concatenates
prefixes using each incoming separator. FILTER, default and literal separators, CHAR
casts, mixed numeric aggregates and adjacent DISTINCT-count views retain their positional
layouts. Both halves and the exchange remain columnar.

The local drains before checkpoints; the global persists its ordered prefix through the
existing state codec. Four profiles exercise two restores on memory/native RocksDB with
bundle sizes 3 and 11. Checkpoint drains can change intermediate mini-batch updates, so
recovery compares the materialized changelog, including exact final string order and counts;
uninterrupted SQL checks retain raw kinded changelog comparisons. All 79 SQL/operator checks
are covered on each release (2.2 uses the 73 unchanged controls plus the corrected six-case
recovery suite; 1.18 runs all 79 together). A native partial/merge/restore test and 92 release
fixtures cover default/literal separators, NULL-only groups, 17-row physical chunks and
independent final prefix/count oracles.
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

Single-phase DISTINCT is qualified separately on release+mimalloc, JDK 17, p=1,
2 GiB heap, UTC, 1,024-row production transposes and IPC, with two warmups and three
alternating trials. Complete row-source/row-sink jobs retain JNI and state costs and
assert native execution. All 65 SQL/operator checks pass on both released Flink lines,
including four two-restore memory/RocksDB profiles; 76 native fixtures pass (20 new
DISTINCT update/checkpoint fixtures).

| DISTINCT workload | Stock median | Previous StreamFusion median | Native median | Native vs stock |
|---|---:|---:|---:|---:|
| 500k rows, 16,384 keys, 16-byte strings, append | 1.014 s | 0.924 s | 0.416 s | 2.44× |
| Same, insert/insert/delete | 1.252 s | 1.202 s | 0.422 s | 2.97× |
| 500k rows, 8,192 keys, 128-byte strings | 1.204 s | — | 0.705 s | 1.71× |
| 1M rows, one key, 16-byte strings | 0.633 s | — | 0.325 s | 1.95× |
| Wide profile, Flink 1.18.1 | 1.211 s | — | 0.717 s | 1.69× |

Actual previous `cc17301b` falls back for DISTINCT, with the identical query and fixture;
append/retract medians improve by 2.22×/2.85×. Its paired stock medians are 1.015/1.181 s.
An ordinary LISTAGG control remains native in both revisions: 0.333 s current versus
0.326 s previous, with overlapping 0.307–0.344/0.321–0.335 s ranges. The 2.1% median
difference does not establish a general regression or improvement. These short complete
jobs retain startup costs; arbitrary per-key string sizes are not qualified. All 90 jobs,
means, medians, variability, source/library hashes and allocation probes are retained in
[the DISTINCT artifact](../benchmarks/results/listagg-distinct-2026-10-19.json).
Reproduce with the command above plus `-Dstringagg.distinct=true`; use
`stringagg.warmup=2` and `stringagg.runs=3` for this measurement configuration.

Ordinary append **TWO_PHASE** measurements use release+mimalloc, JDK 17, p=1,
2 GiB heap, UTC, 1,024-row transposes and 4,096-row mini-batches. Two warmups and three
alternating trials retain both transposes, production IPC/JNI, local/global state,
row source/sink and native execution metrics.

| 500k-row split workload | Stock median | Previous StreamFusion median | Native median | Native vs stock |
|---|---:|---:|---:|---:|
| 64 keys, 16-byte strings | 0.636 s | 0.598 s | 0.263 s | 2.42× |
| 16,384 keys, 16-byte strings | 0.872 s | 0.819 s | 0.551 s | 1.58× |
| 8,192 keys, 128-byte strings | 2.072 s | — | 1.875 s | 1.11× |
| Wide profile, Flink 1.18.1 | 2.062 s | — | 1.889 s | 1.09× |

Actual previous `a0dd888d` falls back for these split queries; compact low/high medians
improve 2.27×/1.49×. Its stock controls are 0.613/0.829 s. The ordinary ONE_PHASE control
remains native: 0.334 s current versus 0.343 s previous, with overlapping
0.312–0.345/0.313–0.346 s ranges. Wide-profile improvements are modest and do not cover
arbitrarily larger per-key strings; the 1.18 wide profile includes one measured native
trial slower than its paired stock trial. Mean and median comparisons across each full
profile still favor native. Low-cardinality jobs retain startup costs and substantial
variation (0.173–0.291 s native). All 80 jobs, variability, source/library hashes and
allocation records are retained in
[the two-phase artifact](../benchmarks/results/listagg-two-phase-2026-10-19.json).
Reproduce with `StringAggregateBenchmark`, `-Dstringagg.phase=TWO_PHASE
-Dstringagg.batchSize=4096 -Dstringagg.rows=500000 -Dstringagg.warmup=2
-Dstringagg.runs=3` and the table's `stringagg.keys`/`stringagg.width`.
`stringagg.phase` defaults to ONE_PHASE; the separate control disables mini-batching.

Positive-retention admission is pinned by 20 direct operator comparisons per release.
Tests extract Flink's generated keyed aggregate from a real SQL transformation graph,
synchronize the TTL provider and processing-time clocks, and compare each kinded output.
They cover two keys, NULL write refresh, immediately-before/exact expiry, checkpoint
restore, both separator forms, memory/native RocksDB, and single/single-mini/global-mini
contexts. Six additional SQL profiles force two restores with one-hour retention. The
combined integration passes 288 checks on Flink 2.2.1 and 252 with 36 declared host skips
on 1.18.1. All 47 grouped native state tests and 352 selected release fixtures pass;
four new fixtures independently check refresh, restore and fresh INSERT after expiry.

Positive-TTL complete jobs use the integrated runtime, p=1/JDK 17/2 GiB/UTC,
release+mimalloc, 500k nullable rows, 1,024-row transposes and both IPC/JNI boundaries.
Retention is one hour; controlled-clock tests establish expiry separately. Two warmups
and three alternating trials retain every measurement.

| Retention workload | Stock median | Previous StreamFusion median | Native median | Native vs stock |
|---|---:|---:|---:|---:|
| ONE_PHASE, 16,384 keys, 16-byte strings | 1.226 s | 1.224 s | 0.400 s | 3.06× |
| ONE_PHASE, 8,192 keys, 128-byte strings | 2.261 s | — | 1.612 s | 1.40× |
| TWO_PHASE, 16,384 keys, 16-byte strings | 1.494 s | 1.450 s | 0.535 s | 2.79× |
| TWO_PHASE, 8,192 keys, 128-byte strings | 2.883 s | — | 2.090 s | 1.38× |
| ONE_PHASE mini-batch, compact profile | 1.353 s | — | 0.531 s | 2.55× |
| TWO_PHASE wide profile, Flink 1.18.1 | 2.882 s | — | 1.888 s | 1.53× |

The actual previous `5c110c1a` falls back for positive retention; compact ONE/TWO medians
improve 3.06×/2.71×. Its paired stock medians are 1.261/1.450 s. The current build also
integrates the independently qualified typed ordered-value implementation, preserving
all earlier feature heads. No-TTL controls remain native: ONE_PHASE medians are
0.342/0.342 s current/previous. Wide TWO_PHASE medians are 1.930/1.837 s (current about
5% slower), while means are 1.919/1.927 s and ranges overlap (1.891–1.935/1.821–2.122 s).
These controls do not establish a general no-TTL change. Native allocation probes show
identical output-buffer observations and only two extra small per-batch requests in the
shared dispatch; allocation requests do not measure copied bytes.

All 120 jobs, means, medians, variability, provenance and allocation probes remain in
[the retention artifact](../benchmarks/results/listagg-retention-2026-10-19.json).
Short jobs retain startup costs; arbitrary per-key widths/cardinalities are unmeasured.
Reproduce with `StringAggregateBenchmark`, `-Dstringagg.ttl='1 h'`, the table's
`stringagg.phase`, `stringagg.keys` and `stringagg.width`, plus `stringagg.batchSize=4096`,
`stringagg.rows=500000`, `stringagg.warmup=2` and `stringagg.runs=3`.
`stringagg.miniBatch=true` separately selects single-phase mini-batching; its default
follows TWO_PHASE. Omitting `stringagg.ttl` retains the prior zero-retention default.

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
multisets. JSON_ARRAYAGG also admits inline ORDER BY and WITHIN GROUP ordering clauses.
Released Flink 2.2.1 and 1.18.1 retain arrival order for these clauses rather than sorting;
the native accumulator preserves that released behavior, including retractions. These
clauses therefore do not promise sorted JSON arrays. Positive state TTL is native for
both functions in immediate and single-phase mini-batch plans. Each array element and
object key retains its own last-write time; NULL-omitting updates refresh the enclosing
accumulator without refreshing older entries. A successful array retraction refreshes
every surviving element, matching the released ListView rewrite; an unsuccessful
retraction does not. Expired views are read before computing the previous output, so
UPDATE_BEFORE also reflects expiry. Checkpoints retain entry deadlines on memory and
native RocksDB backends. JSON_OBJECTAGG also admits real local/global plans for append
and retracting input. Locals emit Arrow lists of serialized key/value map entries; globals
merge only the entries that survive each local map. Both NULL policies retain the released
rules: an omitted local NULL leaves no key for the global duplicate check, while duplicate
keys still fail inside a local map or across nonempty partials. Local maps admit NULL keys. Released 1.18's split MapView conversion reads a heap NULL
key as an empty STRING and rebuilds a map, including collapse with a real empty key in the
same partial; the native partial preserves that behavior. Released 2.2 retains its global
NULL-key failure. Duplicate NULL keys still fail within a local map on both lines. Positive TTL applies
when the global bundle flushes, independently for each merged field.
JSON_ARRAYAGG has no merge method in either released version: even a
TWO_PHASE request retains a single-phase plan, which runs natively. Released Flink
2.2.1 and 1.18.1 reject DISTINCT JSON aggregates, JSON_OBJECTAGG WITHIN GROUP ordering,
and JSON aggregate FILTER syntax; these are host limitations rather than acceleration
gaps. Eight additional released-SQL checks pin the actual phase plans, native execution,
NULL modes, and parser/validator failures.

The retention implementation passes 67 SQL/operator checks on each released line,
including both NULL policies, append and retract input, immediate and mini-batch
execution, exact inclusive expiry, key reuse and two successive restores on memory
and native RocksDB. Six focused Rust checks cover JSON state and expiry; the complete
native suite passes 832 checks (two diagnostic tests ignored). Upstream execution
contracts include both mixed JSON aggregate tests; consolidated unchanged-upstream
runtime validation remains required before delivery.

Release+mimalloc/JDK 17 complete jobs with p=1, memory state, 2 GiB heap, one-hour TTL,
65,536 keys and 16-character values retain sources, both transposes, JNI and rowwise
sinks. Two warmups and three interleaved trials give:

| Release | Function | Rows | Stock seconds | Native seconds |
| --- | --- | ---: | ---: | ---: |
| 2.2.1 | JSON_ARRAYAGG | 1,000,000 | 8.719 | 1.062 |
| 2.2.1 | JSON_OBJECTAGG | 131,072 | 2.017 | 0.223 |
| 1.18.1 | JSON_ARRAYAGG | 1,000,000 | 10.350 | 1.033 |
| 1.18.1 | JSON_OBJECTAGG | 131,072 | 1.962 | 0.213 |

Every candidate trial requires consumed native aggregate work. The million-row object
warmup was time-boxed after more than two minutes in stock TTL incremental cleanup;
a warmup thread dump locates `TtlIncrementalCleanup` through `TtlMapState.get` and
`JsonObjectAggFunction.getValue`. Both engines use the smaller object fixture above.
The matched canonical `d13e9dc4` planner fallback takes 8.688 s for arrays and 2.060 s
for objects (stock 8.925 and 1.993 s). It requires zero planner substitutions and uses
the same compatible library; no affected aggregate executes natively in that control.
The [raw artifact](../benchmarks/results/json-view-retention-2026-10-21.json) retains
all completed trials, the previous fallback control, hashes and the abandoned diagnostic.
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

### JSON array ordering clauses

`FlinkJsonArrayOrderingSqlHarnessTest` passes twelve checks on each of released Flink
2.2.1 and 1.18.1. ASC, DESC and WITHIN GROUP deliberately use sort keys that disagree
with arrival order. Each append/retract case checks all three NULL policies, exact
ordered RowKinds, result types and native aggregation. Nullable and multiple sort keys
also preserve arrival order. Four recovery cases each
exercise both inline and WITHIN GROUP forms through two restores, using memory or
native RocksDB state and append or retracting input. They verify native work and
released memory. The existing 19 JSON aggregate checks also pass on both releases.
Ordering metadata uses the existing JSON array accumulator and its existing native
string/JSON update and checkpoint Criterion fixtures; it adds no sort state or kernel.

Release complete-job measurements on Flink 2.2.1/JDK 17 use parallelism one, 2 GiB
heap, production Arrow IPC, mimalloc, 1,024-row transposes, ONE_PHASE and no
mini-batching. Each profile has two warmups and three alternating measured trials
per engine in a fresh JVM. The runtime row source, both transposes, serialization,
JNI, aggregate state and row blackhole sink remain measured. Strings are 16 bytes;
values include NULLs, and retracting profiles cycle insert/insert/delete rounds.

| Value, order, changelog, rows / keys | Stock median | Previous StreamFusion median | Native median |
|---|---:|---:|---:|
| STRING, ASC, append, 2M / 65,536 | 9.080 s | 9.035 s | 2.026 s |
| STRING, ASC, retracting, 2M / 65,536 | 5.699 s | 5.500 s | 1.498 s |
| STRING, DESC, append, 2M / 65,536 | 9.340 s | 9.053 s | 1.962 s |
| STRING, DESC, retracting, 2M / 65,536 | 5.706 s | 5.409 s | 1.504 s |
| BIGINT, ASC, append, 2M / 65,536 | 9.279 s | 9.288 s | 1.543 s |
| BIGINT, ASC, retracting, 2M / 65,536 | 5.916 s | 5.918 s | 1.373 s |
| BIGINT, DESC, append, 2M / 65,536 | 9.256 s | 9.162 s | 1.508 s |
| BIGINT, DESC, retracting, 2M / 65,536 | 5.861 s | 5.656 s | 1.349 s |
| STRING, ASC, append, 1M / 16,384 | 6.751 s | 6.757 s | 1.252 s |
| STRING, DESC, retracting, 1M / 16,384 | 3.477 s | 3.435 s | 0.961 s |

All ten profiles beat stock by **3.617–6.136×** and frozen prior StreamFusion
`d85b241a` by **3.574–6.074×**. Every candidate trial range lies below the stock
and prior ranges. Separate-JVM paired stock medians differ by at most 4.6%, so
fine differences between ordering directions are not meaningful. These are bounded
per-key arrays, not a performance guarantee for arbitrary array sizes. The unchanged
native array accumulator provides the gain when the planner admits these clauses.
[Raw measured/warmup jobs, trial arrays, configuration and source/library hashes](../benchmarks/results/json-array-ordering-2026-10-18.json)
retain all measurements. Reproduce using `StringAggregateBenchmark` above with
`stringagg.order=ASC|DESC`, `stringagg.value=string|bigint`, and the recorded row,
key, warmup and trial counts.

Two additional profiles run the new planner with the frozen prior native library,
independently of the unshipped SINGLE_VALUE optimization present in the development
library. STRING/ASC append measures **1.966 s native versus 9.508 s stock**; BIGINT/DESC
retracting measures **1.380 s versus 5.946 s**. These retain the same resources, warmups,
three alternating measurements and native-consumption assertions. Their complete jobs
are also retained in the artifact; no new native operation is needed for this admission.

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
TIMESTAMP_LTZ. Results follow Flink's declared type, precision and value semantics.
FIRST_VALUE/LAST_VALUE support per-aggregate FILTER. On Flink 2.2, they additionally admit
BINARY/VARBINARY, ARRAY, MAP, ROW, MULTISET and both interval families through the existing
Arrow bridge. DISTINCT additionally admits ARRAY and intervals; the released planner rejects
DISTINCT binary, MAP, ROW and MULTISET values. SINGLE_VALUE retains its released scalar factory
limits. These additions preserve the single-phase plan even when TWO_PHASE is requested.
Flink 1.18.1 cannot plan CHAR or temporal FIRST_VALUE/LAST_VALUE and fails a type assertion for
SINGLE_VALUE over TIME(0), including string casts to TIME(3) that resolve to TIME(0);
those host limitations remain unavailable on that release. Typed TIME(3) input is supported.

FIRST_VALUE and LAST_VALUE skip NULLs and follow arrival order within a key. Append-only
input retains one scalar. Retracting input retains the ordered non-NULL occurrences;
a retraction removes the oldest matching occurrence, including when values repeat across
Arrow batches. Generic binary FIRST/LAST follow the released byte-array identity behavior:
in the verified plans, equal newly decoded binary values do not remove a cached occurrence. Floating retractions use Java boxed equality: NaN payloads match, while opposite
zero signs do not. Removing every contributing value yields NULL, and removing the last
record deletes the group. Results depend on arrival order, so SQL parity fixtures use a
controlled source rather than asserting equal results from independently reordered inputs.

`FIRST_VALUE(DISTINCT value)` and `LAST_VALUE(DISTINCT value)` also run in a single
phase over the same scalar types as their ordinary forms, plus ARRAY and intervals on Flink 2.2. They retain the arrival order of unique live values and their
multiplicities: inserting a duplicate does not move its position, and only the last
retraction removes it. Reinsertion after complete removal gives it a new position. NULLs
are ignored, and filtered instances have independent membership. Checkpoints retain both
order and multiplicities. Floating membership treats all NaN payloads as one value and
distinguishes the two zero signs; the ordered queue preserves the first inserted payload.
Typed maps retain integer widths, physical DATE/TIME values, exact decimals and inline
timestamp nanoseconds.
Positive retention keeps the shared DISTINCT membership clock separate from the cached
selection and, for retracting input, the inner value/order maps. Membership expiry does not
remove an already selected value. Append-only duplicates do not refresh membership; a new
filter bit does. Retracting views preserve signed counts when an expired value is retracted.
The cached scalar, membership and inner maps retain their independent state across restores.

Ordered aggregate state now resides inline in the existing per-aggregate storage, avoiding a
separate allocation for every group; dynamic strings and retraction queues remain accounted
against the task memory budget. Checkpoint representation and aggregate semantics are unchanged.



An immediate, single-phase group with one unfiltered SINGLE_VALUE emits directly from its accumulator
without a duplicate cached result or temporary tuple vector. A later touch reconstructs
the preceding result from that accumulator. Filtered and mixed aggregates retain their cache;
mini-batch emission and snapshot formats are unchanged.

SINGLE_VALUE counts every element, including NULL. Zero elements yield NULL; one element
yields that value. A second element raises Flink's release-specific exception with the same
cardinality diagnostic. Retraction clears the retained value and decrements the count;
released Flink rejects a SQL FILTER clause on SINGLE_VALUE during planning.

Released Flink's SQL planner removes DISTINCT from SINGLE_VALUE. Consequently
`SINGLE_VALUE(DISTINCT v)` runs as ordinary SINGLE_VALUE: duplicate values and duplicate
NULLs still count as separate elements and can raise the same cardinality error. This
SQL form already runs natively; it is not a distinct-membership acceleration gap.
An internal physical aggregate that retains a DISTINCT flag still has no native path.
`FlinkSingleValueDistinctSqlHarnessTest` verifies twelve checks on each released line:
duplicate and NULL failures, singleton/empty input, and actual local/global string
plans with one-row and multi-row bundles. It compares host/native failure classes,
diagnostics and evaluation phase, alongside exact successful changelogs and types.

After a local bundle update or flush fails, cleanup never flushes that partially
updated bundle again. Close still releases its native handle and memory reservation.
This preserves the original cardinality error when Flink 1.18 cleanup invokes finish;
a secondary Arrow key-gather failure must not replace it. The operator harness covers
both one-row physical batches and a multi-row batch, followed by finish, checkpoint
and watermark callbacks, and verifies that no partial output escapes.

Checkpoints preserve the scalar/count or ordered occurrences. Append-only first/last and
SINGLE_VALUE support the enclosing group's TTL. Retracting FIRST_VALUE/LAST_VALUE also admit
positive retention, including STATE_TTL hints. Their cached selection, value-to-order map and
order-to-value map have separate lifetimes. Expired supporting entries do not recompute the
cached FIRST/LAST scalar. A successful map write refreshes that entry; reads do not. Removing
the oldest occurrence refreshes surviving orders for that value and surviving values in its
arrival bucket, matching the released functions. NULL rows refresh only the group accumulator.
Optional framed Arrow checkpoint extensions retain both map clocks, order lists and the cached
selection without changing fresh TTL-off snapshots. Disabling retention after restore keeps
the cached selection and supporting maps while ignoring their expiry clocks. The processing clock is sampled per native batch,
as in the existing TTL policy; within-bucket arrival order remains stable.
These aggregate states use the existing raw keyed snapshot path with both memory and RocksDB
backends; the direct RocksDB accumulator-row codec does not yet encode ordered occurrences.
FIRST_VALUE/LAST_VALUE have no merge implementation on the supported released Flink lines:
requesting TWO_PHASE retains a single-phase aggregate, which runs natively, including DISTINCT.
Execution contracts verify the selected phase, values, result types and consumed native work.
Internal physical split FIRST_VALUE/LAST_VALUE plans, physical SINGLE_VALUE with a retained
DISTINCT flag, two-argument value/order dialects, and other value types retain explicit gates.

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

Generic Flink 2.2 FIRST/LAST coverage was measured separately on Linux x86_64, JDK 17,
release+mimalloc, p=1, memory state, 2 GiB heap, 64 keys, cardinality 1024 and NULL every
eighth row. Complete jobs retain both transposes, JNI and a rowwise sink, with two warmups
and three interleaved measured trials. Each trial requires consumed native aggregate work.

| Input/profile | Rows | Stock seconds | Initial native | Final native |
| --- | ---: | ---: | ---: | ---: |
| ARRAY append | 1,000,000 | 0.508 | 1.940 | 0.753 |
| ROW append | 1,000,000 | 0.573 | 5.780 | 1.794 |
| MAP append | 1,000,000 | 0.867 | 7.043 | 3.081 |
| BYTES append | 1,000,000 | 0.416 | 0.502 | 0.466 |
| ARRAY DISTINCT | 1,000,000 | 0.453 | 2.729 | 1.758 |
| ARRAY retracting | 131,072 | 0.347 | 0.399 | 0.268 |

The matched canonical `d13e9dc4` ARRAY fallback takes 0.471 s (stock 0.484 s).
The initial profile located allocation-heavy nested equality and scalar extraction in
the native update loop. Two measured passes skip unused FIRST/NULL inputs, reuse shared
non-floating values and compare nested children through borrowed arrays. The slower
results remain enabled as correctness-verified groundwork under the maintainer's bounded
coverage agreement, without claiming acceleration. Further scalar extraction and nested
output work is tracked in [issue #443](https://github.com/datafusion-contrib/StreamFusion/issues/443).
The [raw artifact](../benchmarks/results/generic-first-last-2026-10-21.json) retains every
warmup/trial, variability, profile counts and library hashes. These are complete-job
diagnostics; they do not substitute a Java timer for the Rust/Arrow small-function boundary.

The final generic implementation passes 84 released 2.2 phase/type/recovery checks,
including explicit host rejections for unavailable DISTINCT types. Another 16 immediate
changelog and 12 immediate recovery checks cover the untimed initial implementation.
Flink 1.18 passes 72 explicit factory/planning-failure checks; its 12 generic recovery
cases are unavailable. Typed snapshots restore the Arrow value schema before state.

**Idle-state TTL.** `table.exec.state.ttl` runs natively here (and on the two-phase global merge
below — the local half is transient and holds no TTL-eligible state). Semantics match Flink
exactly: every stored value carries its last-**write** wall-clock timestamp (reads never refresh
it), expiry happens at `last_write + ttl` inclusive, and expired state reads as absent and is
deleted on read. The `STATE_TTL` hint overrides the job-wide retention on aggregates specifically.
Mini-batch operators read retained state at the logical bundle flush and use that flush's
clock for expiry and writes. Input Arrow buffers remain reserved until that merge; receiving
a partial before expiry does not revive state that has expired by the flush.

### Shared DISTINCT entry retention

Single-phase and local/global scalar COUNT DISTINCT and numeric SUM/AVG DISTINCT retain each membership
entry independently of the cached aggregate and the group's own idle deadline. Expiring an
entry forgets membership without undoing the previously accumulated result. Filtered
aggregates sharing an argument share the entry deadline: a newly set append-only filter bit
refreshes it, while duplicate-only reads do not. Retracting counters write on every admitted
update and retain negative counts after an expired value is retracted.

Checkpoints preserve cached COUNT/SUM/AVG values separately from live membership, plus the
original shared deadlines. With the native RocksDB backend these combinations use native raw
keyed snapshots, keeping the shared clock together rather than assigning unrelated clocks
to each aggregate's element table. Fresh zero-retention snapshots keep their previous format.
An older snapshot without membership clocks seeds each member from the saved group-write
time, or the restore time when the old snapshot had no TTL timestamp.
FIRST/LAST use the same shared membership policy while retaining their own inner selections.
Single-phase DISTINCT LISTAGG uses the same membership clock; explicit-separator calls
share only with the same two-argument view. Its inner ordered lists retain their own clocks.

Local partials remain transient and have no entry TTL. At the global bundle flush, shared
filtered views expire together before their deltas fold into cached results. Floating and
wide-decimal merges retain the released map iteration order and zero-entry behavior.
Restoring older global snapshots seeds missing entry clocks before the first new bundle
refreshes the group timestamp. The global view layout is configured before loading partitions.

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
Retracting two-phase extrema carry signed maps, or scalar partials when Flink proves monotonicity;
mapped extrema support independent entry retention alongside their cached extreme and map-size counter.

Decimal `SUM`/`MIN`/`MAX`/`AVG` carry through the split too: `SUM`'s partial is the i128 running
sum as `DECIMAL(38, s)` (a bundle overflow emits NULL and latches the merged `AVG` NULL, skipped
by the `SUM` merge — the host's own null-propagation), `MIN`/`MAX` partials keep `DECIMAL(p, s)`
through the extremes multiset, and `AVG` merges the `(DECIMAL(38, s), bigint)` pair into the exact
division emit.

SUM0 has one typed partial: zero for an all-NULL local bundle, a wrapped integer
sum, a FLOAT/DOUBLE sum rounded at its width, or the widened decimal sum. A NULL decimal partial means overflow and latches
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
BOOLEAN, DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ, ARRAY and both interval families; `SUM(DISTINCT)` over bigint/int/smallint/tinyint/float/double and DECIMAL
with precision up to 38.
Decimal sum partials must be DECIMAL(38, input-scale), while the distinct view preserves each
input value's original precision and scale. Boolean and timestamp views retain their Arrow
value types, including fractional timestamp nanoseconds. Floating sums preserve the released
Java map's iteration order through local insertion, serialized copies and each global logical
bundle, including shared and independently filtered views. Physical Arrow batch boundaries do
not change that order. Opposite zero signs remain distinct; NaN payloads share membership
while transport retains the original key bits.
DATE membership uses epoch days; TIME views carry internal milliseconds even if Flink resolves
a lower declared precision. Both timestamp families use the existing millisecond/nanosecond
component representation, retaining precision 0/3/6/9 and wide/pre-epoch values without a session-zone
conversion. Runtime SQL checks cover shared/filtered views, NULL-only groups, empty global input,
and repeated checkpoint recovery with memory and native RocksDB state.
The [release job measurements](../benchmarks/basic-type-coverage.md#temporal-two-phase-distinct-2026-10-16)
retain both transposes and IPC shuffle: 1.32-1.54x stock at 2M rows/64 keys and 1.07-1.15x
at 10M rows/1,024 keys. Persistent recovery is verified for correctness; no RocksDB speedup is claimed.

Insert-only `AVG(DISTINCT)` also rides the split for integers, FLOAT, DOUBLE and DECIMAL precision up to 38.
The declared partials are a widened sum and a BIGINT count, while the membership view retains
the original input value type. Floating sums widen to DOUBLE for AVG. Empty/all-NULL bundles emit a
zero sum and zero count. The global merges membership entries into the same exact average
state used by single-phase AVG DISTINCT; it does not add the local sums/counts, which would
count values appearing in several bundles more than once. Decimal sum fields must retain
precision 38 and the input scale, and the result type must match Flink's AVG type.
Released shared AVG/COUNT/SUM reductions may use DISTINCT SUM0 plus COUNT; SUM0 retains its
positive-zero initialization. Aggregates sharing one floating view use one transient membership
map, then update their independent durable accumulators in the verified merge order.

The [floating partial measurements](../optimizations/aggregate-specialization-fast-paths.md#ordered-floating-distinct-partials)
separate Rust/Arrow functionality admission from conversion-inclusive complete-job costs.

Two-phase `SINGLE_VALUE` over the single-phase scalar value types, including retracting input,
retains a typed
value and an INT count in each local bundle. NULL increments the count. Both local accumulation and global merging
raise Flink's cardinality exception on a second element; global checkpoints preserve that
count even when the value is NULL. The global consumes the count partial, rather than
counting bundles. The declared value partial must match the input type, and its count must
be INT. Flink may declare a TIME(3) partial for TIME(0) input; both use millisecond storage
and preserve the input value. Signed local value/count partials merge through the same
released SINGLE_VALUE rule; deleting and recreating a group preserves NULL and count-zero cases.
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
shape) is native for `COUNT`, `AVG`, `SUM0`, numeric `SUM`, scalar `SINGLE_VALUE` and typed `MIN`/`MAX`.
SUM_RETRACT uses a separate `(sum, BIGINT count)` partial pair; the other admitted layouts are invariant under
retraction. The local subtracts `-U`/`-D` rows, and the appended (or reused) `count1` `COUNT(*)`
partial drives per-key liveness in the global (`-D` and state drop when the merged count reaches
zero at the logical bundle flush, Flink's `RecordCounter` semantics).

Ordinary retracting SUM admits TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE and DECIMAL, with
matching declared sum partial/result widths (DECIMAL widens to precision 38) and a BIGINT
non-null contribution count. Its local sum adjustment
survives a zero-net-count bundle; the global adds the adjustment and updates the non-null count
separately. A zero non-null count emits NULL, while the separate count1 field controls group
liveness. Numeric zero is not an empty group. The record counter removes a group only at exactly
zero at the logical flush, matching Flink even when duplicate retractions make it negative.
A temporary zero count inside the bundle retains its arithmetic history; clearing it early
loses floating cancellation residue and changes SUM/AVG results. Local SUM preserves a nullable
raw accumulator, including negative zero and a nonzero adjustment at count zero. The global first
combines each bundle's partial sums and then merges that temporary sum into retained state.
This two-level reduction preserves decimal reset-after-overflow and floating rounding order.
A NULL decimal SUM partial is ignored while its count still merges; AVG overflow remains sticky.

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
`MIN`/`MAX` with a retained physical `DISTINCT` flag, and — under a
retracting input — aggregates outside `COUNT`/`AVG`/`SUM0`, ordinary numeric `SUM`,
scalar `SINGLE_VALUE`, and typed `MIN`/`MAX`. Retracting DISTINCT admits scalar COUNT and numeric
SUM/AVG, including FLOAT/DOUBLE and decimal precision through 38, using ordered shared views.

Staged membership memory accounting includes signed size changes between physical pushes of
one logical bundle. Removals return released buffer bytes to the reservation; debug and release
builds retain the same results without unsigned subtraction overflow.
Decimal-overflow failure parity checks retain the exception type, execution phase and native
route on both supported JDKs, and check helpful JVM null-dereference text when it is available.

## Type support

The separate [integer COLLECT](#integer-collect) route admits INT/BIGINT through the released
JVM handler. The table below describes the arithmetic native accumulators.

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

Complete-job diagnostics use symmetric stock/native preflight. Earlier v1 measurements
below explained only native before timing and remain historical diagnostics. The corrected
[method and first complete-job trials](../benchmarks/jvm-aggregates.md#symmetric-preflight-correction)
retain the distinct-key regression for deferred performance work.

JVM aggregate functions now default enabled under the accepted UDF performance exception;
remaining coverage is tracked by [#237](https://github.com/datafusion-contrib/StreamFusion/issues/237).
The system property `streamfusion.experimental.jvmGroupAggregates` defaults to true and admits
released Flink bridging aggregate functions and its exact legacy Table API aggregate wrapper
in the synchronous single-phase plan when every
input and output field is a verified scalar: CHAR/VARCHAR, BOOLEAN, signed integers,
FLOAT/DOUBLE, DECIMAL, BINARY/VARBINARY, DATE, TIME, TIMESTAMP/TIMESTAMP_LTZ or intervals.
Fixed BINARY requires the existing variable-byte transport (enabled by default).
Legacy JVM functions reuse the released legacy type inference and POJO accumulator converters;
DISTINCT/non-DISTINCT sharing, FILTER, NULL arguments, lifecycle, positive retention and repeated
memory/RocksDB recovery use the same managed-state boundary. Wrapper subclasses with custom
argument-dependent construction (including Hive wrappers), Python functions and specialization
retain fallback. The unchanged mixed-DISTINCT upstream WeightedAvg fixture requires positive
JVM aggregate work on both lines, except for Flink 1.18's already excluded changelog-state
backend variant, which requires explicit full fallback. A release complete-job diagnostic measures 0.306032 s
native versus 0.281323 s stock and 0.284807 s preceding fallback for mixed legacy DISTINCT
weighted average; [method and all trials](../benchmarks/jvm-aggregates.md#released-legacy-wrapper-coverage-2026-10-23)
retain the ranges and startup/JIT limits without an acceleration claim.
ARRAY/MAP/ROW fields at the operator boundary retain fallback; managed accumulator fields and
DataViews still use Flink's own declared serializers and converters. It retains Flink's generated function,
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
records every stage. Cross-version migration, stock/native state-layout migration and
two-phase merge/reset remain pending.

DISTINCT and FILTER use the released generated handlers, including shared membership state,
NULL handling, retractions and managed DataViews. Released Flink still rejects DISTINCT on
binary arguments during planning. Positive idle retention and, on Flink 2.2,
STATE_TTL hints use the released grouped function's managed-state TTL. Per-operator hints take
precedence over job retention. Positive TTL disables the optional accumulator cache and input
coalescing so each row reads and updates expiring state on Flink's clock. Expiration does not
emit a delete; later input observes the same empty-state behavior as stock Flink, including
ignored expired retractions and unsuppressed no-op updates.

For non-compact DECIMAL grouping keys, the Java grouped operator projects keys through the
released RowData serializer, matching the native exchange's existing BinaryRow layout.
Flink's generated key projection reserves extra bytes for a NULL non-compact decimal, whereas
its serializer omits them. Mixing those layouts gives different hashes and can route NULL
keys into the wrong subtask. The selected keys own their bytes and always have INSERT kind,
independent of the input changelog kind; other key types retain the generated selector.
This does not establish stock/native savepoint-layout migration.

The typed and modifier regressions compare complete values, declared types and per-key raw
changelogs on both supported lines. Precision-38 decimals, nanosecond/wide-date timestamps,
actual BINARY byte lengths, nullable keys, delete/reinsert and managed accumulator fields
survive two forced restores with both backends. A mixed typed-UDAF/DISTINCT/DataView/FILTER
workload also restores checkpoints and canonical savepoints through 1 → 3 → 2 subtasks,
checking source offsets, operator identities and key-group ownership. Controlled-clock operator
checks compare TTL refresh, exact expiry and restored state against the released host runtime.
The unchanged upstream DISTINCT UDAF/DataView fixtures require positive native operator work.
[Complete-job diagnostics](../benchmarks/jvm-aggregates.md#scalar-types-distinctfilter-and-idle-retention-2026-10-23)
retain stock, prior-fallback and native costs, including regressions under the accepted UDF exception.

Mini-batching, asynchronous state, specialized functions, approximate aggregates and ordered
calls are excluded from this boundary. Two-phase merge/reset, OVER functions and the remaining nested/table-function
type gaps stay tracked in [#237](https://github.com/datafusion-contrib/StreamFusion/issues/237).
Verified [nested scalar results](calc-filter.md#nested-java-scalar-results) and
[scalar table-function types](unnest-correlate.md#jvm-table-functions) are already enabled
under their own documented boundaries.

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
retain the variability, GC observations and interference records. These historical profiles
did not establish a throughput win. Unlisted key/accumulator contracts remain unverified;
the newer scalar contract and preceding-fallback comparisons are recorded in the linked
benchmark page. The isolated results below cover both Flink lines. An additional cold-key bypass run measured
1.770 s native versus 1.769 s stock but overlapped another build, so it is diagnostic only.
[Isolated release diagnostics](../benchmarks/jvm-aggregates.md) build and finish correctness
checks before measuring typed key/cardinality cases and collecting Java/Rust profiles on
separate GitHub runners. Default UDF coverage now follows the accepted performance exception.

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
The feature defaults on under the accepted UDF exception; the cache remains default off.
Explicit `streamfusion.experimental.jvmGroupAggregates=false` restores host execution.
#237 stays open for the remaining contracts.
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
retained. It does not establish a general speedup or complete #237.

The next default-off [borrowed STRING exit experiment](../optimizations/zero-copy-exit-transpose.md#experimental-borrowed-string-exit)
shares the batch-scoped UTF-8 view helper used by cache probes, retaining owned cache keys and
mandatory output copies. Matched profiles reduce inclusive aggregate allocation samples by
22%, while the latest local distinct-key job remains 7.6% slower than stock. Normal-capacity
validation passes 73 combined cases on each Flink line; the small-capacity JSON/JVM selection
passes 108 on 2.2 and 106 with two compatibility skips on 1.18. All trials, profiles and test
logs are [retained](../benchmarks/results/jvm-aggregate-borrowed-string-exit-2026-10-16.json).
The aggregate implementation defaults on; the cache remains default off.

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

- Released Flink rejects LISTAGG WITHIN GROUP ordering and JSON DISTINCT/object-ordering
  syntax; these are host limitations. JSON_ARRAYAGG ordering clauses preserve the
  released host's arrival-order behavior described above.
- A UDAF outside the verified synchronous single-phase JVM boundary described above.
  Verified UDAFs default on under the accepted performance exception.
- `AVG`/`SUM`/`MIN`/`MAX` over a value type outside [Type support](#type-support)'s ✓ set.
- Physical SINGLE_VALUE with a retained DISTINCT flag.
  (`COUNT(DISTINCT x)` keeps a per-key
  value set; `SUM(DISTINCT x)` adds a running sum folded as values enter/leave it; `MIN`/`MAX
  (DISTINCT)` run as their plain, multiplicity-blind forms.)
- COLLECT outside its verified single-phase INT/BIGINT, scalar-input, non-character-output boundary; ARRAY_AGG.
- An approximate aggregate.
- FIRST_VALUE/LAST_VALUE/SINGLE_VALUE over a value type outside the single-phase list above,
  or with more than one argument.
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

- Any aggregate other than SUM/MIN/MAX/COUNT/AVG/SUM0, typed SINGLE_VALUE over the
  supported single-phase scalar types, ordinary or DISTINCT LISTAGG over the append string pair or
  retracting list partial with its membership view, or JSON_OBJECTAGG over its map partial.
- A SUM/MIN/MAX value type outside bigint/int/smallint/tinyint/float/double/decimal (MIN/MAX also admit
  strings, DATE, TIME, BOOLEAN and timestamps), or an AVG value type outside
  bigint/int/smallint/tinyint/float/double/decimal.
- A `COUNT(DISTINCT)` value type outside bigint/int/smallint/tinyint/float/double/string/decimal/
  BOOLEAN/DATE/TIME/TIMESTAMP/TIMESTAMP_LTZ/ARRAY/intervals, or a `SUM(DISTINCT)` value outside bigint/int/smallint/tinyint/FLOAT/DOUBLE/DECIMAL;
  DECIMAL sum partials with a different scale/precision other than 38;
  `AVG(DISTINCT)` outside integers/FLOAT/DOUBLE/DECIMAL or with incorrectly widened partials;
  `MIN`/`MAX` with a retained physical `DISTINCT` flag (Flink normally removes it).
- A partial whose declared type differs from what the native side emits — defensive only, not
  reachable from Flink's own planner.
- A retracting input outside COUNT/AVG/SUM0/numeric SUM, scalar SINGLE_VALUE, typed MIN/MAX,
  ordinary/DISTINCT LISTAGG and JSON_OBJECTAGG.
  DISTINCT COUNT admits the scalar, ARRAY and interval types above; DISTINCT SUM/AVG admits integers,
  FLOAT/DOUBLE and DECIMAL precision through 38 with the declared partial layout.

**Global group aggregate (two-phase merge) only:**

- Any merge other than SUM/MIN/MAX/COUNT/AVG/SUM0, the typed SINGLE_VALUE split, or
  ordinary/DISTINCT LISTAGG with its append (separator, prefix) pair or retracting list partial, or
  JSON_OBJECTAGG with its map partial.
- A partial column outside bigint/int/smallint/tinyint/float/double/decimal, except the typed
  MIN/MAX, SINGLE_VALUE, LISTAGG and JSON_OBJECTAGG partials described above.
- An AVG whose partial pair isn't `(bigint, bigint)` for an integer average, `(double, bigint)` for
  float/double, or `(decimal(38, s), bigint)` for decimal.
- A distinct merge outside the local half's `COUNT`/`SUM`/`AVG`/`LISTAGG` scope.
- A retracting merge outside COUNT/AVG/SUM0/numeric SUM, scalar SINGLE_VALUE, typed MIN/MAX,
  ordinary/DISTINCT LISTAGG and JSON_OBJECTAGG.
  Scalar DISTINCT COUNT and numeric DISTINCT SUM/AVG use signed membership transitions;
  floating/wide-decimal arithmetic retains the released shared-view iteration order.
  Mapped MIN/MAX preserve cached extrema independently of expiring map entries.
  The `count1` partial drives per-key liveness.
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
Retracting two-phase COUNT DISTINCT also admits these scalar types. SUM/AVG DISTINCT admit
integer, FLOAT/DOUBLE and DECIMAL precision through 38 inputs. Local views retain negative
multiplicities instead of dropping a deletion that precedes its insertion in a bundle.
Global merges fold or retract a value only when its signed count crosses into or out of
positive membership, and retain negative entries through snapshots. Hidden count1 fields
precede the native views. A SUM_RETRACT DISTINCT partial also carries Flink's separate
non-null count, while SUM0 DISTINCT keeps one field. Floating and wide-decimal views preserve
shared-filter key order through deletion, reinsertion, resize, collision trees and transport.
Flink's multi-counter global merge retains zero-count keys; the native temporary map does too,
because removing those positions changes later floating and decimal folds. A single-counter
merge removes a key at zero. All filtered counters update before deciding local key removal.

The expanded matrix passes 89 checks per released Flink line: seventeen scalar shapes,
shared and independent filters, NULL predicates, hidden counters, group recreation, twelve
repeated memory/native-RocksDB recoveries, finite cancellation and 96-key collision traces
at four bundle sizes. The native suite passes 836 checks (two diagnostics ignored), including
592 JDK-qualified map mutations. Both native phases must consume rows. Overflow recovery
cuts align with mini-batches so the test preserves the arithmetic reduction boundaries.
Released Flink rejects the DECIMAL(38,38) SUM0 initializer; two 1.18 high-scale collision
profiles instead assert its matching DecimalData null-unboxing failure. The unchanged upstream
`AggregateITCase.testDistinctWithMultiFilter` passes seven fixture invocations on Flink 2.2 and six on 1.18, all requiring native work.

Release+mimalloc/JDK 17 complete jobs with one million rows, 128 inner/16 outer keys,
p=1, memory state, 2 GiB heap, 512-row bundles, two warmups and three interleaved trials
measure the updating MAX-to-DISTINCT-SUM/COUNT/AVG cascade with a filtered DISTINCT SUM:

| Release/type | Stock seconds | Native seconds |
| --- | ---: | ---: |
| 2.2.1 BIGINT | 0.607 | 0.390 |
| 2.2.1 DECIMAL(12,2) | 0.741 | 0.417 |
| 1.18.1 BIGINT | 0.615 | 0.391 |
| 1.18.1 DECIMAL(12,2) | 0.732 | 0.428 |

The actual canonical `d13e9dc4` planner fallback takes 0.579/0.702 s for BIGINT/DECIMAL
(paired stock 0.576/0.700 s), with the affected native aggregate route absent. Candidate
jobs retain sources, both transposes, JNI, both native stages and rowwise sinks; every
candidate requires actual native input. The [raw artifact](../benchmarks/results/retracting-distinct-partials-2026-10-21.json)
retains all warmups, observations, ranges and hashes. These profiles need no optimization passes.

The floating/wide extension also wins all eight one-million-row complete-job profiles on
both releases, at **1.53–1.99x** stock throughput. These retain the same query, resource limits, warmups, transposes and native-work
checks as the exact profiles above. The
[ordered retraction artifact](../benchmarks/results/retracting-ordered-distinct-partials-2026-10-21.json)
records every observation, actual previous-planner controls, source/DSO hashes and trial ranges.
No performance optimization passes were needed for these winning profiles.

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

#### Floating DISTINCT average comparison

Single-phase FLOAT/DOUBLE DISTINCT averages reuse the typed membership set and widened
sum/count state. Eighteen SQL checks on each released Flink line cover duplicates, filters,
retractions, group recreation, global/empty output, NaN payloads, infinities, signed zero,
and two completed-checkpoint restores on memory and RocksDB. Sixteen Criterion fixtures
exercise the production floating DISTINCT average at 256/4096 rows, one/64 keys and
nullable/non-null input.

The October 18, 2026 release+mimalloc comparison uses Flink 2.2.1, JDK 17, a 2 GiB heap,
parallelism one and two million runtime rows on an Intel i7-12650H/Linux WSL2. One query
computes FLOAT and DOUBLE DISTINCT averages; NULLs occur every seventh row. The source
generates byte/long inputs, with both casts retained in the measured SQL. Each engine gets
two warmups and five measured jobs, alternating engine order. Native jobs assert both
transposes and consumed native group-aggregate records. The row source, blackhole sink,
JNI, membership and changelog costs remain included.

| Keys | Stock median (s) | Previous origin (s) | Native median (s) | Stock/native | Origin/native |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 0.894357 | 0.871940 | 0.787801 | 1.135x | 1.107x |
| 4,096 | 2.909029 | 2.744675 | 1.674566 | 1.737x | 1.639x |

At 64 keys, stock samples range 0.867606–1.024014 s and native samples
0.775587–0.884581 s. At 4,096 keys they range 2.733308–3.095016 s and
1.634739–1.715133 s respectively. Original-origin measurements use the identical fixture
with native planning enabled and its aggregate fallback asserted. Their separate JVM has
paired stock controls, retained alongside every warmup/sample in the
[raw artifact](../benchmarks/results/floating-distinct-avg-2026-10-18.json). These are
complete jobs including startup; the smaller difference at 64 keys is workload-specific.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=DistinctAggregateBenchmark -Ddistinct.floating=true \
  -Ddistinct.rows=2000000 -Ddistinct.keys=64 -Ddistinct.warmup=2 \
  -Ddistinct.runs=5 -Dsf.extraJvmArgs=-Xmx2g
# Repeat with distinct.keys=4096. Original origin additionally uses distinct.expectNative=false.
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

### Typed DISTINCT first/last

Runtime-source SQL checks extend the same membership and arrival-order semantics to
integer widths, FLOAT/DOUBLE, DECIMAL, CHAR, DATE/TIME, BOOLEAN, TIMESTAMP and TIMESTAMP_LTZ.
Filtered and unfiltered instances are compared independently. Floating sources include
both zero signs, different NaN payloads and infinities, duplicate removals and group
recreation. Memory and RocksDB recovery fixtures cross two restores before completing
membership removal and reinsertion; execution metrics require the native grouped operator.
Released-host capability skips apply to CHAR and temporal first/last on Flink 1.18.
The focused grouped-value, floating DISTINCT, state-recovery and SINGLE_VALUE DISTINCT
selection, including temporal COUNT DISTINCT partial/recovery checks, passes 171 checks on
Flink 2.2.1 and 135 with 36 host-capability skips on 1.18.1.
The native grouped regression selection passes 128 checks.

The `distinct_ordered_values` release Criterion suite checks 256 combinations against an
independent unique-arrival-order and multiplicity oracle, including exact selected floating
payloads. It covers 64/4,096 rows, one/64 keys, four/128 source values, append/delete batches,
NULL-only groups, narrow/wide decimals, nanosecond timestamps and 8/256-byte strings.
Thirty-two mixed COUNT DISTINCT/COUNT(*) cases additionally verify every ordered changelog
row against an independent preimage/postimage oracle, including NaNs and signed zero.
Fixture construction and retract seeding stay outside timing. Allocation requests and
Arrow sharing accompany every case; they do not measure copied bytes or whole-job speed.

`GroupedValueBenchmark` accepts the existing scalar `grouped.value.types` names with
`-Dgrouped.value.distinct=true`. Optional `grouped.value.keys` and
`grouped.value.cardinality` change the key count and value modulus; defaults remain 64
and 1,024. TINYINT, DATE and BOOLEAN retain their bounded source domains. With
`-Dgrouped.value.retract=true`, the first three quarters of rows insert and the final
quarter delete the identical first-quarter records. Retractions require single-phase
first/last. With DISTINCT first/last, `-Dgrouped.value.mixed=true` adds COUNT(DISTINCT v);
`-Dgrouped.value.recordCount=true` additionally emits COUNT(*), exercising a result change
on every contributing row. The source remains rowwise and the measured job retains both transposes,
JNI/state work and the rowwise sink. Candidate, stock and preceding-StreamFusion runs
use identical fixtures and resources.

The final release+mimalloc comparisons use JDK 17, Flink 2.2.1, parallelism 1, two warmups
and five measured trials on Intel Core i7-12650H/Linux/WSL. Pure two-million-row BIGINT,
DOUBLE, wide DECIMAL, TIMESTAMP and STRING queries improve **1.218–1.462x** over paired
stock Flink. DATE/DOUBLE/STRING mixed COUNT DISTINCT queries, with and without COUNT(*),
improve **1.062–1.435x**. Retracting DATE/TIMESTAMP/STRING jobs with 256 keys, an 8,192-value
modulus and 1 GiB heap improve **2.107–2.251x**; the default jobs use 64 keys, a 1,024-value
modulus and 2 GiB heap. Matched preceding-StreamFusion controls improve **1.059–2.065x**
across these two-million-row profiles. Those controls reuse retained runs of the identical
fixture and resources; their full ranges remain explicit.

At 500,000 rows, the three measured BIGINT/DOUBLE/STRING jobs improve 1.185–1.535x over
stock, with startup/JIT variability retained. A released-1.18.1 DOUBLE job improves 1.207x
at two million rows. These are representative workload results rather than a universal
speed guarantee. The [complete measurement artifact](../benchmarks/results/typed-distinct-ordered-values-2026-10-19.json)
retains 252 new jobs, 196 reused preceding-control jobs, all trials/configurations, source/DSO
hashes and 256 native allocation records. It links the initial type/mixed-query losses,
CPU diagnosis and DATE confirmation losses; profiled timings are excluded from speed claims.

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

### Scalar SINGLE_VALUE partials

Insert-only splits also admit the single-phase scalar value types. Pure local groups
retain Arrow key/value rows and compact counts; fresh singleton global partials reuse
their Arrow columns for output while keeping the existing keyed checkpoint state.
Mixed groups retain the general accumulator. Timestamp state uses an exact inline
nanosecond carrier internally while Arrow and checkpoint fields keep their declared types.

The selected SQL/operator matrix passes 148 checks on Flink 2.2.1 and 129 with 19 host
planning skips on 1.18.1. It includes NULL/composite keys, physical splits, local/global
cardinality failures, two memory/RocksDB restores, mixed aggregates and direct runtime
TIME(3) values on both releases. Native coverage includes 104 group regressions and
218 release Criterion fixtures.

`GroupedValueBenchmark` accepts `-Dgrouped.value.mixed=true` with SINGLE_VALUE to add
AVG and COUNT DISTINCT, and `-Dgrouped.value.types=DECIMAL_WIDE` for actual 20-digit
DECIMAL(20,3) values. Two-million-row scalar jobs improve 1.30–1.53x over stock Flink
and 1.35–1.47x over the preceding StreamFusion revision in the measured configurations.
The [partial-buffer optimization](../optimizations/aggregate-specialization-fast-paths.md#single_value-partials-retain-arrow-rows-through-split-aggregation)
records mixed/scalar/released-1.18 comparisons, variability, the initial INT slowdown and
its longer confirmation, retained intermediate losses, ownership and checkpoint contracts.
Both transposes and the runtime row source/blackhole sink remain measured.

### Retracting SUM partial validation and release comparison

The six-width SQL matrix checks complete ordered changelogs at logical bundle sizes 1/2/4,
including mixed SUM/COUNT/AVG/FILTER, NULL keys/values, zero, negative record counts,
zero-count sum adjustments, deletion/recreation, and signed overflow. A 2,688-row single-key
case spans multiple Arrow batches with deterministic count-trigger cadence; independent keys
can alter the global count-trigger cadence when local bundles enumerate keys in a different order.
The later numeric-partial completion below adds DECIMAL SUM_RETRACT and exact two-level merging;
the preceding INT/BIGINT measurements retain their historical library boundary. Operator tests cover
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

## Floating SUM0 admission

FLOAT/DOUBLE SUM0 is enabled for the admitted grouped and local/global shapes, including
retractions and FILTER. The released Table API exposes `expression.sum0()`; plain SQL
`SUM0(...)` is not registered. Internal planner SUM0 calls use the same native accumulator.
The existing DISTINCT and invalid DECIMAL initializer restrictions remain.

[Validation and timing evidence](../benchmarks/results/floating-sum-zero-2026-10-19.json)
retains 92 distinct selected cases on 2.2.1 across corrected runs and 91 passes plus one
expected session-TVF skip on 1.18.1. Eight added checks per release fail twice after completed
checkpoints on heap/native RocksDB, in one/two phases. They cover NULL prefixes, rounded
cancellation, retractions and zero-net-count local bundles, requiring actual native execution.
Ten native SUM0 tests preserve initialization, rounding, IEEE behavior and restored state;
24 window and 16 grouped release Criterion fixtures pass (12 and eight floating cases added).

The small admission benchmark measures only the production SUM0 accumulator and final value
consumption. Stock uses Flink's released generated Java handler on prebuilt BinaryRowData;
StreamFusion starts inside Rust on matching prebuilt Arrow. At 1,048,576 values with NULLs,
Flink 2.2 FLOAT measures 9.97 ms versus 2.64 ms Rust, and DOUBLE 10.27 ms versus 2.20 ms.
Both output bit patterns match exactly. All warmups and trials, smaller inputs and Flink 1.18
controls are retained. These function measurements do not claim faster complete GROUP BY or
window jobs; grouping, state-backend I/O and perimeter conversions are outside this boundary.

The [default-admission validation](../benchmarks/results/jvm-feature-defaults-2026-10-19.json)
records the integrated UDAF/nested bridge on both released Flink lines, including explicit
false fallback. UDF overhead is accepted; no standalone acceleration is claimed.

### Floating retracting SUM partials

FLOAT and DOUBLE SUM now share the local/global retracting path with integer SUM.
The sum and contribution count remain separate partials. A zero record count inside a
logical bundle does not erase the running sum: only the completed bundle can remove a
group. This preserves floating cancellation residues and count-zero sum adjustments.
Positive-TTL mini-batches read and refresh retained state at the flush clock, including
the SINGLE_VALUE specialization; pending Arrow input is reserved until that merge.

On JDK 17, release+mimalloc, one million input rows, parallelism 1, memory state and a
2 GiB heap, the existing MAX-to-SUM/COUNT/AVG/FILTER cascade uses 128 inner keys,
16 outer keys and 512-row logical bundles. Both native local/global pairs must consume
rows. Two warmups and three alternating measured trials retain the runtime RowData
source, both transposes, JNI, state and rowwise blackhole sink.

| Flink / type | Stock median, s | Native median, s | Stock/native |
| --- | ---: | ---: | ---: |
| 2.2.1 / FLOAT | 0.528 | 0.374 | 1.41× |
| 2.2.1 / DOUBLE | 0.493 | 0.286 | 1.72× |
| 1.18.1 / FLOAT | 0.483 | 0.394 | 1.23× |
| 1.18.1 / DOUBLE | 0.452 | 0.276 | 1.64× |

The actual previous canonical revision `d13e9dc4` takes stock fallback for these queries:
paired FLOAT medians are 0.515 s stock / 0.493 s configured StreamFusion, and DOUBLE
0.489 s / 0.491 s. These separate invocations establish the previous route and measured
baseline; timings vary between invocations. No optimization pass was needed for these
winning cases. [Every warmup, trial and DSO hash](../benchmarks/results/floating-retracting-sum-2026-10-20.json)
is retained. The timed v1 build precedes a SINGLE_VALUE-only TTL dispatch correction;
validation uses the corrected v2 build. The SUM benchmark has TTL disabled, so that
correction does not change its execution path. Each released line passes 20 complete
numeric changelog checks, 22 controlled retention/recovery checks and 26 focused
SINGLE_VALUE checks. The later decimal-partial completion below extends this historical
FLOAT/DOUBLE implementation and corrects rounding-sensitive two-level reductions.


### Retracting SINGLE_VALUE partials

The local/global value-and-INT-count layout also accepts updating inputs. The existing native
merge applies Flink's signed count rule, preserves NULL values, removes/recreates keyed groups
and keeps a keyless global aggregate's empty result. This is planner admission over the existing
scalar state. Released 1.18.1 and 2.2.1 factories reject binary and collection/ROW SINGLE_VALUE;
those type roots do not establish missing native coverage. SQL DISTINCT lowering and the
released SINGLE_VALUE FILTER rejection remain as described above.

Each release passes 36 complete changelog cases across nine scalar families, nullable keys,
NULL-producing projections and logical bundles of one/three; two interleaved materialized
relation checks; two keyless empty-result checks; and four memory/RocksDB cases that recover
twice. Per-key cycles and recovery cuts align both mini-batch stages for raw changelog
comparisons. Interleaved keys can change the count-trigger cadence through local map order,
so their deterministic materialized relation is compared separately. Every admitted test
requires native local and global input consumption.

`RetractingSumPartialsBenchmark -Dsumretract.singleValue=true` selects an updating
MAX-to-SINGLE_VALUE/COUNT/AVG/filtered-SUM cascade. One million DOUBLE rows, 128 inner and
128 outer keys, parallelism one, memory state, a 2 GiB heap, 512-row mini-batches, JDK 17,
release+mimalloc, two warmups and three alternating measurements retain both conversions,
production JNI/state, the runtime RowData source and rowwise blackhole sink. Metrics require
both native local/global pairs to consume input.

| Flink | Stock median, s | Native median, s | Stock/native |
| --- | ---: | ---: | ---: |
| 2.2.1 | 0.541 | 0.382 | 1.42× |
| 1.18.1 | 0.541 | 0.367 | 1.47× |

The actual previous canonical `d13e9dc4` takes stock fallback: paired configured/stock medians
are 0.535/0.536 seconds on 2.2.1. No optimization pass was needed for this winning workload.
[All trials, configuration, library hashes and validation limits](../benchmarks/results/retracting-single-value-2026-10-21.json)
are retained; these measurements do not establish a speedup for every scalar type or backend.

### Retracting MIN/MAX partials

Two-phase MIN/MAX now transports each retracting accumulator as one Arrow struct containing
its selected value, positive-entry count and signed value-to-count map. Negative local counts
must survive a bundle: a deletion can remove a value that was accumulated by an earlier bundle.
The global first combines each key's heap partials, then merges that buffer into retained state.
NULLs are excluded and FILTER applies before the local fold. Cached extrema and map-size counters
follow the released accumulator independently of the signed map. In particular, a replacement
scan includes negative entries, matching Flink even when its cached winner has a negative count.
A monotonicity-exempt scalar extreme can share a partial row with a mapped extreme; its local
fold ignores retractions exactly as Flink does. This internal accumulator layout does not add
new SQL STRUCTURED payload support.

The 55 checks per released Flink line cover INT, BIGINT, FLOAT, DOUBLE, DECIMAL, STRING, DATE, TIME,
TIMESTAMP(9) and BOOLEAN at bundle sizes 1/2/4, preserving complete output changelogs and
types. Repeated restores cover both memory and native RocksDB state. Another 32 controlled-clock
operator checks per release cover BIGINT, DOUBLE, DECIMAL and STRING, append/scalar controls,
mapped retractions, independent entry/group expiry, flush-time expiry and two successive restores.
The full native suite passes 878 checks (two ignored); suite-agent checks pass 50 (one skip), and
35 route-audit checks pass.

Positive retention expires map entries independently of the cached extreme and map-size counter.
An expired entry read behaves as absent; its removal does not rebuild the cached result. Successful
map writes refresh that entry alone, and a replacement scan removes expired entries before choosing
a key. Canceled heap partials do not write to retained entries. The cached fields, signed counts
and original deadlines use native raw keyed snapshots on both backends. Older raw snapshots
rebuild their previous cached value/count and seed entry deadlines from the saved group write,
or restore time when no old group timestamp exists. The direct per-element RocksDB codec remains
in use for ordinary scalar extrema; mapped extrema use the raw keyed representation.

Unchanged released upstream checks pass 21 invocations on 2.2: MIN/MAX retraction-condition and
DISTINCT-with-retraction fixtures. Flink 1.18 passes twelve decimal MIN/MAX and DISTINCT-with-retraction
invocations. Admitted split variants require consumed native local and global work. The 1.18 scope
uses its actual released decimal fixture rather than nonexistent retraction-condition methods;
its DISTINCT display names are corrected as well. Independent clock semantics are established
by the focused operator checks above.

The earlier zero-retention million-row updating MAX→MIN/COUNT/AVG/MAX cascade uses 128 inner and 16 outer keys,
parallelism 1, memory state, 512-row bundles, a 2 GiB Java heap, two warmups and three alternating
measurements. Both transposes, JNI, state and job completion remain timed. Metrics require two
native local/global pairs to consume rows. After the shared temporary-sum correction below, DOUBLE medians are 0.554 s stock versus 0.368 s
native on Flink 2.2.1 (1.51×), and 0.518 s versus 0.379 s on Flink 1.18.1 (1.37×). Actual canonical `d13e9dc4` falls back, with 0.543 s enabled versus 0.512 s stock in its separate run.
These are complete-job measurements for this cascade, not isolated extrema throughput claims. Raw trials, the actual
previous fallback and library hashes are retained in
[the partial-map artifact](../benchmarks/results/retracting-extrema-partials-2026-10-21.json).

The retained implementation has a separate
[complete-job artifact](../benchmarks/results/mapped-extreme-retention-2026-10-21.json).
Release+mimalloc/JDK 17 jobs use p=1, 2 GiB heap, one-hour retention, 512-row bundles,
two warmups and three alternating measured runs. Both native local/global pairs must consume
input. The numeric outer query computes MIN/COUNT/AVG/MAX; the string query computes
MIN/COUNT/MAX/COUNT(*). Inner MAX sees ascending values, while inner MIN sees descending
values, so both pipelines exercise updating results. Every seventh value is NULL.

| Profile; rows, inner/outer keys | Flink 2.2 stock/native median, s | Flink 1.18 stock/native median, s |
| --- | ---: | ---: |
| INT MAX→MIN, memory; 1M, 128/16 | 0.549 / 0.374 | 0.521 / 0.371 |
| DOUBLE MIN→MAX, memory; 1M, 4096/64 | 2.165 / 1.830 | 2.073 / 1.794 |
| STRING MAX→MIN, memory; 262144, 1024/64 | 0.840 / 0.744 | 0.831 / 0.705 |
| INT MIN→MAX, RocksDB; 262144, 4096/64 | 4.017 / 1.006 | stock run time-boxed |

All seven completed profiles beat stock; no optimization pass was needed. Actual canonical
`d13e9dc4` takes planner fallback at 0.484/2.120/0.818/3.350 s on those four 2.2 profiles,
with paired stock medians 0.508/2.113/0.778/3.158 s. The artifact preserves all trials,
ranges, source/DSO hashes and consumed-native evidence.

Flink 1.18's stock RocksDB warmup exceeded the 180-second case budget. A smaller matched
32,768-row/512-inner-key attempt also exhausted its 90-second budget before all three measured
trials completed. At the second timeout its upstream global aggregate was in `RocksDB.put`
through TTL value state. Both incomplete attempts and the worker-stack excerpt are retained;
they contribute no median or RocksDB speedup claim on 1.18.

### Nullable sums and global temporary partials

Retracting local SUM now emits its nullable running sum and signed non-null count separately.
It retains negative zero and a sum adjustment when the net contribution count is zero. DECIMAL
widens the partial to precision 38 at the input scale, resets after arithmetic overflow on the next
non-null accumulate/retract, and ignores a NULL partial during merge while still merging its count.
AVG preserves its distinct, sticky decimal overflow semantics.

For each logical global mini-batch, SUM/AVG first combine partial sums in temporary per-key
accumulators, then merge that result into retained state. Folding each incoming partial directly
into retained state changes the point of decimal overflow and floating rounding. The temporary
buffers are memory-accounted, flushed before checkpointing, and used by memory and RocksDB state.
Positive TTL still reads retained state at the logical flush clock.

Each released Flink line passes 26 new checks: four decimal shapes (12/20/38 digits, including
scale 38) at three bundle sizes; four repeated decimal recoveries; six FLOAT/DOUBLE rounding,
signed-zero and non-finite fixtures; and four repeated floating recoveries. They compare complete
changelogs and result types and require both native phases to consume rows. The prior 19 numeric
changelog and ten direct state/recovery/rescaling/TTL checks also pass on each line. The full
release+mimalloc native suite passes 825 tests (two ignored), the suite agent passes 50
(one skip), and 35 route-audit checks pass.

The unchanged upstream `testPrecisionForSumWithRetractAggregationOnDecimal` now requires
native local/global work in its two-phase fixture and native grouping in its single-phase fixture,
with the established Flink 1.18 changelog-state fallback retained. Runtime-route scope is updated;
actual unchanged upstream execution and required CI remain pending the consolidated PR.

The release+mimalloc million-row MAX→SUM/COUNT/AVG/FILTER cascade retains both transposes,
JNI, temporary/persistent state and rowwise completion. It uses 128 inner/16 outer keys,
parallelism 1, memory state, 512-row bundles, a 2 GiB heap, two warmups and three alternating trials.
Consumed-work metrics require two native local/global pairs.

| Type | Flink 2.2 stock/native median, s | Stock/native | Flink 1.18 stock/native median, s | Stock/native |
| --- | ---: | ---: | ---: | ---: |
| FLOAT | 0.588 / 0.384 | 1.53× | 0.478 / 0.374 | 1.28× |
| DOUBLE | 0.510 / 0.280 | 1.82× | 0.459 / 0.287 | 1.60× |
| DECIMAL(20,3) | 0.704 / 0.432 | 1.63× | 0.631 / 0.339 | 1.86× |

Actual canonical `d13e9dc4` takes decimal fallback: 0.674 s enabled versus 0.660 s stock in its separate run.
All measured cases win their complete-job comparison; no optimization pass was needed.
Warmups, trials, variability, exact boundaries and hashes are retained in
[the numeric-partial artifact](../benchmarks/results/retracting-decimal-partials-2026-10-21.json).
Higher decimal scales and RocksDB have parity/recovery coverage, without corresponding throughput claims.

### Generic FIRST/LAST values

The native accumulator binds generic value schemas from the input Arrow field and from typed
checkpoint fields on restore. It retains nested fields, nullability, element representation and
row kinds through selection and repeated recovery. Constructed complex values can cross a
projection and exchange into a verified FIRST/LAST consumer; the enclosing aggregate retains
its representation metadata. Other representation-dependent operator and columnar-sink gates
remain in place. These are existing ARRAY/MAP/ROW bridge types, not new SQL STRUCTURED support.

The supported release matters: Flink 2.2 uses generic FIRST/LAST factories, while Flink 1.18
rejects these extra types. The planner also rejects DISTINCT binary, MAP, ROW and MULTISET on
both lines. The SQL checks preserve those planning failures rather than inventing new dialects.
Positive-retention ordinary retracting first/last now retain independent value/order maps.
DISTINCT first/last also admit positive retention, with shared filtered membership kept
separate from cached selections and inner arrival-order views.

The unchanged upstream Table API `testGroupAggregateWithExpression` now requires consumed
native grouping in both state-backend fixtures, with Flink 1.18's established changelog-state
fallback retained. It protects the mixed scalar FIRST/LAST context; dedicated SQL fixtures
establish the new value types. Actual unchanged upstream runtime validation is reserved for
consolidated delivery.


### Independent FIRST/LAST retention

Controlled-clock operator comparisons against released Flink cover BIGINT/STRING,
immediate/mini-batch and memory/native-RocksDB state: sixteen cases on each line, including
two successive restores, independent entry deadlines, stale cached selections, duplicate
removals, NULL-only group refresh and whole-group expiry. The host fixture separates real
arrival-clock samples while controlling the TTL provider, so bucket collisions cannot make
this deterministic deadline test flaky. Native unit checks separately exercise shared-clock
buckets and refresh of surviving values. Thirteen typed SQL profiles run each immediate and
requested TWO_PHASE choice with positive retention, FILTER and consumed native work. Flink
1.18 skips its four unavailable FIRST/LAST factories. FIRST/LAST has no released merge
implementation, so its requested TWO_PHASE shape remains single phase. The native suite
passes 839 checks, with two diagnostics ignored.

The [retention measurements](../benchmarks/results/ordered-value-retention-2026-10-21.json)
retain complete-job trial boundaries, source/DSO hashes, previous-planner controls and the
aborted large stock warmup diagnostic. Both engines receive identical rows, groups, NULLs,
changelog and resources; row/Arrow conversions, JNI and the rowwise sink remain timed.

The broader selected suite verifies 272 cases on Flink 2.2; Flink 1.18 passes 212 and skips
60 explicit released-host capability cases. This includes eighteen new complex retained SQL
profiles and eight new complex recoveries on 2.2. Actual positive STATE_TTL hints require
native execution. The unchanged upstream mixed FIRST/LAST/LISTAGG Table API fixture passes
two backend invocations per release with consumed native aggregate work; its append input
protects shared integration rather than establishing the new retention semantics alone.

At 131,072 rows, 1,024 groups, cardinality 4,096, one-eighth NULLs, one-hour retention,
p=1, memory state and 2 GiB heap, two warmups and three alternating measured runs give:

| Type | Flink 2.2 stock/native median, s | Flink 1.18 stock/native median, s |
| --- | ---: | ---: |
| BIGINT | 2.042 / 0.165 | 2.039 / 0.176 |
| STRING | 2.711 / 0.219 | 2.656 / 0.217 |
| DOUBLE | 2.097 / 0.158 | 2.053 / 0.158 |
| ARRAY | 2.627 / 0.356 | released factory unavailable |

The actual `d13e9dc4` planner fallback takes 1.975/2.612/2.048/2.701 s for those four
2.2 profiles (paired stock 1.908/2.675/1.985/2.664 s). These specific native jobs win;
no optimization passes were needed. An initial million-row/64-group stock warmup was
terminated after its FIRST_VALUE retraction scan exceeded 80 seconds; it contributes no
measured trial. Flink samples arrival clock inside the aggregate while native samples once
per Arrow call, so clock-bucket density influences this retention-specific result. The
numbers do not establish general FIRST/LAST speedups.


### Retracting LISTAGG entry retention

The released host's ListViews expire individual occurrences and pending retractions, while
successful removal refreshes the complete surviving value list. The native state retains those
two timestamp sequences alongside its existing versioned string accumulator, keeping fresh
TTL-off snapshots unchanged. The focused selection passes 51 SQL/operator checks on each
release: sixteen new default/literal separator, immediate/mini-batch, heap/native-RocksDB and
two-restore cases, plus the retained append/split controls. Two native tests independently check
inclusive deadlines, survivor refresh, unmatched retraction expiry and snapshot clocks. The
full native suite passes 841 tests, with two diagnostics ignored; the suite agent passes 50
with one skip and the route-scope checks pass 35.

The unchanged upstream `AggregateITCase.testListAggWithRetraction` passes seven invocations
on Flink 2.2 and six on 1.18 with explicit routing contracts. Single-phase variants require
consumed native grouping; two-phase variants now require positive local and global work
with their declared retracting partial layout. The entry-retention behavior is qualified by
the focused controlled-clock tests.
[Raw complete-job measurements](../benchmarks/results/retracting-listagg-retention-2026-10-21.json)
retain warmups, trial ranges, source/DSO hashes and actual previous-planner controls.

Release+mimalloc/JDK 17 complete jobs use p=1, 2 GiB heap, one-hour retention,
1,024-row transposes, two warmups and three alternating measured trials. The source cycles
INSERT/INSERT/DELETE per key and supplies NULL every seventh value. Both transposes, JNI,
string materialization and the rowwise blackhole sink remain timed.

| Rows / keys / string width | Flink 2.2 stock/native median, s | Flink 1.18 stock/native median, s |
| --- | ---: | ---: |
| 1,000,000 / 65,536 / 16 bytes | 6.685 / 0.789 | 8.125 / 0.784 |
| 262,144 / 32,768 / 128 bytes | 1.728 / 0.402 | 1.899 / 0.340 |

The actual canonical `d13e9dc4` fallback takes 6.576/1.561 s on those two 2.2
profiles (paired stock 6.718/1.583 s). Every candidate requires consumed native work.
All four profiles win; no slow-case optimization passes or performance opt-ins were needed.

### Shared DISTINCT retention comparison

The single-phase retention checks pass 108 SQL/operator cases on each released Flink line:
68 typed append/retract/mini-batch combinations, 24 typed cases with two restores on memory
and native RocksDB, and 16 controlled-clock operator comparisons. Native tests additionally
pin cached results after entry expiry, disabled retention after restore, legacy snapshot
migration and timestamp keys spanning the complete millisecond range. The unchanged released
`AggregateITCase#testDistinctWithMultiFilter` consumes native grouping in all seven 2.2 and
six 1.18 invocations; split variants also consume the existing native local stage.

The [raw measurement and profile artifact](../benchmarks/results/shared-distinct-retention-2026-10-21.json)
records one million rows, 64 keys, p=1, a 2 GiB heap, one-hour retention, single-phase
execution, two warmups and three alternating measured complete jobs. Source, both transposes,
JNI and the rowwise blackhole sink remain timed.

| Query profile | Flink 2.2 stock/native median, s | Flink 1.18 stock/native median, s |
| --- | ---: | ---: |
| BOOLEAN/timestamp COUNT DISTINCT and decimal SUM DISTINCT | 1.425 / 0.846 | 1.406 / 0.837 |
| FLOAT/DOUBLE SUM and AVG DISTINCT | 4.324 / 0.875 | 4.291 / 0.892 |

Exactly two measured optimization passes reduced the first native profile from 7.773 to
1.652 to 0.846 s. CPU profiles identified timestamp hash collisions, then temporary Arrow
scalar reconstruction; direct component hashing and compact `i128` clock keys remove those
costs. The actual canonical previous Java runtime, with its unchanged production-native
sources, takes 0.553/0.901 s on the two 2.2 profiles (paired stock 1.425/4.249 s). Its existing
release DSO uses the system allocator; the candidate uses release+mimalloc. The typed query
adds independently retained entry state to the previous whole-group lifetime. It still beats
stock, but this no-expiry workload does not claim an improvement over the old native path.
The final DSO adds only old-snapshot migration to the measured second-pass update path.

### Global DISTINCT entry retention

Both released Flink lines pass 116 local/global retention checks: 68 typed append/retract
profiles with one- and four-row bundles, 24 typed jobs with two memory/native-RocksDB restores,
and 24 controlled-clock operator comparisons, including two successive restores. The latter
compare operators extracted from actual released plans and separately advance partial arrival
and global flush clocks. BIGINT, DOUBLE and wide DECIMAL cover exact and ordered merge paths.
The complete native suite passes 854 tests, with two diagnostics ignored. Native regression
checks additionally preserve old entry deadlines when the first restored bundle contains only
NULL arguments. Existing upstream multi-filter DISTINCT contracts remain unchanged and consume
native grouping in seven released 2.2 and six 1.18 invocations.

The [complete-job artifact](../benchmarks/results/global-distinct-retention-2026-10-21.json)
records one million rows, 64 keys, p=1, 2 GiB heap, one-hour TTL, 1,024-row mini-batches,
two warmups and three alternating measured runs. Source, both transposes, JNI and the
rowwise blackhole sink remain timed. No builds or tests compete with measurements.

| Query profile | Flink 2.2 stock/native median, s | Flink 1.18 stock/native median, s |
| --- | ---: | ---: |
| BOOLEAN/timestamp COUNT DISTINCT and DECIMAL(38,2) SUM DISTINCT | 1.222 / 0.992 | 1.149 / 1.003 |
| FLOAT/DOUBLE SUM and AVG DISTINCT | 3.552 / 1.232 | 3.574 / 1.216 |

The initial typed native job took 1.348 s versus stock 1.199 s. Its CPU profile found
422 compact-key and 317 scalar-reconstruction samples among 1,257 native group-update
samples; these inclusive counts overlap. One measured optimization pass reads timestamp
components from the already-bound view reader and reduces the native job to 0.992 s.
All four final profiles beat stock, so no second optimization pass was needed.

Actual canonical `d13e9dc4` takes 0.768 s for the already-native typed query and 3.646 s
for its floating fallback (paired stock 1.150/3.614 s). The earlier native path has only
whole-group retention and uses a release/system-allocator DSO; the candidate adds entry
clocks and uses release+mimalloc. This no-expiry workload does not claim an improvement
over that faster previous typed path. Controlled-clock tests qualify the new semantics.

### Independent DISTINCT FIRST/LAST retention

Positive retention admits DISTINCT FIRST/LAST across the released scalar types and Flink 2.2
ARRAY/interval types. The shared outer membership follows append bitsets or signed retracting
counters; it expires independently of the cached selected scalar. Retracting selections also
keep independent value-to-order and order-to-value deadlines. A newly reintroduced membership
can therefore fold into an older cached selection, matching released Flink rather than
reconstructing the answer from the surviving membership set.

Raw keyed checkpoints preserve membership, cached selections and inner ordered views separately.
They record the append/retract mode so disabling TTL after restore keeps the same accumulator
semantics. Older zero-retention snapshots migrate their ordered queue before enabling retained
views. Memory and native RocksDB share this raw representation. FIRST/LAST still have no
released merge implementation; asking for TWO_PHASE produces the supported single-phase plan.

The broad SQL/operator selection passes 400 checks on Flink 2.2. Flink 1.18 passes 300 with
100 explicit released-host capability skips. It includes 16 controlled-clock append/retract,
immediate/mini-batch and memory/RocksDB comparisons, with two successive restores, plus
60 new typed retention profiles and 30 typed retained recovery profiles. Complex retained
profiles cover ARRAY, nested ARRAY and intervals where Flink supplies the function, and retain
its explicit DISTINCT binary/MAP/ROW/MULTISET planning failures. Unchanged released upstream
mixed FIRST/LAST and multi-filter DISTINCT fixtures consume native grouping on both lines.

The [complete-job artifact](../benchmarks/results/distinct-ordered-retention-2026-10-21.json)
retains 15 final candidate and nine actual canonical previous-fallback profiles, all warmups,
measured samples, ranges, source/DSO hashes and CPU profiles. Jobs use release+mimalloc,
JDK 17, p=1, a 2 GiB heap, one-hour retention and both transposes. Two warmups precede three
alternating measurements. Retraction profiles use 131,072 rows/1,024 keys and global value
cardinalities 4,096 or 65,536; append profiles use one million rows/64 keys/cardinality 128.
The source's key/value correlation yields four or 64 value IDs per retracting key and two
per append key. Some retracting keys are all-NULL. Controlled-clock tests separately establish
expiry; the one-hour timed jobs do not expire entries.

Thirteen final profiles beat paired stock. Scalar append/retract profiles win 1.13–2.20x;
the higher-cardinality profiles win 4.73–7.88x across both releases. ARRAY's two low-cardinality
profiles remain slower after exactly two measured optimization passes:

| Flink 2.2 ARRAY workload | Stock median, s | Initial native, s | First pass, s | Final native, s |
| --- | ---: | ---: | ---: | ---: |
| Retractions, four value IDs/key | 0.304 | 0.971 | 0.380 | 0.352 |
| Append, two value IDs/key | 0.700 | 3.036 | 1.558 | 1.445 |
| Retractions, 64 value IDs/key | 2.331 | — | — | 0.493 |

The remaining append ARRAY CPU profile records 660 scalar-extraction samples, 625 retained
selection samples and 207 output-reconstruction samples among 2,489 native update samples.
Coverage ships under the accepted bounded groundwork policy, with the remaining array costs
tracked under [#443](https://github.com/datafusion-contrib/StreamFusion/issues/443). There is
no performance-only opt-in and no third optimization pass. NaNs, both zero signs, infinities
and original selected payloads are compared with retention off/on and through two restores.

Canonical previous ARRAY fallback medians are 0.293 s for low-cardinality retractions,
0.685 s for append, and 1.879 s for higher-cardinality retractions (paired stock
0.299/0.693/1.863 s). The raw artifact retains every range, including variation in the
higher-cardinality stock controls. The two slower native ARRAY profiles are also slower
than those previous fallback controls; their admission is accepted groundwork rather than
a claim of acceleration.

## Generic DISTINCT local/global views

`COUNT(DISTINCT)` admits ARRAY and both interval families in single-phase and local/global
plans on Flink 1.18.1 and 2.2.1, including retracting input. The local operator binds its
view element type from the Arrow input before creating a group, so empty and all-NULL
bundles retain the declared nested/interval schema. Shared unfiltered and independently
filtered counts merge signed multiplicities through the existing DISTINCT view.
Nested Arrow child field names and nullability are not membership values; transport aligns
those fields before combining values from input batches, partials and restored snapshots.
Floating ARRAY members use Java boxed equality: NaN payloads share membership while signed
zeros remain distinct. Original payloads remain in the view.

Representation-preserving collection projections can feed these verified DISTINCT consumers;
the planner retains the collection representation through the local aggregate and transposes.
Primitive STRING columns emitted by a collection Calc use their existing UTF-8 bytes at the
aggregate boundary, with the parent NULL mask applied. The UTF-8 buffers remain shared;
ARRAY/ROW/MAP payloads retain their nested string caches. Key hashing treats the represented
STRING and array-element copy carriers as their logical types, so they consume the same
precision descriptors and fixed slot widths as Flink. This preserves nullable STRING keys,
scalar string aggregate composition and memory/native RocksDB recovery.
Other collection boundary restrictions remain in force. Runtime coverage compares RowKinds,
resolved schemas and consumed native local/global work for ARRAY of INT, FLOAT, DOUBLE,
STRING, DECIMAL, ROW and ARRAY, plus year-month/day-time intervals. Recovery covers memory
and native RocksDB state with two successive restores, with retention enabled and disabled.

Released Flink's `AggregateUtil.createDistinctKeyType` rejects single-argument DISTINCT
BINARY, VARBINARY, MAP, ROW and MULTISET keys on both supported lines. These retain the
same planning failure rather than being counted as missing native local/global functions.
Flink 2.2's additional VARIANT root is outside this task's existing type scope.

### Measurements and accepted limits

The small functionality gate compares **131,072 prebuilt Java BinaryRowData rows with the
same 131,072 rows already in Rust Arrow buffers**, at 64 keys, 128 values per key and
1,024-row bundles. Preparation, Arrow IPC loading and operator setup are outside timing.
Production local/global accumulation, partial copying/packing and an identical RowKind/count
digest are timed in each runtime. Every row count and checksum matches. TTL is disabled in
this small gate; the separate complete jobs enable one-hour retention and retain all transposes,
JNI, state, source/sink work and completion. Both use release+mimalloc, JDK17, parallelism one,
a 2 GiB Java heap, NULL every seventh value, two warmups and three measured trials.

| Small gate, count-only | Flink 2.2 / Rust, s | Flink 1.18 / Rust, s |
| --- | ---: | ---: |
| ARRAY INT | 0.04764 / 0.08693 | 0.06005 / 0.09025 |
| ARRAY DOUBLE | 0.05185 / 0.09554 | 0.05229 / 0.09633 |
| ARRAY STRING | 0.06292 / 0.10091 | 0.05323 / 0.10077 |
| INTERVAL MONTH | 0.03129 / 0.01278 | 0.02886 / 0.01365 |
| INTERVAL DAY | 0.03520 / 0.01277 | 0.03520 / 0.01330 |

All eight interval comparisons, including independently filtered shared counts, beat stock
by 1.68–2.76x. ARRAY remains slower in all twelve isolated comparisons: this is accepted
native coverage groundwork after the maintainer's two measured optimization passes, not a
claim of acceleration. The new paths are enabled under that accepted timebox, without a
performance opt-in. [Follow-up #446](https://github.com/datafusion-contrib/StreamFusion/issues/446)
tracks the remaining scalar, view and retained-entry work.

| Flink 2.2 complete job | Initial native, s | First pass, s | Final native / stock, s |
| --- | ---: | ---: | ---: |
| ARRAY INT, 1M rows / 64 keys / 128 values | 3.172 | 3.195 | 2.969 / 0.943 |
| ARRAY DOUBLE, 262144 rows / 256 keys / 128 values | 7.208 | 1.086 | 1.055 / 0.606 |
| ARRAY STRING, 262144 rows / 256 keys / 32 values | 5.917 | 2.367 | 1.658 / 0.474 |
| INTERVAL DAY, 1M rows / 1024 keys / 128 values | 1.743 | 1.752 | 1.642 / 3.609 |

Flink 1.18 complete-job native/stock medians are 2.993/1.072, 1.061/0.574,
1.655/0.499 and 1.560/3.547 s for the same four shapes. Actual canonical `d13e9dc4`
fallback controls take 0.976/0.612/0.477/3.610 s. These observations preserve the unfavorable
ARRAY end-to-end results. The
[artifact](../benchmarks/results/generic-distinct-partials-2026-10-21.json) retains all
initial, intermediate, final and previous trials, ranges, DSO/source hashes, native CPU
profiles, planner configurations and digest evidence. The
[optimization ledger](../optimizations/aggregate-specialization-fast-paths.md#hash-nested-distinct-values-without-copying-arrow-cells)
explains the two passes.

To reproduce the small gate, build the release `distinct_partials_admission` example, then run
`FloatingDistinctPartialsAdmissionBenchmark` with `SF_BENCHMARK=true`, Maven's `bench` profile,
`-Dpartials.types=array_int,array_double,array_string,interval_month,interval_day`,
`-Dpartials.rows=131072 -Dpartials.keys=64 -Dpartials.cardinality=128 -Dpartials.bundle=1024`,
`-Dpartials.warmups=2 -Dpartials.runs=3 -Dpartials.export=/tmp/generic-distinct`.
For each exported type/shape pair, run the Rust example with the matching `.arrow` and `.json`
files followed by `2 3`; its timer starts after reading Arrow and preparing the operator.
The Flink 1.18 run adds Maven's `flink-1.18` profile. Complete jobs use
`DistinctAggregateBenchmark -Ddistinct.generic=ARRAY_DOUBLE -Ddistinct.rows=262144
-Ddistinct.keys=256 -Ddistinct.cardinality=128 -Ddistinct.retentionMillis=3600000
-Ddistinct.warmup=2 -Ddistinct.runs=3` with `SF_BENCHMARK=true` and the `bench` profile;
other shapes use the table's type/rows/keys/cardinality. Criterion's `typed_group_values`
adds nullable ARRAY INT/DOUBLE/STRING and both interval families at four/256 values,
with setup outside its Rust timer and independent distinct-count checks before timing.

Final verification passes 886 native tests (two ignored), 130 Criterion fixtures and 236
SQL/operator checks on each release. The latter include 116 generic COUNT contracts,
64 controlled expiry/recovery profiles and 56 related FIRST/LAST regressions. The unchanged
released `testCountDistinctArrayType` consumes native work in all seven Flink 2.2 and six
Flink 1.18 invocations. Final small gates and complete jobs were repeated after the STRING
carrier correctness fix; earlier two-pass measurements remain separately identified.

### DISTINCT LISTAGG membership retention

Positive-TTL single-phase and local/global LISTAGG DISTINCT use the shared DISTINCT write policy above.
Its cached append prefix survives membership expiry; retracting input retains the existing
independent ordered-value and pending-retraction clocks. One-argument LISTAGG and COUNT
DISTINCT may share a membership clock. A literal separator is a second DISTINCT argument,
so its clock is shared only with consumers of that same argument pair.

The SQL and released-operator harnesses cover append/retract input, omitted/literal separators,
filtered shared views, separate argument views, immediate/mini-batch execution, and two
successive restores on memory/native RocksDB. The native migration regression restores an
older snapshot without entry clocks and verifies the new deadlines and cached COUNT separately.
The unchanged released `AggregateITCase#testListAggWithDistinct` consumes native grouping
in single-phase and split variants on both lines. Split variants require consumed local
and global native work; Flink 1.18 changelog-state variants retain their existing backend restriction.

[Raw measurements and execution evidence](../benchmarks/results/listagg-distinct-retention-2026-10-21.json)
retain the complete-job trials against stock Flink and the actual canonical previous planner
(`d13e9dc4`), with both row/Arrow transposes, JNI and a rowwise sink. Runs use JDK 17, one task,
a 2 GiB heap, release+mimalloc, one-hour retention, two warmups and three measured trials.
Each trial asserts positive native aggregate work; previous-version controls assert fallback.
The strings contain sixteen recurring labels, NULL every seventh input, and the configured
16/128-byte widths. No performance optimization passes were needed for these profiles.

Final-library complete-job medians (seconds):

| Released Flink / input | Rows / keys / bytes | Stock | Native |
| --- | --- | ---: | ---: |
| 2.2.1 append | 1,000,000 / 65,536 / 16 | 12.676 | 1.168 |
| 1.18.1 retracting | 262,144 / 32,768 / 128 | 4.033 | 0.485 |
| 2.2.1 duplicate-heavy append | 1,000,000 / 4,096 / 16 | 5.680 | 0.974 |

These final profiles improve 5.83–10.85x over stock. The artifact also retains the
initial four release/shape profiles and separately paired previous-version fallback controls.

### JSON_OBJECTAGG map partials

Real local/global plans pass native Arrow map-entry lists between the two aggregate stages.
A local preserves the released heap MapView's live entries and binary-string hash order;
transport also preserves rebuilding that map after removals or a NULL/empty-key conversion.
Only surviving entries reach global duplicate checks. Retractions remove local entries,
while the global merges the resulting map and uses the separate record-count partial for
group liveness, matching the released functions rather than inventing signed map entries.

The split SQL harness composes both NULL policies with COUNT, string MIN, and COUNT DISTINCT,
including DECIMAL/CHAR JSON values, append/updating input, disabled/positive TTL, and two
memory/native-RocksDB restores. Controlled-clock tests extract both generated released
operators and both native operators. They delay global flush beyond partial arrival, compare
exact changelog rows, expire fields independently, and restore twice. Failure tests cover
local/global duplicates, map-copy iteration order, local duplicate NULLs, and each release's
NULL/empty transport behavior. Errors at a deferred flush retain their Flink execution type.

Released upstream fixtures combining ARRAY and OBJECT aggregates retain their native
single-phase contract, because JSON_ARRAYAGG has no merge method. Their unchanged tests
remain regression witnesses; the dedicated SQL and generated-operator tests require positive
work in both native phases for real JSON_OBJECTAGG splits.

The initial million-row stock split/TTL warmup was stopped after 102 seconds; a JVM thread
stack located it in `TtlIncrementalCleanup.runCleanup` while materializing JSON fields at
the global bundle flush. This is retained as a diagnostic rather than a speedup sample.
The matched complete-job measurements use 131,072 rows for both engines and the previous
planner, with both row/Arrow conversions, JNI, state, Arrow exchange and the rowwise sink.

| Released Flink / input | Keys / bytes | Stock median (s) | Native median (s) |
| --- | --- | ---: | ---: |
| 2.2.1 append | 65,536 / 16 | 2.345 | 0.350 |
| 2.2.1 retracting | 32,768 / 128 | 2.798 | 0.444 |
| 1.18.1 append | 65,536 / 16 | 2.218 | 0.330 |
| 1.18.1 retracting | 32,768 / 128 | 2.899 | 0.428 |

These profiles improve 6.30–6.78x over stock Flink. The previous canonical planner
(`d13e9dc4`) falls back, measuring 2.085 s and 2.890 s on the two 2.2.1 shapes.
Runs use JDK 17, one task, a 2 GiB heap, release+mimalloc, one-hour TTL, 1,024-row
bundles, two warmups and three measured trials, alternating engine order. Every native
trial requires consumed local and global work. No slow native gate or optimization
passes were needed. [Raw trials, source/library hashes and execution evidence](../benchmarks/results/json-object-partials-2026-10-21.json)
also retain the large stock warmup timebox and its TTL cleanup stack.


### Retracting LISTAGG local/global lists

Released retracting LISTAGG has one structured partial containing its value list, unmatched
retraction list, and optional delimiter. The native local emits an Arrow struct containing
both ordered lists and a separator. The global first merges temporary partials, including
cancellation within a single partial, then merges that temporary result into retained state.
Moving owned strings between lists avoids serializing a host ListView between native stages.

Local groups follow Flink's binary-row HashMap order. That order determines which partials
land in each global count-triggered bundle; preserving only the final set of groups would
change later FIFO retractions. FILTER, default/empty/Unicode literal separators, NULL keys,
CHAR/VARCHAR values, numeric partials and trailing COUNT DISTINCT views retain their declared
column offsets. The append-only separator/prefix layout remains unchanged.

At global merge, surviving value and pending lists receive the new write time. The released
explicit-separator function skips an empty incoming partial, while the omitted-separator
function still rewrites the surviving lists. Temporary cancellation must happen first to
preserve that distinction. Controlled tests delay global flush beyond partial arrival,
compare exact RowKinds, expire both lists independently and restore twice on memory/native
RocksDB. SQL recovery comparisons restore both engines at offsets 24 and 72: checkpoint
bundle drains can change released retracting LISTAGG order, so a recovering run is compared
with the same checkpoint cuts rather than an independently bundled uninterrupted run.

DISTINCT LISTAGG local/global membership replay uses the separate shared-view path below.

The focused final selection passes 179 SQL/operator checks on each released Flink line,
including eight released layout contracts, twenty-two new split SQL checks and eight
controlled-clock/recovery profiles. The native suite passes 894 tests with two diagnostics
ignored. The unchanged upstream retracting/distinct LISTAGG tests pass fourteen invocations
on 2.2.1 and twelve on 1.18.1: ordinary retracting splits now require consumed native local
and global operators; DISTINCT splits now require both operators as described below.

Complete-job medians include source/sink row conversion, JNI, list state and the Arrow
exchange. Runs use one task, JDK 17, release+mimalloc, a 2 GiB heap, one-hour TTL,
1,024-row bundles, two warmups and three measured trials in alternating engine order.

| Released Flink / separator | Rows / keys / bytes | Stock median (s) | Native median (s) |
| --- | --- | ---: | ---: |
| 2.2.1 omitted comma | 1,000,000 / 65,536 / 16 | 11.315 | 1.725 |
| 2.2.1 literal comma | 262,144 / 32,768 / 128 | 2.978 | 0.714 |
| 1.18.1 omitted comma | 1,000,000 / 65,536 / 16 | 12.597 | 1.685 |
| 1.18.1 literal comma | 262,144 / 32,768 / 128 | 3.459 | 0.687 |

These profiles improve 4.17–7.47x over stock Flink. Every native trial requires positive
local and global work. No slow native gate or optimization passes were needed.
[Raw trials, source/library hashes and unchanged upstream execution evidence](../benchmarks/results/retracting-listagg-partials-2026-10-21.json)
also include separately paired actual previous-planner fallback controls.
Reproduce with `StringAggregateBenchmark`, `stringagg.phase=TWO_PHASE`,
`stringagg.updating=true`, `stringagg.ttl=1 h`, and `stringagg.literalSeparator=false`
for the omitted separator (the default `true` selects a literal comma).

### DISTINCT LISTAGG local/global membership replay

The local operator carries each shared DISTINCT argument view as ordered string/count entries.
It preserves released StringData hashing for one argument and BinaryRow hashing for a literal
separator argument, NULL membership positions, map copies, removals and reinsertion. Separator
literals travel with the Arrow view schema even when every row is NULL or a consumer's FILTER
rejects the entire partial; a cached list accumulator cannot supply that argument reliably.

The global first combines transient views in released map order, then replays only positive
membership crossings against retained state. Append membership uses presence bits; retracting
membership retains signed counts, including negative pending counts. It does not concatenate
DISTINCT list partials. Shared filtered consumers retain their independent counters and common
membership deadline, and an explicit separator keeps its distinct argument view separate from
one-argument COUNT DISTINCT. Inner retracting lists retain their own entry clocks; successful
removal refreshes survivors without refreshing entries on duplicate membership updates.

Exact SQL and controlled-clock checks cover both released Flink lines, both input modes,
default/empty/Unicode separators, shared filters, nullable keys/values, mixed aggregate layouts,
and two restores through memory and native RocksDB state.

Complete-job measurements use JDK 17, release+mimalloc, one task, a 2 GiB heap, one-hour
retention and 1,024-row local/global mini-batches. Both perimeter conversions, JNI, state,
Arrow exchange and the rowwise sink remain inside timing. Each profile runs alone with two
warmups and three alternating-engine measured trials; both native stages must consume rows.

| Flink | Input; rows / keys / string bytes | Stock seconds | Native seconds | Stock / native |
| --- | --- | ---: | ---: | ---: |
| 2.2.1 | Retracting, omitted separator; 1M / 65,536 / 16 | 13.489 | 3.091 | 4.36x |
| 2.2.1 | Append, literal comma; 262,144 / 32,768 / 128 | 3.985 | 1.371 | 2.91x |
| 1.18.1 | Retracting, omitted separator; 1M / 65,536 / 16 | 14.359 | 3.063 | 4.69x |
| 1.18.1 | Append, literal comma; 262,144 / 32,768 / 128 | 3.852 | 1.325 | 2.91x |

The actual previous canonical planner falls back for these split queries. Its medians on 2.2.1
are 14.071 s for the retracting profile and 4.113 s for the wide append profile, with unchanged
production source and only the identical benchmark fixture copied. All four candidate profiles
beat stock without a performance optimization pass. The focused current reports pass 348
SQL/operator checks per release; the Rust suite passes 897 tests with two diagnostics ignored.
Unchanged upstream retracting/distinct LISTAGG tests pass fourteen invocations on 2.2.1 and twelve
on 1.18.1, requiring consumed native local/global work for split variants.
[Raw trials, source/library hashes, previous-planner controls and execution evidence](../benchmarks/results/distinct-listagg-partials-2026-10-21.json)
retain the measured variability and exact configurations. Reproduce with `StringAggregateBenchmark`,
`stringagg.function=LISTAGG`, `stringagg.distinct=true`, `stringagg.phase=TWO_PHASE`,
`stringagg.ttl=1 h`, and the corresponding rows/keys/width settings. The retracting profile sets
`stringagg.updating=true` and `stringagg.literalSeparator=false`; the append profile sets those
properties to `false` and `true` respectively.


## Integer COLLECT

COLLECT over INT/BIGINT uses the existing columnar JVM aggregate runtime with released Flink
handlers and managed MapViews. Input fields remain scalar, and the initial output row cannot
contain character fields, including grouping keys or scalar co-aggregates. Other COLLECT
value types, nested results and ARRAY_AGG retain fallback. ARRAY_AGG is absent from released
Flink 1.18.1. No new native multiset accumulator or state serializer is introduced.

NULL values do not contribute; repeated values keep their multiplicities. Retractions retain
Flink's zero/negative-count behavior, group deletion and reinsertion, and resolved MULTISET
schema. DISTINCT, FILTER and mixed scalar aggregates use the same released generated handlers.
Grouped and global aggregation are verified on both supported lines, including consumed
NativeJvmGroup work. Heap and native RocksDB cases restore twice across nonempty accumulation
and retraction prefixes, retaining managed DataView state and changelog tags.

The existing `streamfusion.experimental.jvmGroupAggregates` switch controls this route and is
true by default. Mini-batching, async state, approximate/ordered calls and unverified function
specialization retain fallback. A COLLECT result may reach a final row sink or a verified
collection-preserving Calc. Unverified state, ordering, exchange and native-writer boundaries
retain the existing collection guards. Updating retained MAP/MULTISET payloads remain deferred
in [#437](https://github.com/datafusion-contrib/StreamFusion/issues/437).

[Release comparisons](../benchmarks/collection-aggregates.md#integer-collect) retain source,
keyed state, sink and both row/Arrow transposes, including startup and all trial ranges.
The optimization investigation is limited to two profiling/optimization passes per increment.
