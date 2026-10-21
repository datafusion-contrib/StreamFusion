# Window aggregate

**Status:** Native, both event-time and processing-time, with the gaps enumerated below.

This page covers the windowed `GROUP BY` aggregate — `TUMBLE`/`HOP`/`CUMULATE`/`SESSION`, single-phase
and the two-phase local/global split — and the windowing-TVF operator that assigns each row to its
window(s) ahead of a downstream consumer (an aggregate, a [window join](joins/window-join.md), or
window Top-N/dedup).

## Parallelism and state key groups

The two-phase global window aggregate without business grouping keys is a SINGLETON:
new plans use parallelism and maximum parallelism of one, even when the execution
environment has a larger maximum parallelism. Its columnar exchange uses the same
single target key group, including unaligned-checkpoint reassembly. The upstream
splitter and local aggregate remain parallel and retain their own maximum parallelism.
Global aggregates with business grouping keys continue to honor the configured
maximum parallelism.

Deployment integrations must supply their effective maximum parallelism before SQL
translation, not rewrite it after native operators have bound their state namespaces.
StreamFusion honors Flink's configured maximum and does not hardcode a platform-wide
default. Resource-only changes may adjust eligible parallelism within that fixed
namespace; they must not change the namespace or replace the columnar shuffle.

Regenerate saved resource plans after this change. A previous keyless global state
with maximum parallelism 128 (or another value above one) is not automatically
compatible with the new single-key-group plan. Retain the old deployment for recovery
or use a separately verified state migration; regenerating JSON alone does not migrate
a checkpoint/savepoint. Runtime namespace validation remains enabled.

`WindowAggregateMaxParallelismTest` checks the parallel Source/Local chain and
keyed/keyless Global JobGraphs with maximum 32768. `SingletonColumnarWindowShuffleTest`
executes stock/native CUMULATE parity and checkpoint completion with ordinary
maxima 128, 256, and 32768. `SingletonGlobalWindowRecoveryTest` verifies results
after two checkpoint restores with both heap-backed and native RocksDB state;
the execution tests cover aligned and unaligned-enabled configurations.

Grouping-only windows (a windowed `GROUP BY` without aggregate calls) retain each distinct group
on disk as well as in memory. RocksDB persists their window bounds even though there are no
accumulator columns; duplicate groups, watermark firing, and checkpoint recovery preserve the
same output as the memory backend.

Ordinary fixed-grid COUNT-only RocksDB windows coalesce decoded updates across batches within
an 8 MiB tracked payload limit and the task off-heap budget. Watermarks persist only closing
groups; checkpoints and canonical exports persist all pending updates before snapshotting.
Reservation pressure writes the bundle through. Shared regional slices, regional calendar
callbacks and mixed aggregate lists retain their existing write-through lifecycle. Firing
iterators stay within each key-group range, including when its records are all deletion markers.

Aligned windows fire when the watermark or processing clock reaches `window_end - 1ms`,
including the local/global split and persistent state. Event-time rows for a fired final window are late
and cannot change its result. Processing-time arrivals remain admissible, including arrivals
at the same clock reading after a timer callback, matching Flink's processing-time behavior. Native storage indexes exclusive window ends, so the Java boundary
translates the firing frontier by one millisecond before flushing.

## Mixed aggregates and AVG partials

SUM, MIN, MAX, COUNT and AVG can share a window and read the same or different numeric columns.
String, DATE, TIME and BOOLEAN MIN/MAX can share those windows and their local/global partial layout as well.
On insert-only input, MIN/MAX also accept TIMESTAMP and TIMESTAMP_LTZ value columns at
precisions 0 through 9. These are aggregate values, separate from the window's time attribute.
The accumulator owns signed milliseconds and the fractional nanosecond remainder, compares
that pair without narrowing the date range, ignores NULLs and emits NULL for an all-NULL group.
Local/global partials and checkpoint state use one nullable component timestamp with the
original logical precision. Ordinary COUNT also admits timestamp values through their validity bitmap.
Both timestamp kinds can share a window with numeric aggregates, string extrema and COUNT(DISTINCT).
Mixed string/temporal SQL comparisons cover TUMBLE/HOP/CUMULATE at precision 0/3/6/9
in both phases. They verify default event-time TUMBLE/HOP/CUMULATE admission and explicit overrides;
adding temporal extrema does not bypass the [string admission gate](#string-minmax).

Temporal extrema are enabled for single-phase and local/global TUMBLE/HOP/CUMULATE,
event-time SESSION, admitted attached windows and legacy HOP. Processing-time fixed windows
retain the same typed value state and timer contract. The ordinary filtering, input-changelog,
attached-window and LTZ assignment gates still apply.

SQL comparisons cover precision 0/3/6/9, years 0001 through 9999, pre-epoch fractions,
nanosecond differences within one millisecond, independent filters, both aggregation phases,
legacy/session shapes and empty input. TIMESTAMP_LTZ **values** retain instant ordering in
UTC, Asia/Shanghai and America/New_York; these tests use a plain TIMESTAMP rowtime so the
separate historical-offset gate on LTZ **window assignment** remains unchanged. TUMBLE/HOP/SESSION recovery
injects two failures after completed checkpoints on memory and RocksDB and requires work
from every native stage. Controlled-clock processing-time tests verify value state, NULL-only
groups and timer firing after restore on both backends.

AVG supports INT/BIGINT, SMALLINT/TINYINT, FLOAT/DOUBLE and DECIMAL in single-phase execution
and the event-time two-phase split for TUMBLE, HOP, CUMULATE and admitted attached windows.

The local emits every accumulator field in aggregate order. AVG contributes adjacent sum and
count fields; later aggregates and an optional synthetic row count start after that pair.
The global merges each pair together before dividing. Integral sums widen to BIGINT with Java's
wrapping arithmetic, but the average retains its declared integral result type. FLOAT sums widen
to DOUBLE and narrow only the result. DECIMAL preserves the sum scale, sticky overflow, exact
division and Flink's result scale. Empty/all-NULL groups keep the host NULL/count behavior. Session output declares aggregate
fields nullable, allowing an all-NULL SUM/MIN/MAX group to cross the Arrow boundary while
COUNT and COUNT(DISTINCT) still emit zero.

Partials remain Arrow through the local, exchange and global operators. A checkpoint barrier
drains local slices into the global before snapshotting; AVG pairs use the existing flattened
accumulator checkpoint layout in memory and direct RocksDB state. Restore tests merge subsequent
partials and verify every hopping window after RocksDB checkpoints and both backend transitions.

Flink can reduce shared `COUNT(v), AVG(v), SUM(v)` calls to COUNT and internal `$SUM0`, with
the final NULL correction and division in a Calc. Exact integer widths and DECIMAL sums admit
this layout. SUM0 starts at typed zero and ignores NULL inputs; an all-NULL group still returns
COUNT 0 and user-visible SUM/AVG NULL through Flink's unchanged outer Calc. Integer sums wrap
at their declared width, integer averages truncate, and decimal overflow remains NULL through
local/global merging and checkpoints. A NULL decimal partial carries overflow and must not
be discarded as though it were a NULL input. The single-field SUM0 buffer also supports the
existing updating aligned event-time windows, with their separate live-row counter.
Session and legacy windows use the same accumulator factory. Supported append-only attached
bounds admit both single-phase and local/global execution. SQL comparisons cover these layouts as well.

Released Flink cannot initialize AVG/SUM0 over `DECIMAL(38,38)`. That shape stays on Flink,
including its planning failure. Floating SUM0 uses typed zero-initialized state; DISTINCT SUM0
stays outside admission. FLOAT additions/subtractions round at FLOAT width and DOUBLE at
DOUBLE width, preserving the difference from ordinary SUM's first-value initialization.

The local stage retains rows whose slice has already fired: that slice can still belong to
an open HOP or CUMULATE window. The global merge admits each partial only into final windows
that have not fired, so late partials cannot reopen completed windows. This also applies after
checkpoint restore and when the late row introduces a new key. TUMBLE drops the partial once
its single final window has closed. Tests compare explicit watermarks, mixed ordinary/distinct
aggregates and both aggregation phases against released Flink.

## DATE, TIME and BOOLEAN extrema

Insert-only MIN/MAX admits DATE, TIME and BOOLEAN values in the enclosing window's supported
single-phase and local/global plans. NULLs are ignored, and all-NULL groups retain a typed NULL.
Each accumulator owns one fixed-width value. Typed readers borrow the grouped row positions
without allocating an Arrow selection array for each aggregate and key. Exact HOP/CUMULATE
partials fold once per slice before merging into open final windows; session merges combine
retained values directly. Partial and checkpoint state remains one nullable value of the input
SQL type, compatible with the previous DataFusion accumulator.

SQL comparisons cover TUMBLE/HOP/CUMULATE in both phases, independent filters, nullable keys,
all-NULL groups, legacy event-time windows, SESSION, admitted attached bounds and processing-time
admission. HOP/SESSION recovery injects two failures after completed checkpoints on memory and
RocksDB, then verifies late-row admission and native execution. Released Flink 1.18 passes its
applicable cases and skips its three unavailable SESSION table-function checks. Existing filtering,
updating-input, processing-time and attached-window restrictions still apply.

The `window_scalar_extrema` release Criterion target checks 72 fixtures across four window shapes,
128/4096 rows and one/64/1024 keys, including all-NULL keys and session merges that change extrema.
Its independent oracle checks typed values before timing; allocation diagnostics retain the
production memory accounting. A separate compatibility test reads the previous DataFusion MIN/MAX
state and verifies that the previous accumulator can read the new state.

The October 18, 2026 release+mimalloc comparison uses Flink 2.2.1/JDK 17 on Intel
i7-12650H/Linux WSL2, a 2 GiB heap and parallelism two. Two warmups precede three alternating
measured jobs per engine. Twenty-one profiles use one million rows/64 keys across
TUMBLE/HOP/CUMULATE in both phases and single-phase SESSION. Six larger HOP profiles use
two million rows/4,096 keys. DATE spans 31 days, TIME varies within each key, and every seventh
value is NULL. Every query retains both transposes, the row source and blackhole sink, casts,
JNI, production IPC and state costs. Plans and execution counters require native window work.

Across all 27 profiles, native mean job time is **1.218–1.649x faster than stock Flink** and
**1.159–1.633x faster than previous-origin fallback**. The larger HOP results follow:

| Value | Phase | Stock mean (s) | Previous origin mean (s) | Native mean (s) | Stock/native |
| --- | --- | ---: | ---: | ---: | ---: |
| BOOLEAN | Single | 1.548655 | 1.531089 | 1.222616 | 1.267x |
| BOOLEAN | Local/global | 1.739300 | 1.723851 | 1.284483 | 1.354x |
| TIME | Single | 1.514093 | 1.441267 | 1.243303 | 1.218x |
| TIME | Local/global | 1.735037 | 1.719916 | 1.275368 | 1.360x |
| DATE | Single | 1.478080 | 1.451693 | 1.199571 | 1.232x |
| DATE | Local/global | 1.740873 | 1.727981 | 1.355902 | 1.284x |

The [raw artifact](../benchmarks/results/window-scalar-extrema-2026-10-18.json) retains all
warmup/measured samples, stock controls, source/native hashes, commands and allocation
diagnostics. It also retains the failed type-only high-cardinality profiles that motivated
the typed-reader/pane optimization. Previous-origin runs use identical fixtures and native
planning with fallback asserted, in separate JVMs with paired stock controls. Startup remains
included, and observed variability is not discarded.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=WindowBasicTypeBenchmark -Dwindow.cases=extrema_boolean,extrema_time,extrema_date \
  -Dwindow.phases=ONE_PHASE,TWO_PHASE -Dwindow.shapes=TUMBLE,HOP,CUMULATE,SESSION \
  -Dwindow.rows=1000000 -Dwindow.keys=64 -Dwindow.warmup=2 -Dwindow.runs=3 \
  -Dsf.extraJvmArgs=-Xmx2g
# Larger control: window.rows=2000000 window.keys=4096 window.shapes=HOP.
# Previous origin additionally uses window.expectNative=false.
```

## COUNT over values

Ordinary `COUNT(value)` accepts character, DATE, TIME, BOOLEAN, TIMESTAMP and TIMESTAMP_LTZ
values alongside the existing numeric types, and ordinary COUNT also accepts VARBINARY, both interval families, ARRAY, MAP, ROW and
MULTISET parents. Empty containers and non-NULL parents with NULL children count once. It reads Arrow validity without decoding the
payload. All-NULL groups report zero. Filtered counts retain group liveness even without a
companion `COUNT(*)`; groups whose rows have all retracted produce no window result.
The existing window assignment, filtering and changelog restrictions still apply.

SQL comparisons cover single/local-global TUMBLE, HOP and CUMULATE, NULL-only keys,
independent filters, and signed input with complete group removal. Mixed timestamp COUNT/MIN/MAX
regressions also require native execution in both aggregation phases. Recovery covers string
counts alongside extrema and BOOLEAN/TIME counts across raw checkpoints and canonical
memory/RocksDB transitions. Thirty-six release Criterion fixtures compare an independent
validity-by-key oracle at 128/4096 rows, one/64/1024 keys, NULL-only keys, 8/256-byte strings,
DATE, TIME, BOOLEAN and nanosecond timestamps.

Complex-value SQL comparisons cover both phases of event-time TUMBLE/HOP/CUMULATE, filters,
legacy TUMBLE/HOP/SESSION and the session table function where the released host supports it.
Retraction cases cover updates, complete group removal and NULL-only groups on Flink 2.2;
Flink 1.18 retains its host restriction on retracting window table functions. Sixteen profiles
force two restores across both phases, the four parent types and memory/native RocksDB.
Twenty-four direct processing-time cases check all three aligned shapes and restored timers on
both backends. The existing scalar COUNT controls run alongside these cases. Retracting COUNT
normalizes parent validity to the same owned BIGINT bitmap carrier used by append COUNT;
aggregate result state remains BIGINT. DISTINCT still excludes these complex types.

### Complex-value COUNT measurements (2026-10-19)

`WindowComplexCountBenchmark` uses one million runtime rows, parallelism two, 64 keys,
nullable ARRAY/MAP/ROW parents, empty containers and NULL children. Array/map payloads have up
to four entries. Complete release+mimalloc jobs retain both transposes, Arrow IPC, JNI, window
state and a row blackhole sink. JDK 17 uses a 2 GiB heap; two warmups precede three measured
jobs per engine with alternating order. Plans and execution counters require the intended
native local/global stages. The previous control is exact integration revision `ddf8154e`,
with only the identical benchmark fixture copied and aggregate fallback asserted. Both use
the same native library because production Rust is unchanged.

| Parent | Phase | Stock median (s) | Previous SF median (s) | Native median (s) | Stock/native |
|---|---|---:|---:|---:|---:|
| ARRAY | ONE_PHASE | 0.555 | 0.562 | 0.479 | 1.159× |
| ARRAY | TWO_PHASE | 0.591 | 0.563 | 0.275 | 2.146× |
| MAP | ONE_PHASE | 0.982 | 0.957 | 0.936 | 1.049× |
| MAP | TWO_PHASE | 1.054 | 1.066 | 0.829 | 1.271× |
| ROW | ONE_PHASE | 0.516 | 0.525 | 0.366 | 1.409× |
| ROW | TWO_PHASE | 0.565 | 0.570 | 0.333 | 1.698× |

At 16,384 keys and up to 32 entries per payload, two-phase ARRAY/MAP stock/native medians
are 0.866/0.572 s (1.512×), 3.183/3.059 s (1.040×).
Released Flink 1.18 two-phase profiles span 1.325–1.937× stock/native.
Single-phase MAP and wide MAP gains are modest; these complete-job observations do not
establish universal throughput gains. The [170-job artifact](../benchmarks/results/window-complex-count-2026-10-19.json)
retains all warmups/trials, ranges, source/library hashes, actual previous controls, 36 native
allocation probes and both released qualification summaries (132 checks on 2.2; 90 passed
and 42 host-capability skips on 1.18).

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  -Dtest=WindowComplexCountBenchmark -Dwindow.cases=array,map,row \
  -Dwindow.phases=ONE_PHASE,TWO_PHASE -Dwindow.shapes=TUMBLE \
  -Dwindow.rows=1000000 -Dwindow.keys=64 -Dwindow.width=4 \
  -Dwindow.warmup=2 -Dwindow.runs=3 -Dsf.extraJvmArgs=-Xmx2g
```

The October 18, 2026 complete-job comparison uses released Flink 2.2.1/JDK 17,
release+mimalloc, production IPC, a 2 GiB heap, parallelism two, one million runtime rows
and 64 keys on Intel i7-12650H/Linux WSL2. Two warmups precede three measured jobs per
engine, alternating engine order. Both transposes, row source, blackhole sink, casts, JNI
and window state remain included; native plans and profile execution counters are checked.

| Value | Phase | Window | Stock mean (s) | Previous origin (s) | Native mean (s) | Stock/native |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| STRING | Single | TUMBLE | 0.869391 | 0.849825 | 0.673364 | 1.291x |
| STRING | Single | HOP | 0.860723 | 0.798490 | 0.609107 | 1.413x |
| STRING | Local/global | TUMBLE | 0.877160 | 0.897436 | 0.542144 | 1.618x |
| STRING | Local/global | HOP | 0.882599 | 0.856293 | 0.548919 | 1.608x |
| TIMESTAMP | Single | TUMBLE | 0.748317 | 0.739427 | 0.595948 | 1.256x |
| TIMESTAMP | Single | HOP | 0.742453 | 0.749904 | 0.601748 | 1.234x |
| TIMESTAMP | Local/global | TUMBLE | 0.800433 | 0.796118 | 0.550682 | 1.454x |
| TIMESTAMP | Local/global | HOP | 0.800873 | 0.799912 | 0.563302 | 1.422x |
| BOOLEAN | Single | TUMBLE | 0.760702 | 0.756317 | 0.584107 | 1.302x |
| BOOLEAN | Single | HOP | 0.746617 | 0.751287 | 0.591577 | 1.262x |
| BOOLEAN | Local/global | TUMBLE | 0.865827 | 0.809163 | 0.550993 | 1.571x |
| BOOLEAN | Local/global | HOP | 0.805551 | 0.810091 | 0.558708 | 1.442x |
| TIME | Single | TUMBLE | 0.768842 | 0.760456 | 0.583193 | 1.318x |
| TIME | Single | HOP | 0.729824 | 0.743693 | 0.580932 | 1.256x |
| TIME | Local/global | TUMBLE | 0.802607 | 0.810159 | 0.552518 | 1.453x |
| TIME | Local/global | HOP | 0.805824 | 0.819389 | 0.562197 | 1.433x |
| DATE | Single | TUMBLE | 0.746787 | 0.759608 | 0.614492 | 1.215x |
| DATE | Single | HOP | 0.747322 | 0.749718 | 0.694767 | 1.076x |
| DATE | Local/global | TUMBLE | 0.802537 | 0.810491 | 0.574407 | 1.397x |
| DATE | Local/global | HOP | 0.806350 | 0.808076 | 0.594443 | 1.356x |

TUMBLE uses ten-second windows; HOP uses a two-second slide and ten-second size. The
source emits five seconds of event time and defers the final watermark. DATE retains its
cast from a timestamp in the query. Previous-origin controls use identical fixtures with
native planning enabled and window fallback asserted, in a separate JVM with paired stock
controls. The [raw artifact](../benchmarks/results/window-count-types-2026-10-18.json)
retains every warmup and measured trial, ranges, commands and hashes. Startup remains
included; the small DATE/HOP single-phase advantage is workload-specific.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=WindowBasicTypeBenchmark \
  -Dwindow.cases=count_string,count_timestamp,count_boolean,count_time,count_date \
  -Dwindow.phases=ONE_PHASE,TWO_PHASE -Dwindow.shapes=TUMBLE,HOP \
  -Dwindow.rows=1000000 -Dwindow.keys=64 -Dwindow.warmup=2 -Dwindow.runs=3 \
  -Dsf.extraJvmArgs=-Xmx2g
# Original origin additionally uses window.expectNative=false.
```

## String MIN/MAX

Append-only windows admit `MIN` and `MAX` over CHAR and VARCHAR (including SQL STRING).
Local/global and single-phase execution are admitted by default in every otherwise supported
shape, including processing time, sessions and legacy group windows. Explicit
`streamfusion.window.string-extrema.single-phase.enabled=false` forces single-phase string
extrema to stock. A mixed query follows the same admission as its string extrema. The [operator-format audit](../benchmarks/feature-admission-2026-10-19.md) supersedes the
previous complete-job admission gate, while retaining those complete-job regressions below.
The later Rust-side profile and string selection change also admit HOP; see the
[Rust admission report](../benchmarks/rust-feature-admission-2026-10-19.md).
Released Flink 1.18 cannot plan CHAR extrema; STRING/VARCHAR are covered on both 1.18 and 2.2.
They compare UTF-8 bytes lexicographically, without locale collation or Unicode normalization,
matching Flink's materialized string ordering. Empty strings are values, NULL inputs are ignored,
and a group with only NULL values emits NULL. CHAR casts keep Flink's padding/truncation at the
expression boundary rather than changing the accumulator's comparison.

The implementation, SQL/state regression tests and opt-in benchmark harness retain the
project's Java 11 bytecode target, including when built with JDK 17.

The local stage emits one nullable UTF-8 accumulator field for each string MIN/MAX. The global
stage merges that field with the same extreme operation. Mixed numeric aggregates, AVG's paired
fields, and COUNT(*) retain their positional layout. Existing DataFusion extrema accumulators,
Arrow partial carriage, memory snapshots and RocksDB row-state codecs are reused; no JNI or
checkpoint format changes are needed.

SQL parity covers single- and two-phase TUMBLE/HOP/CUMULATE, attached two-phase windows, and
legacy and TVF sessions where released Flink supports them. Fixtures include nullable keys,
all-NULL groups, empty strings, long strings, prefixes, embedded NUL, combining characters and
supplementary Unicode characters. Native and operator tests retain string partials across
checkpoints and memory/RocksDB backend transitions.

Existing window/time and FILTER restrictions still apply. Updating MIN/MAX runs in the fixed
windows described under [Retracting extrema](#retracting-extrema-and-legacy-fixed-windows). Ordinary COUNT(string) uses the existing validity-count accumulator;
SUM(string) and AVG(string) are outside the admitted numeric reductions.

`StringWindowBenchmark` is an opt-in complete-job comparison with a rowwise generated source,
rowwise blackhole sink and both production transposes. It groups by key and window and emits
key, MIN, MAX and COUNT(*); window properties are not projected. Defaults are 1,000,000 rows,
64 keys, 64-byte ASCII values (every seventh value NULL), parallelism 2, UTC, and a 4096-row
mini-batch with 1-second latency. Run with
`SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench -Dtest=StringWindowBenchmark`.
`stringWindow.shape` selects TUMBLE/HOP/CUMULATE, while `stringWindow.keys` and
`stringWindow.width` select cardinality and value width. Both aggregation phases run two
warmups and five measured trials, alternating stock/native execution order. Use
`stringWindow.expectNative=false` only with the previous implementation to require its
window fallback rather than treating fallback as native acceleration.

The release comparison below uses Flink 2.2.1, Corretto JDK 17.0.20.1, the production mimalloc
native library, Arrow IPC on local exchanges, and an Apple M2 (8 cores, 16 GiB, macOS 15.5).
The previous implementation is upstream `9724b9d98` with the same new benchmark fixture;
its planner retained stock window aggregation. The single-phase opt-in was added after the
measurements and changes admission only, not the measured native implementation.
All values are complete-job median seconds; the stock column is the candidate's matched control.

| Window | Phase | Keys / bytes | Previous SF | Stock Flink | Native SF | Stock / native |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| CUMULATE | ONE_PHASE (opt-in) | 64 / 64 | 0.590738 | 0.688513 | 0.536418 | 1.284x |
| CUMULATE | TWO_PHASE | 64 / 64 | 0.707389 | 0.773591 | 0.530448 | 1.458x |
| CUMULATE | ONE_PHASE (opt-in) | 4096 / 128 | 0.788588 | 1.047310 | 1.564120 | **0.670x** |
| CUMULATE | TWO_PHASE | 4096 / 128 | 1.350799 | 2.101143 | 1.324256 | 1.587x |
| HOP | ONE_PHASE (opt-in) | 64 / 64 | 0.566805 | 0.676604 | 0.462997 | 1.461x |
| HOP | TWO_PHASE | 64 / 64 | 0.648353 | 0.707462 | 0.380463 | 1.859x |
| TUMBLE | ONE_PHASE (opt-in) | 64 / 64 | 0.609306 | 0.614691 | 0.319694 | 1.923x |
| TUMBLE | TWO_PHASE | 64 / 64 | 0.710261 | 0.709730 | 0.320627 | 2.214x |

The previous and candidate sweeps are separate; their stock controls vary, especially in the
high-cardinality case. Several stock and native trials have roughly five-second long tails.
For example, low-cardinality two-phase CUMULATE native samples range from 0.367151s to 5.649622s.
These short bounded jobs include startup and scheduling, so the medians are not sustained
throughput guarantees. No samples were removed: all 160 measured samples, including both sweeps'
stock controls, are retained in [the raw CSV](../benchmarks/string-window-extrema-2026-10-10.csv).
At that revision, the single-phase regression prevented default admission. Inspection of the
native fold shows per-group Arrow gathers and generic string accumulator updates; optimization
of that high-cardinality path remained necessary. Contiguous groups
now use retained Arrow slices as described in the later measurements; those TUMBLE results do
not establish every HOP/CUMULATE shape. The later operator-format audit changes admission
for TUMBLE/CUMULATE; the subsequent Rust-side selection optimization also admits HOP.

Additional SQL comparisons cover embedded NULs, long values, supplementary characters and
runtime lone-surrogate strings at parallelism one and two, including Flink source serialization
before extrema ordering. HOP recovery injects two failures after completed checkpoints on memory
and RocksDB and requires work from every native stage. Controlled-clock processing-time tests
verify extrema, NULL-only groups and timer firing after restore on both backends. Current SQL
and recovery checks use default admission; the earlier evidence explicitly enabled the switch.

## Temporal extrema benchmarks

The pre-integration October 13, 2026 comparison uses main `49ebe3cf` as its base, released Flink 2.2.1,
JDK 17 on an Apple M4 Pro, UTC, parallelism two, and a release+mimalloc native library.
Local exchanges use Arrow IPC. Every complete job retains its runtime row source, both
transposes, JNI, state and collecting row sink. Two warmups precede five measured trials;
stock, native and forced-window-fallback runs rotate order. All output fields and RowKinds
are compared, and current-job counters must show work from every intended native stage.
Forced window fallback represents the previous planner behavior for these value types.

The author’s pre-integration production-path measurements below use precision 9. Times are median complete-job
seconds. Startup and scheduling remain included; the 20-million-row cases also exercise
sustained processing. No timing samples are discarded.

| Window / phase | Rows / keys | Value | Stock Flink | Previous fallback | Native | Stock / native |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| TUMBLE / one | 2M / 1 | TIMESTAMP | 0.758354 | 0.662719 | 0.633011 | 1.198x |
| TUMBLE / one | 2M / 1 | TIMESTAMP_LTZ | 0.755901 | 0.757004 | 0.638541 | 1.184x |
| TUMBLE / one | 20M / 4096 | TIMESTAMP | 5.627059 | 5.745932 | 4.215417 | 1.335x |
| TUMBLE / one | 20M / 4096 | TIMESTAMP_LTZ | 6.229142 | 6.139699 | 4.731588 | 1.317x |
| HOP / two | 2M / 4096 | TIMESTAMP | 1.143439 | 1.146968 | 0.763992 | 1.497x |
| HOP / two | 2M / 4096 | TIMESTAMP_LTZ | 1.202599 | 1.108017 | 0.862292 | 1.395x |
| CUMULATE / two | 2M / 4096 | TIMESTAMP | 1.119121 | 1.108795 | 0.757615 | 1.477x |
| CUMULATE / two | 2M / 4096 | TIMESTAMP_LTZ | 1.122305 | 1.094508 | 0.853236 | 1.315x |
| SESSION / one | 2M / 4096 | TIMESTAMP | 1.086465 | 1.093417 | 0.645294 | 1.684x |
| SESSION / one | 2M / 4096 | TIMESTAMP_LTZ | 1.080101 | 1.080380 | 0.638098 | 1.693x |

Earlier candidate comparisons are retained as well. Two-million-row TUMBLE sweeps exercise
precision 3/9, both phases and 64/4096 keys. Their source precedes the final session merge and
selection-index ownership optimization, so those samples are not labeled final-head results.
Session merging originally took 1.873077/1.840005 seconds for TIMESTAMP/LTZ at 4096 keys,
before borrowing selected timestamps and merging owned extrema directly. Those separate
process runs have drifting stock controls; they do not isolate each optimization's causal gain.

The pre-integration single-phase overlapping candidate was rejected: at 4096 keys,
HOP took 1.009755/0.991080 seconds against stock 0.806110/0.895012, or
0.798x/0.903x for TIMESTAMP/LTZ. CUMULATE took 0.775960/0.772689 against
0.794308/0.926869. Those historical observations remain in the original artifact.

The integrated implementation reduces timestamp extrema and ordinary counts once per
input slice, then merges scalar states into each open window. It accounts fixed-size
state only when a group is created. Compact extrema use millisecond input vectors;
COUNT copies argument validity rather than unused values. Full-precision extrema retain
both timestamp components. The final Linux/JDK 17 comparison uses released Flink 2.2.1,
UTC, parallelism two, release+mimalloc, IPC exchanges, runtime row inputs and a collecting
row sink with both transposes. Two warmups precede three measured trials per engine;
stock, native and forced previous-window-fallback jobs rotate execution order.

At two million rows and 4096 keys, final precision-3 single-phase medians are:

| Window | Value | Stock Flink | Previous fallback | Native | Stock / native |
| --- | --- | ---: | ---: | ---: | ---: |
| HOP | TIMESTAMP | 0.988690 | 1.451889 | 0.863396 | 1.145x |
| CUMULATE | TIMESTAMP | 0.996699 | 0.994038 | 0.754463 | 1.321x |
| HOP | TIMESTAMP_LTZ | 0.857069 | 0.983879 | 0.772530 | 1.109x |
| CUMULATE | TIMESTAMP_LTZ | 0.963046 | 0.950239 | 0.747960 | 1.288x |

Final precision-9 single-phase medians with the same data and resources are:

| Window | Value | Stock Flink | Previous fallback | Native | Stock / native |
| --- | --- | ---: | ---: | ---: | ---: |
| HOP | TIMESTAMP | 1.012819 | 1.033396 | 0.882862 | 1.147x |
| CUMULATE | TIMESTAMP | 0.875389 | 0.880503 | 0.769802 | 1.137x |
| HOP | TIMESTAMP_LTZ | 0.868805 | 0.965907 | 0.751604 | 1.156x |
| CUMULATE | TIMESTAMP_LTZ | 0.858336 | 0.866395 | 0.743625 | 1.154x |

The eight final local/global HOP/CUMULATE comparisons at precision 3/9, including
both timestamp kinds and 4096 keys, are 1.058–1.245× faster than stock and also
beat their matched forced-fallback medians. Their samples are retained in the same artifact.

At 64 keys, the four final TIMESTAMP overlap cases span 1.018–1.183× stock.
The precision-3 medians are close: HOP is 0.749551 seconds versus stock 0.771266,
and CUMULATE is 0.739467 versus 0.753017. This startup-inclusive comparison
establishes a modest improvement in those cases, rather than a large throughput gain.

Final TIMESTAMP TUMBLE controls at precision 3/9 are 1.184/1.189× stock;
SESSION controls are 1.554/1.667×. These use two million rows and 4096 keys with
the same release library and full production boundary. The precision-3 high-cardinality
overlap samples above preceded removal of the planner guard; their native implementation
is identical to the final admitted build. Both released Flink lines then passed
188 Java checks (four existing capability skips on 1.18), alongside 686 native tests
and 48 release Criterion fixture checks.

Every trial compares all output values and RowKinds; current-job counters establish execution
of every intended native aggregate stage. Startup and scheduling remain included. Native HOP
TIMESTAMP samples range from 0.855970 to 0.969962 seconds; its fallback observations include
1.128929, 1.451889 and 2.215375 seconds. The
[integration artifact](../benchmarks/results/window-temporal-overlap-integration-2026-10-13.json)
retains every warmup, measured sample, unfavorable intermediate candidate, source fingerprint
and library hash. Earlier direct folding, temporary Arrow partials and compact-vector-only
candidates were slower and were superseded by the fixed-state-accounting implementation.

The 48 Rust release fixtures cover accounted TUMBLE/HOP/CUMULATE update/flush and SESSION update/merge/flush
at 256/4096 rows, one/64/all-distinct keys, NULLs and precision 3/9. The historical pre-integration precision-9
TUMBLE estimates are 65.21/73.92/809.31 microseconds for 4096 rows and one/64/4096 keys.
SESSION estimates are 218.11/282.93/3871.35 microseconds, including two input batches and
state merging. Setup stays outside timing; grouping, accumulation, output and disposal remain
inside. Native timing and allocation probes use the counting System allocator, separately
from the mimalloc complete jobs. The
[raw artifact](../benchmarks/results/window-temporal-extrema-2026-10-13.json)
retains every sample, estimate interval, allocation probe, source fingerprint and validation
record, including negative and intermediate candidates.

Reproduce complete-job comparisons with `SF_BENCHMARK=true mvn -Pbench
-pl streamfusion-runtime -am test -Dtest=WindowTemporalExtremaBenchmark
-Dsf.testForks=1 -Dsurefire.failIfNoSpecifiedTests=false`. Fixture properties are
`window.temporal.rows`, `window.temporal.groups`, `window.temporal.precisions`,
`window.temporal.phases`, `window.temporal.ltz`, `window.temporal.shapes`,
`window.temporal.warmup` and `window.temporal.runs`. SESSION uses ONE_PHASE;
HOP/CUMULATE support ONE_PHASE and TWO_PHASE under production admission.

## Filtered MIN/MAX on append-only input

Aligned event-time TUMBLE/HOP/CUMULATE admit `MIN(value) FILTER (WHERE predicate)` and
`MAX(value) FILTER (WHERE predicate)` for integer widths, FLOAT/DOUBLE, DECIMAL, CHAR/VARCHAR,
TIMESTAMP and TIMESTAMP_LTZ. Both single-phase and local/global execution apply each predicate
independently. FALSE or NULL masks the aggregate value, while the row still establishes its
window and group. A group with no selected non-NULL values emits NULL extrema.

The existing Arrow validity-mask preparation and extrema accumulators are reused. Local
partials already contain the filtered extrema; the global merge does not reapply the
predicate. No native buffer, checkpoint or JNI layout changes. SQL parity covers all numeric
types and window shapes, with native work required from each selected stage. Floating tests
cover selected/rejected NaNs, infinities and signed-zero ties. Memory and RocksDB restore
checks retain independent extrema and windows whose predicates reject every value.

Updating input still requires a retractable extrema state and falls back. Append-only
processing-time, session and legacy windows apply the same masks; attached windows apply
them in one or two phases. Rejected rows still establish and extend session membership.

## Retracting COUNT/SUM/AVG and grouping-only windows

Aligned event-time TUMBLE, HOP and CUMULATE accept updating input, including native Top-N,
for SUM/AVG over all numeric types, COUNT over the
supported value columns (including nullable collection parents), COUNT(*), and grouping-only windows without aggregate
functions. Both single-phase and local/global execution remain columnar.

Every input retains its INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE sign. SUM carries Flink's
nullable sum and signed non-NULL count; COUNT carries a signed count. A separate live-row
count, or an existing unfiltered COUNT(*), suppresses a final group only when that count equals zero.
A live all-NULL group therefore emits COUNT 0/SUM NULL, while an unmatched delete can produce
negative counts and sums as it does in released Flink. Integer sums preserve their declared
width and wrapping behavior. Top-1 replacing 10 with 20 leaves SUM 20, and moving the last
live row out of a window removes its old group.

`FILTER (WHERE predicate)` is supported for these COUNT/SUM/AVG forms on both insert-only
and updating input, in either aggregation phase. Each aggregate uses its own predicate:
FALSE and NULL skip that aggregate's addition or retraction, while TRUE retains the value's
normal NULL behavior. Every row still contributes to window membership. A filtered COUNT(*)
therefore cannot replace the unfiltered live-row count: an all-rejected live group emits
COUNT 0 and SUM/AVG NULL, and only removal of its last live row removes the group.

Filters are applied to each aggregate's Arrow value validity during the existing batch
preparation. Local operators apply them before emitting partials; global operators merge
those partials without filtering them again. Accumulator and checkpoint formats are unchanged.
Tests compare numeric widths, independent filters, NULL predicates, explicit late watermarks,
and recovery on memory and RocksDB backends with released Flink, and require native input/output
counters for each expected window stage.

FLOAT SUM rounds every addition/subtraction at FLOAT precision; DOUBLE SUM uses DOUBLE
precision. Both retain a nullable sum and signed BIGINT count. The first non-NULL insertion
or partial is assigned directly, preserving an initial negative zero. An unmatched retraction
starts from positive zero, as in Flink. Zero-count partials retain finite residual sums and
NaN after infinity cancellation; emitting NULL for an empty aggregate does not erase that state.
Local/global merging and checkpoint restore preserve these rules on both state backends.

DECIMAL SUM widens the buffer and result to DECIMAL(38, input scale), with the same signed
BIGINT count. Arithmetic overflow makes the sum NULL without changing the count; the next
non-NULL insertion or retraction restarts the sum with that signed value. A NULL partial
contributes its count but no sum. Zero-count partials retain their residual sum. These rules
match Flink's retracting decimal SUM and survive local/global merging and checkpoint restore.

Integer AVG uses the existing BIGINT sum/count pair, subtracting values and non-NULL counts
for retractions. A zero count produces NULL; negative counts still divide, matching Flink.
The sum wraps at 64 bits, division truncates toward zero, and the result narrows to its declared
integer type. Java's `Long.MIN_VALUE / -1` overflow is preserved. AVG partials, hidden group
liveness and checkpoints use the same layouts as append-only AVG and retracting COUNT/SUM.

FLOAT and DOUBLE AVG use a DOUBLE sum and signed BIGINT count, applying each insertion or
retraction in input order. FLOAT narrows only the final result. Partial sums merge in order
into the existing sum rather than being added together first; regrouping floating additions
can change the result. Zero-count partials preserve their sum, including NaN after retracting
an infinity. Checkpoints retain both fields. Tests compare signed-zero and nonfinite results
with Flink and restore mixed integer/floating averages on both state backends.

DECIMAL AVG keeps a DECIMAL(38, input scale) sum and signed BIGINT count, returning
DECIMAL(38, max(6, input scale)) with Flink's decimal division and rounding. Overflow is
sticky: later insertions, retractions and partial merges cannot repair a NULL sum. Zero-count
partials preserve both finite residuals and overflow. An empty signed accumulator stores
numeric zero, distinguishing it from overflow even when the count is zero. The existing
live-row-count configuration selects these semantics; append-only AVG checkpoints keep
their existing interpretation. No additional keyed state or JNI arguments are required.

A grouping-only window uses the same signed live-row count as COUNT/SUM, but keeps that
count out of the SQL result. It emits one row per nonzero `(key, window)` group, including
Flink's negative-count behavior after an unmatched delete. Deleting the last occurrence
removes the group; duplicates, NULL payloads and updates that move rows between windows
retain their normal membership semantics. The existing count partial and checkpoint layout
are shared by both aggregation phases and both state backends.

Local partials preserve the full (sum, count) pair and the live-row count in Flink's field
order. A checkpoint can split an insertion from its retraction: the next local partial may
be negative, and the global merges it into existing state. A zero-count partial must also
survive: replacing a value can change SUM without changing group membership. Closed slices
can still update
open HOP/CUMULATE windows, but cannot reopen a final window that has fired. All state fields
participate in memory checkpoints, raw keyed savepoints and direct RocksDB checkpoints.

SQL tests force a failure after a checkpoint containing live groups, then verify restored
updates, deletes, NULLs, duplicates and negative counts on both backends. Per-job operator
metrics require nonempty input and output from the native Top-N and the expected window
stages. Native tests also cover late changes after restore and canonical RocksDB-to-memory
state transfer. Updating DISTINCT and other remaining forms stay on Flink.

## Window COUNT(DISTINCT)

Unfiltered `COUNT(DISTINCT value)` is native for integer, FLOAT/DOUBLE, DECIMAL, CHAR/VARCHAR, DATE,
BOOLEAN, TIME, TIMESTAMP and TIMESTAMP_LTZ values. It ignores NULLs and counts each value once per key/window.
TUMBLE, HOP and CUMULATE support single-phase and local/global execution; attached window
results use the existing two-phase path. SESSION and admitted legacy group windows reuse the
same distinct accumulator. Processing-time windows retain their timer-driven lifetime.

BOOLEAN and TIME sets use typed partial values and ignore NULLs. TIME retains the internal
millisecond value even under a resolved TIME(0) declaration. Recovery checks continue
duplicate membership across raw and canonical memory/RocksDB state transitions.

Each local partial carries an Arrow list of distinct values. The global unions those lists,
so duplicates split across tasks or checkpoint barriers count once. Ordinary aggregates and
AVG's two-field partials can share the same window. Flink's extra MapView partial fields are
replaced by these lists throughout the native local/exchange/global pipeline.

`COUNT(DISTINCT value) FILTER (WHERE predicate)` is also native for the same value types
on append-only, aligned event-time TUMBLE/HOP/CUMULATE, in both aggregation phases.
FALSE/NULL predicates mask only that aggregate's value. Each distinct call keeps its own
selected set, including when several calls share an argument with different predicates.
The global stage unions the already-filtered local sets without applying the predicate
again. Rejected rows still establish groups; a live group with no selected non-NULL values
emits zero. Filtered distinct counts can share a window with ordinary aggregates.

This uses the existing Arrow validity masks and distinct list state, without a new JNI or
checkpoint format. SQL tests cover the admitted types, independent filters, nullable keys
and values, duplicates across batches and tasks, late watermarks, both aggregation phases,
and checkpoint continuation on memory and RocksDB. Native work is required from each window
stage. Updating DISTINCT input remains outside this extension. Append-only processing-time,
session and legacy windows also use the masks; attached windows retain shared membership state.

Timestamp distinct keys follow Flink's serialized key representation: precision 0–3 uses
milliseconds; precision 4–9 retains the fractional nanos. This matters when an internal cast
leaves fractions in a value declared as a compact timestamp.

Distinct sets are included in checkpoints and key-group snapshots, then removed with their
window on firing. Late raw input cannot recreate a closed window. Variable-sized list state
uses the existing snapshot fallback when the RocksDB backend is selected, rather than direct
per-window RocksDB rows. Tests cover checkpoint continuation and canonical savepoints in both
backend directions.

## Floating extrema

FLOAT/DOUBLE MIN/MAX initializes from the first non-NULL value and replaces it only when a
later value is strictly smaller/larger using primitive comparisons. A first NaN remains;
a later NaN does not replace a finite value. Signed zeros tie, preserving the earlier sign.
Batch updates and partial-state merges use the same nullable accumulator rule.

## Legacy group windows

The deprecated `GROUP BY TUMBLE(...)` and `GROUP BY HOP(...)` syntax is native for both event time
and processing time, reusing the same single-phase fixed-window operator as TVF-planned aggregates.
Legacy event-time and processing-time `SESSION(...)` are native too. Legacy group windows have no offset and never use
Flink's two-phase local/global optimization.

The auxiliary properties retain Flink's legacy layout and types: start and end are plain
`TIMESTAMP(3)`, rowtime is the window end minus one millisecond, and the internal proctime marker is
null before the outer Calc materializes the current clock. Queries that select no auxiliary window
property are native as well.

## Event-time assignment

`TUMBLE`, `HOP`, and `CUMULATE` aggregates admit explicit positive and negative SQL offsets
in one or two phases; `SESSION` needs no offset. Assignment offsets shift the aggregation
grid independently of time-zone rendering. Regional HOP/CUMULATE shared slices use the same
shifted grid and cumulative bucket base, including after restoration. The
window bounds render differently depending on the rowtime attribute's type: in the session time zone
for a local-time-zone attribute, in UTC (the raw wall-clock value) for a plain `TIMESTAMP`.

## Processing-time (proctime) assignment

`TUMBLE`/`HOP`/`CUMULATE` are native on proctime: each row is assigned to the window(s) covering the
operator's current processing-time clock and fired on a processing-time timer. `HOP` and `CUMULATE`
leave several windows open at once, so the timer chains — each firing emits the earliest-ending open
window and schedules the next slide boundary, until the clock has passed the latest open window's end.
Positive and negative SQL offsets shift both assignment and timer grids. Released slicing
TVFs require the slide/step to divide size. Legacy HOP also admits non-dividing and gapped
windows: timers follow the start grid shifted by the window size, so they fire at actual ends.

`SESSION` is native on proctime too: the gap is measured on the processing-time clock, and each batch
registers a cleanup timer at `now + gap` — the earliest the session could close with no further input.
A later element extends the session (merged in the native aggregator) and registers its own later
timer, so a firing emits only the sessions the clock has truly left behind by a full gap.

Proctime support is **single-phase** for TUMBLE/HOP/CUMULATE and SESSION. Both released
Flink planners forbid splitting processing-time windows because clock materialization must
happen in one node. A TWO_PHASE request therefore retains the native single-phase operator;
this is a released-host limitation, not a missing native merge path. Per-aggregate FILTER
also runs on these clocks, including filtered COUNT(*), without discarding rejected rows
from session bounds.

Watermarks, including the terminal `Long.MAX_VALUE` watermark of a bounded source, do not
fire processing-time windows. Completing bounded input also leaves an unfinished window open,
as in Flink; completion cannot substitute for advancing the processing-time clock. Checkpoints
preserve the open state and processing-time timer deadline for recovery. This applies to fixed
and session windows with either memory or disk state.

Because proctime results depend on wall-clock timing, they are non-deterministic — routing and
execution are tested, but the result is not byte-compared against Flink.

## Windowing TVF (window assignment)

The windowing TVF assigns rows to windows the same way as the aggregate above — by rowtime for
event-time, by the processing-time clock instead of a rowtime column for proctime — and is native
with positive, negative or zero SQL offsets for `TUMBLE`/`HOP`/`CUMULATE`; both event-time and
proctime assignment paths use the same shifted grid. SQL offsets do not shift the input payload
or get added a second time to the emitted window-time instant.

Standalone event-time TVFs accept both `TIMESTAMP(3)` and `TIMESTAMP_LTZ(3)` rowtime.
Plain `TIMESTAMP` retains its wall-clock boundaries in every session zone, including downstream
window Top-N and window deduplication. Native assignment preserves hidden sub-millisecond input
fractions while emitting millisecond window boundaries; negative epochs and years 0001/9999 are
covered by runtime SQL parity tests, including explicit positive and negative assignment offsets.

The TVF emits `window_start`/`window_end` as local wall-clock TIMESTAMP values, while
`window_time` stays an instant for LTZ input. Fixed offsets, including offsets not aligned with
the window slide, participate in assignment on the local grid. Regional zones use batched
released-Flink calendar conversion, including DST gaps, overlaps and historical offsets.
TVF, single/two-phase aggregates, joins and rank/dedup share the same local bounds. Stateful
consumers convert each local end to its released-Flink epoch timer deadline and fire at the
last millisecond, including after restoration. SESSION connectivity is computed in the local
assignment domain and uses the same deadline conversion.

Assignment reads Flink's millisecond component without changing the original timestamp payload.
In particular, `1969-12-31 23:59:59.999999999` belongs to the window before the epoch, just like
`TimestampData.getMillisecond() == -1`. NULL event-time rows are dropped; processing-time assignment
uses the clock even when the payload's timestamp is NULL.

The native assignment kernel reads the timestamp pair and all primitive Arrow timestamp units.
SQL window boundaries use the same lossless pair as input timestamps, so a daily window after
2262 retains both its date and its correct millisecond boundaries.

A downstream [window join](joins/window-join.md) or window Top-N/dedup consuming the TVF's output
closes windows on a chained processing-time timer (the same next-slide-boundary model described
above) rather than a watermark, under the same slide-divides-size constraint — see those operators'
own pages for their admission conditions.

### Standalone plain-TIMESTAMP measurement

`PlainTimestampTvfBenchmark` measures a row source through standalone assignment to a rowwise
blackhole sink, with both row/Arrow transposes verified in the native plan. On a local release
build (`-Pbench`, mimalloc), 2 million input rows, parallelism 1, 4096 cyclic time samples, NULL
every eighth row, two warm-ups and five interleaved measured runs per engine gave these medians:

| Assignment | Flink seconds | Native seconds | Flink/native |
| --- | ---: | ---: | ---: |
| TUMBLE 10 s | 0.492 | 0.820 | 0.600x |
| HOP 5 s / 10 s | 0.734 | 1.125 | 0.652x |
| CUMULATE 5 s / 10 s | 0.614 | 0.958 | 0.640x |

The plain timestamp session zone is America/Los_Angeles; the process runs with `TZ=UTC`.
Sink rowtime insertion is disabled for both engines because the projection contains both the
original rowtime and window_time. These standalone shapes are slower than Flink. The coverage
enables columnar composition with downstream consumers; it is not a standalone throughput win.

## Matcher declines

- Fixed-window assignments outside the supported TUMBLE/HOP/CUMULATE shapes or timestamp attributes.
- Standalone SESSION TVFs and SESSION attached-bound consumers. Fused SESSION aggregates
  are native; the remaining assignment contexts stay under the original window-context audit.
- Internal processing-time local/global plans; both released planners retain one phase.
- Slicing TVF `HOP` slide / `CUMULATE` step that doesn't divide size; released Flink rejects them.
- Legacy row-count `TUMBLE`/`HOP` windows from the Table API.
- Legacy early/late firing or allowed lateness.
- A grouping key outside the [verified scalar types](#scalar-grouping-and-partition-keys).
- A value type/aggregate mismatch.
  DATE/TIME/BOOLEAN and TIMESTAMP/TIMESTAMP_LTZ values are admitted for append MIN/MAX
  and COUNT(DISTINCT); updating MIN/MAX uses the fixed-window forms below.
  Ordinary COUNT(value) also admits character, VARBINARY, both interval families, DATE, TIME, BOOLEAN, timestamp, ARRAY, MAP, ROW and MULTISET values;
  it counts validity without interpreting the payload.
- Single-phase string MIN/MAX when explicitly disabled with
  `streamfusion.window.string-extrema.single-phase.enabled=false`. All otherwise supported
  shapes default native; local/global admission is unchanged.
- Attached bounds outside TUMBLE/HOP/CUMULATE with admitted aggregates.
  Otherwise supported single-phase attached bounds are admitted by default;
  `streamfusion.window.attached.single-phase.enabled=false` forces stock execution.
- Windowed DISTINCT other than single-argument COUNT over the types listed above:
  SUM/AVG DISTINCT and complex values.
  Non-windowed DISTINCT has separate coverage; see [GROUP BY](group-by.md).
- Updating DISTINCT windows. Other updating windows admit grouping-only, numeric SUM/AVG
  (including shared exact SUM0), typed MIN/MAX and ordinary COUNT with optional FILTER.
  Fixed and SESSION windows support both clocks; released Flink 1.18 still rejects updating TVFs.
  Attached bounds retain their assigned window and row kinds. Admission checks the **input** changelog
  even when final output is append-only.
  The diagnostic names the supported retracting forms. Remaining coverage is tracked in
  [#99](https://github.com/datafusion-contrib/StreamFusion/issues/99).
- Unverified physical layouts from Flink's optional
  `table.optimizer.distinct-agg.split.enabled=true` rewrite. With two-phase aggregation,
  the verified TUMBLE/HOP/CUMULATE layouts now run natively: `HASH_CODE` assigns distinct
  buckets and exact SUM0 reductions merge their counts. The unchanged upstream DISTINCT
  window tests require nonempty work from both native local and global stages. SQL parity
  also covers multiple distinct arguments, mixed exact aggregates, NULL-only groups, result
  types and RowKinds. The operator-format audit also admits the verified attached
  `ONE_PHASE` layouts; typed regressions compare both phases. Other split layouts remain
  subject to the individual aggregate and partial-layout gates, tracked in
  [#166](https://github.com/datafusion-contrib/StreamFusion/issues/166).

A **zero-aggregate grouping-only window** (`GROUP BY key + window`, no aggregate function)
is a windowed distinct, emitting one row per `(key, window)`. It supports insert-only input
and the aligned retracting event-time forms above, in single- and two-phase execution.
See [GROUP BY](group-by.md) for how the non-windowed case handles `DISTINCT`.

## Retracting window benchmark

`RetractingWindowBenchmark` runs Top-1 by descending nullable BIGINT, followed by COUNT/SUM
in a 2-second/10-second HOP. Released Flink 2.2.1 is the baseline, matching the prior complete
fallback path. Runs use `-Pbench`, parallelism 2, 64 keys, two warmups and five interleaved
measurements per engine. The row source, native Top-N, native window stages, both transposes
and rowwise blackhole sink stay in the measured path. No competing builds or tests ran during
measurement. Medians:

| Input rows | Strategy | Flink (s) | Native (s) | Flink / native |
| --- | --- | ---: | ---: | ---: |
| 1 million | Single-phase | 0.397698 | 0.472575 | 0.842× |
| 1 million | Local/global | 0.445563 | 0.458983 | 0.971× |
| 10 million | Single-phase | 2.712645 | 4.177479 | 0.649× |
| 10 million | Local/global | 3.042152 | 3.860909 | 0.788× |

Both sizes were slower than Flink. This implementation establishes native changelog and
checkpoint coverage for complete updating pipelines and future batching improvements.
These whole-query results do not isolate the cost of Top-N, window accumulation or exchanges.

With `-Dwindow.filtered=true`, both COUNT and SUM accept only even values using aggregate
FILTER clauses. The same release/mimalloc setup, 1 million rows, parallelism 2, 64 keys,
two warmups and five interleaved measured runs gave these medians:

| Strategy | Flink (s) | Native (s) | Flink / native |
| --- | ---: | ---: | ---: |
| Single-phase | 0.490809 | 0.488290 | 1.005× |
| Local/global | 0.591475 | 0.479669 | 1.233× |

Single-phase was approximately even; local/global was faster for this workload. Both
transposes, native Top-N/window stages and the rowwise sink remain in the measured plan.
No competing builds or tests ran during measurement. These whole-query results do not
isolate filter-mask overhead or establish a gain for other selectivities and cardinalities.

With `-Dwindow.groupingOnly=true`, the same benchmark selects only the grouping key, retaining
Top-N, both transposes and the rowwise sink. On an M1 Max, a release build (`-Pbench`, mimalloc),
1 million rows, parallelism 2, 64 keys, two warmups and five interleaved measured runs gave:

| Strategy | Flink (s) | Native (s) | Flink / native |
| --- | ---: | ---: | ---: |
| Single-phase | 0.456570 | 0.487513 | 0.937× |
| Local/global | 0.477981 | 0.468305 | 1.021× |

No competing builds or tests ran during measurement. Single-phase was slightly slower and
local/global was approximately even. Grouping-only admission extends the existing changelog
and checkpoint foundation; this measurement does not establish a throughput improvement.

With `-Dwindow.average=true`, the benchmark selects COUNT and integer AVG. Under the same
release/mimalloc setup, 1 million rows, two warmups and five interleaved measured runs gave:

| Strategy | Flink (s) | Native (s) | Flink / native |
| --- | ---: | ---: | ---: |
| Single-phase | 0.420240 | 0.509358 | 0.825× |
| Local/global | 0.532535 | 0.485407 | 1.097× |

Both transposes, Top-N and the rowwise sink remain timed, with no competing local builds or
tests. Local/global improved on this workload; single-phase remained slower than Flink.

The same benchmark with `-Dwindow.average=true -Dwindow.averageType=FLOAT` or `DOUBLE`
casts the input to that type before AVG. With the same release/mimalloc setup, 1 million rows,
two warmups and five interleaved measured runs:

| AVG type | Strategy | Flink (s) | Native (s) | Flink / native |
| --- | --- | ---: | ---: | ---: |
| FLOAT | Single-phase | 0.427617 | 0.473997 | 0.902× |
| FLOAT | Local/global | 0.572479 | 0.465843 | 1.229× |
| DOUBLE | Single-phase | 0.500904 | 0.496658 | 1.009× |
| DOUBLE | Local/global | 0.613188 | 0.487260 | 1.258× |

Both transposes, Top-N and the rowwise sink remain timed; no competing local builds or tests
ran. Local/global was faster for both types. Single-phase FLOAT was slower and DOUBLE was
approximately even. These results describe the complete query rather than isolated AVG cost.

With `-Dwindow.sumType=FLOAT` or `DOUBLE`, the same benchmark measures COUNT and SUM of
the cast input. The same release/mimalloc setup, 1 million rows, two warmups and five
interleaved measured runs gave:

| SUM type | Strategy | Flink (s) | Native (s) | Flink / native |
| --- | --- | ---: | ---: | ---: |
| FLOAT | Single-phase | 0.496960 | 0.489965 | 1.014× |
| FLOAT | Local/global | 0.627083 | 0.466394 | 1.345× |
| DOUBLE | Single-phase | 0.451853 | 0.486156 | 0.929× |
| DOUBLE | Local/global | 0.619748 | 0.478603 | 1.295× |

Both transposes, Top-N and the rowwise sink remain timed; no competing local builds or tests
ran. Local/global improved for both types. Single-phase FLOAT was approximately even and
DOUBLE was slower. These are complete-query timings, not isolated SUM measurements.

With `-Dwindow.sumType=DECIMAL(12,2)`, SUM returns DECIMAL(38,2). Under the same
release/mimalloc setup, 1 million rows, two warmups and five interleaved measured runs:

| Strategy | Flink (s) | Native (s) | Flink / native |
| --- | ---: | ---: | ---: |
| Single-phase | 0.513898 | 0.496961 | 1.034× |
| Local/global | 0.634582 | 0.488709 | 1.298× |

Both transposes, Top-N and the rowwise sink remain timed, with no competing local builds or
tests. Single-phase was approximately even; local/global improved on this workload.

With `-Dwindow.average=true -Dwindow.averageType=DECIMAL(12,2)`, AVG returns
DECIMAL(38,6). The same release/mimalloc setup, 1 million rows, two warmups and five
interleaved measured runs gave:

| Strategy | Flink (s) | Native (s) | Flink / native |
| --- | ---: | ---: | ---: |
| Single-phase | 0.487469 | 0.516194 | 0.944× |
| Local/global | 0.610879 | 0.505131 | 1.209× |

Both transposes, Top-N and the rowwise sink remain timed, with no competing local builds or
tests. Single-phase was slower; local/global improved on this workload. These are complete
query timings rather than isolated decimal arithmetic measurements. The single-phase path
adds verified native composition and recovery coverage without claiming a speedup.

## Shared exact aggregate benchmarks

`SharedWindowSumBenchmark` runs the reduced `COUNT(v), AVG(v), SUM(v)` query over a
runtime row source and a collecting row sink. Native plans require both transposes and
the selected single-phase or local/global window stages. Every measured trial compares
the complete values and RowKinds with released Flink. The October 12, 2026 comparison
uses Flink 2.2.1, JDK 17.0.12, release+mimalloc, parallelism 2, eight million nullable
rows, 4,096 keys, a ten-second TUMBLE, two warmups and five alternating observations.
DECIMAL inputs are DECIMAL(12,2), summed at DECIMAL(38,2).

| Phase / input | Matched stock seconds | Native seconds | Stock/native |
| --- | ---: | ---: | ---: |
| Single / BIGINT | 1.455581 | 1.098882 | 1.325x |
| Single / DECIMAL | 1.554659 | 1.517730 | 1.024x |
| Local/global / BIGINT | 2.179928 | 1.382075 | 1.577x |
| Local/global / DECIMAL | 2.212144 | 1.627779 | 1.359x |

Previous main executed the same queries through Flink fallback. Its native-enabled
medians, measured in separate processes, were 1.227292, 1.358161, 1.992226 and
1.995531 seconds in the same table order. Stock controls also drift between runs;
the initial single-phase DECIMAL result does not establish improvement over that fallback.
The matched 1.024x comparison is near parity, with overlapping sample ranges. The
subsequent same-process controls below address that uncertainty. These complete jobs include startup and collection
polling as well as processing; the one-million-row/64-key and 4,096-key runs remain
in the artifact rather than being discarded as startup-dominated results.

The native COUNT/SUM0 update/flush fixtures separately record all timings and allocation
probes for 256/4,096 rows and 1/64/all-distinct keys. See the
[Criterion inventory](../benchmarks/native-criterion.md) and
[raw samples and configuration](../benchmarks/results/window-sum0-2026-10-12.json).

Reproduce the complete jobs with
`SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench
-Dtest=SharedWindowSumBenchmark -Dsurefire.failIfNoSpecifiedTests=false
-Dwindow.shared.rows=8000000 -Dwindow.shared.groups=4096`.
The test requires at least one warmup and three measured observations. For a same-process
fallback control, add `-Dwindow.shared.compareFallback=true`; this rotates stock Flink,
native windows and a native-planner job with windows disabled through the existing
per-operator switches. The latter requires zero substitutions and checks the full
result against stock, reproducing the previous whole-query fallback execution path.

Focused single-phase DECIMAL controls retain the eight-million-row fixture, three
warmups and nine observations for each engine in one JVM. All results agree. Two
independent 4,096-key controls and a 64-key workload measured:

| Keys / run | Stock seconds | Fallback seconds | Native seconds | Stock/native | Fallback/native |
| --- | ---: | ---: | ---: | ---: | ---: |
| 4,096 / A | 1.557810 | 1.563126 | 1.317393 | 1.182x | 1.187x |
| 64 | 1.443835 | 1.472860 | 0.978591 | 1.475x | 1.505x |
| 4,096 / B | 1.571749 | 1.561060 | 1.417395 | 1.109x | 1.101x |

Native sample ranges at 4,096 keys were 1.286-1.412 s and 1.385-1.513 s;
matched stock ranges were 1.542-1.668 s and 1.549-1.898 s. At 64 keys, native
was 0.960-0.994 s versus stock 1.412-1.680 s. These controls establish an improvement
over both stock and the previous fallback path on the measured workloads, supporting
default admission without claiming every DECIMAL workload is faster. They also show
why a cross-process comparison with the earlier 1.358 s fallback median is insufficient.
Append `-Dwindow.shared.phases=ONE_PHASE -Dwindow.shared.types=DECIMAL
-Dwindow.shared.warmup=3 -Dwindow.shared.runs=9
-Dwindow.shared.compareFallback=true` to reproduce; select 64 or 4,096 keys.

## Mixed AVG benchmark

`MixedWindowAvgBenchmark` compares mixed COUNT/AVG/SUM/MIN/MAX over a 2-second/10-second HOP
with released Flink 2.2.1. A release build (`-Pbench`), 1 million rows, parallelism 2, 64 keys,
nullable INT/BIGINT values, two warmups and five interleaved measured runs gave these medians.
The row source, both row/Arrow transposes and the rowwise blackhole sink remain in the measured
path; the test asserts the expected single- or two-phase native window plan.

| Phase | Flink seconds | Native seconds | Flink/native |
| --- | ---: | ---: | ---: |
| Single | 0.501354 | 0.481034 | 1.042x |
| Local/global | 0.569200 | 0.514087 | 1.107x |

These are small local gains; the primary change is coverage for mixed aggregates and paired
AVG partials, including narrow integer and FLOAT result types, decimal overflow, and restore.

With `-Dwindow.distinct=true`, the same benchmark replaces COUNT(*) with COUNT(DISTINCT v),
retaining the mixed ordinary aggregates and both transposes. On an M4 Pro with JDK 17 and
`TZ=UTC`, a release build (`-Pbench`, mimalloc), 1 million rows, parallelism 2, 64 keys,
two warmups and five interleaved measured runs gave:

| Distinct phase | Flink seconds | Native seconds | Flink/native |
| --- | ---: | ---: | ---: |
| Single | 0.578242 | 0.536922 | 1.077x |
| Local/global | 0.681199 | 0.583311 | 1.168x |

These local measurements cover repeated nullable BIGINT values in overlapping windows;
they do not establish a gain for every distinct value type or cardinality. They were rerun
with the late-slice correction. This on-time workload is a performance control for that
correctness fix, not a measurement of late-data throughput or a before/after speedup.

### Filtered extrema benchmark (2026-09-18)

With `-Dwindow.filteredExtrema=true`, this mixed workload keeps COUNT/AVG/SUM and applies
`MOD(v, 2) = 0` to MIN and `MOD(v, 3) = 0` to MAX. Apple M1 Max, JDK 17, released Flink
2.2.1, release/mimalloc, 1 million rows, parallelism 2, 64 keys, two warmups and five
alternating trials measured:

| Filtered extrema phase | Flink seconds | Native seconds | Flink/native |
| --- | ---: | ---: | ---: |
| Single | 0.498391 | 0.494711 | 1.007× |
| Local/global | 0.608975 | 0.516241 | 1.180× |

The single-phase result is effectively tied; local/global was faster on this workload.
These are complete mixed-query measurements, including the row source, both transposes
and the rowwise blackhole sink, with no competing local builds or tests. They do not
isolate extrema or filter-mask cost or establish a gain at other cardinalities/selectivities.
[Raw measured trials](../benchmarks/filtered-window-extrema-2026-09-18.csv) accompany the medians.
Reproduce after building the release native library with:

```sh
SF_BENCHMARK=true mvn -B -ntp -Pbench -pl :streamfusion-runtime -am \
  -Dnative.build.skip=true -Dtest=MixedWindowAvgBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false -Dwindow.filteredExtrema=true \
  -Dwindow.rows=1000000 -Dwindow.warmup=2 -Dwindow.runs=5 test
```

### Filtered COUNT(DISTINCT) benchmark (2026-09-18)

`-Dwindow.filteredDistinct=true` selects `COUNT(DISTINCT v) FILTER (WHERE MOD(v, 3) = 0)`
in the same mixed-aggregate HOP workload, retaining ordinary AVG/SUM/MIN/MAX. On Apple M1
Max, JDK 17 and released Flink 2.2.1, release/mimalloc, 1 million rows, parallelism 2,
64 keys, two warmups and five alternating measured trials gave:

| Filtered distinct phase | Flink seconds | Native seconds | Flink/native |
| --- | ---: | ---: | ---: |
| Single | 0.587880 | 0.557623 | 1.054× |
| Local/global | 0.685695 | 0.582893 | 1.176× |

The single-phase difference is small and trial ranges overlap. These local medians measure
complete mixed queries with nullable BIGINT values, the row source, both transposes and
the rowwise blackhole sink; no other local build or test competed with the benchmark.
They do not isolate distinct filtering cost or establish a gain for other cardinalities,
selectivities or value types. [Raw trials](../benchmarks/filtered-window-distinct-2026-09-18.csv)
retain every measured iteration. Reproduce with the release library and:

```sh
SF_BENCHMARK=true mvn -B -ntp -Pbench -pl :streamfusion-runtime -am \
  -Dnative.build.skip=true -Dtest=MixedWindowAvgBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false -Dwindow.filteredDistinct=true \
  -Dwindow.rows=1000000 -Dwindow.warmup=2 -Dwindow.runs=5 test
```

## Idle-state TTL

Flink applies no idle-state TTL to window operators — `table.exec.state.ttl` changes nothing here;
windows are bounded by their own firing and eviction instead. Contrast with [`OVER`](over.md), which
does run TTL natively across all four of its frame shapes. See [Configuration](../configuration.md)
for the TTL flag surface.

## Fixed-offset TVF benchmark

`LtzWindowTvfBenchmark` compares standalone assignment with Flink 2.2.1 using a release native
build (`-Pbench`), 2 million rows, parallelism 1, two warmups and five interleaved measured runs.
The session zone is `GMT+08:00`; every eighth timestamp is NULL and the other rows cycle across
negative and positive epochs with fractional milliseconds. Both row/Arrow transposes and the
blackhole sink remain in the measured path.

| Shape | Flink median | Native median | Flink / native |
| --- | ---: | ---: | ---: |
| TUMBLE 10s | 0.755124s | 0.935219s | 0.807x |
| HOP 5s / 10s | 1.092357s | 1.323315s | 0.825x |
| CUMULATE 5s / 10s | 0.921502s | 1.123302s | 0.820x |

These standalone native plans are slower than Flink. The boundary correction is required for
correctness of the existing native path; these results do not establish a performance benefit.


The legacy `TUMBLE_PROCTIME` SQL property regression keeps an ongoing input alive
until a clock timer emits a completed window, and checks materialization in both
stock and native execution. Bounded input completion is not a substitute for that
timer; fixed-clock operator tests separately verify that finish and terminal
watermarks leave processing-time windows open.

Final legacy group-window output without window properties moves unchanged key and aggregate
Arrow buffers into vectors carrying the declared output schema. Outputs carrying properties keep
the original preallocated copy path. Integer narrowing and window timestamp rendering retain their conversion loops;
closing the imported native flush does not invalidate emitted batches. See the
[allocation technique](../optimizations/allocation-discipline.md#final-window-output-buffers).

## Output handoff benchmarks

The opt-in `WindowOutputHandoffBenchmark` times native final flush, Arrow C Data import, and
Java output shaping with distinct output groups. Its nullable aggregate results, long string
keys, narrowed integer keys, and fractional timestamp keys cover compatible buffer handoffs
and required type conversions. Input/state setup and result validation stay outside timing.

`WindowOutputHandoffJobBenchmark` measures complete stock Flink and native jobs, including the
native input/output transposes and collecting sink. Defaults are 262,144 input rows and 65,536
output groups, with BIGINT and 64-byte STRING keys, nullable sums, counts, and window bounds.
It alternates stock/native execution order, validates every output group, and prints the native
row-sink plan witness plus all samples. `EXPLAIN SELECT` omits its collecting sink, so the
witness explains an equivalent insert into a row sink; the measured query still collects and
validates every result. Run with `SF_BENCHMARK=true mvn test -Pbench
-Dtest=WindowOutputHandoffJobBenchmark -Dhandoff.job.warmup=2 -Dhandoff.job.runs=5`.
Use the same fixture, release native libraries, JDK, and machine for the saved previous-native
baseline and candidate; compare each against its stock run rather than comparing separate
workloads. Recorded raw samples and limitations are linked in the allocation technique. The final recorded
shaping and legacy complete-job pair ran with background workloads explicitly accepted by the
user; its raw samples, control variation, and complete-job regressions are documented. Earlier
provisional measurements remain archived; buffer-sharing validation is independent of timing.

`WindowOutputShapingBenchmark` isolates the handoff further: a benchmark-only subclass exports
prebuilt native-shaped Arrow results, then invokes the unchanged production `emitFinal` method.
Timing includes C Data export/import, output shaping, and Arrow emission, with no native aggregate
computation. Each fixture runs with both zero window properties (valid legacy group-window output)
and two properties (the usual start/end), reporting `properties=0` or `properties=2`. The zero-property
case measures buffer handoff without timestamp rendering; the two-property case measures the
original copy and conversion path, providing a control where transfer is disabled. Source buffers are prepared before timing and closed before output validation;
result-buffer address equality reports sharing directly. Nullable long results, nullable long
string keys, timestamp component keys, and integer narrowing use the same three output sizes.
Run with `SF_BENCHMARK=true mvn test -Pbench -Dtest=WindowOutputShapingBenchmark
-Dshaping.warmup=20 -Dshaping.runs=31` for baseline and candidate.

For focused control runs, shaping accepts comma-separated `-Dshaping.shapes=INT,TIMESTAMP`,
`-Dshaping.rows=16384`, and `-Dshaping.properties=2`; omitted selectors keep all 30 fixtures.
Selectors matching nothing fail instead of producing an empty successful benchmark. These controls
can use longer warmup and identical JVM compilation flags for baseline/candidate runs without
changing the production path or measured boundary.

For zero-property legacy group-window complete jobs, append `-Dhandoff.job.properties=0`.
The benchmark verifies the group-window native node and both transposes, collecting and checking
key/count/sum results without window fields.

## String extrema measurements

`WindowStringExtremaBenchmark` compares runtime nullable STRING MIN/MAX plus COUNT(*) against
stock Flink and the previous whole-query fallback. It requires both transposes, the expected
single-phase or local/global operators and positive consumed-row metrics for the current native
job. Each trial compares values and RowKinds. Strings vary within each key, every seventh value
is NULL and one key contains only NULLs.

On 2026-10-13, Apple M4 Pro, JDK 17.0.12, released Flink 2.2.1, UTC, parallelism 2 and
release/mimalloc, two warmups and five rotating-order samples per engine gave these medians.
These measurements precede the rebase onto #336: the implementation base was `a3d62316`,
and the old native control was an unshipped admission-only prototype. Its whole-query fallback
control represents that earlier main, not #336's now-admitted local/global string windows.
Single-phase measurements explicitly enable
`streamfusion.window.string-extrema.single-phase.enabled`; the default was off at that revision.
The bench profile keeps Arrow IPC shuffle enabled. SQL submission through the collecting rowwise
sink is timed; the shared MiniCluster startup and view registration are outside measurement.

| Rows | Keys | Phase | String bytes | Flink (s) | Native (s) | Previous fallback (s) | Flink/native |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 2,000,000 | 64 | ONE_PHASE | 16 | 0.648232 | 0.437016 | 0.656118 | 1.483x |
| 2,000,000 | 64 | ONE_PHASE | 256 | 0.651583 | 0.534951 | 0.652633 | 1.218x |
| 2,000,000 | 64 | TWO_PHASE | 16 | 0.666331 | 0.429723 | 0.672041 | 1.551x |
| 2,000,000 | 64 | TWO_PHASE | 256 | 0.668973 | 0.535110 | 0.664206 | 1.250x |
| 2,000,000 | 4096 | ONE_PHASE | 16 | 0.666074 | 0.542064 | 0.665584 | 1.229x |
| 2,000,000 | 4096 | ONE_PHASE | 256 | 0.926774 | 0.811265 | 0.928040 | 1.142x |
| 2,000,000 | 4096 | TWO_PHASE | 16 | 0.885648 | 0.639103 | 0.885113 | 1.386x |
| 2,000,000 | 4096 | TWO_PHASE | 256 | 1.252294 | 0.917272 | 1.260347 | 1.365x |
| 20,000,000 | 4096 | ONE_PHASE | 256 | 6.655892 | 5.621734 | 6.557417 | 1.184x |

The unshipped admission-only prototype was slower for high-cardinality wide strings. Replacing
contiguous per-key `take` copies with Arrow slices removes that gap; noncontiguous and nested
values retain `take`. Some short jobs vary by about 0.1 seconds. The 20M-row control provides a longer processing
boundary; its medians improve on both stock Flink and the previous fallback. These are local complete-job diagnostics, not isolated operator throughput,
a production SLA or exhaustive performance coverage for every admitted window shape/backend.
All samples, ranges and the prototype control are retained in
[the result artifact](../benchmarks/results/window-string-extrema-2026-10-13.json).

```sh
SF_BENCHMARK=true mvn -B -ntp -Pbench -pl streamfusion-runtime -am test \
  -Dtest=WindowStringExtremaBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dwindow.string.rows=2000000 -Dwindow.string.groups=4096 \
  -Dwindow.string.warmup=2 -Dwindow.string.runs=5
```

Use `-Dwindow.string.groups=64` for the lower cardinality and
`-Dwindow.string.rows=20000000 -Dwindow.string.widths=256 -Dwindow.string.phases=ONE_PHASE`
for the longer control. Native Criterion's `window_string_extrema` suite separately verifies
and measures production update/flush at 256/4096 rows, 1/64/batch-sized keys and 16/256-byte
nullable UTF-8; it excludes construction and the JVM boundary.

### Revalidation against merged main #336

The follow-up comparison uses main `b2e6e629` as the actual native baseline, including its
string admission, with unchanged production Java in the candidate. The matched Criterion
harness is added to the baseline without changing its Rust operator. Complete jobs use
release/mimalloc with the same setup, two warmups and five observations as above: 2M rows,
4096 keys, nullable strings, both transposes and a collecting row sink. All eight fixtures
compare output and verify current-job native work.

| Phase | String bytes | Main Flink (s) | Main native (s) | Candidate Flink (s) | Candidate native (s) | Candidate Flink/native |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ONE_PHASE | 16 | 0.562129 | 0.651480 | 0.662468 | 0.548461 | 1.208x |
| ONE_PHASE | 256 | 1.024714 | 1.020580 | 0.930639 | 0.811809 | 1.146x |
| TWO_PHASE | 16 | 0.778079 | 0.753612 | 0.883681 | 0.637581 | 1.386x |
| TWO_PHASE | 256 | 1.263004 | 1.224792 | 1.258469 | 0.920997 | 1.366x |

Main and candidate run in separate JVMs; their stock baselines differ, so the observed
1.18-1.33x native-before/after ratios are not confidence bounds. Candidate native beats its
matched stock and forced-fallback controls in these TUMBLE fixtures. This does not establish
HOP/CUMULATE performance. Admission was opt-in at that revision; the current operator-format
audit is linked above. No samples are discarded.
The [new result artifact](../benchmarks/results/window-string-extrema-main336-2026-10-13.json)
retains all complete-job samples, library hashes and six matching Criterion cases with raw
samples and confidence intervals. The [optimization ledger](../optimizations/string-copy-reduction.md#window-groups-borrow-contiguous-value-ranges)
records the native gains and the noncontiguous control's small regression.

### BOOLEAN/TIME DISTINCT performance

Released Flink 2.2.1/JDK 17, release/mimalloc, 2 million rows, 64 keys, parallelism
two, two warmups and three alternating measured jobs per engine. The complete job
includes startup, runtime row generation, both transposes, Arrow IPC and the row
blackhole sink. Split-distinct is disabled. Previous StreamFusion falls back for
these types; the same harness asserts that absence of native window aggregation.

| Value | Phase | Window | Stock mean seconds | Previous SF seconds | Native mean seconds | Stock/native |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| boolean | ONE_PHASE | TUMBLE | 1.581753 | 1.559476 | 1.206371 | 1.31× |
| boolean | ONE_PHASE | HOP | 1.638337 | 1.655612 | 1.155592 | 1.42× |
| boolean | ONE_PHASE | CUMULATE | 1.463504 | 1.462124 | 1.117949 | 1.31× |
| boolean | TWO_PHASE | TUMBLE | 1.742704 | 1.642221 | 1.117088 | 1.56× |
| boolean | TWO_PHASE | HOP | 1.736487 | 1.649931 | 1.144570 | 1.52× |
| boolean | TWO_PHASE | CUMULATE | 1.671661 | 1.649290 | 1.095747 | 1.53× |
| time | ONE_PHASE | TUMBLE | 1.471052 | 1.448388 | 1.142120 | 1.29× |
| time | ONE_PHASE | HOP | 1.543498 | 1.491625 | 1.146777 | 1.35× |
| time | ONE_PHASE | CUMULATE | 1.456913 | 1.434737 | 1.127920 | 1.29× |
| time | TWO_PHASE | TUMBLE | 1.699655 | 1.650318 | 1.092664 | 1.56× |
| time | TWO_PHASE | HOP | 1.721434 | 1.679724 | 1.127246 | 1.53× |
| time | TWO_PHASE | CUMULATE | 1.720180 | 1.666371 | 1.097980 | 1.57× |

All 12 measured forms outperform stock and the previous fallback on this workload.
[Raw trials, source/library hashes and configuration](../benchmarks/results/window-basic-distinct-2026-10-13.json)
retain every sample. These elapsed-time means include startup and are separate from
the sustained-throughput method used for Top-1. Other workloads may differ.
The 20 native fixtures cover 64/4,096 rows, one/64/all-row keys, NULLs and both
single-phase updates and local-partial/global unions. Cross-line SQL/recovery checks
passed 57 cases on 2.2 and 56 plus one existing capability skip on 1.18.

## Calendar window validation

Nonaligned fixed offsets and regional LTZ calendars use the released SDK for local assignment,
visible window time and timer conversion. The runtime tests compare DST gaps/overlaps, historical
offsets, negative epochs, shared slices and SESSION connectivity against independent stock
operators, including heap/RocksDB restore and backend transitions. SQL controls cover TVF,
single/two-phase aggregation, join, rank and deduplication; completed-checkpoint controls verify
restored windows still fire and drop late rows correctly. The final regression selection passes
155 checks on each released Flink line, including SESSION connectivity, processing-time planner
coverage, mixed built-in/string DISTINCT shared windows, joins, ranks and late-slice controls.

The release fixture suites contain 1,496 cases, including persistent timestamp payloads. Complete-job comparisons use
`CalendarWindowBenchmark` with a row source, row blackhole sink, both transposes, Arrow IPC and
production state. Stock and the previous fallback are separate controls; all warmups and measured
trials are retained in [the calendar artifact](../benchmarks/results/calendar-windows-2026-10-14.json).
The original 36-profile matrix, SESSION jobs, all nine wide-key profiles and the
additional persistent consumers are retained by revision. The final regional TVF
recheck averages about 1.07× stock for TUMBLE, HOP and CUMULATE. Persistent HOP/CUMULATE
rank uses 200,000 rows and 64 keys; its means are about 20.4×/16.6× stock. The
100,000-row, 4,096-key regional HOP join averages 1.65× stock, and the million-row
regional CUMULATE dedup averages 1.18× stock. These comparisons include the rowwise
source/sink, IPC, JNI and state work. Stock rank/join variability remains visible.

Two persistent aggregate cases remain slower: with one million rows and 64 keys,
HOP averages 0.690s native versus 0.651s stock, and CUMULATE 0.644s versus 0.590s.
Improvement over an earlier native revision does not establish acceleration; these
limits require a maintainer decision before this candidate ships. No trial is
discarded, including the earlier fallback outlier. The candidate remains under
review in [PR #341](https://github.com/datafusion-contrib/StreamFusion/pull/341).


### Single-phase aggregation over attached bounds

Append-only event-time results from fixed-grid TUMBLE/HOP/CUMULATE windows are
re-aggregated natively in ONE_PHASE plans by default, including regional calendars, wider
overlap and upstream aggregates beyond COUNT. The outer aggregate may mix the existing
supported numeric SUM/MIN/MAX/COUNT/AVG operations. Unary projections/exchanges preserve this
admission. Existing AUTO/TWO_PHASE local/global admission remains unchanged.

The optional `streamfusion.window.attached.single-phase.enabled=false` retains stock execution;
`true` now matches the default. The former COUNT-only, five-slide and fixed-zone performance
restrictions were removed after the [operator-format audit](../benchmarks/feature-admission-2026-10-19.md)
and fixed-offset boundary optimization. The outer operator folds each row into its supplied
`window_start`/`window_end` pair; it does not assign another set of overlapping windows.
Existing accumulator state, checkpoints, type gates and single-phase string admission remain
in force. FILTER, retracting input and attached sessions remain outside this fixed-window path.

The final attached operator owns its firing watermark. End-minus-one closes a window;
subsequent rows naming that closed window are dropped and counted, including after restoring
memory or native RocksDB state. Local attached partials continue to defer late admission to
their global merge. The calendar retains session-local bounds for regional LTZ input and
converts only firing deadlines; fixed offsets retain the existing epoch carriage/rendering.
A tumbling-shaped state handle avoids re-expanding already assigned HOP/CUMULATE bounds.

SQL tests cover uneven group counts, nullable keys/values, all aggregate NULL behavior,
resolved types and complete insert changelogs, empty input, multiple Arrow batches and consumed
rows in both inner and outer native windows. ONE_PHASE runs TUMBLE/HOP/CUMULATE across UTC,
Shanghai and Los Angeles (including the DST boundary); AUTO TUMBLE/HOP controls retain their
native path. Controlled source watermarks prove exact-boundary firing and late rows. State
checks retain open and closed bounds across memory/RocksDB restore and backend migration in
UTC, GMT+08:00 and Los Angeles, then drain with the final watermark without re-emission.


### Assigned-bound ONE_PHASE release measurements

The following retained matrix is the initial comparison before singleton-shuffle and bounded
COUNT coalescing. Current COUNT-only default admission is described above; its new matched
comparison is recorded separately below.

`AttachedWindowOnePhaseBenchmark` executes the issue's COUNT→MAX cascade with a rowwise
runtime source and rowwise blackhole sink. Actual metrics require both native windows to
consume rows, and the native plan retains both transposes. The final five-million-row matrix
uses released Flink 2.2.1/JDK17, release/mimalloc, ONE_PHASE, parallelism 1, 64/4096 keys,
nullable INT keys, TIMESTAMP(3) rowtime, memory/native RocksDB state, an 8g Java heap and 48g
task off-heap reservation. Each profile has two warmups and three alternating measured trials.
Setup/explain are excluded; startup, execution, JNI, conversions, state and completion are included.

| Window / state / keys | Stock median (range), s | Native median (range), s | Stock/native |
| --- | ---: | ---: | ---: |
| TUMBLE / memory / 64 | 1.200 (1.171–1.280) | 0.659 (0.632–0.679) | 1.82× |
| TUMBLE / memory / 4096 | 1.183 (1.171–1.190) | 0.963 (0.955–0.969) | 1.23× |
| TUMBLE / RocksDB / 64 | 1.014 (1.010–1.028) | 1.321 (1.312–1.322) | 0.77× |
| TUMBLE / RocksDB / 4096 | 2.951 (2.874–3.119) | 8.125 (8.054–8.141) | 0.36× |
| HOP / memory / 64 | 1.070 (1.069–1.072) | 0.565 (0.561–0.568) | 1.89× |
| HOP / memory / 4096 | 1.357 (1.356–1.372) | 1.786 (1.753–1.792) | 0.76× |
| HOP / RocksDB / 64 | 1.342 (1.249–1.349) | 3.565 (3.556–3.679) | 0.38× |
| HOP / RocksDB / 4096 | 7.992 (7.473–8.425) | 38.294 (38.112–38.502) | 0.21× |

Previous canonical `74822fd4` executes the same query/data/resources through its explicit
whole-island fallback with zero native substitutions. Its eight paired stock/enabled medians
range from 0.94 to 1.01× stock/enabled, remaining near stock. Independent invocation
times vary and are not a direct before/after ratio. The retained artifact includes all 320
completed jobs across initial/optimized 1M runs and final/prior 5M runs, including warmups.

That revision retained **default-off** admission: low-cardinality memory workloads accelerated,
while high-cardinality HOP and persistent COUNT cases lost substantially. Those results explain
the original safety gate; the bounded COUNT revision is measured separately. Further
persistent-window work is linked to [#350](https://github.com/datafusion-contrib/StreamFusion/issues/350);
its existing calendar/count investigation covers the same read/write machinery, while the
matrix here also records ordinary UTC windows and higher cardinality.

The initial native-only 20M HOP/4096/Rocks CPU capture has 11,097 samples; 4,338 include bulk
reads, 2,637 include DB writes, and 9,705 include the inner window push. These are inclusive
stacks that can overlap. The diagnostic was deliberately stopped after capture and its
instrumented timings are not comparison results. Exact COUNT panes and sorted bulk state I/O
subsequently reduced the 1M high-cardinality memory HOP native median from 0.812 to 0.402s;
that profile still lost its paired stock control. Complete-job measurements retain those
limits and all initial/revised unfavorable trials. The
[trial artifact](../benchmarks/results/attached-windows-2026-10-14.json) and
[CPU capture](../benchmarks/results/attached-windows-2026-10-14-cpu.collapsed.gz) retain the evidence.
That revision's final release DSO SHA256 is
`d09c0f6c60b1cc9aaae634af63b154dfbb33c130aeba54cd2550de6f94a7b3db`.

### Bounded COUNT and default attached execution

The 2026-10-18 comparison uses main `3679d312`, release/mimalloc libraries, Flink 2.2.1,
JDK 11.0.27, an Apple M4 Pro with 14 logical CPUs and 24 GiB RAM, and macOS 26.5.
Every complete job processes five million bounded runtime rows at parallelism one, with an
8 GiB JVM heap limit and the same task off-heap reservation authority. The logical reservation
ceiling is 48 GiB; it is not an allocation or a statement of available physical memory.
Nullable INT keys use domains of 64/4096; TIMESTAMP(3) advances 10 ms per 1,024 rows, with a
one-second watermark delay. A rowwise blackhole sink and both perimeter transposes remain
in the native plan. Exchanges use production Arrow IPC, not the local zero-copy test mode.

Each profile has two warmups and three measured stock/enabled jobs, alternating execution
order per trial. Timing covers `executeSql` through completion, including submission planning,
startup, JNI, conversions, state work and final drain. It is a complete-job latency measure;
the shorter subsecond native cases include substantial startup cost. The native assertion
requires at least two window operators to consume rows, with one consuming all source rows.
The [artifact](../benchmarks/results/attached-window-count-2026-10-18.json) retains individual
trials, ranges, allocator/library identities, fallback proof and earlier unfavorable results.

For ONE_PHASE COUNT-to-MAX with ten-second windows and a two-second HOP slide/CUMULATE step,
the existing experimental override is unset. All twelve profiles execute natively by default:

| Window | State | Keys | Stock median (s) | Native median (s) | Stock/native |
|---|---|---:|---:|---:|---:|
| TUMBLE | Memory | 64 | 1.274 | 0.591 | 2.16× |
| TUMBLE | Memory | 4,096 | 1.326 | 0.699 | 1.90× |
| TUMBLE | RocksDB | 64 | 1.352 | 0.511 | 2.65× |
| TUMBLE | RocksDB | 4,096 | 2.706 | 0.938 | 2.88× |
| HOP | Memory | 64 | 1.224 | 0.515 | 2.37× |
| HOP | Memory | 4,096 | 1.556 | 1.100 | 1.41× |
| HOP | RocksDB | 64 | 1.425 | 0.516 | 2.76× |
| HOP | RocksDB | 4,096 | 8.680 | 3.256 | 2.67× |
| CUMULATE | Memory | 64 | 1.260 | 0.548 | 2.30× |
| CUMULATE | Memory | 4,096 | 1.445 | 0.889 | 1.63× |
| CUMULATE | RocksDB | 64 | 1.387 | 0.557 | 2.49× |
| CUMULATE | RocksDB | 4,096 | 6.060 | 2.469 | 2.45× |

The original high-cardinality HOP/RocksDB case measures stock 8.369–9.074s and native
3.064–3.286s across its three measured trials. Ordinary persistent COUNT keeps bounded decoded
updates across batches, commits closing groups at watermarks and all pending groups before
checkpoints/canonical exports, then uses key-group-bounded firing iterators. The
[optimization ledger](../optimizations/shared-window-state.md#bounded-persistent-count-updates)
explains the state and memory contract, including the rejected unbounded iterator scan.

After the optional connector merge, both libraries are rebuilt on `0a02ecb8`. A matched
confirmation retains the previous main's default fallback and its opt-in native path:

| Profile | Paired stock median (s) | Previous default fallback (s) | Previous opt-in native (s) | New default native (s) |
|---|---:|---:|---:|---:|
| HOP / RocksDB / 4,096 keys | 8.833 | 8.690 | 27.072 | 3.077 |
| HOP / memory / 4,096 keys | 1.481 | 1.400 | 1.087 | 1.087 |
| TUMBLE / memory / 64 keys | 1.247 | 1.225 | — | 0.499 |
| CUMULATE / RocksDB / 64 keys | 1.349 | 1.364 | — | 0.558 |

The paired stock column is the candidate invocation's control; previous default/opt-in columns
come from independent invocations of the identical fixture on clean main production sources.
The artifact retains their own paired controls as well. Previous fallback has zero substitutions;
both native revisions assert actual window consumption. The original persistent native median
falls from 27.072 to 3.077s, about 8.80× faster, while the already-native memory path is unchanged.
The candidate's four current stock/native ratios are 1.36–2.87×. In the original persistent case,
current stock trials range from 8.752 to 8.963s, previous opt-in native from 27.011 to 27.204s,
and new native from 3.055 to 3.085s.

Additional complete-job HOP/4,096-key controls retain mixed outer SUM/MIN/MAX/AVG/COUNT and
TWO_PHASE execution. Memory controls on `3679d312` measure 1.486s stock / 1.081s native for
mixed ONE_PHASE, and 1.365s / 1.056s for TWO_PHASE COUNT-to-MAX. Rebuilt RocksDB controls on
`0a02ecb8` measure 8.870s / 3.126s and 15.731s / 1.912s respectively. TWO_PHASE admission was
already enabled; these controls do not claim that this patch introduces its native coverage.

At that revision, wider overlap was deliberately excluded from default ONE_PHASE admission. With a thirty-second
HOP and a two-second slide, 4,096-key memory jobs measured stock 1.668s versus native 2.499s
before narrowing the gate; an independent final opt-in comparison measured 1.612s versus
2.355s. Native values matched, but the default then remained stock. Regional calendar callbacks
and non-COUNT upstream layouts also retained fallback. The later operator-format audit above
changes that admission boundary; these complete-job regressions remain valid evidence. This does not resolve the separate regional investigation in
[#350](https://github.com/datafusion-contrib/StreamFusion/issues/350).
The remaining wider-overlap optimization is tracked in
[#394](https://github.com/datafusion-contrib/StreamFusion/issues/394).
The wider RocksDB diagnostic also loses: the first completed uninstrumented warmups take
19.137s stock and 77.439s native. A capture during the next native warmup puts 277 of 404
task-thread samples in hydration and 105 in bundle writes. That second warmup was interrupted;
these diagnostic figures are retained without presenting them as measured-trial medians.
The final wider-memory default-fallback control measures 1.582s stock and 1.585s with
StreamFusion enabled, with zero substitutions as required by the narrowed gate.

Validation includes typed results and RowKinds, nullable COUNT values and keys, mixed outer
SUM/MIN/MAX/AVG, three nested windows, both aggregation phases/backends, two completed checkpoints
before recovery, late rows, backend migration and split key-group restore. The selected JDK 11
regression set passes 800 checks on Flink 2.2.1 and 793 with seven existing capability skips on
Flink 1.18.1. Rust release core passes 753 tests with two ignored; without default features,
629 pass. Each revision also passes all 72 new persistent COUNT Criterion fixtures.
After rebasing onto main `0a02ecb8` and rebuilding both release/mimalloc libraries, the focused
98-check selection passes on Flink 2.2.1; Flink 1.18.1 passes 97 with one existing SESSION TVF
skip. Rust release core is rebuilt on that base and again passes 753 with two ignored. The
629 no-default-features checks above ran on `3679d312`, before the optional connector merge;
the changed window code is identical across that rebase.

To reproduce the complete-job fixture for one profile:

```sh
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=AttachedWindowOnePhaseBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dattached.rows=5000000 -Dattached.window=HOP -Dattached.rocks=true \
  -Dattached.keys=4096
```

Use a release/mimalloc engine library for the selected checkout. For a clean previous-main
comparison, copy only this test fixture into its worktree: `-Dattached.expectNative=false`
asserts the previous default fallback, while `-Dattached.optIn=true` exercises its previous
native route. `-Dattached.mixed=true`, `-Dattached.phase=TWO_PHASE` and
`-Dattached.sizeSeconds=30` select the additional profiles; the wider ONE_PHASE native
comparison required the opt-in on that previous revision. The current default
admits the supported wider outer operator under the operator-format gate.


## Floating window distinct membership

FLOAT and DOUBLE COUNT(DISTINCT) use Java boxed floating equality: every NaN payload and sign
has one membership, while +0 and -0 are separate values. NULL is ignored; infinities,
subnormals and ordinary finite values retain their exact bits. Membership uses compact
32/64-bit keys and canonicalizes NaNs while probing the set. Other aggregates continue to
observe the original input values. DataFusion's default floating distinct buffer hashes raw
payloads, so this small accumulator supplies Flink's equality under the existing window engine.

Local partials and checkpoints retain the existing typed Arrow FLOAT/DOUBLE lists. Global
merges use the same membership rule, including when a NaN reappears with another payload in
a later partial or after recovery. Hash capacity enters native memory accounting; state owns
keys and creates Arrow arrays only when exporting partial/checkpoint state. There is no new
JNI interface or checkpoint framing.

Released SQL checks cover TUMBLE/HOP/CUMULATE in both aggregation phases, independent filters,
legacy grouped windows and SESSION table-function availability. Counts are also compared with
independently supplied Java canonical-bit integer columns. Direct operator checks exercise
ordinary and distinct counts across raw/canonical memory/RocksDB backend transitions and
require all native memory to be released. The focused native suite adds 60 fixtures across
FLOAT/DOUBLE, TUMBLE/HOP/CUMULATE, single/partial execution, 64/4096 rows and one/64/unique keys,
with independent bit-set oracles and four physical updates/partial merges.


Already-complete CUMULATE partials retain independent end times. Their global stride equals
the maximum window size: they must merge into exactly the end they name. They do not use the
ordinary cumulative slice prefix. This also fixes an existing split-plan overcount for exact
integer controls; a completed later window was previously merged into an earlier end.
Shared storage/checkpoint framing is unchanged. Native calendar fixtures check independent
ends, late closed partials, heap checkpoint continuation, RocksDB state and budget cleanup.
SQL split-CUMULATE profiles inject two restores on memory and native RocksDB.

The combined released suites pass 95 checks on Flink 2.2.1; Flink 1.18.1 passes 94 with one
existing SESSION capability skip. This includes 28 new SQL checks, eight direct backend
transition checks, 51 existing distinct-count controls and eight BOOLEAN/TIME controls.
The native membership unit check and 75 window regressions pass. The release fixture selection
has 72 checks: 60 floating membership cases plus 12 actual JVM-calendar partial/state cases.

Complete-job comparisons use release+mimalloc, JDK 17, parallelism two, UTC, a 2 GiB heap,
two warmups and three measured trials. Both engines retain runtime row sources, rowwise
blackhole sinks, both transposes, production Arrow IPC exchange, JNI/state costs and execution
metrics. Timings cast the existing nullable BIGINT field to FLOAT/DOUBLE; NaN payloads and
other floating edge values are correctness-covered separately.

| Type / shape / phase / backend / rows / keys | Stock mean (range), s | SF mean (range), s | Actual previous SF mean (range), s | Library |
| --- | ---: | ---: | ---: | --- |
| float / TUMBLE / ONE_PHASE / memory / 500,000 / 64 | 0.512 (0.474–0.556) | 0.411 (0.397–0.428) | 0.520 (0.504–0.545) | S1 |
| double / TUMBLE / TWO_PHASE / memory / 500,000 / 64 | 0.614 (0.598–0.645) | 0.390 (0.381–0.399) | 0.589 (0.585–0.593) | S1 |
| float / HOP / TWO_PHASE / memory / 500,000 / 64 | 0.724 (0.650–0.770) | 0.408 (0.399–0.416) | — | S1 |
| double / HOP / ONE_PHASE / memory / 500,000 / 64 | 0.571 (0.556–0.595) | 0.415 (0.391–0.430) | — | S1 |
| float / CUMULATE / ONE_PHASE / memory / 500,000 / 64 | 0.542 (0.513–0.571) | 0.408 (0.403–0.414) | — | S1 |
| double / CUMULATE / TWO_PHASE / memory / 500,000 / 64 | 0.654 (0.634–0.688) | 0.412 (0.404–0.426) | — | S1 |
| float / SESSION / ONE_PHASE / memory / 500,000 / 64 | 0.543 (0.506–0.593) | 0.417 (0.412–0.427) | 0.538 (0.525–0.546) | S1 |
| double / SESSION / ONE_PHASE / memory / 500,000 / 64 | 0.519 (0.499–0.532) | 0.326 (0.318–0.335) | 0.496 (0.469–0.511) | S1 |
| double / HOP / ONE_PHASE / memory / 1,000,000 / 1 | 0.909 (0.879–0.925) | 0.643 (0.574–0.694) | — | S1 |
| double / TUMBLE / TWO_PHASE / rocksdb / 500,000 / 64 | 0.654 (0.636–0.687) | 0.446 (0.421–0.479) | 0.621 (0.578–0.660) | S1 |
| double / CUMULATE / TWO_PHASE / memory / 500,000 / 64 | 0.646 (0.613–0.698) | 0.397 (0.388–0.412) | — | S2 |
| double / CUMULATE split / TWO_PHASE / memory / 500,000 / 64 | 0.681 (0.648–0.713) | 0.426 (0.409–0.437) | 0.655 (0.609–0.682) | S2 |

Representative stock/current means improve 1.246–1.772x; corresponding median speedups are
1.241–1.829x. The actual previous revision `54f56699` controls improve 1.264–1.539x by mean.
Flink 1.18's DOUBLE/HOP/ONE_PHASE check measures 0.576s stock / 0.415s native (1.388x by mean).

S1 is the membership implementation (`9aba8df6` library) used for the first 160 jobs, before
the subsequent complete-CUMULATE prefix correction. Those jobs use the default split=false;
ordinary CUMULATE steps are smaller than the maximum size. S2 is the final library
(`67ec92bc`): 30 additional jobs measure an ordinary default-CUMULATE control and repaired
split CUMULATE against stock and actual previous StreamFusion. The default control measures
0.412s before / 0.397s after; paired stock means are 0.654s / 0.646s. The repaired split profile
measures 0.681s stock / 0.426s native / 0.655s previous SF. Both split global stages are
required in its native plan. Earlier samples retain their original library identity.

The [190-job artifact](../benchmarks/results/window-floating-distinct-2026-10-19.json)
retains all warmups/trials, means/medians, paired baseline drift, both source/library versions,
72 final allocation/buffer reports and 60 earlier diagnostic reports. The final allocation
probes exclude operator construction; the earlier diagnostic probes include it. Neither is
interpreted as copied bytes. Criterion construction remains outside timing in both versions.

The timings sample alternating type/phase combinations across the fixed shapes, SESSION,
one RocksDB profile and a one-million-row/single-key profile. They do not establish a
universal speedup or separately time legacy syntax/proctime. Actual-prior controls sample
TUMBLE, SESSION, RocksDB and split CUMULATE; only the final 30 jobs use the final prefix-fix
library. Reproduce with `WindowBasicTypeBenchmark -Dwindow.cases=float,double`, phase/shape,
row/key, warmup/run and backend properties above. `-Dwindow.splitDistinct=true` enables the
optional split rewrite; the default remains false.


### MULTISET COUNT

Ordinary MULTISET COUNT uses the same parent-validity path as ARRAY/MAP/ROW. Empty bags,
a bag containing the NULL element and bags with different element multiplicities each count
as one non-NULL parent; element occurrences do not contribute separately. DISTINCT retains
fallback. Existing window assignment, aggregate phase, filtering and changelog gates remain.
The physical entry schema is MAP-of-element-to-non-null-INT, while aggregate state is BIGINT.

Released SQL coverage adds twelve append and twelve retracting/filter/phase cases, legacy
TUMBLE/HOP/SESSION, the available session table function, four two-restore phase/backend
profiles and six processing-time restored-timer controls. Flink 1.18 keeps its existing signed
window restrictions. The combined window/OVER selection passes 181 checks on Flink 2.2;
Flink 1.18 passes 127 with 54 host capability skips. Release fixtures expand the existing
window suite to 48 cases with independently counted parent validity and chunked/local-global
updates. The [OVER page](over.md#multiset-count) describes the matching event-time coverage.


Complete-job release+mimalloc timings use JDK 17, UTC, parallelism two, a 2 GiB heap,
1,024-row entry batches and production IPC. Nullable runtime bags contain empty values, a NULL
element and positive occurrence counts. Both row/Arrow transposes, JNI, state, startup and a
row blackhole sink remain measured. Two warmups precede three alternating trials. Native
plans/counters verify execution; separately compiled actual previous revision `a038f539`
asserts its window/OVER fallback. Other local heavy work was excluded.

| Profile / rows / keys / elements | Stock median (range), s | Native median (range), s | Actual previous SF median (range), s |
| --- | ---: | ---: | ---: |
| ONE_PHASE TUMBLE / 1,000,000 / 64 / 4 | 1.002 (0.967–1.005) | 0.923 (0.911–0.928) | 1.006 (1.005–1.015) |
| ONE_PHASE HOP / 1,000,000 / 64 / 4 | 0.954 (0.923–0.989) | 0.822 (0.814–0.827) | 0.962 (0.940–0.974) |
| ONE_PHASE CUMULATE / 1,000,000 / 64 / 4 | 0.876 (0.853–0.920) | 0.825 (0.816–0.829) | 0.892 (0.875–0.933) |
| TWO_PHASE TUMBLE / 1,000,000 / 64 / 4 | 0.990 (0.981–0.991) | 0.740 (0.737–0.747) | 0.984 (0.981–0.985) |
| TWO_PHASE HOP / 1,000,000 / 64 / 4 | 0.980 (0.967–0.990) | 0.745 (0.744–0.752) | 0.915 (0.909–0.985) |
| TWO_PHASE CUMULATE / 1,000,000 / 64 / 4 | 0.930 (0.902–0.984) | 0.742 (0.739–0.752) | 0.919 (0.912–0.996) |
| TWO_PHASE HOP / 250,000 / 4,096 / 32 | 1.013 (0.981–1.075) | 0.951 (0.912–0.970) | 0.982 (0.954–0.999) |

These profiles improve 1.062–1.338x over paired stock and 1.033–1.331x
over actual previous StreamFusion. The Flink 1.18 two-phase TUMBLE control improves
1.293x over stock (1.048/0.811 s). Wide-bag gains are modest and
ranges overlap previous execution; startup/source allocation remain measured. These are
representative complete jobs, not universal or sustained-throughput claims. The
[260-job artifact](../benchmarks/results/multiset-count-2026-10-19.json) retains every warmup,
trial, prior stock control, source/library hash and all 308 native allocation/buffer reports.
Reproduce with `WindowComplexCountBenchmark -Dwindow.cases=multiset`
and the row/key/width/phase/frame properties shown above.

Floating SUM0 shares the [typed zero-initialized accumulator](group-by.md#floating-sum0-admission).
The FLOAT/DOUBLE one/two-phase SQL cases pass on both released lines; local/global state
keeps the declared floating width. The standalone accumulator admission measures the function,
not window assignment, timers or a complete window job.


## Scalar grouping and partition keys

Grouping keys admit TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, CHAR/VARCHAR, BOOLEAN, DATE, TIME,
TIMESTAMP/TIMESTAMP_LTZ, DECIMAL, BINARY/VARBINARY and both interval families. Composite
keys may combine these scalar columns. Collection/ROW/object key columns
retain fallback; a composite of scalar columns does not imply support for a ROW-valued key.
The same scalar whitelist applies to time-based OVER partitions, non-time ROW_NUMBER
partitions on Flink 2.2, and interval/window/event-time temporal join keys.

New narrow-integer, TIME, binary and interval keys use their natural Arrow carriage:
Int8, Int16, millisecond Time32, length-delimited Binary, Int32 months or Int64 milliseconds.
TIME(0) preserves runtime millisecond values, matching released Flink. Binary equality uses
all actual bytes, including empty values, embedded zeroes and raw lengths different from
SQL BINARY(n). Disabling both `streamfusion.experimental.nestedScalarUdfs` and
`streamfusion.experimental.binaryElt` selects the legacy fixed-width transport; BINARY keys
then retain stock execution. Either default-enabled option provides the verified transport.
Existing INT grouping keys keep their widened Int64 state encoding, so this extension does
not reinterpret existing INT checkpoints.

The released-Flink SQL matrix checks all eight new logical variants, nullable composite
keys, seven-row physical batches, object reuse and parallelism three. Its 216 cases per
release cover both fixed-window phases, sessions, all seven existing time-based OVER
frames, interval/window/temporal joins and two memory/direct-RocksDB restores. Flink 2.2
also verifies eight non-time ROW_NUMBER partition cases; Flink 1.18 retains its host
restriction on non-time OVER. Repeated recovery covers TUMBLE, SESSION, bounded ROWS OVER,
interval joins and window joins, with controlled watermarks and full output/type comparison.

### Key materialization measurements

`ScalarKeyAdmissionBenchmark` compares the released Flink key selector's projection and
BinaryRowData hash with production native key materialization/hash on equivalent prebuilt
Arrow inputs. The Rust driver verifies every row's exact signed hash against Flink outside
timing; both timed paths consume a sum. Fixtures contain a nullable key, an INT shard and
an unused BIGINT payload, forcing a real key projection. Rust clocks begin after Arrow
import. Perimeter JNI, import/export and transposes are outside this functionality gate.
This measures key routing, not aggregate/join execution or complete-job throughput.
Representative operator and checkpoint paths have separate Criterion/SQL coverage.

Final measurements and all warmup/trial values are retained in
[the scalar-key artifact](../benchmarks/results/scalar-keys-2026-10-20.json).

The Flink 2.2.1/JDK 17 release comparison uses one runtime thread per engine, a 2 GiB Java
heap, 1,024-row native batches, fifty warmups and nine measured trials. No competing build
or test ran during measurement. Small inputs have 16,384 rows, nominally 64 keys and four-byte
binary values; large inputs have 262,144 rows, nominally 4,096 keys and 64-byte binary values.
TINYINT wraps to its actual value domain. NULL occurs every eleventh row; binary values
include empty, nominal and nominal+1/+2 lengths. Medians (milliseconds):

| Key | Small Flink | Small Rust | Large Flink | Large Rust |
|---|---:|---:|---:|---:|
| tinyint | 0.572 | 0.327 | 9.841 | 5.234 |
| smallint | 0.510 | 0.328 | 10.332 | 5.288 |
| time | 0.551 | 0.376 | 10.290 | 6.431 |
| time0 | 1.244 | 0.376 | 10.941 | 6.044 |
| binary | 0.621 | 0.387 | 28.378 | 10.175 |
| varbinary | 0.669 | 0.386 | 25.062 | 10.236 |
| months | 0.489 | 0.362 | 13.275 | 5.866 |
| millis | 1.272 | 0.326 | 16.265 | 5.488 |

Key materialization is 1.35–3.91× faster by these medians. Every per-row hash is checked
against released Flink before timing, rather than relying on a checksum alone. The actual
preceding matcher was also executed against 48 selected SQL fixtures on Flink 1.18 and
keeps the requested operations on Flink; its key projection/hash therefore follows that
stock path. This boundary supplies no whole-job speedup claim. The artifact retains all
variability, including Java JIT/GC effects; fifty warmups replace the exploratory five-warmup
run, whose early cases overstated some improvements.


### Floating partition keys

FLOAT and DOUBLE grouping/partition/equi-key columns use Float32/Float64 Arrow vectors and
the existing Flink BinaryRow key hash and raw-bit key codec. Signed zero and distinct NaN
payloads retain their identity; this does not change SQL scalar floating comparison rules.
The direct fixed-window/session RocksDB key decoder uses the same natural widths. No
previously admitted key changes its persisted encoding.

The floating extension adds 56 SQL/recovery cases on Flink 2.2.1 and 54 on 1.18.1, with two
non-time ROW_NUMBER capability skips on 1.18. Both phases of fixed windows, sessions, all
seven time-based OVER shapes and interval/window/temporal joins are covered. Fixtures
include NULL, signed zero, positive/negative NaN payloads, infinities and subnormals.
They construct NaNs after source deserialization and observe raw bits in the output,
so serializer canonicalization cannot hide a key mismatch. Two restores on both state
backends cover TUMBLE, SESSION, bounded ROWS OVER and interval/window joins. The actual
previous matcher keeps all twelve selected controls on Flink.

The key materialization admission uses the same Java/RowData versus Rust/Arrow boundary
as the preceding scalar extension, including an exact per-row signed hash oracle. Its
[raw artifact](../benchmarks/results/floating-keys-2026-10-20.json) retains all warmups,
measurements and configuration. This does not establish complete-job throughput.

To reproduce the floating key gate after compiling on JDK 17:

```sh
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=ScalarKeyAdmissionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.testForks=1 -Dscalar.keys.types=float,double \
  -Dscalar.keys.rows=262144 -Dscalar.keys.cardinality=4096 \
  -Dscalar.keys.warmups=50 -Dscalar.keys.runs=9 \
  -Dscalar.keys.export=target/scalar-keys
cargo run --manifest-path native/Cargo.toml -p streamfusion --release \
  --features mimalloc --example scalar_key_hash -- target/scalar-keys-float.arrow 50 9
cargo run --manifest-path native/Cargo.toml -p streamfusion --release \
  --features mimalloc --example scalar_key_hash -- target/scalar-keys-double.arrow 50 9
```

Prepare/export inputs before the Rust clock, and run timing serially without competing tests
or builds. Use 16,384 rows and 64 keys for the small profile. The JSON rows contain the
measured runtime seconds and consumed hash sum; the driver first checks every golden hash.


Floating-key medians use the same 16,384-row/64-key and 262,144-row/4,096-key profiles,
fifty warmups and nine samples, with no concurrent tests/builds. The nullable key contains
finite quarter values and raw IEEE edge values. Every signed hash matches released Flink.

| Key | Small Flink, ms | Small Rust, ms | Large Flink, ms | Large Rust, ms |
|---|---:|---:|---:|---:|
| float | 0.486 | 0.329 | 7.452 | 5.310 |
| double | 0.521 | 0.334 | 11.252 | 5.490 |

These key materialization medians improve 1.40–2.05× over stock and the preceding
fallback path. The raw artifact includes each trial and range; these are not window/join
job speedups. Existing checkpoint/key-group formats for earlier key types are unchanged.

## Binary and interval COUNT

Ordinary COUNT accepts VARBINARY and both interval families in otherwise supported window shapes.
It reads only the top-level validity bitmap; empty binary values and zero/negative intervals are
non-NULL values. No new operator, native accumulator or performance flag is introduced. DISTINCT,
fixed BINARY and aggregates that interpret these payloads retain their separate admission gates.

Twelve SQL comparisons cover TUMBLE/HOP/CUMULATE, one/two phases and FILTER. The expanded
processing-time harness has 42 cases across memory/native RocksDB checkpoint continuation,
including 18 binary/interval cases. All 54 pass on each released Flink line. The release Criterion
suite has 84 fixtures (36 added), including NULL-only groups, empty values, chunking and local/global
partial merges checked against an independent validity oracle.

The [Rust-side admission evidence](../benchmarks/results/window-count-binary-interval-2026-10-19.json)
compares Java on prebuilt BinaryRowData against Rust on prebuilt Arrow, with five warmups and nine
trials. COUNT of binary and both interval columns together measures 2.08 ms Rust versus
32.15–36.43 ms stock for TUMBLE (131,072 rows, 64 keys, width 64), and 33.16 ms versus
146.48–160.07 ms for HOP (262,144 rows, 4,096 keys, width 256). Full output values and RowKinds
match. Prior admission was stock fallback. These are final-watermark operation measurements;
checkpoint I/O, periodic-watermark processing and full-job boundaries are not timed here.

The matching high-cardinality Flink 1.18 HOP cohort measures 30.91 ms Rust versus 152.73 ms
stock, with complete output/RowKind parity. Every warmup and stock cohort is retained in the artifact.

### Offset and attached FILTER verification

The focused offset matrix compares exact values and result widths on released Flink 1.18.1
and 2.2.1, requiring actual single/local/global native work. It covers TUMBLE/HOP/CUMULATE,
both phases, NULL keys and values, filtered string extrema, DISTINCT counts, negative epochs,
plain TIMESTAMP and LTZ in UTC, Kathmandu and New York. Four additional memory/RocksDB
cases each recover twice across a DST overlap. Flink 2.2 updating offset windows verify both
phases; Flink 1.18 rejects updating TVFs. Both releases reject non-dividing/gapped slicing
TVFs before execution; legacy group windows have separate assignment rules.

Attached one/two-phase aggregation applies each FILTER mask before accumulation while keeping
the assigned window alive even when every value is filtered out. Ordinary COUNT reads parent
validity without interpreting its payload. The shared BIGINT transfer optimization applies only
when no mask or COUNT validity conversion is needed. Fused proctime aggregate offsets
and session FILTER use that same preparation. Documented changelog restrictions remain.
Standalone TVFs admit offsets for both clocks; their output can feed attached aggregates without
falling out of the native island. The global cumulative merge retains the original offset when
reconstructing the start from a local partial's window end.


The release/mimalloc complete-job comparison uses one million rows, JDK 17, a 2 GiB heap,
memory state, two warmups and three alternating trials. HOP computes COUNT/AVG/SUM over
BIGINT with a one-second offset, size ten seconds, slide two seconds, 64 keys and parallelism
two. The attached COUNT-to-filtered-MAX cascade uses parallelism one, 64 nullable keys and
TUMBLE ten seconds. Both retain the RowData runtime source, both transposes, native state,
JNI and rowwise blackhole sink; metrics require actual native window input.

| Flink / query | Stock median, s | Native median, s | Stock/native |
| --- | ---: | ---: | ---: |
| 2.2.1 / offset HOP, one phase | 0.827 | 0.714 | 1.16× |
| 2.2.1 / offset HOP, two phases | 0.887 | 0.549 | 1.62× |
| 2.2.1 / attached FILTER | 0.311 | 0.170 | 1.83× |
| 1.18.1 / offset HOP, one phase | 0.820 | 0.659 | 1.24× |
| 1.18.1 / offset HOP, two phases | 0.879 | 0.563 | 1.56× |
| 1.18.1 / attached FILTER | 0.327 | 0.170 | 1.92× |

Actual previous canonical `d13e9dc4` uses the stock window path for all three cases:
configured StreamFusion medians are 0.815/0.857/0.303 seconds versus paired stock
0.811/0.851/0.305 seconds. These are separately executed controls, not interchangeable
paired trials. [Every warmup, measurement and library hash](../benchmarks/results/window-context-offset-filter-2026-10-20.json)
is retained. No optimization pass was necessary for these winning complete-job cases.
These results describe aggregates and attached aggregation, not standalone TVF throughput.


`WindowAssignmentAdmissionBenchmark` exports exactly matching Arrow input for the
`window_assignment` Rust example. The small functionality timer starts in Java on prebuilt
GenericRowData for Flink's released TVF operator, and inside Rust on prebuilt Arrow batches
for StreamFusion. Assignment, payload fan-out, allocation and consumption of all five output
columns remain timed; setup, IPC input reading and perimeter JNI/transposes are excluded.
With 131,072 rows, 1,024-row Arrow batches, 4,096 cyclic timestamps straddling epoch zero,
one NULL timestamp per seven rows, SQL offset -1 s, size 9 s and slide/step 3 s, three warmups
and seven trials produce the following medians. Output counts and wrapping checksums match
exactly on both released lines, whose exported Arrow inputs are byte-identical.

| Shape | Flink 2.2.1, ms | Flink 1.18.1, ms | Rust, ms |
| --- | ---: | ---: | ---: |
| TUMBLE | 17.625 | 11.680 | 3.184 |
| HOP | 20.512 | 21.188 | 8.181 |
| CUMULATE | 14.529 | 15.745 | 5.740 |

This gate establishes 2.51–5.53× / 2.59–3.67× measured improvements over 2.2.1 / 1.18.1
for this native-input boundary. It does not revise the conversion-inclusive standalone TVF
measurements above. Five controlled-clock cases per release additionally check proctime
assignment offsets with NULL payload timestamps and positive/negative time-zone offsets.
Forty-four focused Rust window checks pass. The artifact retains all trials, input hashes,
release library and driver hashes, and the separately measured actual previous-revision controls.

### Processing-time and session context verification

Processing-time aggregates now admit SQL offsets and per-aggregate masks, and legacy
processing-time SESSION and non-dividing/gapped HOP reuse the native operators. Fixed
HOP timers follow the start grid plus size; this also preserves the previous dividing
shapes. Session masks change aggregate contributions while every row still extends
session bounds. No Rust operator or JNI protocol is added.

The fixed-window operator suite passes 44 checks on each released line, including eight
new shape/offset cases with exact timer boundaries and memory/RocksDB restore. Session
checks include filtered SUM and COUNT(*), FALSE/NULL bridge rows, both clocks and both
backends. Four SQL checks per release run eight phase/shape jobs and require positive
input in the intended native operator, separately confirming that released proctime
planning remains single-phase. Existing SQL window checks pass on both lines, with
two explicit SESSION TVF skips on 1.18.

A conversion-inclusive million-row filtered legacy SESSION job, 64 keys, p=2, memory
state, JDK 17, 2 GiB heap and release+mimalloc measures median stock/native
**0.795/0.667 s on 2.2** and **0.805/0.690 s on 1.18**, with two warmups and three
alternating trials. The query computes filtered COUNT(*), AVG and SUM over nullable
BIGINT values; cyclic timestamps merge into one ten-second session per key. The
actual canonical `d13e9dc4` window fallback measures 0.798 s against paired stock
0.821 s. Sources, both transposes, JNI, state, final output and rowwise sinks remain
timed. [Raw trials, ranges, hashes and scope](../benchmarks/results/processing-window-contexts-2026-10-21.json)
retain the precise protocol and distinguish this session comparison from the earlier
Rust/Arrow assignment admission gate.


## Retracting extrema and legacy fixed windows

Updating MIN/MAX admits integer widths, FLOAT/DOUBLE, DECIMAL, VARCHAR, BOOLEAN, DATE,
TIME, TIMESTAMP and TIMESTAMP_LTZ. Flink 2.2 additionally admits CHAR; Flink 1.18 has no
retracting CHAR extrema factory, so its native planner preserves that released planning error.
All admitted types retain duplicate counts, cached winners and signed pending entries. Removing
a winner reseeks the remaining keys with Flink's comparison rules, including negative map entries.
The existing GROUP BY extrema state supplies those rules; windows keep the same owned multiset.

Flink 2.2 slicing TUMBLE/HOP/CUMULATE supports single-phase and local/global execution. A local
extreme is one Arrow structured column containing the cached value, map size and value/count
list. The global combines incoming partials in a temporary native multiset before merging once
into retained state at a watermark or checkpoint, matching Flink’s global combiner. A transient
insert/delete pair must cancel before it can change a retained cached winner or trigger a reseek.
The temporary views count against the native memory budget and drain before memory/RocksDB
snapshots; no JVM MapView or per-entry JNI callback is used.
Snapshots carry the same structure through memory and RocksDB-backend restores. The structured
state uses the backend’s snapshot compatibility path; direct Rust RocksDB row storage is not
admitted for this shape. Timestamp values
retain fractional nanoseconds. Released TIME aggregates may declare a TIME(0) cache for TIME(3)
input; the common scalar carrier preserves the actual milliseconds rather than truncating them.

Legacy fixed TUMBLE/HOP accepts updating COUNT/SUM/AVG/MIN/MAX on both releases, including
non-dividing and gapped HOP grids. It uses the same signed native accumulators and live-row
counter as slicing windows. COUNT over nullable complex parents reads validity; all-rejected
filters retain a live group, and only a zero total input count suppresses the final result.
Legacy plans remain one phase when TWO_PHASE is requested. Existing processing-time assignment
and timer handling apply to admitted legacy fixed windows.

SESSION windows admit the same signed forms on both clocks. Processing-time fixed TVFs
admit them on Flink 2.2; Flink 1.18 cannot execute updating TVFs. Early/late firing and allowed
lateness retain their explicit fallback.

Exact comparisons cover fifteen types across three slicing shapes and both phases on 2.2,
four legacy fixed grids on both releases, independent FILTERs, nullable keys/values, unmatched
deletes, complete group removal and interleaved aggregate partials. Two restores exercise
VARCHAR, DECIMAL and nanosecond timestamps on memory and the native RocksDB backend’s
snapshot compatibility path, through both TVF phases on 2.2 and legacy HOP on both releases.
The 1.18 CHAR cases compare the expected host planning failure rather than claiming native execution. Unchanged upstream legacy window tests
require consumed native input for the retracting fixed-window case and explicit fallback for
the early-firing MIN/MAX control.

### Updating extrema complete-job measurements

One million inputs feed an updating Top-1 with 64 keys, followed by COUNT/MIN/MAX over
2 s/10 s HOP windows and a rowwise blackhole sink. These release+mimalloc, JDK 17 jobs
retain both perimeter transposes, exchange, JNI and state. Parallelism is two with two
task slots and a 2 GiB heap; two warmups precede three alternating measured engine pairs.
Native trials require consumed final windows, and consumed local/global operators for
TWO_PHASE. These are complete-job diagnostics; no Java timer around JNI is presented
as the Rust/Arrow small-functionality gate.

| Flink / shape / type / phase | Stock median (s) | Native median (s) | Stock/native |
|---|---:|---:|---:|
| 1.18 / legacy HOP / BIGINT / ONE_PHASE | 1.160 | 0.448 | 2.59x |
| 1.18 / legacy HOP / STRING / ONE_PHASE | 1.281 | 0.566 | 2.26x |
| 2.2 / TVF HOP / BIGINT / ONE_PHASE | 0.624 | 0.534 | 1.17x |
| 2.2 / TVF HOP / BIGINT / TWO_PHASE | 0.585 | 0.290 | 2.02x |
| 2.2 / TVF HOP / DECIMAL(38,8) / ONE_PHASE | 0.658 | 0.593 | 1.11x |
| 2.2 / TVF HOP / DECIMAL(38,8) / TWO_PHASE | 0.632 | 0.358 | 1.76x |
| 2.2 / legacy HOP / STRING / ONE_PHASE | 1.270 | 0.610 | 2.08x |
| 2.2 / TVF HOP / STRING / ONE_PHASE | 0.708 | 0.634 | 1.12x |
| 2.2 / TVF HOP / STRING / TWO_PHASE | 0.707 | 0.338 | 2.09x |

Actual previous canonical production code (44a99949), with only the identical benchmark
fixture copied in, kept these windows on Flink. Its StreamFusion fallback medians were
0.587/0.573 s for BIGINT TVF ONE_PHASE/TWO_PHASE, and 1.281 s for legacy STRING HOP
on 2.2. The paired stock medians were 0.564/0.587/1.246 s. These controls preserve the
previous runtime and planner rather than simulating it by changing candidate settings.

A CPU profile of the first implementation identified repeated string copies and allocator
work. One measured ownership-transfer optimization pass improved STRING; no second pass
was needed. The [optimization ledger](../optimizations/aggregate-specialization-fast-paths.md#updating-string-extrema-transfer-owned-keys)
explains that change. The [evidence artifact](../benchmarks/results/updating-window-extrema-2026-10-21.json)
retains every measured trial and range, binary/source hashes, previous controls, CPU sample
summary, parity/recovery checks and unchanged upstream native-execution evidence.

## Updating SESSION and processing-time windows

SESSION keeps signed COUNT/SUM/AVG and typed MIN/MAX state, including independent FILTERs,
all-NULL groups and complete deletion. A zero total input count suppresses final output; the
empty namespace still participates in session merging until its timer closes. Flink 2.2 admits
updating SESSION TVFs; both releases admit legacy SESSION. Requesting TWO_PHASE retains the
released single-phase SESSION plan. Processing-time fixed TVFs on 2.2 use the same signed
fixed-window kernels with clock assignment and timers.

Retractions preserve arrival order across session bridges. Sorting all incoming rows by time
and merging a whole connected component before applying its rows could move a merge ahead of
an earlier deletion and change a cached winner. The updating path batches only equal timestamps,
which share an assignment. It retains the first existing accumulator and merges other overlapping
sessions into it, preserving Flink's retained-state boundary while avoiding unnecessary Arrow
partial materialization. Append-only sessions keep their existing larger connected runs.

Checkpoint tests cover signed sessions on memory and the native RocksDB backend, two SQL
restores with STRING/DECIMAL/nanosecond extrema, and event/processing-time COUNT with backend
transitions. COUNT uses direct typed RocksDB state; mapped extrema retain the snapshot
compatibility path. Controlled clocks test processing-time firing independently of live-job
wall-clock timing, and SQL tests require consumed native operators on admitted paths.

Assigned-window consumers retain the Arrow row-kind sidecar when preparing their aggregate
batches. Standalone TVFs are changelog-safe: 2.2 preserves input row kinds, while 1.18 retains
its released insert-only output behavior. Attached event-time windows admit signed local/global
state, including filters that depend on assigned bounds. Processing-time attached consumers
register the native state's actual earliest window end and reconstruct it on restore, so HOP
and CUMULATE fire at each assigned end rather than only a fixed enclosing grid boundary.
Their released processing-time plan remains single-phase.

A predicate that calls on processing-time window bounds makes released Flink lower the
query to ordinary GROUP BY over the assigned columns, rather than a window aggregate.
StreamFusion retains that lowering and requires native assignment plus the native group
aggregate. Event-time bounds-dependent predicates retain the attached window aggregate.

### Updating SESSION and attached-window measurements

Release+mimalloc, JDK 17, one million inputs, 64 keys, parallelism two with two task slots
and a 2 GiB heap; two warmups and three alternating measured engine pairs. SESSION uses a
ten-second gap. Attached HOP uses a 2 s/10 s grid and an aggregate FILTER that references
window_start, retaining standalone assignment. Complete jobs include native Top-1 input,
both perimeter transposes, JNI, exchange/state and a rowwise blackhole sink. Every candidate
requires consumed native final windows; attached splits require consumed local/global and
assignment operators. These diagnostics do not substitute a Java JNI timer for the small
Rust/Arrow functionality boundary.

| Flink / context / type / phase | Stock median (s) | Native median (s) | Stock/native |
|---|---:|---:|---:|
| 1.18 / legacy SESSION / BIGINT / ONE_PHASE | 0.886 | 0.398 | 2.23x |
| 1.18 / legacy SESSION / STRING / ONE_PHASE | 0.938 | 0.495 | 1.90x |
| 2.2 / attached HOP / BIGINT / ONE_PHASE | 1.491 | 0.992 | 1.50x |
| 2.2 / attached HOP / BIGINT / TWO_PHASE | 1.635 | 0.675 | 2.42x |
| 2.2 / SESSION TVF / BIGINT / ONE_PHASE | 0.839 | 0.493 | 1.70x |
| 2.2 / SESSION TVF / DECIMAL(38,8) / ONE_PHASE | 0.864 | 0.508 | 1.70x |
| 2.2 / legacy SESSION / STRING / ONE_PHASE | 0.936 | 0.498 | 1.88x |
| 2.2 / SESSION TVF / STRING / ONE_PHASE | 0.919 | 0.512 | 1.80x |

Actual previous production code at 06813ae3 retained SESSION and updating attached windows
on Flink. Previous StreamFusion medians were 0.890 s for STRING SESSION TVF and 0.942 s
for legacy STRING SESSION. Attached BIGINT HOP measured 1.531/1.621 s in ONE_PHASE/TWO_PHASE,
versus candidate 0.992/0.675 s. Only the identical
benchmark fixture was copied and compiled for these controls; its planner/runtime/POM and
previous native DSO were preserved. The [evidence artifact](../benchmarks/results/updating-window-changelogs-2026-10-21.json)
retains each previous-control trial alongside the candidate, stock and pre-optimization
trials, source/binary hashes, CPU sample summary and exact execution/recovery checks.
One measured optimization pass removed redundant SESSION accumulator reconstruction;
no second pass was needed. Processing-time tests use controlled clocks and make no separate
throughput claim for live-job timer scheduling.
