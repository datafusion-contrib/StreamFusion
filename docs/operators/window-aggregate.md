# Window aggregate

**Status:** Native, both event-time and processing-time, with the gaps enumerated below.

This page covers the windowed `GROUP BY` aggregate — `TUMBLE`/`HOP`/`CUMULATE`/`SESSION`, single-phase
and the two-phase local/global split — and the windowing-TVF operator that assigns each row to its
window(s) ahead of a downstream consumer (an aggregate, a [window join](joins/window-join.md), or
window Top-N/dedup).

Grouping-only windows (a windowed `GROUP BY` without aggregate calls) retain each distinct group
on disk as well as in memory. RocksDB persists their window bounds even though there are no
accumulator columns; duplicate groups, watermark firing, and checkpoint recovery preserve the
same output as the memory backend.

Aligned windows fire when the watermark or processing clock reaches `window_end - 1ms`,
including the local/global split and persistent state. Event-time rows for a fired final window are late
and cannot change its result. Processing-time arrivals remain admissible, including arrivals
at the same clock reading after a timer callback, matching Flink's processing-time behavior. Native storage indexes exclusive window ends, so the Java boundary
translates the firing frontier by one millisecond before flushing.

## Mixed aggregates and AVG partials

SUM, MIN, MAX, COUNT and AVG can share a window and read the same or different numeric columns.
String MIN/MAX can share those windows and their local/global partial layout as well.
On insert-only input, MIN/MAX also accept TIMESTAMP and TIMESTAMP_LTZ value columns at
precisions 0 through 9. These are aggregate values, separate from the window's time attribute.
The accumulator owns signed milliseconds and the fractional nanosecond remainder, compares
that pair without narrowing the date range, ignores NULLs and emits NULL for an all-NULL group.
Local/global partials and checkpoint state use one nullable component timestamp with the
original logical precision. Ordinary COUNT(timestamp) retains its separate numeric type gate.
Both timestamp kinds can share a window with numeric aggregates, string extrema and COUNT(DISTINCT).
Mixed string/temporal SQL comparisons cover TUMBLE/HOP/CUMULATE at precision 0/3/6/9
in both phases. They verify default single-phase string fallback and the explicit native opt-in;
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
Append-only session and legacy windows use the same accumulator factory; attached bounds
retain their existing two-phase-only gate. SQL comparisons cover these layouts as well.

Released Flink cannot initialize AVG/SUM0 over `DECIMAL(38,38)`. That shape stays on Flink,
including its planning failure; floating SUM0 and DISTINCT SUM0 also stay outside admission.

The local stage retains rows whose slice has already fired: that slice can still belong to
an open HOP or CUMULATE window. The global merge admits each partial only into final windows
that have not fired, so late partials cannot reopen completed windows. This also applies after
checkpoint restore and when the late row introduces a new key. TUMBLE drops the partial once
its single final window has closed. Tests compare explicit watermarks, mixed ordinary/distinct
aggregates and both aggregation phases against released Flink.

## String MIN/MAX

Append-only windows admit `MIN` and `MAX` over CHAR and VARCHAR (including SQL STRING).
Event-time local/global execution is admitted by default. Single-phase execution, including
processing-time, session and legacy windows, requires the explicit table setting
`streamfusion.window.string-extrema.single-phase.enabled=true`. Its default is `false` because
the high-cardinality single-phase CUMULATE benchmark regressed against Flink. Numeric aggregates
keep their existing admission; a mixed single-phase query containing string extrema falls back
as a whole unless the option is enabled.
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

Existing window/time and FILTER restrictions still apply. In particular, MIN/MAX over updating
input remains unsupported. Ordinary COUNT(string), SUM(string) and AVG(string) are not newly
admitted by the string-extrema gate.

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
The single-phase regression is not accepted as a default acceleration path. Inspection of the
native fold shows per-group Arrow gathers and generic string accumulator updates; optimization
of that high-cardinality path remains necessary before removing the opt-in. Contiguous groups
now use retained Arrow slices as described in the later measurements; those TUMBLE results do
not establish the performance of every HOP/CUMULATE shape, so the opt-in stays in place.

Additional SQL comparisons cover embedded NULs, long values, supplementary characters and
runtime lone-surrogate strings at parallelism one and two, including Flink source serialization
before extrema ordering. HOP recovery injects two failures after completed checkpoints on memory
and RocksDB and requires work from every native stage. Controlled-clock processing-time tests
verify extrema, NULL-only groups and timer firing after restore on both backends. Single-phase
SQL and recovery coverage explicitly enables the experimental switch; default admission stays
as documented above.

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

Updating input still requires a retractable extrema state and falls back. Processing-time,
attached, session and legacy filtered windows also retain their current gates.

## Retracting COUNT/SUM/AVG and grouping-only windows

Aligned event-time TUMBLE, HOP and CUMULATE accept updating input, including native Top-N,
for SUM/AVG over all numeric types, COUNT over the
supported numeric value columns, COUNT(*), and grouping-only windows without aggregate
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

Unfiltered `COUNT(DISTINCT value)` is native for integer, DECIMAL, CHAR/VARCHAR, DATE,
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
stage. Updating input, filtered processing-time/attached/session/legacy windows, and the
split-distinct rewrite remain outside this extension.

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
Legacy event-time `SESSION(...)` is native too. Legacy group windows have no offset and never use
Flink's two-phase local/global optimization.

The auxiliary properties retain Flink's legacy layout and types: start and end are plain
`TIMESTAMP(3)`, rowtime is the window end minus one millisecond, and the internal proctime marker is
null before the outer Calc materializes the current clock. Queries that select no auxiliary window
property are native as well.

## Event-time assignment

`TUMBLE`, `HOP`, and `CUMULATE` are native only at **zero offset**; `SESSION` needs no offset. The
window bounds render differently depending on the rowtime attribute's type: in the session time zone
for a local-time-zone attribute, in UTC (the raw wall-clock value) for a plain `TIMESTAMP`.

## Processing-time (proctime) assignment

`TUMBLE`/`HOP`/`CUMULATE` are native on proctime: each row is assigned to the window(s) covering the
operator's current processing-time clock and fired on a processing-time timer. `HOP` and `CUMULATE`
leave several windows open at once, so the timer chains — each firing emits the earliest-ending open
window and schedules the next slide boundary, until the clock has passed the latest open window's end.
This requires **slide divides size**, so every window end lands on a slide boundary.

`SESSION` is native on proctime too: the gap is measured on the processing-time clock, and each batch
registers a cleanup timer at `now + gap` — the earliest the session could close with no further input.
A later element extends the session (merged in the native aggregator) and registers its own later
timer, so a firing emits only the sessions the clock has truly left behind by a full gap.

Proctime support is currently **single-phase only**: a single-phase `TUMBLE`/`HOP`/`CUMULATE` whose
slide divides its size, or a single-phase `SESSION`. The two-phase local/global split is not yet on
the processing-time-timer path.

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
under the same **zero-offset** `TUMBLE`/`HOP`/`CUMULATE` restriction; both its event-time and
proctime assignment paths are native.

Standalone event-time TVFs accept both `TIMESTAMP(3)` and `TIMESTAMP_LTZ(3)` rowtime.
Plain `TIMESTAMP` retains its wall-clock boundaries in every session zone, including downstream
window Top-N and window deduplication. Native assignment preserves hidden sub-millisecond input
fractions while emitting millisecond window boundaries; negative epochs and years 0001/9999 are
covered by runtime SQL parity tests. TUMBLE and HOP with an explicit nonzero offset fall back,
matching the existing aggregate and CUMULATE restriction.

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

- Window not event-time `TUMBLE`/`HOP`/`CUMULATE` (zero offset) over a local-time-zone or plain
  `TIMESTAMP` rowtime.
- Proctime: anything other than a single-phase `TUMBLE`/`HOP`/`CUMULATE` with slide dividing size, or
  a single-phase `SESSION` — the two-phase local/global path isn't yet native on proctime.
- `HOP` slide / `CUMULATE` step that doesn't divide the window size.
- Legacy row-count `TUMBLE`/`HOP` windows from the Table API.
- Legacy early/late firing or allowed lateness.
- A legacy group window over retracting or updating input.
- Legacy proctime `HOP` when the slide does not divide the size. Event-time legacy `HOP` supports
  non-dividing and gapped windows.
- Legacy processing-time `SESSION`.
- Key type outside bigint/int/string/boolean/date/timestamp/decimal.
- A value type/aggregate mismatch.
  TIMESTAMP/TIMESTAMP_LTZ values are admitted for MIN/MAX and COUNT(DISTINCT),
  while ordinary COUNT(value) remains numeric.
- Single-phase string MIN/MAX without `streamfusion.window.string-extrema.single-phase.enabled=true`.
  The flag is experimental and defaults off; local/global string extrema do not require it.
- Attached bounds outside append-only event-time TUMBLE/HOP/CUMULATE, or with FILTER.
  Admitted attached bounds support opt-in single-phase and existing local/global execution.
- Single-phase attached bounds without `streamfusion.window.attached.single-phase.enabled=true`.
  The default-off gate preserves stock execution for profiled HOP/high-cardinality and
  persistent COUNT cases that remain slower.
- Windowed DISTINCT other than single-argument COUNT over the types listed above:
  SUM/AVG DISTINCT, FLOAT/DOUBLE and complex values.
  Non-windowed DISTINCT has separate coverage; see [GROUP BY](group-by.md).
- Filtered MIN/MAX on updating input, and filtered aggregates in processing-time, attached,
  session or legacy windows.
- Retracting input outside aligned event-time TUMBLE/HOP/CUMULATE with grouping-only,
  numeric SUM/AVG (including shared exact SUM0), numeric COUNT(value), and COUNT(*) with optional FILTER.
  DISTINCT, MIN/MAX, processing-time, attached, session
  and legacy windows still fall back on updating input. Admission checks the **input**
  changelog even when final output is append-only.
  The diagnostic names the supported retracting forms. Remaining coverage is tracked in
  [#99](https://github.com/datafusion-contrib/StreamFusion/issues/99).
- Unverified physical layouts from Flink's optional
  `table.optimizer.distinct-agg.split.enabled=true` rewrite. With two-phase aggregation,
  the verified TUMBLE/HOP/CUMULATE layouts now run natively: `HASH_CODE` assigns distinct
  buckets and exact SUM0 reductions merge their counts. The unchanged upstream DISTINCT
  window tests require nonempty work from both native local and global stages. SQL parity
  also covers multiple distinct arguments, mixed exact aggregates, NULL-only groups, result
  types and RowKinds. Attached single-phase aggregation still falls back as a complete
  pipeline; a forced `ONE_PHASE` regression verifies that gate. Other split layouts remain
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
`streamfusion.window.string-extrema.single-phase.enabled`; the production default remains off.
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
HOP/CUMULATE performance, and single-phase admission remains opt-in. No samples are discarded.
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

Append-only event-time results from TUMBLE/HOP/CUMULATE can be re-aggregated natively in
ONE_PHASE plans over their attached `window_start`/`window_end` with
`streamfusion.window.attached.single-phase.enabled=true`. The option defaults off: the first
release comparison found slower high-cardinality HOP and persistent COUNT pipelines. The outer operator folds each
row into the one window it names, using the existing accumulator, Arrow output and keyed
checkpoint/store representation. It does not assign another set of overlapping windows or
reconstruct a start from a slice end. The normal numeric/COUNT(DISTINCT)/timestamp aggregate,
key-type and single-phase string-extrema admission rules remain in force. FILTER and retracting
attached input remain stock; attached sessions are outside this fixed-window whitelist.

With the option disabled, fallback diagnostics retain the existing "attached-window aggregation
requires two-phase execution" prefix and explain the single-phase opt-in requirement.

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

The native route therefore remains **default off**. Low-cardinality memory workloads accelerate;
high-cardinality HOP and persistent COUNT cases retain substantial losses. Enabling the option
is an explicit experimental choice and does not promise acceleration for those cases. Further
persistent-window work is linked to [#350](https://github.com/datafusion-contrib/StreamFusion/issues/350);
its existing calendar/count investigation covers the same read/write machinery, while the
matrix here also records ordinary UTC windows and higher cardinality.

The initial native-only 20M HOP/4096/Rocks CPU capture has 11,097 samples; 4,338 include bulk
reads, 2,637 include DB writes, and 9,705 include the inner window push. These are inclusive
stacks that can overlap. The diagnostic was deliberately stopped after capture and its
instrumented timings are not comparison results. Exact COUNT panes and sorted bulk state I/O
subsequently reduce the 1M high-cardinality memory HOP native median from 0.812 to 0.402s;
that profile still loses its paired stock control. Complete-job measurements retain those
limits and all initial/revised unfavorable trials. The
[trial artifact](../benchmarks/results/attached-windows-2026-10-14.json) and
[CPU capture](../benchmarks/results/attached-windows-2026-10-14-cpu.collapsed.gz) retain the evidence.
The final release DSO SHA256 is
`d09c0f6c60b1cc9aaae634af63b154dfbb33c130aeba54cd2550de6f94a7b3db`.
