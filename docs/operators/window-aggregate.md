# Window aggregate

**Status:** Native, both event-time and processing-time, with the gaps enumerated below.

This page covers the windowed `GROUP BY` aggregate — `TUMBLE`/`HOP`/`CUMULATE`/`SESSION`, single-phase
and the two-phase local/global split — and the windowing-TVF operator that assigns each row to its
window(s) ahead of a downstream consumer (an aggregate, a [window join](joins/window-join.md), or
window Top-N/dedup).

## Mixed aggregates and AVG partials

SUM, MIN, MAX, COUNT and AVG can share a window and read the same or different numeric columns.
AVG supports INT/BIGINT, SMALLINT/TINYINT, FLOAT/DOUBLE and DECIMAL in single-phase execution
and the event-time two-phase split for TUMBLE, HOP, CUMULATE and admitted attached windows.

The local emits every accumulator field in aggregate order. AVG contributes adjacent sum and
count fields; later aggregates and an optional synthetic row count start after that pair.
The global merges each pair together before dividing. Integral sums widen to BIGINT with Java's
wrapping arithmetic, but the average retains its declared integral result type. FLOAT sums widen
to DOUBLE and narrow only the result. DECIMAL preserves the sum scale, sticky overflow, exact
division and Flink's result scale. Empty/all-NULL groups keep the host NULL/count behavior.

Partials remain Arrow through the local, exchange and global operators. A checkpoint barrier
drains local slices into the global before snapshotting; AVG pairs use the existing flattened
accumulator checkpoint layout in memory and direct RocksDB state. Restore tests merge subsequent
partials and verify every hopping window after RocksDB checkpoints and both backend transitions.

The local stage retains rows whose slice has already fired: that slice can still belong to
an open HOP or CUMULATE window. The global merge admits each partial only into final windows
that have not fired, so late partials cannot reopen completed windows. This also applies after
checkpoint restore and when the late row introduces a new key. TUMBLE drops the partial once
its single final window has closed. Tests compare explicit watermarks, mixed ordinary/distinct
aggregates and both aggregation phases against released Flink.

## Retracting COUNT/SUM/AVG and grouping-only windows

Aligned event-time TUMBLE, HOP and CUMULATE accept updating input, including native Top-N,
for unfiltered SUM/AVG over all numeric types, COUNT over the
supported numeric value columns, COUNT(*), and grouping-only windows without aggregate
functions. Both single-phase and local/global execution remain columnar.

Every input retains its INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE sign. SUM carries Flink's
nullable sum and signed non-NULL count; COUNT carries a signed count. A separate live-row
count, or an existing COUNT(*), suppresses a final group only when that count equals zero.
A live all-NULL group therefore emits COUNT 0/SUM NULL, while an unmatched delete can produce
negative counts and sums as it does in released Flink. Integer sums preserve their declared
width and wrapping behavior. Top-1 replacing 10 with 20 leaves SUM 20, and moving the last
live row out of a window removes its old group.

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
TIMESTAMP and TIMESTAMP_LTZ values. It ignores NULLs and counts each value once per key/window.
TUMBLE, HOP and CUMULATE support single-phase and local/global execution; attached window
results use the existing two-phase path. SESSION and admitted legacy group windows reuse the
same distinct accumulator. Processing-time windows retain their timer-driven lifetime.

Each local partial carries an Arrow list of distinct values. The global unions those lists,
so duplicates split across tasks or checkpoint barriers count once. Ordinary aggregates and
AVG's two-field partials can share the same window. Flink's extra MapView partial fields are
replaced by these lists throughout the native local/exchange/global pipeline.

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
`window_time` stays an instant for LTZ input. The fixed session-zone offset participates in
assignment itself, so projections, filters, joins and ranking observe the same boundary values
as Flink. Window rank and join translate their clock threshold into this local domain and close
at `window_end - 1` millisecond, including processing-time timers and restored state.

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
- Fixed-grid windows (TVF **and** legacy, event-time or proctime) over `TIMESTAMP_LTZ` unless the
  session zone has one fixed offset for the entire timestamp range that is an integral multiple of the window slide
  (the max size for `CUMULATE`). Flink assigns and fires on a DST-aware local-time grid while the
  native operators bucket on the epoch grid; the two coincide exactly under that condition. The gate
  applies uniformly to every consumer of the assignment — the windowing TVF, single- and two-phase
  window aggregates, window join, and window Top-N/dedup — so a whole window pipeline falls back
  together rather than mixing host-assigned and native-assigned bounds.
- `SESSION` windows (TVF and legacy) over `TIMESTAMP_LTZ` when the session zone has any historical
  or recurring transition; changing offsets can alter gap connectivity and session merges. A fixed offset cancels
  out of the gap arithmetic, so fixed-offset zones stay native with no alignment requirement.
- Region zones with only pre-1970 transitions also fall back: their earlier offsets may change
  assignment and firing for negative epochs. Use a fixed zone such as `GMT+05:30` when fixed-offset
  semantics are intended; it is not equivalent to `Asia/Kolkata` over the full timestamp range.
- Legacy processing-time `SESSION`.
- Key type outside bigint/int/string/boolean/date/timestamp/decimal.
- A value type/aggregate mismatch.
- Single-phase aggregation over attached window bounds. Attached windows are native through
  the two-phase local/global path.
- Windowed DISTINCT other than unfiltered, single-argument COUNT over the types listed above:
  SUM/AVG DISTINCT, filtered COUNT DISTINCT, FLOAT/DOUBLE, BOOLEAN, TIME and complex values.
  Non-windowed DISTINCT has separate coverage; see [GROUP BY](group-by.md).
- Retracting input outside aligned event-time TUMBLE/HOP/CUMULATE with grouping-only,
  unfiltered numeric SUM/AVG, numeric COUNT(value), and COUNT(*).
  DISTINCT, MIN/MAX, filters, processing-time, attached, session
  and legacy windows still fall back on updating input. Admission checks the **input**
  changelog even when final output is append-only.
  The diagnostic names the supported retracting forms. Remaining coverage is tracked in
  [#99](https://github.com/datafusion-contrib/StreamFusion/issues/99).
- Flink's reduction of overlapping `COUNT(v), AVG(v), SUM(v)` calls over the same BIGINT
  or DECIMAL value into internal aggregates and attached window bounds. These rewritten
  plans retain fallback in both aggregation phases; the individual direct aggregate forms
  above remain supported.
- Flink's optional `table.optimizer.distinct-agg.split.enabled=true` rewrite. The unchanged
  split-distinct IT variants introduce extra window layers, including attached single-phase
  aggregation and partial layouts outside current admission. `HASH_CODE` itself is supported;
  the remaining physical window layouts are tracked in
  [#166](https://github.com/datafusion-contrib/StreamFusion/issues/166).
  Those variants fall back as a complete pipeline. The same queries with distinct splitting
  disabled use the native value-set path; upstream execution contracts verify both routes.

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
