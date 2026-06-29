# Coverage and fallbacks

What StreamFusion does **not** accelerate, and **every** condition that makes a query (or part of one)
fall back to stock Flink. The [Compatibility Chart](../readme.md#compatibility-chart) is the source of
truth for what *is* accelerated and under what terms; this is its complement — the boundaries.

> Keep this current. When an operator, type, expression, or connector gains or loses support, update
> this file in the same commit (alongside the readme chart). It is meant to always answer "why didn't
> my query accelerate?" precisely.

A query accelerates only if it forms **one fully-columnar island**: every operator but a rowwise
source/sink runs natively (Arrow in/out). One unsupported interior operator therefore drags the
**whole** query back to Flink (the all-or-nothing gate below). Use `NativePlanner.explain(...)` or
`-Dstreamfusion.logFallbackReasons=true` to see the recorded reason(s) for a given query.

**What counts as a fallback.** A fallback is something **Flink executes that we don't accelerate** —
a real gap we could close. It is *not* a fallback when Flink itself rejects the query in streaming
(e.g. `RANK`/`DENSE_RANK` Top-N, `LEAD`/non-time/`FOLLOWING` `OVER`, non-temporal `ORDER BY`): we match
Flink by also not running it, which is **parity**, not a gap. Nor is it a fallback when the feature
already accelerates via another plan shape (e.g. `UNION` distinct, which the host rewrites to a
`GROUP BY`). This file lists only real gaps; parity cases are called out as such where they'd otherwise
look like one.

---

## (a) What we don't support

### Whole operators with no native path
These have no matcher; any query containing one falls back entirely.

| Operator | SQL surface |
|---|---|
| `Correlate` | lateral table functions, and `UNNEST` with a pushed condition the expression engine can't encode (or any condition over a LEFT unnest). INNER **or LEFT** `UNNEST` of a single `ARRAY` (scalar or `ROW` element, flattened), `MAP` (key+value), or `MULTISET` (element by count) column — optionally `WITH ORDINALITY`, INNER including a pushed element filter — **is** supported (see the chart). |
| `LookupJoin` | dimension-table / async lookup join — an **intentional** architectural fallback, not a gap to close. Its work is an external I/O call into a host-Java `LookupFunction` (JDBC/HBase/…), not vectorizable compute, and the async path needs Flink's `AsyncWaitOperator` + mailbox purely for ordered emit; running it inside the all-or-nothing island would mean JNI-upcalling the JVM connector per batch. (Arroyo can do this natively only because *its* connectors are native Rust; Flink's are JVM.) |
| `Match` | `MATCH_RECOGNIZE` (CEP / row-pattern) |
| `GroupWindowAggregate`, `GroupWindowTableAggregate` | legacy `GROUP BY TUMBLE(...)` windowing, and proctime group windows |
| `IncrementalGroupAggregate` | the three-phase (distinct) non-windowed `GROUP BY`. The ordinary two-phase / mini-batch `LocalGroupAggregate` + `GlobalGroupAggregate` (+ `MiniBatchAssigner`) **is** native — see the feature gaps and §2 below |
| `GroupTableAggregate` | `TableAggregateFunction` |
| `DropUpdateBefore`, `Values` | misc (a non-temporal `Sort` is parity — Flink rejects it in streaming) |
| `LegacyTableSourceScan`, `LegacySink` | legacy connectors |
| `Python*` (`PythonCalc`/`PythonCorrelate`/`PythonGroupAggregate`/`PythonOverAggregate`/…) | PyFlink UDFs |

### Feature gaps inside operators we *do* support
(Real gaps only — Flink runs these and we don't yet. Ordering a nested value, `MAX(array)`/`ORDER BY
array`, is **not** here: Flink rejects it too, so we're at parity.)
- **Aggregates** — non-windowed `GROUP BY` `SUM`/`MIN`/`MAX`/`COUNT` over `DECIMAL` **are** native
  (`SUM` → `DECIMAL(38, s)` with overflow → NULL; `MIN`/`MAX` → `DECIMAL(p, s)`; an i128 at scale `s`,
  matching Flink). Decimal `AVG`, two-phase decimal `SUM`, and window-aggregate decimal `SUM`/`AVG`
  still fall back. `AVG` is modeled only via the host's SUM/COUNT rewrite (not natively); two-phase
  `AVG`; window `AVG` only as a lone aggregate; value types outside
  bigint/double/int/smallint/tinyint/float/decimal (see `aggregate-type-support.md`).
- **Two-phase (mini-batch) `GROUP BY`** — all four operators run native: a native `MiniBatchAssigner`
  emits the proc-time marker, the local is a transient in-memory bundle flushed on that marker / a
  `mini-batch.size` trigger / before each checkpoint (no checkpointed state, like Flink's
  `MapBundleOperator`), the keyed shuffle is a native exchange, and the global reuses the single-phase
  group-aggregate operator (`COUNT` merges as a `SUM` over partial counts).
  Scope: SUM/MIN/MAX/COUNT over bigint/int/double, with **no widening of the partial** — `SUM(INT)`
  (whose partial Flink widens to bigint) routes single-phase, as does `AVG`/distinct (the latter plans
  as `IncrementalGroupAggregate`). Row-time mini-batch falls back.
- **`OVER`** — the unbounded `RANGE … CURRENT ROW` frame (running fold), the bounded
  `ROWS BETWEEN n PRECEDING AND CURRENT ROW` frame (recomputed over the row slice), **and** the
  bounded `RANGE BETWEEN INTERVAL n PRECEDING AND CURRENT ROW` frame (recomputed over the rowtime
  interval), over one ascending rowtime, each aggregate over its own (possibly different)
  bigint/int/smallint/tinyint/double/float value column (narrow ints / 4-byte float keep the host's
  narrow result type). **Proctime** order is native too (arrival order, eager emit) for the running
  and bounded-ROWS frames. Real gap: none beyond the parity cases — a bounded-RANGE frame over
  proctime (wall-clock interval, non-deterministic), more than one window group, decimal bounded
  frames, `FOLLOWING` frames, non-time/descending order, and `LAG`/`LEAD` are all parity (Flink
  rejects or single-groups them in streaming).
- **Deduplication** — all four variants are native: rowtime keep-first (insert-only, watermark-
  released) and keep-last (retracting), and proctime keep-first/keep-last (arrival order, no
  watermark). The proctime order key is materialized by the native `PROCTIME()` expression.
- **Joins** — regular/interval/window joins: a residual non-equi predicate must be expressible by the
  native expression engine (event-time and proctime interval and window joins are all native).
  **Temporal table join** (`FOR SYSTEM_TIME AS OF probe.rowtime`) is native for INNER and LEFT over
  event time: the build side is held as per-key versioned state (changelog `+I`/`+U`/`-D`, indexed by
  rowtime), and on a watermark each buffered probe row joins the version valid at its time — a faithful
  port of Flink's `TemporalRowTimeJoinOperator`, deterministic and value-compared to the host. Gaps:
  a residual non-equi predicate beyond the temporal condition falls back (v1); a **processing-time**
  temporal table join is parity (Flink itself rejects it — FLINK-19830), as is the legacy proctime
  temporal *function* join.
- **Sources/sink** — local `file:` path only (Parquet/ORC source, Parquet sink); Kafka decode limited
  (see below); CDC only Debezium/OGG JSON.
- **Proctime** support, by operator:
  - **Deduplication** and **`OVER`** (running / bounded-ROWS) — native; they emit eagerly in arrival
    order, no wall-clock timer needed.
  - **`TUMBLE`/`HOP`/`CUMULATE` window aggregate** — native; each row is assigned to the window(s)
    covering the operator's current processing-time clock and fired on a processing-time timer. `HOP`
    and `CUMULATE` leave several windows open at once, so the timer chains: each firing emits the
    earliest-ending open window and schedules the next slide boundary, until the clock has passed the
    latest open window's end (the slide must divide the size so every window end lands on a slide
    boundary). Non-deterministic, so routing/execution are tested but the result is not byte-compared
    to the host (see the CLAUDE.md note).
  - **Session window aggregate** — native; the gap is measured on the processing-time clock and each
    batch registers a cleanup timer at its `now + gap`, the earliest the session could close with no
    further input. A later element extends the session (merging in the native aggregator) and
    registers its own later timer, so a firing emits only the sessions the clock has truly left behind
    by a full gap. Non-deterministic, so routing/execution are tested but not byte-compared.
  - **Windowing TVF, window join, window Top-N / dedup** — native; the windowing TVF assigns each row
    to the window(s) covering the clock (instead of reading a rowtime column), and the downstream
    window join (two-input) and window rank close those windows on a chained processing-time timer
    (the same next-slide-boundary model as the window aggregate) rather than a watermark. The slide
    must divide the size. Non-deterministic, so routing/execution are tested but not byte-compared.
  - **Interval join** — native; each row is timed by the operator's processing-time clock (its time
    column is stamped with the clock at push, so the interval is measured in processing time), and
    eviction advances on the clock — each batch registers a cleanup timer at `now + max(upper, -lower)`
    (the latest a row buffered now could still match), the tail draining at the last timer / on finish.
    Non-deterministic, so routing/execution are tested but not byte-compared.
  - A proctime bounded-RANGE `OVER` frame falls back: with processing time materialized as a fixed
    per-batch timestamp, a wall-clock-interval frame has no meaningful definition.
  - **Temporal table join** is event-time only by design — Flink itself rejects a processing-time
    temporal table join (FLINK-19830), so a proctime one is parity, not a gap.

---

## (b) Every cause of fallback, by layer

### 1. Global / gate — these zero out the *entire* query
- **Master switch off**: `-Dstreamfusion.native.enabled=false`.
- **All-or-nothing island gate**: after substitution, if any operator other than a rowwise source
  (leaf) or the sink (root) is still row-wise, nothing is substituted — the whole query runs on Flink.
  So one unsupported interior operator drags the whole query back.
- **Insert-only guard**: every operator except the changelog-aware ones (`GROUP BY`, regular join,
  CDC source, `Calc`, `UNION ALL`, `Expand`, `ChangelogNormalize`, streaming Top-N / `LIMIT`) requires
  an insert-only input; a retracting/updating input falls it back.
- **Per-operator kill switch**: `-Dstreamfusion.operator.<name>.enabled=false` (e.g. `filter`,
  `groupAggregate`, `union`, `limit`, `expand`, `changelogNormalize`, `windowRank`,
  `localGroupAggregate`, `miniBatchAssigner`, …). The two-phase global half reuses the
  `groupAggregate` switch. `kafkaSource` defaults to *false*.

### 2. Per-operator matcher declines (exact conditions)
- **OVER** — a frame not of the form `… PRECEDING .. CURRENT ROW` (a `ROWS`/`RANGE` lower bound that
  is not a constant preceding offset); a bounded-RANGE frame over a proctime order (wall-clock
  interval, non-deterministic); an aggregate that is `AVG`, `COUNT(*)`, or reads a non-numeric /
  decimal column (numeric value columns are bigint/int/smallint/tinyint/double/float); `PARTITION BY`
  key outside bigint/int/string/boolean/date/timestamp/decimal. (More than one window group, decimal
  bounded frames, non-time/descending order, `FOLLOWING` frames, and `LAG`/`LEAD` never reach us —
  Flink rejects or single-groups them in streaming.)
- **Interval join** — not INNER/LEFT/RIGHT/FULL; no equi key; non-null-dropping (non-INNER) keys;
  equi-key type outside the supported set; non-equi residual not expressible. (Event-time and proctime
  bounds are both native — proctime times rows by the clock and evicts on a processing-time timer.)
- **Window join** — same key/type/non-equi conditions; both sides must carry a window-attached
  windowing of the same time semantics (both event-time or both proctime). Proctime closes the
  window on a processing-time timer instead of a watermark.
- **Temporal join** — not INNER/LEFT (Flink rejects RIGHT/FULL); no equi key; non-null-dropping keys;
  equi-key type outside the supported set; a residual non-equi predicate beyond the `FOR SYSTEM_TIME`
  condition (v1); a processing-time temporal join (parity — Flink rejects it for a versioned table).
- **Regular join** — unsupported join type; no equi key; non-null-dropping keys; non-equi residual not
  expressible; an input column type the converter can't carry.
- **Window aggregate / local / global** — window not event-time `TUMBLE`/`HOP`/`CUMULATE` (zero offset)
  over a local-time-zone rowtime — or, for **proctime**, a single-phase `TUMBLE`/`HOP`/`CUMULATE` whose
  slide divides its size, or a single-phase `SESSION`; anything else proctime (the two-phase local/
  global path) is not yet on the processing-time-timer path; `HOP` slide / `CUMULATE` step doesn't
  divide size; key type outside bigint/int/string/boolean/date/timestamp/decimal; value type/aggregate
  mismatch; `AVG` (where noted); two-phase partials not single-field bigint/double.
- **GROUP BY (non-windowed)** — any aggregate other than SUM/MIN/MAX/COUNT (`AVG`, UDAF); a `DISTINCT`
  aggregate other than `COUNT(DISTINCT x)` (`SUM`/`MIN`/`MAX` `DISTINCT` fall back); idle-state TTL ≠ 0;
  an unsupported key/value column type. `SUM`/`MIN`/`MAX`/`COUNT` all admit `DECIMAL` (`SUM` →
  `DECIMAL(38, s)`, `MIN`/`MAX` → `DECIMAL(p, s)`); `COUNT(DISTINCT x)` keeps a per-key value set.
- **Local group aggregate** (two-phase local half) — any aggregate other than SUM/MIN/MAX/COUNT;
  a value type outside bigint/int/double; a partial Flink widens past the value type (e.g. `SUM(INT)`);
  an unsupported grouping-key/input column type.
- **Global group aggregate** (two-phase merge) — any merge other than SUM/MIN/MAX/COUNT; a partial
  column outside bigint/int/double; an unsupported grouping-key/output column type. (Both halves must
  match for the query to accelerate — one staying on the host drags the whole query back via the gate.)
- **Top-N** — a non-constant (variable) rank range; a row type the converter can't carry. (Insert-only
  and changelog input, an `OFFSET`, and a projected rank number are all handled. `RANK`/`DENSE_RANK`
  never reach us — Flink rejects them in streaming.)
- **LIMIT** — missing `FETCH`, or a retracting input (`OFFSET` is handled — it uses the retracting
  ranker over the insert-only input).
- **Deduplicate** — not a time-ordered rank-1. Rowtime and proctime, keep-first (`ASC`) and keep-last
  (`DESC`), are all native; a value-ordered rank-1 is a Top-N (handled separately).
- **Window Top-N / window dedup** — rank not starting at 1 (an `OFFSET`).
- **Windowing TVF** — not `TUMBLE`/`HOP`/`CUMULATE` (zero offset) over a local-time-zone time
  attribute. Both event-time (assign by rowtime) and proctime (assign by the clock) are native.
- **Event-time sort** — a secondary order key beyond the leading ascending rowtime. (A descending or
  non-time leading key is a non-temporal `Sort`, which Flink rejects in streaming — parity.)
- **Union** — a row type the converter can't carry. (`UNION` distinct is not a fallback — the host
  rewrites it to a `GROUP BY`, which routes through the aggregate path.)
- **Expand** — any project cell that isn't a column ref, a NULL literal, or the integer expand id.
- **ChangelogNormalize** — a pushed filter condition; the source-reuse variant; a row type the
  converter can't carry.
- **Watermark assigner** — only substituted when its input is already a columnar producer (otherwise
  left on host to avoid a double transpose — a no-op, not a true fallback).

### 3. Expression level — a `Calc`/filter falls back if *any* node is un-admitted
- **Unsupported function/operator** outside the admitted set (e.g. `MD5`; `CONCAT` for a NULL-semantics
  divergence; …).
- **CAST** — anything that isn't widening numeric (integer→wider int, integer→float/double,
  float→double); narrowing, float→int, and string casts fall back.
- **Incompatible functions** — off by default, native only under
  `-Dstreamfusion.expression.<NAME>.allowIncompatible=true` (or the blanket flag): `UPPER`, `LOWER`,
  `EXP`, `LN`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `LOG10`, and `ROUND` on float/double.
- **Literal/arity guards** — unsupported literal type; `SUBSTRING` non-literal or out-of-range
  start/length; `LEFT`/`RIGHT`/`REPEAT`/`LPAD`/`RPAD` non-literal or negative count; `TRIM` other than
  default BOTH-whitespace; `POSITION` with a FROM start; wrong arity for any admitted function.

### 4. Type level
- **Scalar leaf types.** Every column (and every nested leaf) must be a type the boundary converter
  carries: tinyint/smallint/int/bigint/float/double/boolean/char/varchar/timestamp/timestamp-ltz/date/decimal.
  Anything outside that — `TIME`, interval, raw/binary — falls back. Both gates
  (`FilterCalcMatcher.convertibleRow` for filter/`Calc`, `RowDataArrowConverter.supports` for the
  keyed/stateful operators) check this recursively.
- **Nested `ARRAY`/`MAP`/`ROW`/`MULTISET` are supported** (recursively, down to supported leaves; a
  `MULTISET<E>` rides the Arrow boundary as a `MAP<E, INT>`): carried through filters/projections,
  usable as a GROUP BY / join / dedup **key** (the nested value rides the row state as a DataFusion
  `ScalarValue` and is cast back to its declared column type on emit), and as a `COUNT` value column
  (counted for null-ness only). What still falls back for a nested column:
  - **Ordering a nested value** — `MAX`/`MIN` over it, `ORDER BY` it, or a Top-N/sort on it. Flink
    itself rejects `MAX(array)` and `ORDER BY array`, so this matches the host.
- **Key types** outside bigint/int/string/boolean/date/timestamp/decimal **(plus the nested types
  above)** for join/OVER/window/group keys.
- **Aggregate value types** outside the parity matrix in `aggregate-type-support.md`. Non-windowed
  `GROUP BY` `SUM`/`MIN`/`MAX`/`COUNT` now cover `DECIMAL`; decimal `AVG` and window-aggregate decimal
  `SUM`/`AVG` still fall back.

### 5. Source / sink / connector
- **Filesystem** — non-local path (`hdfs:`/`s3:`/…) for the Parquet/ORC source and Parquet sink; any
  non-Parquet/ORC source format; any non-Parquet sink format.
- **Kafka** — value format outside JSON/CSV/raw/bare-Avro/protobuf; `avro-confluent` (decoder exists,
  not wired); a `key.format`; a topic pattern; specific-offsets / unsupported bounded startup mode;
  protobuf fields needing representation reconciliation (enum/unsigned/bytes/proto3-defaults/well-known
  types).
- **CDC** — anything other than Debezium/OGG full-image JSON: Maxwell/Canal (partial-`old` not
  bit-identical), `schema-include` envelope, `ignore-parse-errors`, metadata/computed columns.
