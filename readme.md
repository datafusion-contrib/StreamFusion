# StreamFusion

Run Apache Flink SQL faster by executing supported operators natively (Rust + Apache
Arrow/DataFusion over JNI) while Flink continues to own planning, coordination, and
everything not yet supported. Substitution is transparent and conservative: a query is
planned by Flink, the operators we can reproduce exactly are swapped for native ones, and
anything else falls back to Flink with identical results.

## Compatibility Chart

What executes natively today and the conditions under which each operator is accelerated.
An operator is substituted only when **all** of its terms (plus the global terms) hold;
otherwise it runs on Flink unchanged. For the complement — everything we *don't* support and
every specific cause of a fallback — see [docs/coverage-and-fallbacks.md](docs/coverage-and-fallbacks.md).

| Operator | Accelerated | Terms |
|---|---|---|
| Projection / `Calc` (`SELECT` expressions, with optional `WHERE`) | Expressions over the admitted operations | A `Calc` — an optional condition followed by arbitrary projection expressions — built from the operations the [native expression engine](src/main/java/io/github/jordepic/streamfusion/planner/RexExpression.java) admits (below). Computed columns, constants, column reorders, searched `CASE`, `CAST` (widening/narrowing numeric, `CHAR`/`VARCHAR`→`VARCHAR`, exact-source →`DECIMAL`), extracting a field from a `ROW`/struct column (`bid.price`, nested), and `PROCTIME()` (materialized as the current processing time, which keeps a proctime-ordered operator's `PROCTIME()` projection columnar) all run natively, columnar in and out (e.g. `SELECT v + k, CASE WHEN s <> 'a' THEN k ELSE v END FROM t WHERE v > 15`). Filtered first, then projected. Any un-admitted operation anywhere in the `Calc` makes the whole node fall back. |
| Filter (`WHERE`), with column projection | Predicate expressions over the admitted operations | A filter whose predicate is built from the operations the native expression engine admits: column refs and literals (including a day-time `INTERVAL` literal, so `TIMESTAMP - INTERVAL '10' SECOND` arithmetic works), arithmetic (`+` `-` `*` `/` `%`), the six comparisons (either operand order), `AND`/`OR`/`NOT` in any nesting, `IS NULL`/`IS NOT NULL`, `IS [NOT] TRUE`/`IS [NOT] FALSE`, searched `CASE`, `COALESCE`/`NULLIF`, extracting a field from a `ROW`/struct column (`bid.price`, nested — mirrors DataFusion's `get_field`, NULL for a null struct), **`CAST`** — widening numeric (integer→wider integer, integer→float/double, float→double), **narrowing integer→integer and float/double→integer** (a native wrapping/saturating kernel matching Flink's primitive Java cast), **`CHAR`/`VARCHAR`→`VARCHAR`** (target length ≥ source, an unpadded no-op), and **→`DECIMAL`** from an exact (decimal/integer) source — `LIKE`, `POSITION`, `ABS`/`FLOOR`/`CEIL`/`SIGN` (float/double only), and the string functions `CHAR_LENGTH`/`TRIM`/`LTRIM`/`RTRIM` (default whitespace)/`SUBSTRING`/`LEFT`/`RIGHT`/`LPAD`/`RPAD` (literal bounds)/`REPLACE`/`REVERSE`/`REPEAT`/`ASCII`/`CHR`/`SPLIT_INDEX` (non-empty literal separator) — plus `DATE_FORMAT` over a plain `TIMESTAMP` with a literal pattern whose Java→chrono translation is byte-identical (the zero-padded numeric fields `yyyy`/`yy`/`MM`/`dd`/`HH`/`mm`/`ss` and literal separators; text/fraction/zone fields fall back), and `EXTRACT`/`YEAR`/`MONTH`/`DAY`/`HOUR`/`MINUTE`/`SECOND` over a plain `TIMESTAMP` (the integer date/time fields, identical in Flink and chrono) — the last being Flink-parity native UDFs — e.g. `WHERE v + k > 25`, `WHERE (a > 1 AND b < 2) OR c <> 3`; `BETWEEN`/`IN`/ranges expand to comparisons. The predicate is encoded and compiled once into a native handle, then evaluated per batch. An optional projection may select/reorder input columns (`SELECT a, c FROM t WHERE …`); arbitrary computed projections route through the `Calc` path above. The whole row is carried through Arrow, so every input column must be a type the [whole-row converter](src/main/java/io/github/jordepic/streamfusion/operator/RowDataArrowConverter.java) handles — the primitives, boolean, string, timestamp, date, decimal, **and nested `ARRAY`/`MAP`/`ROW`** (recursively, to supported leaves) passed through unchanged. Un-admitted operations — other functions (e.g. `CONCAT`), a number↔string `CAST`, a narrowing `VARCHAR`, or a cast to `CHAR(n)` — make the whole node fall back, as do unsupported column types (a `fallbackReasons()` entry names the cause). Integer arithmetic is computed in the operand's declared width and wraps on overflow exactly as the host does, verified at the `INT` boundary and for narrow-int (`TINYINT`/`SMALLINT`) columns; `/` and `%` match on all finite operands (divide-by-zero and `INT_MIN/-1` edges noted in [divergences/07](divergences/07-expression-encoding-and-compile-once.md)). |
| Tumbling window aggregate | Yes | Event-time `TUMBLE` over a rowtime attribute — local-time-zone (window bounds rendered in the session zone) **or a plain `TIMESTAMP`** (bounds are the raw wall-clock, rendered in UTC; the Nexmark shape) — **or proctime** `TUMBLE` (assigned by the operator's processing-time clock and fired by a processing-time timer rather than a watermark; non-deterministic, so not byte-compared to the host); **zero** or more aggregates — `SUM` / `MIN` / `MAX` / `COUNT` (and `AVG` only as a lone aggregate) — each over its own value column (so `SUM(a), SUM(b)` over different columns works), or none at all (a grouping-only window is a windowed distinct, one row per key+window — Nexmark q8); grouped by the window, optionally plus one or more bigint/int/string/boolean/date/timestamp/decimal keys. `COUNT(*)` is supported, including alongside value aggregates (it counts rows over a synthesized non-null column) and two-phase (`TUMBLE`/`HOP`/`CUMULATE`). Value-type support is the parity intersection in [docs/aggregate-type-support.md](docs/aggregate-type-support.md): all five aggregates over every non-decimal numeric type (bigint/double/int/smallint/tinyint/float), with custom accumulators where DataFusion would diverge — integer `SUM` wraps at the input width, integer `AVG` truncates, float `SUM` keeps 4-byte precision, and float/double `AVG` sum in double (float narrows the result); decimal carries `MIN`/`MAX`/`COUNT` (its `SUM`/`AVG` fall back on precision rules). |
| Hopping window aggregate | Yes | Same as tumbling, with `HOP`. One-phase assigns each row to its overlapping windows; two-phase (the default plan) pre-aggregates per slice and combines the shared slices of each window, requiring the slide to divide the size (other ratios fall back). **Proctime** `HOP` is also native (single-phase): each row is assigned to the windows covering the operator's processing-time clock and the overlapping windows close on a chained processing-time timer — each firing emits the earliest-ending open window and schedules the next slide boundary until the clock passes the latest open window (the slide must divide the size; non-deterministic, so not byte-compared to the host). |
| Session window aggregate | Yes | Same aggregate/key/value terms as tumbling, with `SESSION` (optionally `PARTITION BY` one or more bigint keys) — reached either by the windowing-TVF `SESSION` or the legacy `GROUP BY k, SESSION(rowtime, INTERVAL g)` group-window syntax (event-time; the latter is mapped to the same operator, its extra rowtime/proctime window properties emitted and projected away — Nexmark q11). Each element opens a gap-wide window; overlapping or touching windows merge, including when a late element bridges two open sessions. Always single-phase (the host never splits sessions), so no `ONE_PHASE` is needed. Columnar like the other single-phase aggregates: when the session sits on a columnar producer the keyed shuffle stays Arrow and batches feed the session aggregator with no transpose at the input (the result rows are emitted row-wise, as for `TUMBLE`/`HOP`). **Proctime** `SESSION` is also native: the gap is timed on the operator's processing-time clock and the session closes on a processing-time timer at the last element's `now + gap` — each batch registers that cleanup timer, a later element within the gap extends the session and registers a later one (non-deterministic, so not byte-compared to the host). |
| Cumulative window aggregate | Yes | Same terms as tumbling, with `CUMULATE` (zero offset only). Nested windows share a bucket start and grow by the step up to the max size. One- and two-phase: the two-phase global re-buckets each slice partial into every nested window whose end it reaches (and unlike `HOP`, `CUMULATE` carries no synthetic count column, so the partials are just the user aggregates). **Proctime** `CUMULATE` is also native (single-phase): each row joins every nested window covering the processing-time clock, and the chained processing-time timer emits each nested window as the clock crosses its step boundary up to the max size (non-deterministic, so not byte-compared to the host). |
| `OVER` aggregate / window function | Yes | Event-time `OVER ([PARTITION BY k] ORDER BY rt)` over one of three frames ending at the current row: `RANGE UNBOUNDED PRECEDING` (the running fold), `ROWS BETWEEN n PRECEDING AND CURRENT ROW` (a bounded sliding row count), or `RANGE BETWEEN INTERVAL n PRECEDING AND CURRENT ROW` (a bounded sliding rowtime interval). Running `SUM`/`MIN`/`MAX`/`COUNT`/`AVG`/`FIRST_VALUE`/`LAST_VALUE`, each over its own (possibly different) bigint/int/smallint/tinyint/double/float value column (e.g. `SUM(a), MAX(b) OVER w`) — narrow ints and 4-byte float keep the host's narrow result type (integer SUM wraps at the input width, float SUM keeps 4-byte precision) — and the window functions `ROW_NUMBER()`/`RANK()`/`DENSE_RANK()`. Optionally partitioned by one or more bigint/int/string/boolean/date/timestamp/decimal keys, emitting each input row with the running value appended. Columnar — input columns pass through Arrow and it rides the keyed columnar shuffle with no transpose (native source → watermark assigner → exchange → OVER all flow Arrow). Each row is held until the watermark passes its rowtime, then aggregated per partition in rowtime order. The **unbounded** frame folds one persistent DataFusion-equivalent accumulator per key; a **bounded** frame (ROWS by row count, or RANGE by rowtime interval) instead keeps a per-key sorted buffer of the rows still reachable by some future frame and **recomputes** each row's aggregate over its frame slice (byte-identical to Flink's `RowTimeRows/RangeBoundedPrecedingFunction`, and sidestepping MIN/MAX retraction — see [divergences/11](divergences/11-over-incremental-vs-window-exec.md)). Window functions keep a small per-key state (a counter for `ROW_NUMBER`, rank/last-value for `RANK`/`DENSE_RANK`), incremental, matching Flink, since DataFusion's window evaluators can't be checkpointed. `FIRST_VALUE`/`LAST_VALUE` keep the first / most-recent non-null value (Flink ignores nulls in these). **Proctime** order is also supported (`ORDER BY proctime`): rows fold in arrival order and emit eagerly (no watermark), the operator assigning an arrival sequence as the order key so the same running / bounded-ROWS fold applies — only a bounded **RANGE** (wall-clock interval) frame is rejected for proctime as non-deterministic. `LAG`/`LEAD`, `FOLLOWING` frames, decimal bounded frames, more than one window group, and non-time/descending order are rejected or single-grouped by Flink in streaming (parity, not a gap). |
| Interval join (event-time or proctime) | Yes | Interval join — `a JOIN b ON a.k = b.k AND a.rt BETWEEN b.rt - X AND b.rt + Y` — **INNER and LEFT/RIGHT/FULL outer** — with one or more bigint/int/string/boolean/date/timestamp/decimal equi-join keys and an event-time interval on the rowtimes. An outer join is append-only: each row carries a `__rowid__` and a per-side match flag, and a row that never matched is emitted null-padded once its interval is evicted by the watermark (correct because no future row could match it by then) — emitted once, never retracted. (SEMI/ANTI don't arise as interval joins — the planner makes those regular joins.) Columnar: each input is shuffled by its join key through a native columnar exchange and the join emits Arrow batches of the matched pairs (left columns then right columns), so the whole two-input pipeline flows Arrow with no transpose (native sources → watermark assigners → exchanges → join). Both sides buffer; a row is joined against the other side's buffer (a DataFusion hash join on the keys with the interval **and any residual non-equi predicate** as the join filter) as it arrives, so a pair is emitted once, and rows are evicted once the combined (min) input watermark passes the point no future row could match. A residual non-equi condition beyond the interval (e.g. `… AND a.v < b.v`) is folded into the DataFusion join filter (Arroyo's pattern), encoded like the filter engine; a predicate the engine can't express falls back. **Proctime** intervals are also native: each row is timed by the operator's processing-time clock (its time column stamped with the clock at push, so the interval is measured in processing time) and eviction advances on the clock — each batch registers a cleanup timer at `now + max(upper, -lower)`, the latest a row buffered now could still match, so the tail (and any outer null-pads) drains even with no further input; non-deterministic, so not byte-compared to the host. |
| Window join (event-time or proctime) | Yes | Window join — two windowing-TVF inputs (`TUMBLE`/`HOP`/`CUMULATE`) joined on their key within the same window: `a JOIN b ON a.k = b.k` where both sides carry matching `window_start`/`window_end` — **INNER and LEFT/RIGHT/FULL outer**. One or more bigint/int/string/boolean/date/timestamp/decimal equi-join keys. Both sides buffer; when the combined watermark closes a window its rows are joined (a DataFusion hash join keyed on the user key plus the window bounds, so only same-window rows match) and evicted. An outer join null-pads the closed-window rows that found no match, determined within that flush (a window's rows on both sides close together, so the join sees every potential match) — append-only, emitted once. The join is columnar (Arrow pairs out), and the window assignment (the windowing TVF) is native and columnar too — a stateless operator that assigns each row to its window(s) and appends `window_start`/`window_end`/`window_time`, fanning rows out for `HOP`/`CUMULATE`, so the whole `TUMBLE`/`HOP`/`CUMULATE` → join pipeline is one columnar island. A residual non-equi condition beyond the key/window equi keys (e.g. `… AND a.v < b.v`) is applied natively as the DataFusion join filter; a predicate the engine can't express falls back. (SEMI/ANTI are regular joins, not window joins.) **Proctime** is also native: the windowing TVF assigns each row to the window(s) covering the processing-time clock (not a rowtime column), and the join closes those windows on a chained processing-time timer at each window end (the same next-slide-boundary model as the proctime window aggregate) rather than a watermark — both sides must share the same time semantics; non-deterministic, so not byte-compared to the host. |
| Temporal table join (event-time) | Yes | An event-time temporal table join — `probe JOIN versioned FOR SYSTEM_TIME AS OF probe.rowtime ON probe.k = versioned.k` — **INNER and LEFT** (Flink rejects RIGHT/FULL for temporal join), with one or more bigint/int/string/boolean/date/timestamp/decimal equi-join keys. The build (right) side is a *versioned* table — a changelog keyed by the join key, each version timestamped by its rowtime; per key the operator keeps the version history (`rightTime → (row, RowKind)`, last-write-wins per timestamp, every kind retained, a `-D`/`-U` marking that the version has no row). The probe (left) side buffers; when the combined watermark passes a probe row's rowtime it joins the build version *valid at that time* — the latest accumulate version with `rightTime ≤ probe.rowtime`, found by an ordered lookup — emitting `[probe.., build..]` carrying the probe row's RowKind, or (LEFT) a null-padded row when no valid version exists. Old versions behind the watermark are dropped, always keeping the latest still-valid one. A faithful port of Flink's `TemporalRowTimeJoinOperator`; because emission is watermark-gated the result is deterministic (independent of arrival interleaving and cross-key order) and is value-compared to the host. Columnar: each input is shuffled by its join key through a native columnar exchange and the join emits Arrow batches (probe columns then build columns), and the versioned build side is commonly produced by a native row-time deduplication (`ROW_NUMBER() … ORDER BY rt DESC`), so the dedup → temporal-join pipeline is one columnar island. A residual non-equi condition beyond the equi key + `FOR SYSTEM_TIME` (e.g. `… AND o.amount < r.rate`) is applied natively as the join filter, encoded like the filter engine; a predicate the engine can't express falls back. A **processing-time** temporal table join is parity, not a gap — Flink itself rejects it for a versioned table (FLINK-19830). (A processing-time lookup / dimension-table join is native for both synchronous and async connectors — see the q13 note below and `docs/coverage-and-fallbacks.md`; only the calc/residual variants fall back.) |
| Streaming Top-N (`ROW_NUMBER`) | Yes (emits a changelog) | A streaming Top-N — `ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) <= N`, **with or without the rank number projected**, over an insert-only **or** changelog input. Keeps the best `N` rows per partition (sorted by the order keys, ties in arrival order) and emits a changelog as that set changes. **Append-only input** uses a bounded buffer (the closest analog being RisingWave's `TopNCache`, [divergences/14](divergences/14-standalone-streaming-engines.md)); **changelog input** uses the retracting ranker (Flink's `RetractableTopNFunction`), which keeps the **full** per-partition buffer so a deleted top-N row can be replaced by promoting rank N+1. **Without the rank number**: an `INSERT` for a row entering the top-N and a `DELETE` for one leaving. **With the rank number** (`SELECT … rn …`): a changed occupant at a rank emits `UPDATE_BEFORE`(old)/`UPDATE_AFTER`(new), a newly-occupied rank an `INSERT`, a vacated rank a `DELETE` — computed by diffing the top-N by rank position before vs after each input row, which collapses to the same result as Flink's cascade. The order comparator honors per-column ascending/descending and nulls-first independently. `ROW_NUMBER` only (Flink rejects streaming `RANK`/`DENSE_RANK`); a constant rank range — an `OFFSET` (rank range `[offset+1, rankEnd]`) is supported via the retracting ranker's rank window. Columnar (Arrow in/out); the per-partition buffers are checkpointed. |
| Global `LIMIT` / `ORDER BY … LIMIT` | Yes | A global `FETCH`/`LIMIT` — `ORDER BY … LIMIT n` (`SortLimit`) and plain `LIMIT n` (`Limit`). Both lower in the host to a global (no-partition) `ROW_NUMBER` rank, so they run on the **same native Top-N operator** with an empty partition key. `ORDER BY … LIMIT n` keeps the `n` smallest/largest rows by the order keys and emits the same insert/delete changelog as that set changes (a `DELETE` for a displaced row, an `INSERT` for the new one); plain `LIMIT n` has no order keys, so the ranker keeps the first `n` rows by arrival and the newest beyond `n` never enters — insert-only. `FETCH` required (Flink rejects an unbounded streaming sort); an `OFFSET m` is supported (window `[m+1, m+n]`, run on the retracting ranker); insert-only input only (a retracting input makes the host pick its retract strategy, not reproduced). Columnar (Arrow in/out); the buffer is checkpointed. Single gather (parallelism collapses to one), so plain `LIMIT`'s arrival-order pick is deterministic and matches the host. |
| Deduplication (keep-first / keep-last, rowtime or proctime) | Yes | `ROW_NUMBER() OVER (PARTITION BY k ORDER BY <time> ASC\|DESC) = 1`, which the host plans as a deduplicate, where `<time>` is a rowtime or proctime attribute. **Rowtime keep-first** (`ASC`) is insert-only: per key the operator keeps the **minimum-rowtime** row and emits it once the watermark reaches that rowtime (a faithful port of `RowTimeDeduplicateKeepFirstRowFunction` — a candidate is replaced only by a strictly smaller rowtime, emitted on the timer); later rows are ignored and a row below the watermark is dropped as late. **Rowtime keep-last** (`DESC`) is retracting: per key it keeps the **maximum-rowtime** row and emits a changelog eagerly (`RowTimeDeduplicateFunction`) — the first row `INSERT`s; a later row (rowtime `>=` the stored one) emits `UPDATE_BEFORE`(previous, gated on the host's per-node flag) then `UPDATE_AFTER`(new); an older row is ignored. **Proctime** dedup orders by arrival (no watermark): keep-first emits each key's first-arriving row insert-only and drops the rest (`ProcTimeDeduplicateKeepFirstRowFunction`); keep-last keeps each key's latest-arriving row, emitting the same eager retract changelog (`ProcTimeDeduplicateKeepLastRowFunction`). The proctime order key is materialized natively (`PROCTIME()` is a native expression) and used only as the arrival-order signal — the operator never reads its value. Insert-only input; keys carried by the columnar shuffle; per-key state checkpointed. Columnar (Arrow in/out). |
| Window Top-N / window deduplication | Yes | `ROW_NUMBER() OVER (PARTITION BY window[, key] ORDER BY …) <= N` over a windowing TVF (`WindowRank`), and its rank-1 keep-first/keep-last case (`WindowDeduplicate`). Per window (the attached `window_start`/`window_end`) and partition key, the operator keeps the top `N` rows by the order key (ties in arrival order) and emits them — with the rank number when the host projects it — once a watermark closes the window; late rows (whose window already closed) are dropped. Append-only, emitted once. One native operator serves both (deduplication is `limit = 1`). `window_start`/`window_end` are rendered as session-local wall-clock `TIMESTAMP`s on emit (held as epoch internally, so eviction compares against the watermark in UTC). `ROW_NUMBER` only, range starting at 1. Columnar (Arrow in/out); per-window buffers checkpointed. **Proctime** is also native: over a proctime windowing TVF the window closes on a chained processing-time timer at each window end rather than a watermark (non-deterministic, so not byte-compared to the host). |
| Event-time sort | Yes | `ORDER BY rowtime` (a `TemporalSort`): the operator buffers rows and, on a watermark, releases those whose rowtime is at or before it in ascending rowtime order (a stable sort, so equal-rowtime rows keep arrival order), keeping the rest. Insert-only — the watermark guarantees no earlier-rowtime row can still arrive, so the emitted order is final. A single gather (no shuffle). A secondary order key or a descending order falls back. Columnar (Arrow in/out); the buffer is checkpointed. |
| Regular (non-windowed) join | Yes (changelog in and out) | A regular equi-join `a JOIN b ON a.k = b.k` (no window/interval), the "updating join" — **INNER, LEFT/RIGHT/FULL outer, and SEMI/ANTI**. Keeps a per-side keyed multiset of live rows and probes it incrementally per input row — not the DataFusion batch-hash-join the time-bounded interval/window joins use, because retract correctness needs per-row counts ([divergences/14](divergences/14-standalone-streaming-engines.md)). On each input row it emits one output per matching row on the other side (by that row's count), carrying the input row's kind. For the outer/semi/anti families it also tracks a per-row **match-degree** on the outer side (RisingWave's degree table; Flink's `numOfAssociations`): a row whose degree crosses 0↔1 emits or retracts a null-padded row (outer) or a bare row (semi/anti) — a faithful port of Flink's `StreamingJoinOperator`/`StreamingSemiAntiJoinOperator`, so the collapsed (net materialized) result matches the host. Over append-only inputs an INNER join is insert-only while outer/anti joins still emit a changelog (a null-pad retracted when a match arrives); over changelog inputs (e.g. two aggregations joined) a retract on either side retracts the matched pairs. A residual non-equi predicate (e.g. `… AND a.v < b.w`) is applied natively — it gates which same-key pairs are matches (feeding the degree and the emitted output), encoded in the same form as the filter engine and evaluated over the candidate pairs; a predicate the expression engine can't express falls back. Null join keys never match (host equi semantics). Join keys may be any converter-supported type, including nested `ARRAY`/`MAP`/`ROW` (the nested value rides the row state as a `ScalarValue` and is cast back to its declared type on emit). Columnar (Arrow in/out), per-side state (with degree) checkpointed. Unbounded state (no time eviction) — like the host's regular join. |
| Non-windowed `GROUP BY` aggregate | Yes (changelog in and out) | A plain `GROUP BY` (or a global aggregate with no grouping): `SUM`/`MIN`/`MAX`/`COUNT`/`AVG` over bigint/int/double value columns (`AVG` also over smallint/tinyint/float; it keeps a running sum widened to bigint/double plus the non-null count and emits `sum/count` cast back to the input type, integer division truncating toward zero — Flink's `AvgAggFunction` — decimal `AVG` falls back) (and over `DECIMAL(p,s)`: `SUM` accumulates an i128 at scale `s` → `DECIMAL(38, s)`, NULL on overflow, matching Flink; `MIN`/`MAX` keep the extreme as an i128 → `DECIMAL(p, s)`, and also admit a `CHAR`/`VARCHAR` value ordered byte-lexicographically — Flink's `BinaryStringData` comparison; `COUNT` counts non-null — decimal `AVG` still falls back), `COUNT(*)` and `COUNT(DISTINCT x)` (a per-key value→multiplicity set — Flink's `DistinctAccumulator` — counting live distinct values; `SUM`/`MIN`/`MAX` `DISTINCT` fall back) included, and any aggregate may carry a `FILTER (WHERE …)` (the operator folds a row into it only where that filter — a boolean input column the host computes — is true), grouped by any keys the [whole-row converter](src/main/java/io/github/jordepic/streamfusion/operator/RowDataArrowConverter.java) handles — including a nested `ARRAY`/`MAP`/`ROW` grouping key (equal nested values group together; the key is cast back to its declared type on emit), and `COUNT` over a complex value column (counted for null-ness only). `SUM`/`MIN`/`MAX` over a complex value still fall back (Flink rejects ordering them too). Unlike the windowed aggregates it **emits a retract changelog**: a key's first row inserts; a later change retracts the previous value then appends the new (the `UPDATE_BEFORE` gated on the host's per-node flag, an unchanged result suppressed); and a key whose record count reaches zero is deleted. It also **consumes a changelog**: each input row's `RowKind` (carried as a four-way byte column across the row/Arrow boundary, [divergences/13](divergences/13-rowkind-carriage-meta-column.md)) selects accumulate (`+I`/`+U`) or retract (`-U`/`-D`), so a `GROUP BY` over another aggregate's or a join's output runs natively too — every row processed in order so the change sequence matches the host's exactly. Per-key state, checkpointed; columnar (Arrow in/out) — the row↔Arrow conversion is paid only at host edges. `SUM`/`COUNT` retract a running value (`SUM` keeps a non-null count so a fully-retracted `SUM` reports `NULL`); `MIN`/`MAX` retract via a per-key value→count multiset that recovers the next extreme when the current one is retracted (Flink's `*WithRetract` `MapView`); a record count drives the delete. All therefore run over either an append-only or a changelog input (`AVG` retracts by subtracting from its sum and count). Substituted only at zero idle-state TTL (a TTL changes the host's refresh/expire semantics). **Two-phase / mini-batch** (`agg-phase-strategy = TWO_PHASE` with mini-batch enabled) runs as the same four operators Flink plans, all native: a `MiniBatchAssigner` emits the processing-time mini-batch marker, a local pre-aggregate buffers a bundle in memory and flushes one partial row per key on that marker (and on the `mini-batch.size` trigger and before each checkpoint — a transient buffer with no checkpointed state, like Flink's `MapBundleOperator`), the keyed shuffle is a native exchange, and the global merge reuses this same operator (`COUNT` merging as a `SUM` over the partial counts). Same SUM/MIN/MAX/COUNT scope over bigint/int/double, but the partial must not widen (`SUM(INT)`, whose partial Flink widens to bigint, and `AVG`/distinct route single-phase). |
| Changelog normalization (upsert / CDC-with-PK source) | Yes (changelog in and out) | A `ChangelogNormalize` keyed by a unique key — the host inserts it after an upsert source (e.g. `upsert-kafka`) or a duplicate-bearing changelog to materialize a regular changelog. The native operator keeps the **last full row per key** and emits Flink's normalized changelog (a faithful port of `ProcTimeDeduplicateKeepLastRowFunction`'s keep-last-on-changelog path): the first row for a key `INSERT`s; a changed row emits `UPDATE_BEFORE`(previous, gated on the host's per-node flag) then `UPDATE_AFTER`(new); an **unchanged** row is suppressed; a `DELETE`/`UPDATE_BEFORE` emits a `DELETE` of the stored full row (so a key-only tombstone still retracts the right row) and clears the key. Proctime — it emits synchronously per input row, no watermark buffering. Keyed shuffle stays columnar where the input is a columnar producer; per-key state checkpointed; columnar (Arrow in/out). A pushed filter condition or the source-reuse rewrite falls back. |
| `UNION ALL` | Yes (changelog-transparent) | A `UNION ALL` of two or more inputs with the same row type. A union is a pure stream merge — every input record flows to the output unchanged, with no per-row work and no shuffle — so the native node carries no operator at all: like Flink's own union it lowers to a `UnionTransformation` over the inputs' Arrow streams (which also aligns the watermark across inputs, the min). Because it never touches a record it carries `$row_kind$` through untouched and merges changelog inputs as faithfully as insert-only ones (e.g. two `GROUP BY` results unioned). Columnar in and out with N inputs, so a union of columnar islands stays one island. UNION distinct is not a union at the physical level — the host rewrites it to a `GROUP BY`, which routes via the aggregate path. |
| `GROUPING SETS` / `CUBE` / `ROLLUP` | Yes | The host plans a grouped-set query as an `Expand` (fan each input row out to one row per grouping set — copy the grouped-in columns, null the grouped-out ones, stamp a per-set expand id) feeding a `GROUP BY` over the keys plus the expand-id column. The native expansion is a **stateless columnar operator** that reproduces Flink's `ExpandFunction` row for row, then the (already-native) GROUP BY aggregates each set — the whole `Expand → GROUP BY` pipeline is one columnar island. Every project cell must be a column reference, a `NULL`, or the integer expand id (a computed cell falls back). Carries `$row_kind$` through, so it runs over insert-only or changelog input. (Grouped-out keys make the GROUP BY emit NULL key columns — supported; Flink groups NULL as its own key.) `COUNT(DISTINCT)` is *not* an expand — the host plans it as a distinct accumulator, not yet native. |
| `UNNEST` (array) | Yes (INNER, single `ARRAY`) | The host plans `UNNEST` as a `Correlate` over its internal `$UNNEST_ROWS$` table function. The native operator is a **stateless columnar fan-out**: each input row produces one output row per element of its array column, the input columns repeated and the element appended (`[input cols.., element]`) — the same `take`-based fan-out as the windowing TVF / `Expand` ([divergences/15](divergences/15-unnest-take-fanout-not-datafusion-unnestexec.md) explains why this, not DataFusion's `UnnestExec`). An `ARRAY<ROW>` element is flattened into one output column per struct field (`UNNEST(array_of_rows) AS t(a, b)` appends `a, b`), and a `MAP` appends a key and a value column (`UNNEST(map) AS t(k, v)`), a `MULTISET` appends the element repeated by its count, and `WITH ORDINALITY` appends a trailing 1-based ordinal (the element's position) — matching Flink. INNER semantics: a null/empty collection yields no rows; a null *scalar* element yields a null row, while a null *ROW* element is dropped (Flink's quirk). A **LEFT** (outer) unnest instead keeps a row whose collection is empty/null, with the appended columns (and any ordinal) null. Carries `$row_kind$` through (changelog-transparent). A filter on the unnested element (`… WHERE element > x`) is pushed into the `Correlate` as a `condition`; it is applied as a native filter over the unnest output (the condition's ref shifted to index the element), so it accelerates when the expression engine can encode it. Scope: INNER/LEFT `UNNEST` of a single `ARRAY` (scalar or ROW element), `MAP`, or `MULTISET`, optionally `WITH ORDINALITY`; a pushed condition the expression engine can't encode or a condition over a LEFT unnest fall back. |
| Parquet source (`SELECT … FROM`) | Local filesystem | A `filesystem`-connector source with `'format' = 'parquet'` reading from a local (`file:` or scheme-less) `path`. Read natively through DataFusion's file scan straight to Arrow batches (columnar), so the data never becomes `RowData` — feeding a fully columnar pipeline (a copy into a Parquet sink runs columnar end to end). Flink's file source owns discovery, split assignment, and checkpointing; the native reader is handed one file's byte range and reads only the row groups starting in it (splittable — a large file is read by several subtasks at once), with the projection pushed into the decode. Remote filesystems (e.g. `hdfs:`/`s3:`) fall back to the host. |
| ORC source (`SELECT … FROM`) | Local filesystem | A `filesystem`-connector source with `'format' = 'orc'` reading from a local (`file:` or scheme-less) `path`. Same path as the Parquet source — read natively through DataFusion's file scan (via a DataFusion ORC source) straight to Arrow, split at stripe granularity with the framework owning enumeration/assignment/checkpointing and the projection pushed into the decode. Remote filesystems fall back to the host. |
| Parquet sink (`INSERT INTO`) | Local filesystem | A `filesystem`-connector sink with `'format' = 'parquet'` writing to a local (`file:` or scheme-less) `path`. The incoming rows are written to Parquet natively (Arrow → Parquet), committed exactly once via two-phase commit on checkpoint. Remote filesystems (e.g. `hdfs:`/`s3:`) and other formats fall back to the host. |
| Kafka source native decode (`SELECT … FROM`) | JSON / CSV / raw / bare-Avro value formats, incl. nested ROW/ARRAY/MAP | A `kafka`-connector source whose `value.format` is `json`, `csv`, `raw`, or `avro`: Flink's own Kafka source consumes the raw value bytes — owning offsets, checkpointing, and auth — and a native operator decodes a whole batch straight to Arrow, replacing Flink's per-record `RowData` materialization with one native decode per batch. JSON and Avro carry nested objects/records, arrays, and maps (ROW/ARRAY/MAP columns), verified against Flink's own decoder; bare Avro decodes against the reader schema derived from the table's `RowType` by the same converter Flink's `avro` format uses, so the result matches. Requires an explicit topic, a supported startup mode, an unbounded or `latest-offset` bounded scan, and no `key.format`. The registry-framed `avro-confluent` format has a native decoder but is not wired into this routing yet, so it falls back. (The fully-native rdkafka source — Rust owning the consume *and* decode — is opt-in, behind the optional `kafka` build feature and the `kafkaSource` gate; it decodes JSON / bare-Avro / protobuf and benchmarks competitive-to-faster than this decode path, notably lifting JSON past the parity ceiling — see the Nexmark native-source results.) |
| Kafka protobuf source (`SELECT … FROM`) | Messages of supported scalar, nested-message, repeated, and map fields | A `kafka`-connector source with `value.format = protobuf` and a `protobuf.message-class-name`. Flink consumes the raw protobuf messages and a native operator decodes them straight to Arrow via the descriptor reflectively extracted from that generated class (descriptor-driven, no per-record object tree). Substituted when every field (recursively) is reproduced identically to Flink: the scalar types `int32`/`int64`/`sint*`/`sfixed*` → INT/BIGINT, `float`/`double`, `bool`, `string`; nested messages → ROW; `repeated` → ARRAY; `map` → MAP. Verified by host-baseline parity tests (decode via both, compared) for flat, nested, and repeated/map messages. Falls back (descriptor inspected at plan time) on an `enum` or unsigned/`fixed` int (decoded differently here than in Flink), `bytes`, or a well-known type (`google.protobuf.*`, which maps to a dedicated Arrow type rather than the nested row Flink produces). Same topic/startup/bounded/`key.format` prerequisites as the row above. |
| Kafka CDC source (`SELECT … FROM`, emits a changelog) | Debezium / OGG JSON, reproduced identically to Flink | A `kafka`-connector source whose `value.format` is `debezium-json` or `ogg-json`. Flink consumes the raw bytes and a native operator decodes the `{before, after, op}` envelope straight to a columnar changelog — physical columns plus the `RowKind` byte, an update fanning out to UPDATE_BEFORE + UPDATE_AFTER — with no `RowData` envelope decode. Because a CDC source emits a changelog itself, it is substituted even though it is not insert-only, and the downstream changelog (e.g. into a native `GROUP BY`/join) flows with no row materialization. Substituted **only** where the result is identical to Flink's own decoder: full-image dialects only (see Maxwell/Canal below), no `schema-include` wrapper, default error handling (`ignore-parse-errors` unset — an unknown op or a null pre-image *fails* like Flink rather than being dropped), and no metadata/computed columns; anything else falls back. Same topic/startup/bounded/`key.format` prerequisites as the decode row above. |

Two-phase (local + global) aggregation is accelerated too: the native local
pre-aggregate emits partial state, the host shuffles by key, and the native
global merges — for `SUM`/`MIN`/`MAX`/`COUNT` (not `AVG`, whose partial is
multi-field). This is the default planning, so tumbling, hopping, and cumulative
window aggregation no longer need `ONE_PHASE`. Hopping and cumulative use the
host's slice-sharing model (a per-slice local, a global that re-buckets each
slice into the windows it belongs to — the overlapping windows for hopping, the
nested windows up to the max size for cumulative).

When a window (rowtime over a local-time-zone attribute) sits on a columnar
producer — e.g. a native Parquet source through a watermark assigner — the keyed
shuffle before it is kept columnar too: a native exchange splits each Arrow batch
by the grouping keys and routes it, feeding a columnar window with no row
transpose anywhere. This covers both one- and two-phase plans:

- **One-phase:** source → watermark assigner → exchange → window, all Arrow.
- **Two-phase:** a columnar local pre-aggregate emits partial-state Arrow batches,
  a columnar exchange splits them by key, and a columnar global merges — the whole
  local → shuffle → global path flows Arrow with no transpose on either side.

The shuffle only co-locates each key on a channel — the window re-groups by key in
operator state — so its hash need not match Flink's. The exchange is kept columnar
only when its downstream is a native columnar window/merge; otherwise the window
stays row-fed and the shuffle stays on the host.

### Global terms (all native execution)

- **Sub-plan reuse is disabled** when native acceleration is installed (`table.optimizer.reuse-sub-plan-enabled`/`reuse-source-enabled` off). A columnar island rests on a "produced fresh, consumed once" invariant — each Arrow batch is handed to exactly one consumer, which closes its off-heap buffers after reading (so the batch hand-off is zero-copy). Reuse would fan a shared branch's batches to two consumers, the first of which frees the buffers; disabling it keeps the physical plan a tree so the invariant holds. Reuse and no-reuse compute identical results, so this only affects the execution graph, never output.
- **Insert-only input**, with two exceptions: the non-windowed `GROUP BY` aggregate both emits and consumes a changelog, and the Kafka CDC source (Debezium/OGG) *emits* a changelog from its raw input (see their rows above). Every other operator consumes insert-only input; a retracting or updating *input* to them falls back to Flink.

### Not yet accelerated (falls back to Flink)

- Expressions using operations the engine does not admit: scalar functions outside the admitted set and number↔string `CAST` / narrowing-`VARCHAR` / cast-to-`CHAR(n)` (narrowing integer, float→int, `CHAR`/`VARCHAR`→`VARCHAR`, and exact-source →`DECIMAL` casts **are** admitted). `UPPER`/`LOWER` and `REGEXP_EXTRACT` do **not** fall back — they run natively by default via a byte-exact JVM upcall to Flink's own case folding / `regexpExtract`, with a faster pure-Rust path opt-in under `-Dstreamfusion.expression.<NAME>.allowIncompatible=true` (which can diverge on non-ASCII case / advanced regex). Functions where DataFusion diverges from JVM semantics only at a precision edge and have no cheap byte-exact path — `ROUND` on float/double (`BigDecimal` vs binary rounding) and transcendental math (`SIN`/`EXP`/`POWER`/… last-ULP) — fall back by default but are **opt-in** via the same flag (Comet's `allowIncompatible` model). `CONCAT` (NULL handling) is a true value divergence and never opt-in. All documented in [divergences/07](divergences/07-expression-encoding-and-compile-once.md)
- The native columnar sources/sink only on a local (`file:`) path — remote filesystems (`hdfs:`/`s3:`) fall back. Sources read Parquet and ORC; the sink writes Parquet only — other formats fall back
- Kafka value formats on the native decode path beyond CSV/raw/bare-Avro/protobuf: the registry-framed `avro-confluent` decoder exists but isn't planner-wired yet; a `key.format`, a topic pattern, an unsupported startup/bounded mode, or untranslatable consumer properties also fall back
- Protobuf fields needing representation reconciliation — `enum` (int-vs-name), unsigned/`fixed` ints (unsigned-vs-signed), `bytes` (decode is fine but `byte[]` parity is not yet test-covered), proto3 missing-field defaults, and well-known types (`google.protobuf.*`) — fall back to Flink (gated at plan time by inspecting the descriptor); tracked in ticket 34. Nested-message/repeated/map fields now route (the boundary carries ROW/ARRAY/MAP). The host-baseline protobuf parity test lives in the main module; the ORC host-baseline test is isolated in the `orc-baseline` module because Flink's ORC and protobuf formats need incompatible protobuf-java versions
- Kafka CDC formats other than Debezium/OGG JSON, and CDC tables we can't reproduce bit-identically: **Maxwell and Canal** JSON (their partial-`old` pre-image can't be recovered from the decoded image alone — a field changed *to* null is indistinguishable from an unchanged one), a `schema-include` envelope wrapper, `ignore-parse-errors` (Flink skips bad rows; the native decoder fails on them, matching only Flink's default), and tables with metadata/computed columns. The Maxwell/Canal decoders exist and are unit-tested, but the planner falls these back to Flink
- Two-phase hopping where the slide does not divide the size
- `SUM`/`AVG` over decimal (precision rules), and grouping keys outside bigint/int/string/boolean/date/timestamp/decimal
- Any aggregate over decimal — see [docs/aggregate-type-support.md](docs/aggregate-type-support.md)
- Two-phase `AVG` (multi-field partial state)
- The non-windowed `GROUP BY` aggregate, the regular (updating) join, and the streaming Top-N all consume and emit a changelog natively; the non-windowed `GROUP BY` does not model `AVG`'s multi-field running state directly (it works via the host's `SUM`/`COUNT` rewrite).

### Seeing why a query fell back

Substitution is silent by default, so a query that does not accelerate gives no signal on its own.
Two ways to see *why* a node stayed on Flink (mirroring DataFusion Comet's fallback-reason reporting):

- **Per-plan list.** `NativePlanner.install(env)` returns the `PhysicalPlanScan`; its
  `fallbackReasons()` lists each candidate node that fell back and why — a precise expression reason
  for a `Calc` (e.g. `"Calc: unsupported function/operator: ABS"`) and an operator-level reason for a
  stateful node (e.g. `"interval join: needs an INNER equi-join …"`).
- **Plan-time log.** Run with `-Dstreamfusion.logFallbackReasons=true` and each reason is logged as
  it is decided: `[streamfusion] falls back to host — Calc: unsupported function/operator: ABS`.

### Controlling acceleration

Acceleration is configured by JVM system properties (mirroring DataFusion Comet's config surface):

- **Master switch** — `-Dstreamfusion.native.enabled=false` turns off all native substitution; the
  query runs entirely on Flink.
- **Per operator** — `-Dstreamfusion.operator.<name>.enabled=false` keeps one operator on the host
  (`filter`, `calc`, `parquetSource`, `orcSource`, `parquetSink`, `watermark`, `over`, `intervalJoin`,
  `windowJoin`, `windowAggregate`, `localWindowAggregate`, `globalWindowAggregate`). Useful to leave a
  lone row-source operator that measures below 1× on the host rather than pay the transpose round-trip.
- **Opt-in incompatible expressions** — `-Dstreamfusion.expression.<NAME>.allowIncompatible=true` (or
  the blanket `-Dstreamfusion.expression.allowIncompatible=true`), for data that avoids the divergent
  edge. For `UPPER`/`LOWER`/`REGEXP_EXTRACT` (already native by default via a byte-exact JVM upcall) it
  switches to the faster pure-Rust path; for `ROUND` on float/double and transcendental math (which
  otherwise fall back, having no cheap byte-exact path) it enables the native path at all. Default off.
- **Managed-memory accounting** — `-Dstreamfusion.memory.accounting.enabled=false` turns off native
  state accounting (default on). When on, a native stateful operator declares an operator-scope
  managed-memory weight (`-Dstreamfusion.memory.operator-weight-mb`, default 64), reserves the
  resulting share of the slot's managed memory from Flink's `MemoryManager` for its lifetime, and the
  native side bounds its state by that budget — state growth past it fails the task with a
  `NativeMemoryLimitException` naming the remedy instead of an unattributed container OOM
  ([divergences/16](divergences/16-upfront-managed-memory-reservation.md)). Covers every stateful
  native operator: the window aggregates (one- and two-phase, session included), the non-windowed
  GROUP BY (its mini-batch local pre-aggregate included), changelog normalize, both dedups, OVER,
  event-time sort, Top-N (append-only, retracting, and windowed), and the
  interval/window/temporal/updating joins. The interval/window joins' transient working memory (the
  DataFusion hash join they delegate the match to) draws on the same budget as their buffered state.

### Deployment JVM flags

Run the TaskManager JVM with Arrow's safety checks disabled (as Comet/Spark do):

```
-Darrow.enable_unsafe_memory_access=true -Darrow.enable_null_check_for_get=false
```

These turn off Arrow Java's per-accessor bounds and refcount checks. Profiling the row↔Arrow
transpose showed roughly a third of the native-side CPU was those checks (`ArrowBuf.checkIndex` /
`ensureAccessible` / `refCnt` on every `setSafe`); disabling them cut the `RowData→Arrow` transpose
from ~21% to ~12% of CPU. The flags must be set at JVM start (Arrow reads them in a static
initializer); the test/benchmark build sets them in the Surefire `argLine`.

### Determinism (the one parity caveat)

Results are byte-identical to stock Flink for everything admitted, with a single
boundary in event-time operators (windows, `OVER`, interval and window joins):
**late-data dropping on out-of-order streams.** A row is dropped when the watermark has already passed its
window; we replicate Flink's *deterministic* (eager, per-jump) watermark emission
exactly, slicing batches so a window-closing watermark precedes any row it makes
late. But Flink *also* emits watermarks on a periodic processing-time timer, which
makes **Flink itself non-deterministic** there — two runs of the same job can drop
different rows depending on wall-clock timing. We don't reproduce that
non-determinism (there is no single correct answer to match); we match the
deterministic path, which is what governs in-order data and bounded jobs. In-order
/ monotonic rowtimes — the common case, and every benchmark — have no late rows and
no divergence at all. Details: [divergences/09](divergences/09-per-batch-watermark-assignment.md).

## Benchmarks

Two measurements per operator: the native hot loop in isolation, and the whole job
against stock Flink. Both matter, and right now they tell different stories.

### Native operator hot loop (Criterion)

The native compute over an in-memory Arrow batch (no JVM bridge, no job scheduling) —
run with `cd native && cargo bench`. Method and running table:
[docs/benchmarks.md](docs/benchmarks.md).

| Operator | Benchmark | Batch | Time | Throughput |
|---|---|---|---|---|
| Filter (`WHERE`) | compiled predicate `v > 0`, ~50% pass | 4096 rows | 2.5 µs | ~1.63 Gelem/s |
| Tumbling window aggregate | `SUM` over 16 windows, no key | 4096 rows | 84 µs | ~48.8 Melem/s |
| Tumbling window aggregate | `SUM` over 16 windows, 64 bigint keys | 4096 rows | 245 µs | ~16.7 Melem/s |
| Interval join | INNER, 1:1 on key, equi-key + interval filter | 4096 rows | 63 µs | ~65 Melem/s |
| Window join | INNER, 1:1 on key + window bounds | 4096 rows | 130 µs | ~31.5 Melem/s |
| Non-windowed `GROUP BY` | `SUM`, 256 string keys (changelog out) | 4096 rows | 0.82 ms | ~5.0 Melem/s |
| `OVER` running `SUM` | running aggregate (specialized fold), 64 keys | 4096 rows | 515 µs | ~8.0 Melem/s |
| `OVER` `ROW_NUMBER` | per-key counter, 64 keys | 4096 rows | 410 µs | ~10.0 Melem/s |
| Session window aggregate | `SUM`, 64 bigint keys, 500 ms gap | 4096 rows | ~2.5 ms | ~1.7 Melem/s |

The native compute is fast where it batches (a filter clears ~1.6 G elem/s; the joins
delegate to a DataFusion hash join at 30–65 Melem/s). The running `OVER` aggregate folds a
small typed state per row (matching DataFusion's accumulators — wrapping integer sum,
null-skipping — without the per-row accumulator call), at ~8 Melem/s. The session
aggregate, which merges open windows over a per-key `BTreeMap`, is the remaining per-row
outlier (~1.7 Melem/s, high-variance).

### End to end vs. Flink

The same query over 5M rows, native substitution on vs. off, single slot — run with
`SF_BENCHMARK=true mvn test -Pbench -Dtest=ThroughputBenchmark`. The `-Pbench` profile is
required: it loads the **release** native library (the debug build is ~10–20× slower and
gives misleading numbers — see [docs/benchmarks.md](docs/benchmarks.md)).

| Operator | Query | Flink | Native | Native vs. Flink |
|---|---|---|---|---|
| Parquet copy (columnar source → sink) | `INSERT INTO parquet SELECT * FROM parquet` | 1.32 M rows/s | 6.54 M rows/s | **4.97×** |
| Parquet sink (row source) | `INSERT INTO parquet SELECT *` | 1.23 M rows/s | 2.76 M rows/s | **2.24×** |
| Windowed aggregate over a columnar source | `SUM` by 1s window from a Parquet table | 1.80 M rows/s | 3.29 M rows/s | **1.82×** |
| Interval join (event-time) | `a JOIN b ON a.k=b.k AND a.rt BETWEEN b.rt ± 1s` | 0.37 M rows/s | 0.63 M rows/s | **1.71×** |
| `OVER` running `SUM` (row source) | `SUM(v) OVER (ORDER BY rt)` | 0.91 M rows/s | 1.42 M rows/s | **1.56×** |
| Tumbling window aggregate (row source) | `SUM` by 1s window | 1.69 M rows/s | 2.10 M rows/s | **1.24×** |
| Filter (`WHERE`) | `SELECT * FROM f WHERE v > 50` | 3.23 M rows/s | 2.41 M rows/s | **0.75×** |
| Non-windowed `GROUP BY` (row source) | `SUM(v) … GROUP BY k`, 1024 keys | 2.21 M rows/s | 1.48 M rows/s | **0.67×** |
| Non-windowed `GROUP BY` (columnar source) | `SUM(v) … GROUP BY k` from Parquet | 2.16 M rows/s | 1.84 M rows/s | **0.85×** |

The gain tracks how much of the pipeline stays columnar. The **fully-columnar paths lead**:
the Parquet copy at **4.97×** (read as Arrow, through the native sink, written as Arrow —
never `RowData`, while Flink round-trips every row at both ends), the windowed aggregate over
a columnar source at **1.82×** (the whole source → watermark assigner → keyed shuffle →
local/global window pipeline stays Arrow), and the event-time **interval join at 1.71×** (Flink's
interval join is slow; ours buffers per key and delegates the match to a DataFusion hash join).
The **Parquet sink reaches 2.24×** even from a row source: it writes Arrow → Parquet natively and
coalesces batches into size-targeted files (rolling on a row target / checkpoint) rather than one
file per batch, so per-file footer/syscall overhead no longer scales with batch count. **Other
row-source ops still pay a `RowData → Arrow` transpose at the input**, though it is cheaper since the
converter was made row-major + pre-sized (~25% faster build): `OVER` running `SUM` lands at **1.56×**
and tumbling at **1.24×**. The lone **stateless filter remains below 1× at 0.75×** — a single cheap
predicate cannot earn back the `RowData → Arrow → RowData` round-trip; leave it on the host with
`-Dstreamfusion.operator.filter.enabled=false`. The **non-windowed `GROUP BY` from a row source is the
same story at 0.67×**: the operator is columnar, but a row source feeds it through a transpose (and
out through another to the host sink), and a per-key `SUM` is too cheap to earn back those transposes
(a retract stream is up to ~2× the input rows) plus the per-row key read
([ticket 20](.claude/todos/20-profiling-and-benchmarks.md)). **From a columnar (Parquet) source through
a native keyed shuffle the input transpose is gone and it reaches 0.85×** — the same move that put the
windowed aggregate and `OVER` ahead. It is still short of 1× only because the changelog output still
transposes at the host sink edge and the per-row key read remains; a fully-native downstream removes
that last transpose. It can be disabled meanwhile with
`-Dstreamfusion.operator.groupAggregate.enabled=false`. Closing the gap generally is the columnar-flow
work: keep Arrow across adjacent native operators so the transpose is paid once at the edges, not per
operator ([divergences/08](divergences/08-columnar-flow-transitions.md)).

### Nexmark q0–q4 (steelmanned: rowwise source + blackhole sink)

The first five Nexmark queries over the wide event row (the `nexmark` datagen shape — `event_type`
plus nested `person`/`auction`/`bid` structs), 2 M events, single slot — run with
`SF_BENCHMARK=true mvn test -Pbench -Dtest=NexmarkBenchmark`. The source is a **rowwise** `DataStream`
and the sink is `blackhole` (also rowwise), exactly as the published Nexmark plan, so a native island
pays a `RowData → Arrow` transpose at the source **and** an `Arrow → RowData` transpose at the sink.
We keep both transposes in the measured path on purpose — a real deployment feeds us rowwise records
and drains to a rowwise sink, so this is the honest end-to-end number, not the favorable
columnar-source/columnar-sink case above. Object reuse is enabled (a standard tuned-prod setting) for
both engines. q1's decimal arithmetic is exact and native by default (Decimal128 multiply + a HALF_UP
cast to DECIMAL(23,3), matching Flink); measuring it against the opt-in approximate-decimal toggle shows
the two are throughput-identical (the i128 multiply is not the bottleneck), so exact-by-default is free —
see the ‡ note under the matrix table.

| Query | Shape | Flink | Native | Native vs. Flink |
|---|---|---|---|---|
| q2 | filter `WHERE MOD(auction, 123) = 0` | 1.91 M ev/s | 2.87 M ev/s | **1.50×** |
| q1 | `0.908 * price` (exact decimal) | 1.92 M ev/s | 2.15 M ev/s | **1.12×** |
| q0 | pass-through projection of `bid` fields | 2.00 M ev/s | 2.17 M ev/s | **1.08×** |
| q4 | regular join → `MAX` per auction → `AVG` per category | 1.12 M ev/s | 1.15 M ev/s | **1.03×** |
| q3 | regular (updating) join `auction ⋈ person` on seller | 2.93 M ev/s | 1.57 M ev/s | **0.54×** |

**q0/q1/q2 now beat stock Flink**, even on the rowwise perimeter. Four changes got them there, all
profiled on q0: disabling Arrow's per-accessor bounds/refcount checks (deployment flag); object reuse
(drops Flink's per-handoff defensive copy); a zero-copy `ColumnarRowData` at the exit transpose (no
per-row materialization + boxing); and — the big one — **nested projection pushdown at the entry
transpose**, which converts only the columns and struct sub-fields the calc reads (`event_type` +
`bid.{auction,bidder,price,extra}` + `dateTime`) rather than the whole wide row, so the unread
`person`/`auction` structs and `bid.channel`/`url` never touch Arrow. That roughly doubled native
throughput and was the difference between ~0.6× and >1×.

**q4 now reaches parity too** (0.69→1.03× after this round of work): its join is a *regular* updating
join — the `B.dateTime BETWEEN A.dateTime AND A.expires` bound is a data column, not an interval — feeding
two `GROUP BY`s. Batching the INNER join's whole input (one columnar residual-predicate eval, emit by
`filter_record_batch`, rows moved into state rather than re-cloned) removed the per-pair `ScalarValue`
and clone churn a differential profile pinned as the cost. **q3 is the one still below 1×**: it is the
same regular join but with *unbounded, ever-growing* state (`auction ⋈ person`, one popular seller
matching many auctions), and at 2 M events the residue is the per-row state store — a fresh `OwnedRow`
per buffered row where Flink reuses pooled `BinaryRowData` (its cost lands in GC, ours in the system
allocator). An A/B against the pre-session build measures q3 identically (0.54×), so this is the standing
updating-join edge, not a regression; a free-list allocator for the keyed-multiset buffers is the next
lever ([divergences/08](divergences/08-columnar-flow-transitions.md)).

### Nexmark q0–q2 from a Kafka source (native decode)

The native decoder is itself a (Rust) bytes→Arrow transpose. Flink does **not** push projection into
the Kafka scan, so its format decodes the whole record; we push the query's projection into the decode
so it builds only the read columns/fields. Run with `SF_BENCHMARK=true mvn test -Pbench
-Dtest=NexmarkKafkaBenchmark` (Testcontainers Kafka). 2 M events, native decode vs Flink's own format:

| Query | JSON (Flink → Native) | Avro (Flink → Native) | Protobuf (Flink → Native) |
|---|---|---|---|
| q0 pass-through | 0.72 → 0.73 M ev/s — **1.02×** | 0.81 → 1.33 M ev/s — **1.64×** | 1.15 → 1.45 M ev/s — **1.26×** |
| q1 currency | 0.76 → 0.74 M ev/s — **0.98×** | 0.82 → 1.34 M ev/s — **1.63×** | 1.14 → 1.49 M ev/s — **1.30×** |
| q2 filter | 0.80 → 0.77 M ev/s — **0.97×** | 0.83 → 1.52 M ev/s — **1.83×** | 1.17 → 1.60 M ev/s — **1.36×** |

**JSON is ~parity; Avro is a 1.6–1.8× win — and the profiles predicted exactly that** (sample with
`SF_PROFILE=true … #q0NativeProfileLoop`, `-Dprofile.format=json|avro`). Both share a large Kafka-I/O +
thread-sync cost (~38–45%) with the Flink run, plus JIT noise from the harness's short jobs. The decode
itself is bound by completely different work:

- **JSON is tokenize-bound** — ~19% `arrow-json` tape parse of the *whole* document, only **~5% building
  the Arrow arrays** (the part pruning reduces). So pruning's ceiling is ~5%, and Flink's mature JSON
  deserializer edges out the native path + its handoffs → parity.
- **Avro is build/copy-bound** — ~27% `memmove` + ~15% decode, of which **`append_null` for the mostly-
  null `person`/`auction` union branches was ~15% alone**. `arrow-avro` already beat Flink decoding the
  full record (1.06–1.18×); pushing the projection into the decode removed that build/copy of unread
  fields and lifted it to **1.6–1.8×**.

Avro pruning needs more than JSON's: bare-Avro datums are schema-less, so the decode keeps the full
*writer* schema (to parse the bytes) and applies the narrowed output as a *reader* schema, projecting
via Avro resolution.

**Protobuf** is also **build/copy-bound** (~25% `memmove` + ~16% ptars decode), like Avro — and native
protobuf decode was slightly *slower* than Flink's (0.88–0.94×) before pruning. Pruning flipped that to
**1.26–1.36×**: it projects via a **pruned descriptor** — ptars builds a column per descriptor field and
skips wire tags it has no field for, so feeding it a descriptor recursively narrowed to the read fields
(resolving nested message types) makes it build only those columns and skip the unset `person`/`auction`
submessages + unread `bid` fields on the wire.

The remaining lever shared by *all* formats is the I/O path (a native consumer bypassing Flink's
`KafkaSource`), which the profiles show is ~40% of the job — and the only lever for JSON, which pruning
can't help. The next section pulls that lever.

_Apple M1 Max; numbers are comparable only within a machine._

### Nexmark q0–q2 from a Kafka source — the row→columnar ladder

How far into Rust the source-side work moves, on the same q0/q1/q2 over the same produced bytes, all vs
stock Flink. Four rungs, each one layer more native; the query projection is pushed in at every rung that
can (the JVM transpose, the Rust decode, and now the native source — narrowed JSON schema, bare-Avro
reader schema, or pruned protobuf descriptor):

1. **JVM transpose** — Flink consumes *and* decodes to `RowData` with its own format, then a JVM
   `RowData → Arrow` transpose feeds the native calc.
2. **Rust transpose, JVM poll** — Flink's `KafkaSource` polls raw bytes, a native operator decodes them
   straight to Arrow (the shallow decode path).
3. **Rust poll + Rust transpose** — the native rdkafka source: Rust owns the consume *and* the decode
   (librdkafka polls each partition, a background thread decodes to Arrow). No Flink Kafka client, no
   `RowData`.

`SF_BENCHMARK=true mvn test -Pbench -Dnative.cargo.args="build --release --features kafka"
-Dtest=NexmarkKafkaLadderBenchmark` (Testcontainers Kafka; native source needs the `kafka` feature).
2 M events, ×vs stock Flink (the best rung **bold**):

| Format | Flink (ev/s) | JVM transpose | Rust transpose, JVM poll | Rust poll + Rust transpose |
|---|---|---|---|---|
| JSON q0 | 0.76 M | 1.07× | 0.99× | **1.23×** |
| JSON q1 | 0.80 M | 1.04× | 0.93× | **1.19×** |
| JSON q2 | 0.81 M | 1.10× | 1.03× | **1.18×** |
| Avro q0 | 0.85 M | 1.03× | **1.63×** | 1.57× |
| Avro q1 | 0.85 M | 0.96× | **1.65×** | 1.57× |
| Avro q2 | 0.84 M | 1.07× | **1.80×** | 1.61× |
| Protobuf q0 | 1.18 M | 1.07× | **1.33×** | 1.15× |
| Protobuf q1 | 1.20 M | 1.04× | **1.29×** | 1.13× |
| Protobuf q2 | 1.21 M | 1.14× | **1.37×** | 1.10× |

**The best rung depends on the format**, and that's the whole point:

- **JSON → the full native source wins (1.18–1.23×).** JSON decode is tokenize-bound, so the Rust decode
  alone is only ~parity (it still shares Flink's `KafkaSource`); the win comes from owning the **poll**.
  Pushdown lifted it from ~1.0–1.16× to 1.18–1.23× — JSON is *our* code (not a fast path inherited from a
  downstream system), so every bit of read/build we cut counts.
- **Avro / Protobuf → the Rust decode (JVM poll) wins** (1.6–1.8× / 1.3–1.4×). The full native source
  actually *trails* it, because it plateaus at a **~1.33–1.36 M ev/s ceiling regardless of format**
  (avro-source 1.34 M ≈ protobuf-source 1.36 M) — its per-poll FFI drain + per-partition batching + emit
  overhead, fine when decode is the bottleneck (JSON) but a cap once the binary decode is faster than it.
  The decode path has no such ceiling (one batched `decodeInto` per 8192-row batch over Flink's poll),
  so it reaches ~1.5–1.6 M.

The **JVM transpose** rung is a flat 1.0–1.14× everywhere: it still pays Flink's full `RowData` decode,
then a (pruned) transpose — the row→columnar boundary on the JVM. So the ladder reads as: pay the
transpose on the JVM (≈1×), move the decode to Rust (binary formats jump), or move the whole consume to
Rust (JSON jumps; binary hits the source's FFI ceiling). Next lever: lift that source ceiling (fewer
FFI round-trips / larger drains) and attack the JSON tokenize itself.

**Reference — the transpose floor (no Kafka).** The same q0/q1/q2 with the source replaced by the
in-process `nexmark` datagen emitting `RowData` directly — no Kafka client, no format decode, just the
columnar island (pruned `RowData → Arrow` transpose → native calc → `Arrow → RowData` out) over a free
source and `blackhole` sink (`-Dtest=NexmarkBenchmark`). It has its own Flink baseline and is *not*
comparable cell-for-cell to the Kafka rows above — it's the ceiling for what columnar execution buys when
I/O and decode are free:

| Query | Flink (RowData) | Native (JVM transpose, no decode) | speedup |
|---|---|---|---|
| q0 pass-through | 1.93 M ev/s | 2.11 M ev/s | **1.09×** |
| q1 currency | 1.76 M ev/s | 1.97 M ev/s | **1.12×** |
| q2 filter | 1.75 M ev/s | 2.84 M ev/s | **1.62×** |

Both engines run **2–3× faster in absolute ev/s than any Kafka rung** (Flink ~1.8 M here vs ~0.8–1.2 M
over Kafka) — that gap is exactly the Kafka consume + decode the ladder above is about. The native
speedup is pure columnar execution: modest on the projections (q0/q1, transpose-bound) and large on the
filter (q2 — the native filter discards rows in Arrow before they are ever materialized to `RowData`).

_Apple M1 Max; numbers are comparable only within a machine._

### Nexmark — the full accelerating set, every source

Beyond q0–q4, StreamFusion runs **every runnable Nexmark query** natively end-to-end, with no fallback
and no flags (the explain diagnostic `NexmarkExplainTest` enumerates them) — including q14 (decimal +
`count_char` UDF + `EXTRACT(HOUR …)`), q21, q23, and q13:

- **q21** (`CASE` + `REGEXP_EXTRACT` over `lower(channel)`) is fully native by default. `REGEXP_EXTRACT`
  and `UPPER`/`LOWER` — whose Rust regex / case folding can diverge from Java's — route through the
  host's own implementation (`SqlFunctionUtils.regexpExtract`, `BinaryStringData.toUpperCase/toLowerCase`)
  via the same columnar JVM upcall the UDFs use, so they are byte-identical while the rest of the `Calc`
  stays native. The pure-native Rust paths remain the faster opt-in behind `allowIncompatible`.
- **q23** is a multi-way inner equi-join, which the native updating join accelerates
  (`FlinkMultiwayJoinSqlHarnessTest`). Its `q23.sql` does not plan in this Flink build only because it
  references the reserved identifier `dateTime` bare; the quoted form (as the DDL declares it) parses and
  accelerates identically.

- **q13** is a processing-time lookup join against a bounded side input (`JOIN dim FOR SYSTEM_TIME AS OF
  probe.proctime ON …`). The native `NativeLookupJoinOperator` keeps the probe batches Arrow (so the
  probe-side Calc/source stay in the island) and, per row, calls the connector's real synchronous
  `LookupFunction` — the same function Flink's `LookupJoinRunner` calls — so the join is byte-identical
  (`FlinkLookupJoinSqlHarnessTest`, INNER + LEFT). Only the point lookup is row-oriented, as it must be.
  An **async** connector is also native (`NativeAsyncLookupJoinOperator`): it fires the connector's real
  `asyncLookup` for each distinct key in a batch concurrently and awaits them on the task thread before
  emitting — Arroyo/RisingWave's within-batch model, so no operator mailbox is needed (nothing is in
  flight across a batch), and a batch's I/O overlaps (`FlinkAsyncLookupJoinSqlHarnessTest`). The
  calc/residual variants fall back — ticket 40.

**One query stays outside** for a reason that is not StreamFusion's to fix (re-verified on Flink 2.2.1).
**q6** (`AVG(…) OVER (ROWS BETWEEN 10 PRECEDING …)` over a retracting winning-bid Top-1) does not run in
Flink SQL at all: the nexmark file is invalid as written (`WHERE rownum` can't see the same-level
`ROW_NUMBER` alias), and even wrapped correctly Flink rejects it with *"Non-time attribute sort is not
supported for bounded OVER window"* — the Top-N strips `dateTime`'s time-attribute property that a `ROWS`
frame needs. (FLINK-19059, the retraction limit the nexmark README pins q6 on, is fixed in Flink 2.1.0 —
that barrier is gone; a different one remains.) With no host plan there is nothing to override or
parity-check. Analyzed in
[.claude/todos/39-nexmark-q6-exclusion.md](.claude/todos/39-nexmark-q6-exclusion.md).

**Non-builtin UDFs** run natively too: a Flink `ScalarFunction` the expression engine can't implement
itself is invoked by a native→JVM columnar upcall (datafusion-comet's `JvmScalarUdfExpr` pattern) — the
native `Calc` exports the argument columns over the Arrow C Data Interface and the JVM bridge runs the
actual `eval` over the whole batch (one JNI crossing per batch, no per-row boundary), so the result is
byte-identical to Flink and the query stays inside the native island. See `NativeUdf` / `JvmUdf`.

`NexmarkMatrixBenchmark` runs **every query StreamFusion accelerates** (q0–q5, q7–q23 — only q6 is out)
over **every source it can be fed by** — the rowwise generator and Kafka json/avro/protobuf across the
four-rung ladder — all vs stock Flink, same steelmanned perimeter (rowwise source + `blackhole` sink,
object reuse on both engines). 500K events.

`SF_BENCHMARK=true mvn test -Pbench -Dnative.cargo.args="build --release --features kafka"
-Dtest=NexmarkMatrixBenchmark` (Testcontainers Kafka; native source needs the `kafka` feature).

All the stateful operators run **columnar on Arrow byte-state**: Top-N, keep-last dedup, the updating
join, and the group/`DISTINCT` aggregate key and buffer their state as memcomparable arrow-row bytes (à
la RisingWave's value-encoded state + Arroyo's `RowConverter`), not boxed `Vec<ScalarValue>` — so a
native changelog chain pays no per-row scalar materialization, hash, or drop.

_Numbers are one **combined run** — every query in a single JVM, best of 2 after a warmup, 500K events.
A combined run accumulates heap/GC pressure that disproportionately slows the alloc-heavier native side,
so these **understate** native for the aggregate/dedup queries (fresh-JVM-per-query puts the near-parity
ones — q15/q17/q18 — back above 1.0×); it is the conservative read._

**Generator** (the transpose floor — no I/O, no decode), native vs Flink, all 23 accelerated queries
sorted by speedup (q1 and q21 each appear twice — parity default and opt-in path, see ‡/† below):

| Query | Shape | Native vs. Flink |
|---|---|---|
| q11 | session-window `COUNT` per bidder | **2.23×** |
| q12 | proctime tumble `COUNT` per bidder | **1.45×** |
| q7 | tumble `MAX` ⋈ bid | **1.37×** |
| q2 | filter `WHERE MOD(auction, 123) = 0` | **1.30×** |
| q0 | pass-through projection of `bid` | **1.27×** |
| q22 | `SPLIT_INDEX(url, '/', n)` projection | **1.22×** |
| q4 | regular join → `MAX` → `AVG` per category | **1.07×** |
| q1 | `0.908 * price` — exact `Decimal128` (byte-parity) | **1.19×** |
| q1 ‡ | …same, approximate-decimal toggle (opt-in, non-parity) | 1.20× |
| q10 | `DATE_FORMAT` projection | **1.01×** |
| q14 | `HOUR`/`CASE` + `count_char` UDF + decimal | **1.00×** |
| q9 | regular join → `ROW_NUMBER` (≤ 1) | 0.97× |
| q15 | multi-`DISTINCT` `COUNT`s per day | 0.96× |
| q23 | three-way join `bid ⋈ person ⋈ auction` | 0.96× |
| q5 | Hot Items (window re-agg + window join) | 0.94× |
| q17 | group agg + `AVG`/`MIN`/`MAX`/`SUM` | 0.94× |
| q13 | lookup join (bounded dimension) | 0.91× |
| q19 | `ROW_NUMBER` topN (≤ 10) | 0.91× |
| q3 | updating join `auction ⋈ person` | 0.83× |
| q18 | `ROW_NUMBER` dedup (≤ 1) | 0.82× |
| q21 | `CASE` + `REGEXP_EXTRACT`/`LOWER` — JVM upcall (byte-parity) | 0.76× |
| q21 † | …same, pure-native Rust regex/case (opt-in, non-parity) | **1.57×** |
| q20 | updating join (`category = 10`) | 0.75× |
| q16 | multi-`DISTINCT` per channel/day | 0.75× |
| q8 | tumble windowed-distinct ⋈ join | 0.71× |

**Ten clear 1.0× even on this conservative combined run, and another seven (q9/q13/q15/q17/q19/q23/q5)
sit within noise of parity.** Projection/filter/scalar (q0/q1/q2/q22), the windowed and group aggregates
(q11/q12), and the UDF query (q14) win outright. The **updating-join family is the big mover**: a CPU
profile (async-profiler) put ~40% of the worst query (q9 — a *regular* join, since its `BETWEEN … AND
a.expires` bound is a data column, not an interval) in the joiner. Making the INNER join batch its whole
input — gather all candidate pairs against the fixed probe side, evaluate the residual predicate once
columnar (no `ScalarValue` round-trip), emit by `filter_record_batch` (no per-pair clone) — and moving
rows into state instead of re-cloning lifted **q9 0.39→0.97, q4 0.64→1.07, q7 0.91→1.37, q23 0.66→0.96**.
The streaming Top-N shed its allocator churn (defer the per-row `owned()` until a row enters, and share
the with-rank cascade's repeat-emitted rows via `Arc` instead of a byte-buffer clone each): **q19
0.77→0.91** (1.13× fresh-JVM). The lever throughout was a differential profile's clearest signal — on
every changelog operator native spends 10–22% of CPU in the system allocator where Flink spends ~1%,
because Flink reuses pooled `BinaryRowData` (its cost lands in GC) while native allocated a fresh
`OwnedRow` per clone. Cutting those allocations, not swapping the allocator (measured neutral earlier), is
what closed the gap ([divergences/08](divergences/08-columnar-flow-transitions.md)).

What still trails 1× is **not one hotspot** but three distinct residues: q8 is transpose-bound (a window
join with only a ~9% native island — the cheap operator can't earn back the `RowData↔Arrow` round-trip);
q16's multi-`DISTINCT` accumulator still churns `ScalarValue` (byte-encoding it is a group-aggregator
rewrite); and q20/q3 are wide updating joins whose remaining cost is the per-row state store that Flink
pools — the next lever is a free-list allocator for the keyed-multiset buffers.

**† q21 is reported on both paths.** By default its `REGEXP_EXTRACT` and `LOWER` run through a
byte-identical **JVM upcall** (the host's own `SqlFunctionUtils.regexpExtract` /
`BinaryStringData.toLowerCase`, one JNI crossing per batch) — that is the **0.76×** row, the price of
staying exactly Flink-equal on functions whose Rust regex / case-folding can diverge at a locale/regex
edge. Flipping `-Dstreamfusion.expression.allowIncompatible=true` runs them on the **pure-native Rust**
regex/case path, which is **1.57×** — a 2× swing over the parity path, and the honest cost of the
guarantee. The default is parity; the fast path is opt-in and documented in
[divergences/07](divergences/07-expression-encoding-and-compile-once.md). Both are measured against the
same Flink baseline in a single `NexmarkMatrixBenchmark` run (a query may carry a `nativeVariant`).

**‡ q1 is also reported both ways — and here the toggle buys nothing.** q1's `0.908 * price` runs an
exact `Decimal128` multiply + HALF_UP cast to `DECIMAL(23,3)` by default (byte-parity with Flink);
`-Dstreamfusion.expression.decimalArithmetic.approximate=true` switches to a `double`-based path that can
differ at the last digit. Unlike q21's regex, the two measure **identically** (1.19× vs 1.20×, and the
ordering flips run to run) — the exact i128 multiply is not the bottleneck, so exact-by-default costs
nothing and the non-parity toggle is not worth enabling for q1. Measuring it was the point: the parity
default is free here.

**Kafka**, best rung per format (native speedup vs that format's own Flink baseline; rung in parens —
`jvm` = JVM transpose, `decode` = Rust decode / JVM poll, `source` = full native rdkafka source). The
four `DATE_FORMAT`-grouped queries are generator-only here: native `DATE_FORMAT` needs a plain
`TIMESTAMP`, but the Kafka event-time column is `TIMESTAMP_LTZ` (the epoch-millis decode lifted by a
native `TO_TIMESTAMP_LTZ`):

| Query | JSON | Avro | Protobuf |
|---|---|---|---|
| q11 | **1.68×** (jvm) | **2.09×** (decode) | **2.10×** (decode) |
| q7 | **1.32×** (jvm) | **1.52×** (decode) | **1.35×** (decode) |
| q5 | **1.15×** (decode) | **1.59×** (decode) | **1.38×** (decode) |
| q22 | **1.14×** (decode) | **1.51×** (decode) | **1.19×** (decode) |
| q0 | **1.11×** (jvm) | **1.50×** (decode) | **1.21×** (decode) |
| q12 | **1.13×** (jvm) | **1.50×** (decode) | **1.24×** (decode) |
| q2 | **1.04×** (jvm) | **1.45×** (decode) | **1.15×** (decode) |
| q4 | **1.03×** (jvm) | **1.45×** (decode) | **1.18×** (decode) |
| q1 | **1.04×** (jvm) | **1.42×** (decode) | **1.14×** (decode) |
| q3 | **1.03×** (jvm) | **1.40×** (decode) | **1.10×** (decode) |
| q23 | **1.03×** (decode) | **1.40×** (decode) | **1.24×** (decode) |
| q8 | **1.00×** (jvm) | **1.37×** (decode) | **1.14×** (decode) |
| q20 | **1.04×** (jvm) | **1.37×** (decode) | **1.12×** (decode) |
| q13 | 0.98× (jvm) | **1.26×** (decode) | **1.12×** (decode) |
| q9 | **1.11×** (jvm) | **1.14×** (jvm) | **1.18×** (jvm) |
| q19 | **1.08×** (jvm) | **1.15×** (jvm) | **1.10×** (jvm) |
| q21 | **1.02×** (source) | **1.14×** (decode) | 0.94× (decode) |
| q18 | **1.03×** (jvm) | **1.14×** (decode) | **1.02×** (decode) |

Two things the Kafka columns add on top of the generator picture:

- **The source rung compounds the operator verdict.** On the binary formats the Rust decode stacks on
  top of the operator work — q11 reaches **2.1×**, and q0/q1/q2/q3/q4/q5/q7/q8/q12/q13/q20/q22/q23 land
  **1.1–1.6×** on avro/protobuf via the Rust-decode rung (decode-bound, exactly as the q0–q2 ladder
  showed). JSON stays nearer parity (tokenize-bound) and wins via the JVM transpose or the operator.
  Several queries that trailed on the bare generator (q3, q8, q13, q20, q23) turn clearly positive on avro
  once the decode saving is added.
- **The changelog-heavy queries now win on the JVM-transpose rung, and pushing decode into Rust doesn't
  add for them.** q9 and q19 are the tell: their best rung is the JVM transpose (q9 1.11–1.18×, q19
  1.08–1.15×), and their decode/source rungs are flat-to-slightly-lower — a compute/emit-bound operator
  gets no lift from decoding faster, it only fills sooner. This *used* to backfire hard (q9's decode/source
  rungs ballooned to ~7.5 s when the join itself was the bottleneck); with the join and Top-N now near
  parity the backfire is gone, but the ordering holds: native decode is the lever for source- and
  aggregate-bound queries, and a no-op (not a hazard) for changelog-bound ones.

_Apple M1 Max; numbers are comparable only within a machine._

## Related work

Two commercial native Flink accelerators exist, both **closed source**:

- **Iron Vector** (Irontools) — the same stack as us: Rust + Arrow + DataFusion
  over zero-copy JNI, transparent fallback. It serializes the plan to the native
  side with [Substrait](https://substrait.io/) and integrates at the
  StreamTask/subtopology level, exchanging Arrow batches over Flink's network
  rather than `RowData`. Today it is **stateless only** (projections, filters,
  expressions); windows, joins, stateful operators, and exactly-once are
  described as planned. It claims ~97% higher throughput on a stateless ETL
  pipeline.
  ([blog](https://irontools.dev/blog/introducing-iron-vector/))
- **Vera X** (Ververica, the original Flink creators) — a proprietary native
  vectorized engine with a drop-in compatibility layer and a new state store.
  It supports stateful workloads and claims 5–10× on Nexmark SQL and ~52% lower
  resource usage. The implementation (language, columnar library, plan format) is
  undisclosed.
  ([blog](https://www.ververica.com/blog/vera-x-introducing-the-first-native-vectorized-apache-flink-engine))

Where StreamFusion differs: it is **open source**, and every substitution is
gated and verified for identical results against stock Flink by a parity harness
rather than asserted. It is already native on **stateful windowing** — tumbling,
hopping, and cumulative windows (one- and two-phase) plus session windows — and
event-time interval and window joins, which Iron Vector (stateless only) has not yet shipped.
It is earlier-stage than Vera X and does not
match its operator breadth or have published benchmarks, but its acceleration is
auditable and parity-first by construction.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed
as above, without any additional terms or conditions.
