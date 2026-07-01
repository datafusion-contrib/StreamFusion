# Arroyo operator coverage — route everything Arroyo supports

**Status:** open (tracking) — all window aggregates, OVER (subset), event-time INNER
joins, the non-windowed `GROUP BY` aggregate (changelog emission *and* consumption, incl.
MIN/MAX retraction), filter/projection, watermark, shuffle, and Parquet source/sink are done.
What remains is async-gated (lookup join, async UDF — ticket 01) plus operator feature tails
(outer/semi/anti joins, rank-number / RANK / retracting-input Top-N, OVER frames).
**Source:** user direction — "everything Arroyo already supports, routed over"

Goal: reach Flink parity (identical results, verified by the parity harness) for
the operators Arroyo implements. Each line becomes its own change/ticket as it
is picked up. Operators are in `~/data/arroyo/crates/arroyo-worker/src/arrow/`.

## Aggregating windows
- [x] Tumbling aggregate (sum/min/max/count/avg), 0–1 int key
- [x] Two-phase tumbling (sum/min/max/count)
- [x] Multiple aggregates per window over one value column (one- and two-phase)
- [x] Hopping / sliding window — one-phase (multi-window assignment) and
      two-phase (slice-sharing, slide divides size). Reverse-engineered from
      Flink's slicing internals, not Arroyo (see divergences/06).
- [x] Session window (`session_aggregating_window.rs`) — dynamic merge on gap,
      including late elements that bridge two open sessions (ticket 14)
- [x] Cumulative window (`CUMULATE`) — one-phase; nested windows sharing a start.
      Net-new: Arroyo has no cumulative window (see divergences/05). Two-phase
      slice-sharing is done (divergences/06).
- [x] Wider value/key types: every non-decimal numeric value type, decimal MIN/MAX/COUNT,
      multiple value columns, `COUNT(*)`, and bigint/int/string/boolean/date/timestamp/decimal
      grouping keys (see `docs/aggregate-type-support.md`).

## Other stateful operators
- [x] Window functions / OVER (`window_fn.rs`) — event-time running aggregates
      (SUM/MIN/MAX/COUNT/AVG, UNBOUNDED PRECEDING) and ROW_NUMBER/RANK/DENSE_RANK,
      incremental per-key state (divergences/11). Remaining: FIRST_VALUE/LAST_VALUE,
      LAG/LEAD, NTILE, bounded frames, and proctime.
- [x] Interval/temporal join (`join_with_expiration.rs`) — event-time INNER equi-join
      with an interval on the rowtimes; buffer + delegate the match to a DataFusion
      hash join, own watermark eviction (divergences/12). Remaining: outer/semi/anti,
      proctime, and a residual non-equi predicate.
- [~] Instant join (`instant_join.rs`) — its main use, a windowed equi-join, is covered
      by our window join (INNER, on shared window bounds, divergences/12) via a hash
      join rather than a direct port. The general per-instant primitive is not ported.
- [x] Non-windowed group aggregation (`incremental_aggregator.rs`) — emits *and*
      consumes a retract changelog (`+I`/`-U`/`+U`/`-D`, matching the host's per-record
      `GroupAggFunction`). SUM/COUNT/MIN/MAX (AVG via the host's SUM/COUNT rewrite) over
      bigint/int/double; SUM/COUNT retract a running value, MIN/MAX a per-key value
      multiset (Arroyo's batch state). Any converter-supported keys, global aggregation.
- [x] Regular (non-windowed) join — INNER updating equi-join, emits *and* consumes a
      changelog. Per-side keyed multiset probed incrementally (native, not DataFusion
      delegation — divergences/14); INNER only, equi-key, null keys dropped. Remaining:
      outer/semi/anti, residual non-equi predicate.
- [x] Streaming Top-N (`ROW_NUMBER`) — append-only, rank ≤ N, rank number not projected;
      per-partition bounded buffer emitting the insert/delete changelog (divergences/14).
      Remaining: rank-number output (rank-shift updates), `RANK`/`DENSE_RANK`, an offset,
      and a retracting input.
- [x] Keep-first deduplication — `ROW_NUMBER() OVER (PARTITION BY k ORDER BY rowtime ASC) = 1`,
      a rowtime-ordered rank-1 the host plans as an insert-only row-time deduplicate. Per key,
      emit the minimum-rowtime row once the watermark reaches it; drop late rows. Keep-last
      (descending) is retracting and falls back.
- [x] Window Top-N / window deduplication — `WindowRank`/`WindowDeduplicate` over the windowing
      TVF: per window and partition key, keep the top-N (or first/last) rows and emit them when a
      watermark closes the window. Append-only; one native window-rank operator serves both
      (dedup = limit 1). window_start/window_end rendered session-local on emit (UTC internally).
- [x] Event-time sort (`TemporalSort`) — `ORDER BY rowtime`: buffer rows, release them in
      ascending rowtime order as the watermark advances (stable for ties). Insert-only.
- [ ] Lookup join (`lookup_join.rs`) — stateless async enrichment against an external
      table; uses ticket 01's async pattern, not the synchronous stateful path.
- [ ] Async UDF (`async_udf.rs`) — async scalar UDF; same async dependency (ticket 01).

## Stateless
- [x] Filter + projection routed from SQL via the native expression engine — the
      planner's `Calc` node (optional filter + projections) and a broad function set
      (see `docs/coverage-and-fallbacks.md` §3 / divergences/07). Remaining tail:
      number↔string `CAST` and obscure functions.

## Connectors (later)
- [x] Parquet source + sink (local `file:`), exactly-once sink.
- [ ] Remote/columnar sources and sinks (Iceberg, `hdfs:`/`s3:`) — ticket 24.
- [ ] Native columnar sources (Fluss / Iceberg-CDC) via Flink Source API +
      availability futures — ticket 01.

## Cross-cutting (do alongside, not after)
- Acceleration config (allowIncompatible / master / per-operator flags) — done (`NativeConfig`)
- Native columnar shuffle — done (divergences/10)
- Changelog/retract support — done (`RowKind` carriage + the GROUP BY, updating join, and Top-N
  emit/consume changelogs; divergences/13, /14)
