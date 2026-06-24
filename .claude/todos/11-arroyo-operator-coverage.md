# Arroyo operator coverage — route everything Arroyo supports

**Status:** open (tracking) — all window aggregates, OVER (subset), event-time INNER
joins, filter/projection, watermark, shuffle, and Parquet source/sink are done. What
remains is retract-gated (updating aggregation, regular joins — ticket 06) or
async-gated (lookup join, async UDF — ticket 01), plus OVER/join feature tails.
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
- [ ] Updating (non-windowed) group aggregation (`incremental_aggregator.rs` +
      `updating_cache.rs`) — emits retractions; needs ticket 06 (changelog/retract).
- [ ] Regular (non-windowed) join — the updating join with retract; needs ticket 06.
- [ ] Lookup join (`lookup_join.rs`) — stateless async enrichment against an external
      table; uses ticket 01's async pattern, not the synchronous stateful path.
- [ ] Async UDF (`async_udf.rs`) — async scalar UDF; same async dependency (ticket 01).

## Stateless
- [x] Filter + projection routed from SQL via the native expression engine — the
      planner's `Calc` node (optional filter + projections) and a broad function set
      (ticket 19 / divergences/07). Remaining: narrowing/`CAST` and obscure functions.

## Connectors (later)
- [x] Parquet source + sink (local `file:`), exactly-once sink.
- [ ] Remote/columnar sources and sinks (Iceberg, `hdfs:`/`s3:`) — ticket 24.
- [ ] Native columnar sources (Fluss / Iceberg-CDC) via Flink Source API +
      availability futures — ticket 01.

## Cross-cutting (do alongside, not after)
- Acceleration config (allowIncompatible / master / per-operator flags) — done (`NativeConfig`)
- Native columnar shuffle — done (divergences/10)
- Changelog/retract support (ticket 06) gates the updating aggregations & joins
