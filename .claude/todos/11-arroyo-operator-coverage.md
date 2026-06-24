# Arroyo operator coverage — route everything Arroyo supports

**Status:** open (tracking)
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
- [ ] Updating (non-windowed) group aggregation — emits retractions (needs
      ticket 06 changelog support first)
- [ ] Window functions / OVER aggregation (`window_fn.rs`)
- [ ] Interval/temporal join (`join_with_expiration.rs`)
- [ ] Instant join (`instant_join.rs`)
- [ ] Lookup join (`lookup_join.rs`) — stateless async I/O; uses ticket 01's
      async pattern, not the synchronous stateful path

## Stateless
- [x] Filter routed from SQL — single column-vs-literal comparison, whole-row
      converter (ticket 18). General predicates/projections remain.
- [ ] Richer projections (beyond the demo doubling) and expressions

## Connectors (later)
- [ ] Native sources (columnar: Fluss / Iceberg-CDC) via Flink Source API +
      availability futures (ticket 01)
- [ ] Native sinks

## Cross-cutting (do alongside, not after)
- Acceleration config (allowIncompatible / master / per-operator flags) — done (`NativeConfig`)
- Native columnar shuffle — done (divergences/10)
- Changelog/retract support (ticket 06) gates the updating aggregations & joins
