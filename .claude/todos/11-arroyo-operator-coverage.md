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
- [x] Hopping / sliding window — one-phase (multi-window assignment); two-phase
      slice-sharing is a follow-up (ticket 13)
- [ ] Session window (`session_aggregating_window.rs`) — dynamic merge on gap
- [ ] Wider value/key types beyond int (ticket 04)

## Other stateful operators
- [ ] Updating (non-windowed) group aggregation — emits retractions (needs
      ticket 06 changelog support first)
- [ ] Window functions / OVER aggregation (`window_fn.rs`)
- [ ] Interval/temporal join (`join_with_expiration.rs`)
- [ ] Instant join (`instant_join.rs`)
- [ ] Lookup join (`lookup_join.rs`) — stateless async I/O; uses ticket 01's
      async pattern, not the synchronous stateful path

## Stateless
- [ ] Filter routed from SQL (ticket 08)
- [ ] Richer projections (beyond the demo doubling) and expressions

## Connectors (later)
- [ ] Native sources (columnar: Fluss / Iceberg-CDC) via Flink Source API +
      availability futures (ticket 01)
- [ ] Native sinks

## Cross-cutting (do alongside, not after)
- Acceleration policy / keep chains (ticket 09)
- Native columnar shuffle (ticket 10)
- Changelog/retract support (ticket 06) gates the updating aggregations & joins
