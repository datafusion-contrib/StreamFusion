# Native two-phase (local + global) window aggregation

**Status:** designed — ready to implement (tumbling, sum-family)
**Source:** discovered while routing window SQL; design grounded by probing the default plan

## Why
By default Flink splits a window aggregation into a local pre-aggregate, a
hash shuffle, and a global aggregate, to cut shuffle volume and absorb skew.
We currently force `agg-phase-strategy = ONE_PHASE` to get a single
substitutable node, throwing away the pre-aggregation that gives most of the
throughput win on high-volume streams.

## What the default plan looks like (probed)

```
LocalWindowAggregate   in [k, value, rt]            out [k, sum$0, $slice_end]
  -> Exchange (hash by k)
GlobalWindowAggregate  in [k, sum$0, $slice_end]    out [k, total, window_start, window_end]
```

- Flink pre-aggregates per **slice**. For a TUMBLING window, slice == window,
  so `$slice_end` is the window end (epoch millis, BIGINT) and the split is a
  clean partial/final.
- `sum$0` is the partial accumulator. For SUM/MIN/MAX/COUNT the partial is a
  single value, which is exactly what our accumulators' `state()` produces, so
  we can match Flink's intermediate schema directly.

## Design

Substitute **both** physical nodes (`StreamPhysicalLocalWindowAggregate`,
`StreamPhysicalGlobalWindowAggregate`), leaving Flink's exchange between them:

- **Local operator**: keyed by `(key, slice_end)`; folds rows into a partial
  accumulator; on watermark emits `[key, <partial state...>, slice_end]`,
  matching the local node's output type. Key stays at column 0 so the
  exchange's hash-by-key distribution remains valid.
- **Global operator**: keyed by `(key, window)`; reads `[key, partial, slice_end]`,
  derives the window from `slice_end` (window_end = slice_end, window_start =
  slice_end - size), `merge_batch`es the partials; on watermark emits
  `[key, total, window_start, window_end]` (the global node's output type).

Then remove the forced ONE_PHASE for matched queries.

## Scope

- First: tumbling windows, SUM/MIN/MAX/COUNT, 0–1 integer key (matches what we
  already accelerate one-phase).
- Later: AVG (its intermediate is multi-field — must match Flink's avg
  encoding, e.g. `sum$0, count$1`), hopping windows (shared slices), multiple
  aggregates.

## Verification

Parity harness: run the query under **default** (two-phase) planning, compare
full results to the host, and assert both native nodes substituted. Add a
parallelism > 1 case so the shuffle actually splits work across local
instances.

## Open questions

- AVG / multi-field partial intermediate schema (`sum$0, count$1`) parity.
- Hopping slice sharing (a slice feeds multiple windows).
- Whether to match Flink's exact intermediate column names or just types
  (the exchange cares about distribution on the key, not names).
