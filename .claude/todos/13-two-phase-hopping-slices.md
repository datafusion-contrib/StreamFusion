# Two-phase hopping windows (slice sharing)

**Status:** open
**Source:** discovered probing HOP; one-phase HOP is done (ticket 11)

## Why
Under default planning a hopping window is two-phase and **slice-based**, not
the simple local/global tumbling split. One-phase HOP is accelerated, but the
default plan falls back. To accelerate HOP without forcing `ONE_PHASE`, the
slice model must be supported.

## What the default HOP plan looks like (probed)

```
LocalWindowAggregate   in [value, rt]                 out [sum$0, count1$1, $slice_end]
  -> Exchange (hash by key)
GlobalWindowAggregate  in [sum$0, count1$1, $slice_end] out [total, window_start, window_end]
  windowing = SliceAttachedWindowingStrategy, HOP(size=2s, slide=1s)
```

Two differences from tumbling two-phase:

1. **Slices, not windows.** The local pre-aggregates per *slice* (slice size =
   slide), and the global combines the consecutive slices that make up each
   hopping window (size / slide of them). A slice feeds multiple windows.
2. **Extra partial column.** Even a lone `SUM` local emits `[sum$0, count1$1,
   $slice_end]` — an auxiliary slice row-count Flink keeps for slice
   management. The intermediate schema is not just one partial per user
   aggregate.

## Work
- Local: pre-aggregate per slice (slice == slide), emit the slice partial(s)
  matching the host's intermediate schema (including the auxiliary count).
- Global: a slice store; for each window, combine its `size/slide` slices
  (sliding combine) and finalize on watermark. This is the slice-sharing
  Arroyo's `sliding_aggregating_window.rs` does — the genuine reference.
- Match `SliceAttachedWindowingStrategy` + `HoppingWindowSpec`; understand and
  reproduce the `count1$1` auxiliary semantics for parity.

## Verification
Parity harness for default-planned HOP (no `ONE_PHASE`), window-only and keyed,
single and multiple aggregates; plus a parallelism > 1 case for the shuffle.

## Notes
- Cumulative windows (`CUMULATE`) are also slice-based; this likely generalizes.
- The one-phase HOP operator already does multi-window assignment, but the
  two-phase path needs the slice combine, which is the harder part.
- **Arroyo is NOT a usable reference for the Flink intermediate here.** Arroyo's
  `sliding_aggregating_window.rs` stores raw batches in a tiered structure and
  re-aggregates per window on advance — it does not produce Flink's
  slice-partial intermediate. So matching Flink's `[sum$0, count1$1, slice_end]`
  (and the `count1$1` retraction/emptiness semantics) is reverse-engineering
  Flink's sliding internals, which is the real parity risk — budget for it.
