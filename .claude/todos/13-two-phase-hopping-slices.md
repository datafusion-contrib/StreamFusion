# Two-phase cumulative windows (slice sharing)

**Status:** open — two-phase **hopping** is DONE (slide divides size; see
divergences/06). What remains is two-phase **cumulative**, which is also
slice-based, plus two-phase hopping for the `gcd(size,slide) < slide` ratios that
currently fall back.
**Source:** discovered probing HOP; one-phase HOP/CUMULATE and two-phase HOP done

## Remaining
- Two-phase `CUMULATE`: like hopping it is slice-shared by default, but the
  window→slice fan differs (nested windows sharing a start). The global fan-out
  added for hopping (`update_partial`) assumes fixed-size windows stepping by the
  slide; cumulative needs the nested-end mapping instead.
- Two-phase hopping where the slide does not divide the size (finer `gcd` slice).

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

## Concrete design (from reading Flink 2.2 slicing internals)

**Slice width = `gcd(size, slide)`**; `numSlicesPerWindow = size / sliceSize`. A
window ending at `we` covers slices ending at `we, we-sliceSize, …` (n of them).
`count1$1` is a synthetic `COUNT(*)` the planner appends to the local aggCalls
(empty argList) to detect empty windows; it is merged in the global and excluded
from the final output. The intermediate row is
`[grouping?, accFields…(sum$0, count1$1), $slice_end:bigint]`.

**Scope simplification:** support only `size % slide == 0` (then `sliceSize ==
slide`, `numSlicesPerWindow = size/slide`); other ratios fall back. This covers
the common HOP and avoids the gcd-vs-slide slice/window stepping mismatch.

We do *not* need a separate slice store: the global reuses the `(start,end)`
aligned-window map. Because empty windows can't occur in the batch-flush model
(a window is only materialized if a slice fed it), `count1$1` is carried only to
satisfy the intermediate schema — its value never changes which windows emit.

**Native**
- `KIND_COUNT_STAR` accumulator: counts rows (array length), additive merge,
  single bigint partial. The local builds it as the trailing aggregate.
- `update_partial` fan-out: for each slice partial, merge it into every window
  containing the slice — `we = slice_end + j*slide` for `j in 0..numWindows`,
  `numWindows = window_millis/slide_millis` (1 for tumbling → today's behavior).

**Operators / planner**
- Local: create the aggregator with `window = slice = sliceSize` (= slide here),
  kinds `[userKinds…, COUNT_STAR]`. Emit K+1 partials + slice_end (unchanged emit).
- Global: create with the real `(size, slide)`; thread `slideMillis` (today it
  passes size as slide). Emit only the K user results — add an
  `emittedAggregateCount()` hook (default = all) the global overrides to K, so the
  auxiliary count's result column is ignored.
- Local matcher: accept the trailing `COUNT(*)` (empty argList) as the auxiliary;
  user aggs are the first K. Slice size = slide; require `size % slide == 0`.
- Global matcher: accept `SliceAttachedWindowingStrategy` + `HoppingWindowSpec`;
  carry size+slide; partials positional incl. count1; output count = K.

This is reverse-engineered from Flink, not from Arroyo (which re-aggregates raw
slices), so it is also a divergence to record once landed.

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
