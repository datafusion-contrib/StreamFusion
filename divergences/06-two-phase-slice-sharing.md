# Two-phase hopping via Flink's slice-sharing intermediate

**Kind:** structural — an intermediate and combine reverse-engineered from Flink.
**Diverges from:** Arroyo.
**Forced by parity:** yes (it is the default plan shape Flink hands us for HOP).

## Their decision
Arroyo's sliding window (`sliding_aggregating_window.rs`) keeps raw input batches
in a tiered structure and re-aggregates per window as it advances. It does not
produce a slice-partial intermediate, and it has no separate local/global split
across a network boundary.

## What Flink does, and what we therefore do
Under default planning a hopping window is **two-phase and slice-shared**:
- a **local** aggregate pre-aggregates per *slice* (slice width = `gcd(size,
  slide)`), emitting `[grouping?, partials…, count1$1, slice_end]`;
- the host shuffles by key;
- a **global** aggregate combines, for each window, the `size/slice` slices that
  make it up — slices are *shared* across overlapping windows.

To route this plan we reproduce that contract exactly: the native local
pre-aggregates at slice granularity, and the native global fans each slice
partial into every window containing it, merging shared slices. `count1$1` is a
synthetic per-slice `COUNT(*)` the planner injects into the intermediate row (it
is **not** an aggregate call); we fill it in the local and ignore it in the
global. Its only Flink purpose — empty-window detection — cannot arise in our
batch-flush model, where a window is materialized only from slices that carried
rows.

This is reverse-engineered from Flink's slicing runtime, not adapted from Arroyo,
because Arroyo's raw-retain-and-recompute approach produces a different
intermediate. It connects to [03](03-incremental-window-merge.md): there we merge
accumulators instead of retaining raw rows; here the *unit* we merge is the slice
partial rather than the whole-window accumulator.

## Scope / consequences
- Restricted to a slide that divides the size (then slice = slide). Other ratios
  use a finer `gcd` slice with a different window↔slice stepping and fall back.
- Cumulative windows are also slice-based and two-phase by default; two-phase
  cumulative is not done yet and falls back (see
  `.claude/todos/13-two-phase-hopping-slices.md`).
- Output is identical to the host, verified by the parity harness (window-only
  and keyed, single and multiple aggregates).
