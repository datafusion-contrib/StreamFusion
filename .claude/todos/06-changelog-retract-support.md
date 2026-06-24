# Changelog / retract (RowKind) support

**Status:** partial — safety gate done; emitting retractions remains
**Source:** research findings §6 (open question #3); v1 is append-only

> **Long-term north-star: move beyond append-only.** Everything native today is
> insert-only. The largest remaining class of operators — non-windowed `GROUP BY`
> aggregation, regular joins, streaming Top-N, keep-last dedup, temporal joins —
> is gated entirely on this, *not* on any compute or shuffle gap (the keyed /
> event-time backbone already exists; see [ticket 26](26-keyed-event-time-operators.md)).
> Supporting `RowKind` end to end (read retractions, accumulate with retract, emit
> the correct changelog) is the single biggest unlock left and the long-term goal.

## Done
- **Safety gate.** The substitution refuses any node that is not insert-only
  (`ChangelogPlanUtils.isInsertOnly` in the optimizer stage), so a retracting or
  updating stream is left to the host instead of being silently mishandled.
  Correctness is protected; the rest below is about *supporting* retractions
  natively rather than declining them.
- **RowKind carriage across the boundary.** The row↔Arrow transpose can carry a
  changelog row's `RowKind` as a hidden four-way byte column (opt-in; insert-only
  paths are unchanged). This is the representational foundation every changelog
  operator needs — chosen four-way over Arroyo's two-way `is_retract` to preserve
  the `-U`/`-D` distinction upsert sinks require (divergences/13).
- **Changelog *emission* — the non-windowed `GROUP BY` aggregate.** The first
  changelog-emitting operator ships: per-key running state, append-only input,
  emitting `+I` then `-U`/`+U` per the host's `GroupAggFunction` (per input row, in
  order; `-U` gated on `generateUpdateBefore`; unchanged result suppressed).
  SUM/MIN/MAX/COUNT (AVG via the host's SUM/COUNT rewrite) over bigint/int/double,
  any converter-supported grouping keys, global aggregation, checkpointed. Gated to
  zero idle-state TTL (the host refreshes/expires keys with a TTL; we suppress
  unchanged results and never expire, which matches only at TTL 0). See the readme
  compatibility row and the `RowKind` carriage above.
  - **Perf:** row-fed it is **0.77× vs Flink** — a cheap per-key SUM does not earn
    back the input transpose plus the up-to-2× changelog output transpose (the same
    transpose-bound story as the lone filter). It ships for correctness and as the
    changelog foundation; the path past 1× is a **columnar variant** fed from a
    columnar source/exchange (no transpose), mirroring the windowed/`OVER` columnar
    operators — the natural perf follow-up, plus the per-row key read (ticket 20).

## Remaining: *consume* a changelog (honor retracting input)
Everything above emits retractions from insert-only input. The next half is
honoring them on **input**:
- Read the input `RowKind` (now carried) and accumulate with retract — a native
  `retract_batch` path on the accumulators, and the `-D` (count reaches zero)
  branch the append-only operator can skip today.
- Drop the insert-only gate on the input edge once an operator can consume a
  changelog correctly.
This unlocks the retract-*consuming* operators: a non-windowed `GROUP BY` over a
retracting source, regular (non-windowed) joins, and streaming Top-N (ticket 11).

## Problem
Everything so far assumes append-only, insert-only streams. Flink's planner
decides per-operator whether a stream is insert-only, retracting, or upserting,
and encodes it in `RowKind` (+INSERT, -UPDATE_BEFORE, +UPDATE_AFTER, -DELETE)
and changelog traits. Our native operators ignore `RowKind` and only emit
inserts, so substituting them into a retracting/upserting plan would produce
wrong results.

## Goal
Honor `RowKind` on input and produce correct changelog output. At minimum,
refuse to substitute (fall back to host) when the matched node is not
insert-only, so correctness is never compromised; then incrementally support
retract aggregation.

## Acceptance criteria
- A retracting/updating query is left to the host (no incorrect substitution).
- Later: a retracting window aggregation handled natively matches the host's
  changelog stream exactly.

## Pointers
- Research §6: Arroyo's 2-way insert/retract cannot express the 4-way RowKind
  needed for upsert sinks (-U vs -D) — mind this when extending.
- Gate first on the node's `ChangelogMode`/`ModifyKindSet` traits in the
  matcher; emitting retractions comes after.
