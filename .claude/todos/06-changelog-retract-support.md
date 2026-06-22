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
The substitution now refuses any node that is not insert-only
(`ChangelogPlanUtils.isInsertOnly` in the optimizer stage), so a retracting or
updating stream is left to the host instead of being silently mishandled.
Correctness is protected; the rest below is about *supporting* retractions
natively rather than declining them.

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
