# Native two-phase (local + global) window aggregation

**Status:** open
**Source:** discovered while routing window SQL (commit "Run a tumbling-window
SQL query end to end")

## Problem
By default the host engine splits a window aggregation into a local
pre-aggregate, a shuffle, and a global aggregate, to cut network traffic and
absorb skew. We currently sidestep this by forcing `agg-phase-strategy =
ONE_PHASE` so there is a single node to substitute. That throws away the
pre-aggregation that gives most of the throughput win on high-volume streams.

## Goal
Substitute both halves natively: a native combiner that emits partial
accumulator state before the shuffle, and a native merger that combines those
partials after it. Remove the forced ONE_PHASE.

## Acceptance criteria
- A tumbling SUM/MIN/MAX/COUNT query with default (two-phase) planning runs
  natively across both phases with identical results.
- Partial state crosses the shuffle in a serializable form.
- Benchmark shows reduced shuffle volume vs one-phase.

## Pointers
- The accumulator already exposes mergeable partial state (`Accumulator::state`
  / `merge_batch`) — the combiner emits `state()`, the merger consumes it via
  `merge_batch`.
- Arroyo's `split_physical_plan` (`arroyo-planner/src/builder.rs`) is the
  reference for partial/final plan splitting.
- Match `StreamPhysicalLocalWindowAggregate` + `StreamPhysicalGlobalWindowAggregate`.
