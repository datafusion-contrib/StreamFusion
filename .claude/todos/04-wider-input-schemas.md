# Wider input schemas: types, multiple columns, grouping keys

**Status:** partial — a single integer grouping key is supported; the rest remain
**Source:** running theme across the operator/matcher work

## Done
The window operator is keyed by `(window, key)` and supports one integer
grouping key in addition to the window (`GROUP BY k, window_start, window_end`),
matching the host's per-key results and output column order. Remaining below:
additional/non-integer keys, multiple aggregates, and non-integer value columns.

## Problem
The native operators assume a narrow shape: a single int value column, and
for windows a group-by-window-only aggregation. Real queries have other value
types (long/double/decimal), GROUP BY keys in addition to the window, and
multiple aggregates in one window.

## Goal
Generalize the matcher and operators to:
- value columns of types beyond int32/int64,
- one or more extra grouping keys alongside the window,
- multiple aggregate calls in a single window aggregate.

## Acceptance criteria
- A `GROUP BY key, window_start, window_end` tumbling query with two aggregates
  routes to native with identical results.
- Type coverage matches the safe intersection documented in research §8.

## Pointers
- Per-key state: the native aggregator becomes keyed by `(group_key, window)`
  rather than just `window`.
- Reuse Flink's type mapping (`ArrowUtils`) for the value/key columns;
  research §8 (type mapping) is the guardrail for the supported intersection.
- Depends conceptually on [03] for multi-aggregate output schema handling.
