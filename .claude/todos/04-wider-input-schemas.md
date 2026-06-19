# Wider input schemas: types, multiple columns, grouping keys

**Status:** partial — a single integer grouping key is supported; the rest remain
**Source:** running theme across the operator/matcher work

## Done
The window operator is keyed by `(window, key)` and supports one integer
grouping key in addition to the window (`GROUP BY k, window_start, window_end`),
matching the host's per-key results and output column order. Multiple aggregates
per window are supported. The native engine is value-type agnostic; the
accelerated value types are the parity intersection in
`docs/aggregate-type-support.md` — all aggregates over bigint/double, and
`SUM`/`MIN`/`MAX`/`COUNT` over int (`SUM` via a custom wrapping int32
accumulator; double one-phase only). Remaining below:

- **More value types via the parity table:** extend `MIN`/`MAX`/`COUNT` to
  smallint/tinyint/float/decimal (mechanical — an Arrow vector class + getter +
  a value-type code each).
- **`AVG` over int, and `SUM`/`AVG` over smallint/tinyint/float:** need the
  remaining custom truncating/wrapping accumulators to match Flink.
- **More key types:** bigint/int/string keys are done (the native key is a list
  of typed scalars). Decimal/timestamp/etc. keys are a matcher gate + a JVM
  vector each.
- **Double through the two-phase split** (local/global partials are bigint).

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
