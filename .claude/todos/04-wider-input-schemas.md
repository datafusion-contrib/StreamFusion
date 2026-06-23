# Wider input schemas: types, multiple columns, grouping keys

**Status:** partial — a single integer grouping key is supported; the rest remain
**Source:** running theme across the operator/matcher work

## Done
The window operator is keyed by `(window, key)` and supports one integer
grouping key in addition to the window (`GROUP BY k, window_start, window_end`),
matching the host's per-key results and output column order. Multiple aggregates
per window are supported. The native engine is value-type agnostic; the
accelerated value types are the parity intersection in
`docs/aggregate-type-support.md` — all aggregates over bigint/double (both one-
and two-phase) and over int (one-phase; `SUM` and `AVG` use custom
wrapping/truncating accumulators), plus `MIN`/`MAX`/`COUNT` over
smallint/tinyint/float (the type-preserving comparisons/counts; their value
column rides a narrow Arrow vector decoded by a per-type value-type code).
Grouping keys may be bigint/int/string. Remaining below:

- **`SUM`/`AVG` over smallint/tinyint/float:** need the remaining custom
  truncating/wrapping accumulators (i16/i8 wrapping sum, float32 sum) to match
  Flink, which keeps the narrow input type rather than widening.
- **DECIMAL value columns (all aggregates):** precision/scale derivation is
  exotic; a matcher gate + a decimal Arrow vector path.
- **More key types:** bigint/int/string keys are done (the native key is a list
  of typed scalars). Decimal/timestamp/etc. keys are a matcher gate + a JVM
  vector each.

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
