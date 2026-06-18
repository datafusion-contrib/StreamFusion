# AVG and general accumulator state

**Status:** open
**Source:** deferred in commit "Support min, max and count window aggregates"

## Problem
The window operator only handles aggregates that reduce an int column to a
single int with single-value partial state (SUM/MIN/MAX/COUNT). Two
assumptions are baked in: the output column is `BIGINT`, and the checkpoint is
a flat `(window_start, partial)` int64 pair sequence. AVG breaks both — it
outputs `DOUBLE` and its partial state is two values (sum, count).

## Goal
Support aggregates with wider output types and multi-field partial state,
starting with AVG. Generalize the checkpoint to serialize arbitrary
accumulator state, and make the emitted column type match the host's
aggregate output type.

## Acceptance criteria
- `AVG(value)` over a tumbling window routes to native and matches the host's
  result and output type.
- Checkpoint/restore works for multi-field accumulator state.
- SUM/MIN/MAX/COUNT continue to pass unchanged.

## Pointers
- `AggregateFunctionExpr::state_fields()` gives the partial-state schema.
- Serialize per-window state as an Arrow batch (window_start + state columns)
  via Arrow IPC; restore by rebuilding accumulators and `merge_batch`. This is
  the general form the current compact int64 path is a special case of.
- Output type comes from the matched node's row type; stop assuming BIGINT.
