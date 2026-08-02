# Expand

**Status:** native, with one gap.

`Expand` is the plan node behind `GROUPING SETS`/`ROLLUP`/`CUBE`: it re-emits each input row once
per grouping set, projecting a fixed set of output columns per copy. The native operator reproduces
that fan-out over Arrow batches.

## Fallback

Every cell of the expand projection must be one of:

- a plain column reference,
- a `NULL` literal (the columns a given grouping set doesn't group by), or
- the integer expand-id (the synthetic column downstream `GROUP BY` uses to tell grouping sets
  apart).

Any other project cell — an expression more complex than a bare reference — falls the operator
back, taking the whole query with it per the
[all-or-nothing island rule](index.md#the-all-or-nothing-island).
