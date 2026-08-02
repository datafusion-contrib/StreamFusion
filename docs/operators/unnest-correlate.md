# Unnest / Correlate

**Status:** native, with gaps.

`Correlate` is Flink's plan node for lateral cross-application — most commonly `UNNEST`, which
explodes a collection-typed column into rows. Unlike most operators in this section, `Correlate`
has no general matcher: only the specific shape below runs natively, and everything else is an
outright fallback (see [Unsupported operators](unsupported.md)).

## What's native

Plain **INNER or LEFT** `UNNEST` of a single column of type:

- `ARRAY` (a scalar or `ROW` element, flattened),
- `MAP` (unnested to key + value columns), or
- `MULTISET` (unnested to the element, repeated by its count),

optionally with `WITH ORDINALITY`, and — for the INNER case — including a pushed element filter.

## What's not

- **Lateral table functions** — a user-defined table function applied per row — have no native
  path at all.
- **`UNNEST` with a pushed condition the expression engine can't encode**, or **any condition over
  a LEFT `UNNEST`**, falls back.

Either of these falls the operator back, dragging the whole query with it per the
[all-or-nothing island rule](index.md#the-all-or-nothing-island). See
[Unsupported operators](unsupported.md) for the full accounting of these cases.
