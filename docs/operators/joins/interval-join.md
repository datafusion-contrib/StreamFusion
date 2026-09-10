# Interval join

**Status:** Native. A time-bounded join — a `BETWEEN` predicate on rowtime or proctime instead of
(or alongside) an equi-key match window. Unlike the [regular join](regular-join.md), both inputs
must be insert-only; it is not one of the changelog-aware operators, so a retracting/updating input
falls it back per the [insert-only guard](../index.md#global-switches).

Both **event-time and proctime** bounds are native. An event-time interval join times rows by
rowtime and evicts on the watermark; a proctime interval join times rows by the processing clock and
evicts on a processing-time timer instead.

## Admission

Same equi-key/type/residual conditions as the [regular join](regular-join.md): a supported-type
equi-key, null-dropping keys for a non-INNER join, and a non-equi residual the native expression
engine can express. All four join types — INNER, LEFT, RIGHT, and FULL — are native.

Residual predicates are compiled at planning time against the nullable `[left, right]` Arrow
schema and must return `BOOLEAN`. Successful expression encoding alone is not admission: an
unsupported coercion, such as comparing a materialized interval with an interval literal, falls
back with `residual condition does not compile natively` before the first batch is processed.

## Falls back to Flink when

- the join type isn't INNER, LEFT, RIGHT, or FULL;
- there's no equi key;
- the key columns aren't null-dropping for a non-INNER join;
- the equi-key type is outside the supported set;
- the non-equi residual (the interval bound plus any extra condition) isn't expressible by the
  native expression engine.
