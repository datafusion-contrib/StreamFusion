# Wider input schemas: types, multiple columns, grouping keys

**Status:** partial — value types, multiple value columns, wide grouping keys, and
`COUNT(*)` land; decimal values, decimal/timestamp keys, and two-phase `COUNT(*)` remain
**Source:** running theme across the operator/matcher work

## Done
The window operators are keyed by `(window, key)` and the native engine is
value-type agnostic. The accelerated shapes (parity intersection in
`docs/aggregate-type-support.md`):

- **Value types:** all aggregates over every non-decimal numeric type. Bigint/double
  are one- and two-phase; int/smallint/tinyint/float are one-phase with custom
  accumulators that keep the host's type and precision (integer wrap/truncate, float
  4-byte sum, float/double avg in double), verified at the overflow boundary and
  under float accumulation error.
- **Multiple value columns:** each aggregate reads its own value column, so
  `SUM(a), SUM(b)` over different columns is accelerated. The native batch carries one
  `value{i}` column per aggregate, decoded by a per-aggregate value-type code threaded
  through the JNI create/restore entry points. Two-phase is admitted when every
  partial is one the global merges (bigint/double).
- **Grouping keys:** bigint/int/string/boolean/date (the native key path is
  type-general, so each is a matcher gate + a JVM vector).
- **`COUNT(*)`:** supported, including alongside value aggregates, by synthesizing a
  non-null value column the COUNT counts. Single-phase only (its two-phase global
  merge does not match, so the local is held back to avoid a split aggregation).

## Remaining
- **DECIMAL value columns (all aggregates):** precision/scale derivation is exotic;
  a matcher gate + a decimal Arrow vector path.
- **More key types:** decimal/timestamp keys (a JVM vector + boxing each; timestamp
  also carries a precision). The join and `OVER` partition paths still carry only
  bigint/int/string — widening them reuses the same machinery once covered by tests.
- **Two-phase `COUNT(*)`:** its global merge aggregate is not one the global matcher
  recognizes, so default-planned `COUNT(*)` falls back. Routing it needs the global to
  recognize that merge (and the local then to admit the synthetic-count column).
