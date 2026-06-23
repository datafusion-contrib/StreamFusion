# Aggregate × value-type support (parity intersection)

The native side runs DataFusion's aggregates, whose result types and arithmetic
differ from Flink's. Since the prime directive is byte-identical results, the
matcher only accelerates `(aggregate, value-type)` pairs where the two agree.
This table is the guardrail (the matcher enforces it); rows marked ✗ fall back.

Flink semantics (from `flink-table` `aggfunctions/` @ release-2.2.0):
- **SUM** preserves the input type and accumulates in it — `SUM(INT)→INT`,
  wrapping at 2³¹; `SUM(FLOAT)→FLOAT`; `SUM(DECIMAL(p,s))→DECIMAL(38,s)`.
- **AVG** returns the input type; for integers it is truncating integer division
  (sum in bigint, `÷` count, cast back) — `AVG(INT)→INT`.
- **MIN/MAX** return the input type (no arithmetic).
- **COUNT** is always `BIGINT`.

DataFusion: `sum(intN)→Int64`, `sum(floatN)→Float64`, `avg(int)→Float64`,
`min`/`max`→input type, `count`→Int64.

| value type | SUM | AVG | MIN | MAX | COUNT |
|---|---|---|---|---|---|
| BIGINT  | ✓ | ✓ (custom truncating) | ✓ | ✓ | ✓ |
| DOUBLE  | ✓ | ✓ | ✓ | ✓ | ✓ |
| INT     | ✓ (custom wrapping) | ✓ (custom truncating) | ✓ | ✓ | ✓ |
| SMALLINT / TINYINT | ✓ (custom wrapping) | ✓ (custom truncating) | ✓ | ✓ | ✓ |
| FLOAT (REAL) | ✓ (custom f32) | ✓ (custom f64→f32) | ✓ | ✓ | ✓ |
| DECIMAL | ✗ precision rules | ✗ | ✗ | ✗ | ✗ |

Notes / divergences this avoids:
- **SUM over int/smallint/tinyint** is matched with a custom wrapping accumulator
  that keeps the narrow type and wraps at the type's width on every step — exactly
  the host's "store the running sum in the input type, cast back each step"
  semantics (verified at the overflow boundary by a parity test).
- **AVG over integers** is DataFusion `Float64`; Flink truncates to the input
  type. We match this for `BIGINT`/`INT`/`SMALLINT`/`TINYINT` with a custom
  accumulator that sums in int64 and casts the truncated result back to the input
  integer type.
- **SUM/AVG over FLOAT** match with custom accumulators that reproduce the host's
  precision exactly: `SUM` accumulates in float (4-byte, rounding each step) rather
  than DataFusion's widening double sum; `AVG` accumulates the sum in double and
  narrows the quotient to float (as `FloatAvgAggFunction` does). Both fold in the
  same per-row order as the host, so the result is bit-identical — verified by a
  parity test over values whose float running sum carries rounding error.
- **DECIMAL** is excluded entirely (even MIN/MAX): precision/scale derivation and
  the decimal Arrow vector path are not yet built.

## Predicate arithmetic (filter expressions)
The native expression engine admits `+`/`-`/`*` in filter predicates. DataFusion
evaluates integer arithmetic with the wrapping kernels (`add_wrapping` etc.) — the
same two's-complement wrap as Flink's Java integer arithmetic — so the only thing
that has to match is the *type* the arithmetic is computed in. Integer literals are
therefore encoded at their declared width (`TINYINT`/`SMALLINT`/`INTEGER`/`BIGINT`
→ i8/i16/i32/i64), so `int * 2` stays int32 and wraps exactly as the host does
rather than widening to int64. A parity test at the `INT` overflow boundary
(`v * 2` near `INT_MAX`) confirms native and Flink agree, wrap and all.

Arithmetic *between* narrow-integer columns (`TINYINT`/`SMALLINT`) is now verified to
match: parity tests that overflow the narrow range (`a + b` with both `TINYINT` = 100,
`c * c` with `SMALLINT` = 300) route to native and agree with Flink, so DataFusion's
result-type coercion lines up with Flink's promotion/wrap behavior on both sides.
Comparisons are width-insensitive and always safe.

## Status
- Every non-decimal numeric type — `BIGINT`, `DOUBLE`, `INT`, `SMALLINT`, `TINYINT`,
  `FLOAT` — carries all five aggregates, with custom accumulators where DataFusion
  would diverge from the host's type/precision (integer `SUM` wraps, integer `AVG`
  truncates, float `SUM` stays 4-byte, float `AVG` narrows from a double sum, double
  `AVG` divides in double). The native value path is type-general; each type is an
  Arrow vector class + getter + a value-type code.
- `DECIMAL` (all aggregates) stays on the host: precision/scale derivation and the
  decimal Arrow vector path are not yet built.
- `COUNT(*)` is supported as the sole aggregate (single-phase): the operator
  synthesizes a non-null value column so the existing COUNT counts every row. Mixed
  with a value aggregate it needs a second value column (not yet supported), and its
  two-phase global merge does not match — so under default planning it falls back.
  The two-phase **local** is restricted to bigint/double value types (the only
  partials the global merges); narrower types route single-phase only.
- Grouping keys: one or more bigint/int/string/boolean/date keys are supported.
  The native composite key is a list of typed scalars and the native key path is
  type-general (it reads/rebuilds whatever Arrow type arrives), so widening keys is
  a JVM-side matcher gate + vector carriage per type: int widens into int64 and is
  emitted back as int, strings ride as varchar, boolean as a bit column, date as the
  epoch-day Date32. Other key types (decimal, timestamp, …) fall back. (The join and
  `OVER` partition paths still carry only bigint/int/string until their wider-key
  handling is covered.)
