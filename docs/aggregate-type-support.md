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
| FLOAT (REAL) | ✗ FLOAT≠DOUBLE | ✗ | ✓ | ✓ | ✓ |
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
- **SUM/AVG over FLOAT** stays on the host: the host accumulates a float sum at
  4-byte precision (and avg divides in double then narrows to float), which a
  native accumulator would have to reproduce bit-for-bit under the same fold
  order. Deferred until that parity is stress-tested; `MIN`/`MAX`/`COUNT` over
  float are unaffected (no arithmetic).
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
- `BIGINT`, `DOUBLE`, and `INT` carry all five aggregates. `SMALLINT` and `TINYINT`
  carry all five too (`SUM`/`AVG` via custom wrapping/truncating accumulators).
  `FLOAT` carries `MIN`/`MAX`/`COUNT` (its `SUM`/`AVG` are deferred — see above).
  The native value path is type-general; each narrow type is an Arrow vector class +
  getter + a value-type code.
- `DECIMAL` (all aggregates) stays on the host: precision/scale derivation and the
  decimal Arrow vector path are not yet built.
- Grouping keys: one or more bigint/int/string keys are supported. The native
  composite key is a list of typed scalars; int widens into int64 carriage and is
  emitted back as int, strings ride as varchar. Other key types (decimal,
  timestamp, …) fall back; adding them is matcher gate + a JVM vector each.
