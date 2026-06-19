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
| INT     | ✓ (custom wrapping) | ✗ truncate | ✓ | ✓ | ✓ |
| SMALLINT / TINYINT | ✗ | ✗ | ✓ | ✓ | ✓ |
| FLOAT (REAL) | ✗ FLOAT≠DOUBLE | ✗ | ✓ | ✓ | ✓ |
| DECIMAL | ✗ precision rules | ✗ | ✓ | ✓ | ✓ |

Notes / divergences this avoids:
- **SUM over int** is matched with a custom wrapping int32 accumulator (keeps the
  narrow type, wraps at 2³¹). The same pattern would extend to smallint/tinyint
  (i16/i8) — not yet built.
- **AVG over integers** is DataFusion `Float64`; Flink truncates to the input
  type. We match this for `BIGINT` with a custom accumulator; extending to int
  means casting the truncated result back to int32 — not yet wired.
- **DECIMAL** SUM/AVG precision/scale derivation is exotic; excluded.

## Status
- Implemented value types: `BIGINT`, `DOUBLE` (all five aggregates), and `INT`
  for `SUM`/`MIN`/`MAX`/`COUNT` (all but `AVG`). The native value path is
  type-general; adding the remaining MIN/MAX/COUNT types
  (SMALLINT/TINYINT/FLOAT/DECIMAL) is mechanical (an Arrow vector class + getter +
  a value-type code).
- `AVG` over int, and `SUM`/`AVG` over smallint/tinyint/float, await the custom
  accumulators above.
- Grouping keys: one or more bigint/int/string keys are supported. The native
  composite key is a list of typed scalars; int widens into int64 carriage and is
  emitted back as int, strings ride as varchar. Other key types (decimal,
  timestamp, …) fall back; adding them is matcher gate + a JVM vector each.
