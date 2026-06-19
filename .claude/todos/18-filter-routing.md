# Filter (WHERE) routing + whole-row columnar conversion

**Status:** core DONE — whole-row converter (stage 1), native `filterBatch`
(stage 2), and the operator/planner/matcher (stage 3) all landed and
parity-verified for a single column-vs-literal comparison. What remains is the
"Later" list below (general predicates, general projection, more column types).
**Source:** ticket 08 (filter), and the foundational whole-row conversion that
keep-chains/native-shuffle (tickets 09/10) also need.

## Why
`WHERE` is one of the most common operators and is stateless, so it's the
highest-value query *shape* we don't yet accelerate. It also forces the
general **whole-row `RowData`↔Arrow conversion** that projection and the
keep-columnar-chains work will reuse.

## Whole-row conversion — hand-rolled, not Flink's ArrowUtils
Flink's `ArrowUtils`/`ArrowSerializer` live in **flink-python** and bundle **Arrow
13.0.0**, which clashes with our arrow-java 18.3.0. So we hand-roll a converter
over arrow-java 18 (consistent with how the window operators already build typed
vectors).

`RowDataArrowConverter` (new): given a Flink `RowType` + allocator, builds a
`VectorSchemaRoot`, writes a `List<RowData>` into it, and reads `RowData` back.
Per column, dispatch on `LogicalTypeRoot`:
- TINYINT→TinyIntVector, SMALLINT→SmallIntVector, INTEGER→IntVector,
  BIGINT→BigIntVector, FLOAT→Float4Vector, DOUBLE→Float8Vector,
  BOOLEAN→BitVector, CHAR/VARCHAR→VarCharVector.
- Nullability honored (`RowData.isNullAt` / vector `.setNull`).
- Unsupported column types (decimal, timestamp, nested, binary) → the converter
  reports unsupported and the planner falls back. Add them incrementally.

This is independently testable (a `RowData`→Arrow→`RowData` round-trip unit test)
and committable before any filter is wired — **stage 1**.

## Native filter — **stage 2**
A stateless `filterBatch(in, out, colIndex, opCode, literal)` JNI fn: import the
whole batch, compare column `colIndex` against the literal with an Arrow compare
kernel (`gt/ge/lt/le/eq/ne`), then `filter_record_batch` and export the survivors.
The literal is built as a scalar of the column's own type so the comparison
matches the host's (Flink compares in the column type). Scope v1 to a numeric
column vs a numeric literal.

## Operator + planner — **stage 3**
- Stateless `NativeFilterOperator`: buffer rows, convert the batch whole-row,
  call `filterBatch`, emit survivors. Watermarks pass through unchanged.
- Matcher: a `StreamPhysicalCalc` with an **identity projection** (so it's a pure
  filter — projection support is a later ticket) whose `condition` is a single
  comparison `RexInputRef OP RexLiteral` on a converter-supported numeric column.
  Extract `(colIndex, opCode, literal)`; fall back on anything else (projections,
  AND/OR, arithmetic, non-numeric, multi-term — all later).
- Reuse the existing `StreamPhysicalNativeCalc`/exec scaffolding pattern.

## Acceptance criteria
- `SELECT * FROM t WHERE x > 5` (x numeric) routes its filter to native with
  identical results to the host; rows of only supported column types.
- Non-identity projections, compound/non-comparison predicates, and unsupported
  column types fall back cleanly. Insert-only gate still applies.

## Done beyond the core
- Conjunctions of comparisons (AND-flattening), string equality/inequality
  literals, SEARCH/Sarg expansion (so `BETWEEN`/ranges route), and **column
  subset/reorder projection alongside the filter** (`SELECT a, c FROM t WHERE …`).

## Later (separate tickets)
- General predicate translation (OR/NOT, arithmetic, CAST, functions) — a
  Comet-style expression layer (we hand-translate rather than use Substrait; see
  divergences for the Substrait rationale). This also unlocks equality (`=`),
  which today constant-folds the column into the projection.
- General (expression/constant) projection; then chaining filter+project+aggregate
  natively without RowData round-trips (tickets 09/10).
- Decimal/timestamp/nested column types in the converter.
