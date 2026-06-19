# Native expression layer (general RexNode evaluation)

**Status:** in progress — stage 1 native decoder DONE (the `Expr` builder, op codes,
and a compile-once `createFilterExpression`/`filterExpression`/`closeFilterExpression`
handle that caches a `PhysicalExpr` and evaluates synchronously; Rust-tested for
`col > lit` and `a + b > lit`). The encoding follows Comet over Substrait and compiles
once per operator — see [divergences/07](../../divergences/07-expression-encoding-and-compile-once.md).
What remains in stage 1: the planner-side `RexNode` encoder and re-routing the existing
filter through the handle. The compile-once lifecycle came out of the perf sweep in
ticket 20.
**Source:** the foundational piece for general projections and richer predicates
(unblocks equality filters, computed columns, arithmetic/function predicates).

## Why
Today filters carry a fixed shape (conjunctions/disjunctions of column-vs-literal
comparisons) and projections are column subsets. Real Calcs have arbitrary
expressions: arithmetic, functions, CAST, `a + b`, `WHERE a * 2 > b`, and the
constant-folded column that blocks equality. A general RexNode→native evaluator
handles all of these for both the filter condition and the projection.

## Approach — hand-translated, encoded, DataFusion-evaluated
Like Comet (and unlike Substrait — see divergences for the rationale), we
hand-translate `RexNode` to a compact encoding and rebuild a DataFusion `Expr`
natively, then evaluate over the imported batch (`DataFrame.filter`/`.select`).
No new dependency, full control over which ops we admit.

**Encoding** (pre-order, parallel arrays, so JNI stays primitive arrays):
- `int[] kinds`: 0 INPUT_REF, 1 LIT_LONG, 2 LIT_DOUBLE, 3 LIT_STRING, 4 LIT_BOOL,
  5 LIT_NULL, 6 CALL.
- `int[] payload`: INPUT_REF → column index; CALL → op code; LIT_* → index into
  the matching literal pool; else -1.
- `int[] childCounts`: CALL → operand count; else 0.
- literal pools: `long[]`, `double[]`, `String[]` (booleans fold into longs).
The native side walks the pre-order stream, using `childCounts` to recurse, and
builds an `Expr`: INPUT_REF → `col(field name)`, LIT → `lit(value)`, CALL → the
binary/unary op or function, CAST → `Expr.cast_to(type)`.

**Op codes (CALL):** start with the parity-safe core — `+ - *`, the six
comparisons, `AND`/`OR`/`NOT`, `CAST` to a numeric/string type. Expand
cautiously: `/` and `%` (integer-division parity!), string/temporal functions,
etc. each only once a parity test confirms DataFusion matches Flink.

## Parity discipline
DataFusion's expression semantics differ from Flink's in places (integer
division, decimal arithmetic, null handling, string collation). So the **matcher
gates** to the admitted op set and the admitted operand types, and every admitted
op gets a parity test. An un-admitted op anywhere in the expression makes the
whole Calc fall back. This is the same parity-first stance as the rest of the
project; record any deliberate semantic choice in `divergences/`.

## Stages (each green + parity-tested)
1. **Encoding + native decoder.** The `RexExpression` encoder (planner) and the
   native `Expr` builder, plus a `filterByExpression(batch, encoded…)` JNI fn.
   Re-route the existing filter through it (the current comparison/DNF arrays
   become one boolean expression), so all existing filter tests pass via the
   general path. Add arithmetic-predicate tests (`WHERE a + b > 10`).
2. **General projection.** The Calc projection becomes a list of expressions
   evaluated via `select`; output rows built from the result. Subsumes the
   column-subset projection and unlocks computed columns and equality (the folded
   constant projection becomes a literal expression).
3. **Fuse + expand.** One native Calc operator doing projection+filter in a
   single pass; widen the admitted op/function set with parity tests.

## Acceptance criteria
- Existing filter/projection tests pass via the general expression path.
- `SELECT a + b FROM t WHERE a * 2 > b` (arithmetic in projection and predicate)
  routes with identical results to the host; an un-admitted function falls back.
