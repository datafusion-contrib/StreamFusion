# Native expression layer (general RexNode evaluation)

**Status:** stage 1 DONE. Native decoder + compile-once handle
(`createFilterExpression`/`filterExpression`/`closeFilterExpression`, caching a
coerced `PhysicalExpr`, evaluated synchronously). Planner-side `RexExpression`
encoder translates the (SEARCH-expanded) `RexNode` condition to the encoding; the
filter matcher/operator route through the handle, replacing the old comparison/DNF
arrays and the per-batch `filterBatch` plan (now removed). All filter parity tests
pass via the general path, plus new arithmetic-predicate tests and an unsupported-
function fallback test. The encoding follows Comet over Substrait and compiles once
per operator — see [divergences/07](../../divergences/07-expression-encoding-and-compile-once.md).

**Integer-overflow parity: CLOSED.** DataFusion already evaluates `+`/`-`/`*` with
wrapping kernels, matching Flink's Java integer semantics; the remaining mismatch was
that the encoder promoted every integer literal to i64, so `int * 2` computed in i64.
Literals now carry their declared width (i8/i16/i32/i64) so the arithmetic stays in the
host's type. Verified by a parity test at the `INT` overflow boundary. Residual: narrow-
int (`TINYINT`/`SMALLINT`) column arithmetic promotion — see the type-support doc.

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
1. **Encoding + native decoder.** ✅ DONE — `RexExpression` encoder (planner) and
   the native `Expr` builder, plus the compile-once `createFilterExpression`/
   `filterExpression` handle. The filter routes through it; all existing filter
   tests pass via the general path, with added arithmetic-predicate and
   unsupported-function-fallback tests.
2. **General projection.** ✅ DONE — a unified native `Calc` (`CalcExpression`): an optional
   condition then a list of projection expressions, all encoded into one set of pools with each
   tree's root recorded (`encodeCalc`). It filters by the condition, then evaluates each projection
   over the survivors to form the output batch. Columnar in/out (`NativeCalcOperator`,
   `ColumnarInput`/`ColumnarOutput`), routed by `CalcMatcher`; the old doubling-demo projection is
   retired and subsumed. Parity-verified: computed columns, constants, mixed, projection+filter, and
   an un-admitted function falls back. The pure filter-plus-column-subset shape still routes through
   the filter operator (its column-transfer projection avoids evaluating identity exprs) — **unify
   the filter into the Calc later** (the Calc subsumes it; kept separate now to avoid churn/regress).
3. **Fuse + expand.** Projection+filter are now one native pass (done). Widening the admitted
   op/function set with parity tests (each admitted only once a parity test confirms DataFusion
   matches Flink; un-admitted ops fall back):
   - ✅ `IS NOT NULL` (op 31), ✅ searched `CASE` (op 40, n-ary → when/then pairs + else).
   - ✅ `CAST` (node kind 11 + target type code) — **widening numeric only** (integer→wider integer,
     integer→float/double, float→double): lossless/IEEE-identical to the host. Unblocks mixed-width
     `CASE` branches and explicit widening casts. Narrowing / float→int / string casts are
     divergence-prone (overflow/rounding/parsing) and **fall back** (parity test asserts it).
   - ✅ `IS NULL` (op 30) and `IS NOT NULL` (op 31) — both route (Calcite's null-only path reaches
     `IS_NULL` here; no Sarg special-casing was needed). Regression tests for both.
   - ✅ `NULL` literal (kind 5) — a bare/typed NULL encodes to an untyped null
     (`Expr::Literal(ScalarValue::Null)`) the surrounding coercion types.
   - ✅ `COALESCE` — arrives as an `OTHER_FUNCTION` call (not pre-expanded here), so the encoder
     lowers it to the searched `CASE` the host defines it as (`WHEN x IS NOT NULL THEN x … ELSE
     last`), identical first-non-null semantics, no new native op. `NULLIF` rides the same path
     (Calcite lowers it to `CASE … THEN NULL ELSE a`, now that NULL literals are admitted).
   - ✅ `/` (op 3) and `%` (op 4) — integer truncation toward zero and modulo sign-of-dividend match
     Flink on all finite operands (verified with negatives); divide-by-zero fails both sides and the
     lone `INT_MIN/-1` overflow edge is documented (divergences/07).
   - ✅ Narrow-int (`TINYINT`/`SMALLINT`) column arithmetic — verified to match on overflow (parity
     tests `a + b` and `c * c` that overflow the narrow range route and agree; docs/aggregate-type-support).
   - ✅ Scalar string functions `UPPER`/`LOWER`/`CHAR_LENGTH` (ops 50/51/52) — matched by operator
     name (Flink delivers them as `OTHER_FUNCTION`), mapped to DataFusion `upper`/`lower`/
     `character_length`; ASCII-identical, Unicode edges documented (divergences/07). `CONCAT` was
     tried and **rejected**: Flink propagates NULL, DataFusion's `concat` ignores it (fallback test).
   - Remaining: narrowing/float→int/string `CAST` (so `COALESCE(s,'x')` with its `CHAR→VARCHAR` cast
     still falls back); more string functions (`TRIM`/`SUBSTRING`/`POSITION`, each with operand-shape
     or index nuances) and temporal functions (timezone-prone). The long tail; each gated by a
     parity test, admitted only where DataFusion matches Flink.

## Acceptance criteria
- Existing filter/projection tests pass via the general expression path.
- `SELECT a + b FROM t WHERE a * 2 > b` (arithmetic in projection and predicate)
  routes with identical results to the host; an un-admitted function falls back.
