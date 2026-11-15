# Flink decimal result types and overflow

Flink 2.2.1 resolves the result precision and scale before execution. Its
`DecimalData.fromBigDecimal` rounds HALF_UP to that scale, then returns NULL if
the rounded value exceeds the declared precision. Overflow is observable by
surrounding predicates and CASE expressions as well as the row reader.

DataFusion's ordinary decimal CAST uses Arrow's strict cast options and can fail
the query on the same overflow. Exact-source casts therefore use a prepared
native rescale with an explicit precision check. Float and string casts retain
the existing Flink runtime path. All result columns still use Decimal128;
neither the JNI schema nor checkpoint representation changes.

The shared rescale supports both i128 values and i256 arithmetic intermediates.
As in DataFusion Comet's wide decimal expression, wider arithmetic is an
implementation detail, not a Decimal256 column at the Java boundary. StreamFusion
uses Flink's declared result type and NULL semantics rather than Spark's evaluation
modes. Powers and precision bounds are prepared outside the row loop; integer
and decimal inputs are downcast once, and scalar inputs stay scalar.

Add, subtract, and multiply also bypass generic DataFusion arithmetic followed
by CAST: a Decimal128 intermediate can overflow before that cast, and a product
can have scale above 38 even when its final rounded value fits. Fused expressions
first attempt checked i128 arithmetic, then widen to i256 when necessary. Two
valid input decimals bound the wide intermediate by `2 * 10^76`, within i256.
Only the final rounded result is checked against the declared precision. A
precision overflow sets output validity to NULL; it never leaves an invalid
decimal marked valid. Both-scalar expressions return a scalar for safe broadcast.

Regression tests cover rounding carries of either sign, increased scale,
integer inputs, sliced arrays, validity, empty batches, and SQL projections,
`IS NULL`, CASE, filters, and grouping. The fixed-width rescale and arithmetic are compared against the
existing arbitrary-precision HALF_UP implementation across scales and precision
boundaries. Division retains its separate 38-significant-digit rounding stage;
aggregate overflow state machines are unchanged. End-to-end per-expression
measurements are recorded in `docs/benchmarks/decimal-expressions.md`.

Modulo retains arbitrary-precision arithmetic because its scale alignment and
quotient can exceed Decimal128. Flink applies `MathContext(38)` to the integral
quotient as well: mathematical remainder alone is insufficient. A quotient
needing more than 38 significant digits fails with `Division impossible`;
trailing zeroes may be removed to represent large exact powers without an
error. SQL tests exercise both failures and the allowed large quotients, and
native tests cover negative signs and non-zero remainders. This is an error
semantics fix; no modulo speedup is claimed.

Flink 2.2.1 generates per-row AND/OR branches, whereas DataFusion 54's
`BinaryExpr` can evaluate the right operand across a mixed batch (AND's selective
filtering is only a heuristic, and OR has no mixed-row selection). A correct
decimal error must not become an error on a row Flink never evaluates. The same
recursive admission check used for fallible SQL/JSON therefore declines decimal
MOD and division beneath AND/OR, including nested projections and filters.
The restriction covers both the finite-quotient error and division by zero.
CASE result branches retain native evaluation because DataFusion selects their
rows before evaluating them. SQL regressions use mixed TRUE/FALSE/NULL guards,
failing values on skipped rows, and valid values on evaluated rows; direct
remainder error tests still verify Flink's `Division impossible` behavior.

For non-nullable operands Flink may still declare arithmetic results `NOT NULL`.
An overflow then reaches the shared downstream constraint enforcer and fails
the job. SQL tests check the same failure on both engines; correcting the native
NULL bitmap must not silently remove a Flink-declared constraint.

String-to-decimal TRY_CAST uses Flink's generated expression so that only conversion
failures become NULL; an error evaluating its operand still propagates. Decimal-to-decimal
TRY_CAST uses the same native rescale as CAST. One host exception to precision checking
is the compact string parser: parsing `999.995` as DECIMAL(5,2) retains `1000.00` in
released Flink 2.2.1. Already-evaluated internal DecimalData therefore crosses Arrow
without a second precision check. As in Comet's generated compact-decimal output
(`CometBatchKernelCodegenOutput`), the writer stores the unscaled long directly;
the reader reconstructs the internal decimal from the unscaled value. This differs
from Comet's ordinary Spark Arrow writer, which applies Spark's changePrecision
contract first. External BigDecimal UDF results and changes of scale still undergo
Flink's declared precision/scale conversion. Tests distinguish these contracts and
check that compact rounding carries remain non-NULL through projections, filters,
conditional expressions, and aggregation.

DECIMAL TRUNCATE shares ROUND's fixed-width scaling kernel, omitting the HALF_UP
increment to reproduce Flink's rounding toward zero. Arroyo delegates ordinary math
expressions to DataFusion and has no matching Flink decimal truncation contract to
reuse. Flink resolves the output precision/scale before execution and preserves the
input for positions at or above its scale, including internal compact-parser carries.
Positions below -38 call Flink's generated expression so BigDecimal range exceptions
are preserved; native execution must not clamp those failures to zero. The existing
AND/OR admission rule protects row short-circuiting for these fallible positions.

DECIMAL SIGN follows Flink's released compact/noncompact split rather than a floating
DataFusion signum. Comet supplies the typed unary array/scalar pattern but implements
Spark's floating contract; Arroyo registers DataFusion's defaults. For `p > s`, the native
kernel returns the sign times `10^s`. For noncompact `DECIMAL(p,p)`, Flink reconstructs
the sign through `fromBigDecimal`, so nonzero results become NULL and zero retains its
scale. Compact `DECIMAL(p,p)` SIGN instead retains out-of-precision +/-1 and stays
generated. Its character casts and DECIMAL CASE/IF consumers execute in the same generated
expression: character formatting must see the preserved value, whereas conditional branch
conversion can reject it. Normalizing at every intermediate Arrow boundary would make one
of those consumer contracts incorrect. Direct/literal inputs, same-scale lossless widening
and nested verified SIGN are admitted; arithmetic, narrowing and rescaling remain generated.
