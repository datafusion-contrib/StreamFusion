# Calc coverage by topic: numeric

[Topic index and original links](calc-filter.md). Sections retain their recorded scope, dates and evidence.

## Integer division {#integer-division}

Integer `/` truncates toward zero and wraps `MIN_VALUE / -1` back to `MIN_VALUE`,
matching Java. A non-NULL dividend divided by zero fails the job. A NULL operand
produces NULL, including a NULL dividend with a zero divisor.

Integer `/`, `MOD`, and `%` inside `AND` or `OR` stay native when the divisor is a nonzero
integer literal and their operands contain no fallible division. Other divisor shapes fall back
at planning time, including in projections, filters, and nested expressions. DataFusion's batch
evaluation may run division on rows whose guard Flink short-circuits. Constant-divisor filters such
as `event_type = 2 AND MOD(bid.auction, 2) = 0` are safe even when the nested ROW is NULL.
CASE result branches stay native because
CASE selects rows before evaluating them. Floating division remains native under boolean
operators because zero divisors produce IEEE infinity/NaN instead of an exception.


## SIGN {#sign}

INT/BIGINT SIGN returns `-1`, `0` or `1` in the input's resolved integer width, including
at INT_MIN and BIGINT_MIN. Direct inputs, typed literals, recursive lossless integer widening,
nested admitted SIGN calls and recursively verified integer addition/subtraction/multiplication
use Arrow unary kernels without JVM computation. Every arithmetic node keeps its resolved
width and Java overflow wrapping. Input values
remain owned by the input; result values are allocated and sliced validity is shared. Explicit
TINYINT/SMALLINT-to-INT/BIGINT widening is covered. Direct TINYINT/SMALLINT SIGN retains
released Flink's generated-code compilation failure (the numeric result cannot be assigned to `byte/short`).
Division, remainder, narrowing casts and other unverified producers retain the complete generated SIGN
program so overflow, NULLs, lazy branches and selected failures do not change.
This replaces computation inside an already-admitted native Calc without adding a switch.
See the [release measurements and limits](../benchmarks/scalar-functions.md#integral-sign).
The [arithmetic-producer comparison](../benchmarks/scalar-functions.md#integer-sign-over-arithmetic)
retains stock and previous controls, slower tiny batches and complete jobs with both transposes.

DECIMAL SIGN uses a typed Decimal128 kernel for `p > s`, and for precision 19 through 38
when `p = s`. Direct inputs, typed literals/NULLs, same-scale lossless precision widening
and nested admitted SIGN calls retain the resolved precision and scale without JVM evaluation.
DECIMAL-only addition/subtraction/multiplication trees over those producers also remain
native through SIGN. Verified ABS/unary-minus producers, including alternating unary trees
and their admitted arithmetic, also compose through SIGN. Every arithmetic node retains its
resolved precision/scale and overflow NULL. This SIGN-specific proof still excludes compact
`DECIMAL(p,p)` intermediates even if
an enclosing arithmetic operation or widening could normalize them.
For `p > s`, the result is scaled `-1`, `0` or `1`. For noncompact `DECIMAL(p,p)`, nonzero
values become NULL, zero remains scaled zero, and input NULLs remain NULL, matching released
Flink. NOT NULL basic-type final consumers of that noncompact NULL-producing SIGN retain the
complete generated projection write rule: stock can write `false`, `-1` or an empty string
instead of exposing the intermediate NULL. Nullable consumers and retained NULL/truth tests
keep their existing native rules; direct DECIMAL results still reach ERROR/DROP enforcement.
This repairs [#584](https://github.com/datafusion-contrib/StreamFusion/issues/584) without
changing the SIGN kernel or claiming new acceleration. Division, remainder, mixed numeric
operands, precision narrowing, rescaling and other unverified producers and compact
`DECIMAL(p,p)` SIGN keep complete generated evaluation. Widening a compact SIGN intermediate
does not make it eligible. Array evaluation allocates result values without mutating the
input; ordinary-decimal validity is shared and noncompact `p = s` derives result validity.
See the [DECIMAL SIGN measurements](../benchmarks/scalar-functions.md#decimal-sign).
The [arithmetic-producer qualification](../benchmarks/scalar-functions.md#decimal-sign-over-arithmetic)
also covers noncompact zero-integral-width inputs, nullable BOOLEAN consumers and complete
jobs with both transposes. Character CAST/TRY_CAST retain their separate producer proof;
the new arithmetic SIGN admission does not make every enclosing conversion JVM-free.
The separate [unary-producer qualification](../benchmarks/scalar-functions.md#decimal-sign-over-verified-unary-producers-2026-11-05)
retains compact SIGN guards, final defaults/NULL flags and NOT NULL enforcement while
comparing stock and the actual preceding generated evaluator.

For compact `DECIMAL(p,p)` with precision 1 through 18, STRING/CHAR/VARCHAR CAST
and DECIMAL CASE/IF consumers keep SIGN and its enclosing scalar expression in one generated
evaluator. Flink's conditional branch conversion can reject the compact SIGN's out-of-precision
value and return NULL; a separate native selection must not skip that conversion.
Released Flink retains `1` or `-1` with the original scale even though those values exceed
the declared precision; reconstructing that intermediate before character formatting would
incorrectly produce NULL.
Compact direct SIGN results and null predicates keep generated evaluation; TRY_CAST retains
its existing fallback. Noncompact precision 19 through 38 computes the released NULL result
for nonzero `DECIMAL(p,p)` SIGN in Rust.
Conditional branches, filters and the existing decimal-rounding representation guards still
apply. These guards repair [#536](https://github.com/datafusion-contrib/StreamFusion/issues/536)
and [#547](https://github.com/datafusion-contrib/StreamFusion/issues/547).
Remaining producer and compact-decimal native coverage is tracked in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491) and
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

FLOAT/DOUBLE SIGN returns the input for signed zero and NaN, and `-1` or `1` for
nonzero values, including infinities. NULL remains NULL. The result keeps the input's
floating width; dividing by `SIGN(-0.0)` therefore retains negative infinity.


## Floating comparisons {#floating-comparisons}

FLOAT/DOUBLE `=`, `<>`, `<`, `<=`, `>`, and `>=` use Java primitive comparisons in
projections and filters. Positive and negative zero compare equal. NaN compares unequal to
every value, including itself; all four relational comparisons involving NaN return FALSE.
NULL propagates. Mixed primitive numeric operands use Java's FLOAT/DOUBLE promotion.
These scalar rules are separate from grouping-key equality and sort ordering.


## Casts {#casts}

Primitive integer/string, Boolean/string and binary/string cast families resolve through shared
semantic rules. Each family keeps its admission, native lowering, possible row failure and output
representation together. Short-circuit analysis and ASCII or binary-width proofs consult those
same families rather than independently recognizing casts. TRY_CAST still catches conversion
failures only: a fallible argument retains Flink evaluation before the conversion. Decimal,
temporal and collection casts keep their existing exact kernels or generated evaluators.
This organization changes no coverage, configuration or runtime wire code.

`CAST` and `TRY_CAST` from a direct BOOLEAN input or literal to STRING, bounded VARCHAR or CHAR use a
native Arrow formatter. Both also admit recursively verified BOOLEAN-only NOT, AND, OR,
IS NULL, IS NOT NULL, IS TRUE, IS NOT TRUE, IS FALSE and IS NOT FALSE operands; their
inputs must be BOOLEAN references/literals or the same failure-free logical operations.
Both additionally admit integer `=`, `<>`, `<`, `<=`, `>` and `>=` comparisons inside that
logical proof. Each comparison operand must be an integer reference/literal or the verified
wrapping-arithmetic, lossless-widening and integral-rounding tree described below. This removes
the formatting evaluator without dropping comparison inputs. The same verified integer operands
also admit IS NULL/IS NOT NULL, including the optimizer's expansion of nullable comparisons.
For character targets, the same proof additionally admits integer-to-BOOLEAN TRY_CAST
over those verified integer operands and DECIMAL-to-BOOLEAN TRY_CAST over direct inputs
or literals, including surrounding logical and NULL checks. Direct same-type DECIMAL
comparisons and DECIMAL input/literal NULL checks also qualify for character consumers.
This character-consumer extension does not change the separate BOOLEAN-to-integer proof.
The formatting operation is total for these BOOLEAN values; TRY_CAST still preserves
failures while evaluating unverified operands. Computed/mixed-type DECIMAL comparisons, character, floating,
temporal, division, remainder and stateful comparison operands keep their existing routes.
Modern mode emits uppercase `TRUE`/`FALSE`, truncates VARCHAR/CHAR
and pads bounded CHAR with spaces. Legacy mode emits lowercase `true`/`false` and ignores
the target's length and padding. NULL remains NULL in both modes. BOOLEAN formatting cannot
produce a conversion error, so TRY_CAST reuses the CAST kernel. Other computed operands retain
generated Flink evaluation or their existing short-circuit fallback, preserving failures,
laziness and shared function calls.
Wrapping a failing or stateful producer in a logical operator does not admit it through
the proof above.
When the encoder has no cast-mode configuration, TRY_CAST also retains generated evaluation.
The [bitmap word technique](../optimizations/boolean-word-blocks.md) avoids per-row string
objects and copies common spellings eight values at a time. See the
[CAST](../benchmarks/scalar-functions.md#boolean-cast-to-character) and
[TRY_CAST measurements](../benchmarks/scalar-functions.md#boolean-try_cast-to-character),
including small batches and unfavorable full jobs.
Logical-operand CAST and TRY_CAST have separate
[CAST](../benchmarks/scalar-functions.md#boolean-cast-over-logical-operands) and
[TRY_CAST qualification](../benchmarks/scalar-functions.md#boolean-try_cast-over-logical-operands),
separate from direct-input formatting and existing planner simplifications.

Verified BOOLEAN CASE forms can already run without evaluators after released-planner
rewrites. For example, `CAST(CASE WHEN b THEN n<0 WHEN b IS NULL THEN n=1 ELSE NOT b END
AS STRING)` and the corresponding TRY_CAST reduce to existing logical/integer-comparison
trees. The [runtime route record](../benchmarks/results/boolean-case-existing-routes-2026-11-03.json)
covers five CASE shapes, including nested cases and NULL branches, each cast to STRING,
VARCHAR(1/3) and CHAR(4/8), in modern/legacy modes and nullable/NOT NULL inputs. All sixteen
invocations across released Flink 1.18.1/2.2.1 and JDK 11/17 compare complete 5,003-row maps
and resolved schemas, require consumed NativeCalc work and empty JVM evaluators. No planner
or kernel change is needed for these existing rewrites; this supplies no new performance
claim. CASE shapes and producers that do not reduce to the verified tree retain their
existing routes, including failing/stateful and other comparison families tracked in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

Native, unconditionally, with no host involvement:

- **Widening numeric** — integer→wider integer, integer→float/double, float→double.
- **Narrowing integer→integer and float/double→integer** — a purpose-built `NarrowingCast` kernel
  reproduces Flink's primitive Java cast semantics exactly: two's-complement wraparound for an
  integer source, and saturation to INT/BIGINT followed by low-bit narrowing for a float source. Arrow's own
  cast kernel can't do this — it errors on overflow instead of wrapping/saturating.
  Integer inputs map their original typed value buffer in one pass and retain its immutable
  validity buffer, without allocating an intermediate i64 column. Decimal/floating conversion
  paths keep their separate semantics.
- **DECIMAL → TINYINT/SMALLINT/INT/BIGINT** — truncate toward zero, then keep the destination's
  low bits, matching Flink's BigDecimal `longValue()` followed by the Java integer cast.
  Overflow wraps rather than saturating or producing NULL; NULL input remains NULL.
  This also admits casts above DECIMAL aggregates such as `CAST(AVG(d) AS BIGINT)`.
- **DECIMAL → FLOAT/DOUBLE** and **ARRAY<DECIMAL> → ARRAY<FLOAT/DOUBLE>** — retain Flink's
  intermediate double conversion, including compact-decimal division and final FLOAT narrowing.
  This can differ from rounding a decimal directly to FLOAT. Array casts convert the element
  buffers and preserve offsets, empty arrays, nullable containers/elements and declared types.
  Other changes of collection element type still require a separately supported cast.
- **`CAST(d AS STRING)` and `TRY_CAST(d AS STRING)` for DECIMAL** — a native Decimal128
  formatter retains scale and trailing zeros, including zero and precision-38 coefficients.
  It uses ordinary decimal notation, so `DECIMAL(38,38)` with unscaled value 1 becomes
  `0.00000000000000000000000000000000000001`, without passing through floating point.
  NULL stays NULL. Modern and legacy modes share this unbounded STRING conversion.
  TRY_CAST reuses the same non-failing formatting operation; it does not suppress failures
  while computing an operand. Both CAST and TRY_CAST also admit direct DECIMAL inputs/literals to bounded
  VARCHAR/CHAR: modern mode truncates or pads the ASCII decimal text through native LEFT/RPAD;
  legacy mode ignores target length. CHAR with maximum INT length is not padded, matching
  released Flink. Both casts also admit verified alternating DECIMAL ABS and unary-minus trees,
  and same-scale precision widening before or after those operators. Their released-Flink result
  precision/scale stays unchanged, so the same formatter and ASCII width kernels apply. This
  computed-operand proof excludes failures while evaluating the operand; TRY_CAST does not catch
  such failures. Both casts additionally admit DECIMAL-only addition, subtraction and
  multiplication trees, including already verified unary inputs. Every arithmetic node keeps its
  resolved precision/scale, and overflow stays SQL NULL. CAST requires a nullable computed source
  and character result. TRY_CAST's character result is nullable, so it also admits NOT NULL
  arithmetic inputs and retains overflow NULL instead of CAST's empty projection default.
  TRY_CAST additionally admits verified ABS/unary-minus wrappers over these arithmetic trees,
  including alternating unary trees and arithmetic containing those wrappers. Its nullable
  character result normalizes arithmetic NULLs before consumers such as IS NULL observe them;
  this verified boundary does not require the whole projection's default-value evaluator.
  Ordinary CAST keeps its existing arithmetic/unary producer and final-default rules.
  NOT NULL arithmetic CAST retains the generated projection contract described below;
  division/remainder, mixed numeric operands and other producers keep their preceding routes.
  Unknown length configuration and other computed DECIMAL operands retain their existing
  evaluator/admission rules. In particular,
  rounding-result metadata guards still apply;
  this does not replace their host-exact consumers. See the
  [direct DECIMAL formatting measurements](../benchmarks/scalar-functions.md#decimal-to-string)
  and unary [CAST](../benchmarks/scalar-functions.md#decimal-unary-cast-to-character) /
  [TRY_CAST measurements](../benchmarks/scalar-functions.md#decimal-unary-try_cast-to-character),
  and arithmetic [CAST](../benchmarks/scalar-functions.md#nullable-decimal-arithmetic-cast-to-character) /
  [TRY_CAST measurements](../benchmarks/scalar-functions.md#decimal-arithmetic-try_cast-to-character).
- **`CHAR`/`VARCHAR` → `VARCHAR`** when the target length is ≥ the source length — an unpadded
  no-op (e.g. the common `COALESCE(s, 'x')` pattern).
- **Widening timestamp precision** within `TIMESTAMP` or within `TIMESTAMP_LTZ` — Arrow stores both
  at nanosecond precision at the columnar boundary, so widening the Flink declaration is a no-op.
- **`→ DECIMAL` from an exact source** — a `DECIMAL` or integer input, rescaled `HALF_UP`
  before checking the target precision. Overflow produces SQL `NULL`, including a carry caused
  by rounding (`999.995` cast to `DECIMAL(5,2)`), scale increases, and integer inputs. NULLs remain
  visible to surrounding expressions and filters. The result stays an Arrow `Decimal128` column.

Integer narrowing keeps the low bits, matching Java wraparound. FLOAT/DOUBLE to INT or
BIGINT truncates toward zero, saturates at the destination bounds, and maps NaN to zero.
FLOAT/DOUBLE to TINYINT or SMALLINT first performs that INT conversion, then keeps the
low 8 or 16 bits. Thus `128.75` becomes TINYINT `-128`, and positive infinity becomes
TINYINT/SMALLINT `-1`. NULL remains NULL for every target.

### STRING/VARCHAR/CHAR to BOOLEAN {#stringvarcharchar-to-boolean}

`CAST(s AS BOOLEAN)` uses a native Arrow Boolean builder. It accepts `t`, `true`, `y`,
`yes`, `1` as TRUE and `f`, `false`, `n`, `no`, `0` as FALSE, ignoring ASCII case.
It does not trim whitespace; empty strings, other numeric values and Unicode lookalikes
are invalid. NULL remains NULL. Flink's resolved result type/nullability is retained.

With the default cast behavior an invalid token fails the query with Flink's `TableException`
and parse message preserved through JNI, including for a NOT NULL source.
With `table.exec.legacy-cast-behaviour=ENABLED` it produces NULL. For a NOT NULL
source, Flink retains a NOT NULL result declaration even in legacy mode; the existing
sink enforcer therefore rejects or drops malformed rows according to its ERROR/DROP setting.
Both outcomes are tested against Flink. CASE can skip
an unselected failing cast. Default-mode casts nested under AND/OR still fall back so
that Flink's row short-circuiting suppresses errors on unselected rows; legacy-mode
casts can compose under AND/OR because malformed input returns NULL. A bare expression
encoder without table configuration declines this cast instead of guessing the mode.
`TRY_CAST(s AS BOOLEAN)` uses the same native parser with NULL on invalid input,
independently of legacy mode. It composes under NOT, COALESCE, AND/OR and filters.
When its input expression can fail, the existing generated-expression callback preserves
Flink's input evaluation errors: TRY_CAST only catches conversion failures. The reverse
BOOLEAN-to-character casts follow the direct-input and generated paths described above.

### Integer TRY_CAST {#integer-try_cast}

TRY_CAST between TINYINT, SMALLINT, INT and BIGINT reuses the existing native primitive
cast kernels for verified inputs/literals, integer arithmetic, lossless widening and
admitted rounding producers. Widening is exact; narrowing retains the low destination bits,
matching Flink's primitive Java cast. Integer overflow wraps rather than becoming NULL.
Input NULL stays NULL and the resolved TRY_CAST nullability is retained in both modern and
legacy cast modes. Same-type expressions may be removed by the released planner.

Every producer must be failure-free. Division, narrowing producers and other unverified
trees retain their preceding fallback, including failures while evaluating an operand,
untaken CASE branches and empty filtered batches. No conversion error is possible in the
admitted integer subset, so TRY_CAST can reuse CAST without an exception-catching callback.
The [integer TRY_CAST qualification](../benchmarks/scalar-functions.md#integer-try_cast)
retains initial/final kernel measurements, actual previous stock fallback, complete jobs and
shared CAST controls. Other scalar TRY_CAST pairs remain governed by their separate coverage below and
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

### BOOLEAN-to-integer TRY_CAST {#boolean-to-integer-try_cast}

TRY_CAST from verified BOOLEAN inputs/literals, logical trees, NULL checks and integer
comparisons to TINYINT/SMALLINT/INT/BIGINT uses the existing native primitive CAST.
TRUE becomes 1, FALSE becomes 0 and NULL stays NULL, in both cast modes. This follows
Flink's BooleanToNumeric rule and the same DataFusion-compatible primitive path used by
Comet; there is no conversion error to catch. The resolved TRY_CAST type/nullability stays
unchanged. Ordinary BOOLEAN-to-integer CAST already lowers to Flink's native-encodable
CASE tree and needs no new admission.

Division and other unverified/failing/stateful BOOLEAN producers retain fallback, including
their operand failures, lazy branches and empty filtered batches. Other target families
and reverse TRY_CAST conversions are covered separately below.
The [release qualification](../benchmarks/scalar-functions.md#boolean-to-integer-try_cast)
retains actual preceding fallback, independent stock, tiny controls and complete jobs.

### Integer-to-BOOLEAN TRY_CAST {#integer-to-boolean-try_cast}

TRY_CAST from verified TINYINT/SMALLINT/INT/BIGINT input, arithmetic, widening and rounding
trees to BOOLEAN uses a native comparison against a zero of the input's resolved width.
Every nonzero integer, including signed minima, becomes TRUE; zero becomes FALSE and NULL
stays NULL in both cast modes. This follows Flink's NumericToBoolean rule; ordinary CAST
already lowers to this comparison through released planning. Conversion cannot fail.

Division and other unverified/failing/stateful producers keep the preceding fallback,
including operand failures, lazy branches and all-filtered batches. Verified character
consumers of the new BOOLEAN result are covered below; other source families retain
their preceding coverage.
The [release qualification](../benchmarks/scalar-functions.md#integer-to-boolean-try_cast)
retains actual preceding fallback, independent stock, tiny controls and complete jobs.

### Character consumers of integer truth {#character-consumers-of-integer-truth}

CAST and TRY_CAST of the verified integer-to-BOOLEAN TRY_CAST result to
STRING/VARCHAR(n)/CHAR(n) reuse the existing native BOOLEAN formatter. The shared
producer proof also permits surrounding logical/NULL checks inside these character casts;
it does not extend the separately qualified integer-target cast proof or admit a failing
or otherwise unverified integer producer. Modern/legacy
spelling, VARCHAR truncation, CHAR padding and resolved output/nullability are unchanged.
Ordinary integer-to-BOOLEAN CAST already has a released comparison rewrite.
The [release qualification](../benchmarks/scalar-functions.md#character-consumers-of-integer-truth)
retains actual preceding evaluation, independent stock, tiny controls and complete jobs.

### DECIMAL-to-BOOLEAN TRY_CAST {#decimal-to-boolean-try_cast}

TRY_CAST from a direct DECIMAL input or literal to BOOLEAN compares with a DECIMAL zero
of the same precision and scale in Rust. Every nonzero value is TRUE, zero is FALSE and
NULL stays NULL in modern and legacy modes. This follows Flink's NumericToBoolean rule
and Comet's same-type zero-comparison structure; no floating conversion or rescaling is
needed. Logical/NULL checks, filters and nullable CASE consumers retain their native
comparison path. Verified character consumers are covered below; other BOOLEAN producer
families keep their separate formatting proof.

Computed DECIMAL producers retain their preceding routes, including arithmetic overflow,
selected failures, lazy branches and all-filtered batches. Ordinary CAST already reduces
to a native comparison when the source precision exceeds its scale. Released Flink rejects
ordinary DECIMAL(p,p)-to-BOOLEAN CAST while constructing its zero literal; that planning
failure remains unchanged. TRY_CAST for that same source type is independently supported.
The [release qualification](../benchmarks/scalar-functions.md#decimal-to-boolean-try_cast)
retains actual preceding fallback, independent stock, tiny batches and complete jobs.

### Character consumers of decimal truth {#character-consumers-of-decimal-truth}

CAST and TRY_CAST of a verified direct DECIMAL-to-BOOLEAN TRY_CAST result to
STRING/VARCHAR(n)/CHAR(n) reuse the existing native BOOLEAN formatter. Surrounding
verified logical/NULL checks are admitted inside these character casts. Modern/legacy
spelling, VARCHAR truncation, CHAR padding, NULLs and resolved output types are unchanged.
The same-type decimal zero comparison retains the source precision/scale.

This extends only the character-consumer proof. Nested integer-target TRY_CAST,
unverified DECIMAL arithmetic and failing/stateful producers retain their preceding routes.
Direct DECIMAL comparison consumers have their separate proof below; arbitrary comparison
operands do not qualify.
Released ordinary DECIMAL(p,p)-to-BOOLEAN CAST still fails during planning.
The [release qualification](../benchmarks/scalar-functions.md#character-consumers-of-decimal-truth)
retains actual preceding evaluation, independent stock, tiny controls and complete jobs.

### Character consumers of direct decimal predicates {#character-consumers-of-direct-decimal-predicates}

CAST and TRY_CAST to STRING/VARCHAR(n)/CHAR(n) keep comparisons of direct DECIMAL
inputs/literals in the native BOOLEAN formatter when both operands have identical
precision and scale. Equality, inequality and all four ordered comparisons qualify,
along with direct DECIMAL IS NULL/IS NOT NULL and verified logical/NULL wrappers.
The NULL checks also cover released-planner rewrites of nullable comparison predicates.
No decimal-to-floating conversion, rescaling or generated evaluator is needed for these
character consumers. Modern/legacy spelling, truncation, padding and NULLs are preserved.

The released ordinary DECIMAL-to-BOOLEAN CAST rewrite consequently qualifies inside
these character casts when precision exceeds scale. Ordinary DECIMAL(p,p) CAST retains
its released planning failure. Computed, failing, stateful and mixed-type decimal
operands retain their preceding routes. Integer-target TRY_CAST and bounded-position
condition proofs stay separate; this character extension does not expand them.

The [release qualification](../benchmarks/scalar-functions.md#character-consumers-of-direct-decimal-predicates)
retains actual preceding evaluator controls, independent stock, tiny batches and full jobs.

### Integer/string casts {#integerstring-casts}

`CAST` and `TRY_CAST` between `STRING`/`VARCHAR`/`CHAR` and
`TINYINT`/`SMALLINT`/`INT`/`BIGINT` use native Arrow kernels without a JVM callback.
Parsing removes only leading/trailing ASCII spaces, accepts an optional sign and ASCII
digits, and truncates fractional decimal text toward zero after validating every digit.
Like Flink, `.`, `+.`, and `-.9` yield zero. Tabs, newlines, Unicode whitespace/digits,
exponents, internal spaces and additional decimal points are invalid. Values outside the
target integer range fail; this string conversion does not wrap like an integer narrowing cast.

NULL input remains NULL. Ordinary CAST fails on invalid input in default mode; TRY_CAST
and legacy-mode CAST return NULL. CASE suppresses an unselected failing cast. Default CAST
inside AND/OR stays on Flink to preserve row short-circuiting; TRY_CAST and legacy CAST
can compose natively. NOT NULL sink enforcement remains Flink's ERROR/DROP policy.
Native integer-parse failures preserve Flink's `NumberFormatException` and parse message
through JNI. Successful results and NULL-on-error policies match as well.

Integer formatting writes canonical decimal text from a reusable stack buffer directly into
Arrow offsets/values, sharing sliced input validity. It preserves signed minima and zero.
`VARCHAR(n)` truncates to `n` characters; `CHAR(n)` also pads shorter results with spaces.
Legacy mode leaves the formatted text unchanged regardless of the declared length, matching
Flink. Other TRY_CAST pairs, except the integer, BOOLEAN and temporal forms described here and
the binary, DECIMAL and collection forms below, still fall back. Bare encoders without table configuration
decline mode-dependent casts. See the [kernel ledger](../optimizations/scalar-function-kernels.md)
for the release benchmark against the previous host-cast path.

Boolean TRY_CAST regression coverage also verifies native routing in ordinary projections,
filter predicates and CASE consumers, including strings that parse to NULL.

### Temporal TRY_CAST {#temporal-try_cast}

Standalone DATE, TIME and TIMESTAMP/TIMESTAMP_LTZ TRY_CAST use verified parsing fast paths
inside the existing JVM batch callback. DATE accepts four-digit years and one- or two-digit
month/day fields. TIMESTAMP accepts `yyyy-MM-dd HH:mm:ss` with an optional 1–9 digit fraction,
including Flink's SMART normalization of days 29–31 and zero-fraction `24:00:00`. The result
retains the declared precision and the same Java local-time and zone conversion as Flink.
Other formats and year zero use the released Flink parser. Modern-mode composed character
children execute their generated code exactly once, outside the conversion failure handler.
Legacy mode retains generated DATE conversion and only the direct-input timestamp fast path.
TIME recognizes `HH:mm:ss` with optional 1–3 fractional digits and applies the released
version's logical-type precision rule: Flink 2.2 truncates while 1.18 preserves parsed milliseconds.
Complete-row evaluators retain Flink's generated conversion. See
[temporal parsing measurements](../optimizations/host-exact-builtins-upcall.md#canonical-timestamp-try_cast).

`TRY_CAST` from STRING/VARCHAR/CHAR to DATE, TIME, TIMESTAMP and TIMESTAMP_LTZ
uses the existing columnar callback, with the exact temporal subsets above and Flink's
generated conversion for other cases. Malformed text and rejected conversions return NULL;
errors in the input expression still propagate. Flink 1.18.1 accepts `24:00:00` as an internal TIME but throws
`DateTimeException` during external collection; native execution preserves that failure.
Flink 2.2.1 returns NULL for the same TRY_CAST. CASE and filters suppress unselected failing expressions. Default and legacy
cast modes use their configured Flink rules. TIMESTAMP_LTZ interprets local text in the
configured table time zone, including DST gaps and overlaps.

Runtime-source parity covers DATE, TIME(0/3), TIMESTAMP(0/3/6/9) and
TIMESTAMP_LTZ(0/3/6/9), NULL and NOT NULL character inputs, invalid dates, year/range
boundaries, pre-epoch nanoseconds, UTC, Asia/Shanghai and America/Los_Angeles.
Multi-batch tests verify NativeCalc input/output counters as well as values.
Binary TRY_CAST is described separately below.

The initial, pre-optimization release+mimalloc benchmark on Flink 2.2.1/JDK 17, Linux x86_64
(Core i7-12650H), used 2,000,000 row-fed records, NULL every seventh row, two warmups and five alternating
trials. NativeCalc and both row/Arrow transposes are required by the harness. Median
elapsed seconds (ratio = Flink/native):

| Query | Flink | Native island | Ratio |
|---|---:|---:|---:|
| Boolean-text identity | 0.413 | 0.687 | 0.600x |
| Timestamp-text identity | 0.404 | 0.652 | 0.620x |
| TRY_CAST to BOOLEAN | 0.411 | 0.560 | 0.733x |
| TRY_CAST to TIMESTAMP(9) | 3.073 | 3.982 | 0.772x |
| TRY_CAST to TIMESTAMP_LTZ(9) | 3.123 | 4.003 | 0.780x |

These initial standalone results were slower than stock Flink. Cached timestamp writers and
the canonical parser now improve the timestamp cases; see the [current measurements](../optimizations/host-exact-builtins-upcall.md#canonical-timestamp-try_cast).
Boolean TRY_CAST remains slower in these complete-job measurements; its native route preserves
released Flink evaluation and these results do not establish standalone acceleration.
The parser fast path does not establish a speedup for noncanonical formats or composed expressions.

### Binary casts and fixed-width generated expressions {#binary-casts-and-fixed-width-generated-expressions}

`CAST(binary_value AS STRING)` and `TRY_CAST(binary_value AS STRING)` use a native
UTF-8 decoder on both released Flink lines. BINARY and VARBINARY inputs retain their
actual bytes, including fixed padding, embedded zeros and sliced validity. Well-formed
UTF-8 shares Arrow values/offsets/NULLs; fixed-size inputs construct offsets without copying
their bytes. Malformed input uses the running JDK's released UTF-8 replacement contract:
`ED A0 80`, for example, becomes one U+FFFD rather than three. This conversion is distinct
from raw `FROM_BASE64` STRING transport and never exposes malformed Arrow Utf8.

The unbounded conversion has the same decoding behavior in modern and legacy cast modes.
Legacy bounded CHAR/VARCHAR casts also use this kernel because the released rule ignores
their length. Modern bounded targets apply Flink's rule to the decoded text in the same
kernel: VARCHAR(n) keeps at most n UTF-16 units and CHAR(n) also pads with spaces. This is
not truncation by Unicode code points; a cut through a surrogate pair writes `?`, the UTF-8
that Flink produces for the isolated high surrogate. Rows whose UTF-8 length already fits
VARCHAR(n), and ASCII rows of exactly n bytes for CHAR(n), share the decoded Arrow buffers.
Inside a Calc, consumers of a modern bounded result, such as LIKE, comparisons, CASE or
another cast, would observe the original Java String; they keep the generated Flink
expression around the cast, which recomputes the conversion itself.
TRY_CAST catches conversion failures, not failures while computing its operand; binary UTF-8
decoding itself replaces malformed input instead of throwing, so bounded TRY_CAST uses the
same native conversion without catching operand errors. CASE remains lazy and an
empty filtered batch never evaluates the cast.

Admitted value-only consumers such as length, HEX, equality, LIKE and re-encoding compose
with the native conversion. Existing string-ordering and representation-sensitive trim
rules remain in force. A decoded STRING feeding another relational operator retains
fallback so its original Java-backed representation cannot be lost before a comparator
observes it; a CAST delayed until the final projection by the released optimizer does not
cross that boundary. The remaining relational-boundary scope is tracked in
[#493](https://github.com/datafusion-contrib/StreamFusion/issues/493). Bounded-target
measurements, including slower 16-row batches, are in the
[scalar benchmark record](../benchmarks/scalar-functions.md#modern-bounded-charvarchar-targets).

CAST and TRY_CAST from STRING/VARCHAR/CHAR or BINARY/VARBINARY to BINARY/VARBINARY
use a native byte-copy kernel when their operands do not require Flink's rowwise error handling.
Default-mode BINARY(n) zero-pads or truncates
raw bytes to exactly n bytes. VARBINARY(n) only truncates on Flink 2.2. Flink 1.18's
binary-to-binary rule also pads a variable target when the declared input width is
larger than that target. Native planning composes the existing padding kernel with
its variable-binary output conversion to preserve that released behavior and the
resolved VARBINARY type. Character-to-VARBINARY casts keep their truncation rule.
Character inputs use their UTF-8 bytes, including embedded zeros and multibyte
sequences cut at a byte boundary.
NULLs and declared result types are preserved. Failures in the operand expression still
propagate; CASE and filters suppress unselected failing expressions.

Generated expressions can read fixed-size Arrow binary vectors through the existing
batch callback. Native casts produce their declared fixed-size Arrow columns directly. Binary ELT
uses the default-enabled route described under [raw payload boundaries](calc-filter-temporal.md#binary-elt-and-raw-payload-boundaries),
with the older complete-job measurements retained as diagnostics. Other fixed BINARY result
projections use the generated row's declared schema, so default-mode results can feed another
native operator, including grouping. Nested ARRAY/ROW callback arguments and results use the
[verified nested scalar transport](calc-filter-jvm-collections.md#nested-java-scalar-results); custom Java conversion classes
retain their signature gates.

Legacy casts leave the input byte length unchanged even when the SQL result declares
BINARY(n). Their final projections use the existing variable-binary representation, as
Flink 1.18 ENCODE does. Fused scalar consumers and row sinks preserve all bytes; a legacy
fixed-width result crossing another operator boundary or entering a native columnar sink
falls back before execution. The reported reason starts with `legacy binary variable bytes`.

Runtime-source tests cover widths 1/2/4/16, NULLs, zero padding, truncation, non-text bytes,
typed literals, ELT selection, short-circuiting, and 5,003-row inputs. Bridge tests cover
sliced fixed/variable vectors, output ownership after input closure, and allocator balance.
Binary ELT SQL tests require native execution by default and separately verify the
explicit false override, including nested compositions and empty selections.
Separate binary STARTSWITH/ENDSWITH tests still require native execution.

[Low-cardinality grouped-count measurements](../optimizations/scalar-function-kernels.md#binary-keys-composed-with-grouped-counts)
show 19–35% less native time at 2M rows and 22–26% less at 5M for binary-cast and
ELT keys, retaining both transposes. These historical composed measurements are separate
from the Rust-side functionality gate used for current binary ELT default admission.
The admitted binary-cast composed workloads are faster than Flink;
the [synchronous entry optimization](../optimizations/projection-pruning-transpose.md#borrowing-at-the-synchronous-consumer)
also makes the measured non-null standalone STRING-to-BINARY cast faster than both baselines.
Standalone ELT and the sustained BINARY identity control remain slower. This does not establish
a speedup for arbitrary binary projections or high-cardinality grouping.

The original callback-only release+mimalloc measurements on Flink 2.2.1/JDK 17,
Linux x86_64 (Core i7-12650H), use 2,000,000 row-fed records, NULL every seventh row, two warmups and five alternating
trials. The harness requires NativeCalc and both row/Arrow transposes. Median seconds:

| Query | Flink | Native island | Flink/native |
|---|---:|---:|---:|
| String identity (264-byte payload) | 0.825 | 1.194 | 0.691x |
| BINARY(16) identity | 0.323 | 0.536 | 0.603x |
| TRY_CAST string to BINARY(16) | 0.840 | 1.852 | 0.454x |
| Dynamic ELT over BINARY(16) | 0.335 | 0.682 | 0.491x |

Those callback-only standalone cases are slower than stock Flink. The native kernels remove
the callback for the admitted casts; their current measurements and remaining
performance gap are recorded in the [kernel ledger](../optimizations/scalar-function-kernels.md#binary-casts-and-selection).

```bash
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=ScalarFunctionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=TRY_STRING_TO_BOOLEAN,TRY_STRING_TO_TIMESTAMP,TRY_STRING_TO_TIMESTAMP_LTZ \
  -Dscalar.functions=TRY_STRING_TO_FIXED_BINARY,ELT_FIXED_BINARY \
  -Dscalar.rows=2000000 -Dscalar.nullEvery=7 -Dscalar.warmup=2 -Dscalar.runs=5
```

### DECIMAL from proven integer text {#decimal-from-proven-integer-text}

In ordinary Calc, `CAST(CAST(n AS STRING) AS DECIMAL(p,s))` and the corresponding
TRY_CAST combinations use the exact numeric decimal kernel when the inner CAST/TRY_CAST
formats a verified total signed integer producer and `p-s` is at least 3, 5, 10 or 19
for its resolved TINYINT, SMALLINT, INT or BIGINT width. For example, BIGINT text fits
`DECIMAL(22,3)`. Integer arithmetic, lossless widening and admitted integer rounding
producers retain their resolved widths before conversion. NULLs, declared scale,
nullable result schemas, modern/legacy mode, conditional consumers and empty batches
remain exact; the intermediate text and released parser are not evaluated.

This proof requires unbounded STRING. Ordinary source strings, bounded CHAR/VARCHAR,
insufficient integral capacity, fallible/stateful or unverified producers and UNNEST
keep their preceding routes. The released decimal parser has precision- and byte-length-
dependent behavior, so this does not admit arbitrary text parsing or DECIMAL text
roundtrips. Some ordinary CAST forms already simplify to native numeric casts in Flink;
they are existing controls, not newly accelerated signatures. Unreduced TRY_CAST forms
require the explicit producer/capacity proof.
The [separate qualification](../benchmarks/scalar-functions.md#decimal-from-proven-integer-text)
retains actual preceding routes, tiny/full-job losses and exact source/runtime provenance.

### DECIMAL TRY_CAST {#decimal-try_cast}

Outside the proven integer-text subset, `TRY_CAST` from STRING/VARCHAR/CHAR to DECIMAL
uses Flink's generated conversion through the
existing columnar callback. Malformed text and conversion failures become NULL; failures in
operand expressions still propagate. Default and legacy modes follow the configured host rules.
DECIMAL-to-DECIMAL TRY_CAST reuses the native exact rescaling kernel, including HALF_UP and
NULL on narrowing overflow. Tests cover precision 38, invalid text, signs/exponents, whitespace,
filters, conditional consumers, grouping and aggregation across multiple batches.

Direct DECIMAL input references and literals also format to STRING/VARCHAR/CHAR through the
native formatter described above. It preserves scale, plain notation and NULLs in modern
and legacy modes without a JVM evaluator. Modern bounded targets truncate/pad; legacy ignores length.
Verified ABS/unary-minus trees and same-scale precision widening also stay native. The DECIMAL-only
addition/subtraction/multiplication proof also formats through TRY_CAST with nullable or NOT NULL
sources, including verified ABS/unary-minus wrappers over those arithmetic trees. Its character
result remains nullable and arithmetic overflow remains SQL NULL. Other
computed DECIMAL operands keep their previous rules; in particular, TRY_CAST does not catch an
error raised while computing its input. See the direct [TRY_CAST comparison](../benchmarks/scalar-functions.md#decimal-to-string-try_cast)
and [arithmetic qualification](../benchmarks/scalar-functions.md#decimal-arithmetic-try_cast-to-character).

Already-evaluated internal Flink decimals cross Arrow without another precision check. Flink's
compact text parser can retain a rounding carry (`999.995` to DECIMAL(5,2) yields `1000.00`);
the bridge preserves that internal unscaled value and its non-NULL status. External BigDecimal
UDF returns still undergo the declared precision/scale conversion. This follows Comet's generated
compact-decimal output pattern; the host-specific contract is recorded in
[exact decimal results](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/38-exact-decimal-result-types.md).

### The host-exact JVM upcall {#the-host-exact-jvm-upcall}

A second group of casts is **native by default, and this is not a fallback** — it's a real JNI call
back into Flink's own cast machinery (`CastExecutor`/`CastRuleProvider`) for the one column being
cast, with the rest of the expression tree still evaluated natively around it:

- **FLOAT/DOUBLE ↔ string**, **string → DECIMAL**, and **computed DECIMAL → character**.
  Direct DECIMAL inputs/literals use the native formatter for CAST and TRY_CAST to STRING/VARCHAR/CHAR.
  Both casts also keep verified ABS/unary-minus trees and same-scale precision widening native;
  CAST also keeps the nullable DECIMAL-only addition/subtraction/multiplication proof above native;
  TRY_CAST admits the same arithmetic proof with nullable or NOT NULL inputs.
  TRY_CAST also admits verified ABS/unary-minus wrappers over that arithmetic and resolves
  overflow NULLs before basic consumers; ordinary CAST keeps its separate default/producer rules.
  Other computed DECIMAL producers retain the host route or their existing fallback.
  For CAST of an unverified NOT NULL computed DECIMAL, the complete enclosing projection or
  condition stays in one generated evaluator. This retains Flink's separate value and NULL flag
  until the original consumer observes them. For example, overflowing `DECIMAL(38,18)` multiplication
  followed by a final `CAST(d*d AS STRING)` writes an empty string, while the same cast in a
  WHERE comparison still carries a NULL flag. A nullable source or TRY_CAST returns SQL NULL.
  Modern bounded CHAR/VARCHAR casts also retain the empty default on overflow, without padding
  it. A nested CONCAT can instead produce an actual NULL and still reaches the sink's ERROR/DROP
  constraint enforcer. These are generated Flink behaviors, not a global NULL-to-empty rewrite.
  This correctness path uses the existing columnar JVM bridge; unrelated projections and verified
  unary expressions keep their existing execution routes. It does not add JVM-free arithmetic formatting coverage
  or make a performance claim. See [the overflow reproducer](https://github.com/datafusion-contrib/StreamFusion/issues/577).
- **Narrowing a `VARCHAR`** (truncation).
- **String to `CHAR(n)`** (space-padding).
- **`→ DECIMAL` from a `float`/`double`.**

These four are deliberately routed through the host rather than reimplemented, because the host's
output isn't just "a reasonable float-to-string conversion" — it's a specific, JDK-version-dependent
rendering (trailing-zero handling, the scientific-notation threshold, trim semantics), and an
unparsable string input must fail the job exactly the way the host's default cast does. Running the
upcall makes the result byte-identical to Flink by construction instead of by reimplementation, at
the cost of one JNI round-trip per cast column rather than per row family. (The Kafka text sinks
already carry a probed native port of the legacy `Double.toString` spelling; moving the
float-to-string `CAST` onto that port instead of the upcall is a separate, tracked follow-up.)

NOT NULL BOOLEAN, integer, approximate numeric and character projections containing
NOT NULL DECIMAL arithmetic, potentially overflowing exact-numeric DECIMAL casts or
noncompact `DECIMAL(p,p)` SIGN also keep their complete generated expression and original
projection write rule. These operations can set a runtime NULL flag even when the resolved
input is NOT NULL. Released Flink can then
write a separate result term, such as
`false` for a comparison, `-1` for a DECIMAL-to-INT/BIGINT cast, or an empty string for
a comparison cast to STRING. A split Arrow evaluation instead exposes NULL and incorrectly
fails or drops the row at the sink constraint enforcer. This includes enclosing
comparison/NOT/logical/CASE and cast consumers, under modern and legacy cast modes.
Nullable projections and WHERE conditions keep their NULL flags, and selected
division-by-zero still fails. Direct overflowing DECIMAL outputs still reach ERROR/DROP
enforcement; defaults are not substituted globally. Only affected projections use this
correctness path. No option or acceleration claim is introduced. See
[the arithmetic reproducer](https://github.com/datafusion-contrib/StreamFusion/issues/581),
[the explicit CAST reproducer](https://github.com/datafusion-contrib/StreamFusion/issues/582)
and [the SIGN reproducer](https://github.com/datafusion-contrib/StreamFusion/issues/584).
The CAST proof covers integer-to-DECIMAL conversions whose target cannot hold the source
type's full integral range, and DECIMAL conversions that reduce integral capacity or can
round into an extra integral digit without room for that carry. For example, `99.5` from
`DECIMAL(3,1)` overflows `DECIMAL(2,0)` after rounding. Conversions with sufficient integral
capacity, including an extra digit for rounding when needed, retain their existing native
execution. Retained IS NULL/IS NOT NULL and truth-test consumers resolve the NULL flag
themselves, so they do not require the CAST/SIGN protection. A truth test rewritten by
Flink into an ordinary comparison still needs the comparison's projection contract.
FLOAT/DOUBLE-to-DECIMAL casts also retain this protection because their values can exceed
the target precision. Direct TRY_CAST retains its nullable result and preceding route.

Host cast executors compile under Flink's cast-rule library loader, restoring the calling
thread's context loader afterward. Flink's shared Janino expression cache does not include a
loader key; setting only the cast context's loader still lets Janino retain a per-job loader.
The scoped compilation loader prevents later jobs' exceptional casts from accessing a
retired safety-wrapper loader. A two-job regression closes the first loader before exercising
the cached expression's error path in the second job.

With `table.exec.legacy-cast-behaviour=ENABLED`, the verified basic pairs remain native:
STRING/CHAR/VARCHAR to DECIMAL; BOOLEAN or DECIMAL to CHAR/VARCHAR; and character to
character. Other STRING-to-DECIMAL inputs use Flink's complete generated expression, including
its catch-on-conversion-failure contract: malformed strings and overflow become NULL.
Setting only the cast executor's legacy flag would still throw for malformed strings.
BOOLEAN uses the native formatter's legacy lowercase spelling within its verified operand scope.
Computed DECIMAL rendering uses the configured host executor, while direct DECIMAL inputs/literals
use Rust for CAST and TRY_CAST to STRING/VARCHAR/CHAR.
Legacy character casts preserve the original text without applying default
projection trimming or padding, including empty, overlength and surrogate-containing
strings. Sink length and NOT NULL enforcement remain separate downstream contracts.
Legacy FLOAT/DOUBLE to DECIMAL is admitted by default. A direct unary cast first tries a native
batch kernel: adjacent binary floating values, multiplied by an exact power of ten with outward
rounding, bound every round-tripping decimal representation the running JDK could choose. Only
when both bounds round to the same HALF_UP integer does the kernel use that result, with the
declared precision check. Scale above 18, scaled magnitude at least 2^52, ambiguous rounding,
NaN and infinity retain the complete released Flink expression for the entire batch. Numeric
arguments then borrow Arrow rows and decimal results retain internal DecimalData. The host widens
FLOAT to DOUBLE before decimal conversion, rounds with the target scale, returns NULL on precision
overflow, and raises NumberFormatException for NaN/infinity in both modes. Selection and empty
filtered batches suppress conversions for rows that do not reach the projection. The conversion
uses the JVM inside native Calc whenever the batch cannot use the proven native kernel.
CHAR to DOUBLE is also admitted by default in legacy mode, including NULL on malformed
text. Whitespace, padding, exponents, signed zero, NaN/infinity and over/underflow retain the running
host's parser behavior. The complete mixed DECIMAL ROUND projection remains native, including its
CHAR(8) sort-key conversion. Mixed projections keep each expression's selected implementation; a runtime DECIMAL-only control
verifies ROUND independently.
VARCHAR-to-floating and CHAR-to-FLOAT legacy casts remain unverified
and fall back. An unknown cast setting also falls back.

Direct legacy CHAR-to-DOUBLE casts parse ordinary ASCII decimal text in native batches using
the same correctly-rounded binary64 conversion as Java. Hexadecimal text, numeric suffixes,
special values, Unicode and malformed input retain the complete released Flink expression for
the batch, including legacy NULL-on-failure behavior. Composed operands are never mistaken for
direct reads. Multiple fallible expressions keep their normal columnar schedule. These paths
require no additional setting and preserve
`table.exec.legacy-cast-behaviour`. See the release measurements
for [floating DECIMAL](../benchmarks/basic-type-coverage.md#legacy-floating-decimal-casts-2026-10-16)
and [CHAR to DOUBLE](../benchmarks/basic-type-coverage.md#legacy-char-to-double-casts-2026-10-16).

Generated cast executors initialize with a valid value of the declared input type, including
`CHAR`/`VARCHAR NOT NULL`. Startup does not evaluate an illegal NULL input; actual malformed
runtime values still fail according to Flink's cast rules.

### Still falling back {#still-falling-back}

Unverified legacy host-cast pairs and other pairs not listed above. Temporal casts use Flink-generated
expressions; see [temporal functions](temporal-functions.md).

`FlinkLegacyBasicCastSqlHarnessTest` compares both cast settings against independently
executed stock Flink. It covers runtime NULLs, malformed and overflowing strings,
rounding, CASE/filter consumers, character lengths/Unicode, NOT NULL with DROP, and
two successive recoveries with HashMap and native RocksDB. The complete-job release
measurements and their limits are recorded in
[basic type coverage measurements](../benchmarks/basic-type-coverage.md).


## Decimal arithmetic {#decimal-arithmetic}

Mixed DECIMAL/integer arithmetic retains the selected Flink release's implicit conversions
inside the native kernels. Flink 1.18 first converts each non-DECIMAL operand to the result's
precision and scale; an overflowing implicit conversion raises its `NullPointerException`.
For example, `DECIMAL(38,37) / BIGINT` fails when the divisor is 10, although the quotient
could fit. This also applies to AVG's final sum/count projection. Flink 2.2 converts integers
at their own precision and scale and permits that division. Both paths preserve original
operand NULL guards, unselected branches and empty batches. Ordinary result overflow still
returns NULL. No JVM callback or configuration switch is required for these conversions.

When both operands are DECIMAL arrays, addition/subtraction/multiplication use typed
Arrow iterators with an exact output capacity. This bypasses per-row input-type dispatch
and inapplicable legacy integer-cast checks, while keeping each node's resolved
precision/scale, rounding, overflow NULL and sliced validity. Scalar broadcasting and
mixed integer operands retain the checked path. See the
[typed-array implementation and measurements](../optimizations/scalar-function-kernels.md#typed-decimal-arithmetic).

### Integer FLOOR / CEIL / CEILING {#integer-floor-ceil-ceiling}

Single-argument FLOOR, CEIL and CEILING over TINYINT, SMALLINT, INT and BIGINT preserve the input
value, resolved width and NULL. Verified direct inputs, typed literals, nested admitted
integer rounding and recursive lossless integer widening reuse the operand's
native expression without a JVM evaluator or
floating-point conversion. This preserves signed minima and BIGINT values above 2^53.
FLOOR and CEIL/CEILING also admit recursively verified integer addition, subtraction and
multiplication; each node retains its resolved width and Java wrapping overflow.
Division, remainder, narrowing and unverified producers retain the enclosing
generated rounding program, including its selected failures, lazy branches and empty-batch behavior.
DECIMAL and temporal rounding keep their separate contracts below.

Runtime regressions require consumed native work over 5,003 rows per integer width and
`jvm=[]` for admitted queries on both supported Flink lines and JDK 11/17. Prepared 1,024-row
release controls beat stock and the preceding callback; 16-row and complete row-source
jobs retain the measured limits in [Integer FLOOR](../benchmarks/scalar-functions.md#integer-floor)
and [Integer CEIL / CEILING](../benchmarks/scalar-functions.md#integer-ceil-ceiling).
The [FLOOR arithmetic controls](../benchmarks/scalar-functions.md#integer-floor-over-arithmetic)
and [CEIL arithmetic controls](../benchmarks/scalar-functions.md#integer-ceil-over-arithmetic)
separately qualify the composed integer producers and retain complete-job costs.

### Single-argument integer ROUND {#single-argument-integer-round}

`ROUND(value)` over TINYINT, SMALLINT, INT and BIGINT also returns the integer unchanged.
Verified direct inputs, typed literals, recursive lossless widening and nested admitted
integer rounding reuse the native operand expression without a JVM evaluator, a floating
conversion or a new values buffer. Signed minima, exact BIGINTs, NULLs and resolved widths
are preserved. Recursively verified integer addition, subtraction and multiplication also
run natively with their resolved intermediate widths and Java wrapping overflow.
Division, remainder, narrowing and other unverified producers retain their complete
generated rounding program. Explicit positions use the bounded native path below;
unverified positions retain generated evaluation, including extreme-position failures.
DECIMAL and floating ROUND are unchanged.
[Release measurements](../benchmarks/scalar-functions.md#single-argument-integer-round)
retain the slower small-batch and complete-job controls as well as the bulk-batch gains.
The [arithmetic producer measurements](../benchmarks/scalar-functions.md#integer-round-over-arithmetic)
separately cover computed integer operands, including their explicit-position forms.

### Integer ROUND with a literal position {#integer-round-with-a-literal-position}

Integer `ROUND(value, literal_position)` runs in Rust for TINYINT, SMALLINT, INT and BIGINT
with INT positions from -38 through 38, or a typed NULL position. Direct inputs/literals,
recursive lossless widening, nested admitted integer rounding and recursively verified
integer addition/subtraction/multiplication retain their declared width.
Ties round away from zero; an overflowing rounded value wraps to the resolved Java width,
including BIGINT values beyond 2^53 and rounding to 10^19. Nonnegative positions share the
input array, a NULL position folds to typed NULLs only after the operand proof excludes
failures, and negative positions allocate values
while sharing sliced validity. The input remains unchanged.

The kernel follows Comet's native-width/widened integer split, with Flink's HALF_UP and
wrapping contract. It does not use floating point. Unverified runtime positions, literals outside -38..38,
division, remainder, narrowing and other unverified producers retain generated evaluation. In particular,
released Flink throws for nonzero `ROUND(value, 2147483647)` while zero returns zero;
the ordinary nonnegative-position identity must not erase that behavior. CASE and empty
filtered batches retain their existing evaluation guards. This changes computation inside
an already-admitted Calc and adds no configuration switch.
[Release measurements](../benchmarks/scalar-functions.md#integer-round-with-a-literal-position)
include both Flink lines, the actual previous scalar callback, slower small-batch controls
and a complete job retaining both transposes.
The [computed-operand controls](../benchmarks/scalar-functions.md#integer-round-over-arithmetic)
cover wrapping arithmetic, literal positions, NULL folding and complete jobs with position -1.

### Integer ROUND with a bounded runtime position {#integer-round-with-a-bounded-runtime-position}

`ROUND(value, CASE WHEN condition THEN position ELSE position END)` also runs in Rust
when every result is an INT literal in -38..38 or NULL, including nested CASE results.
Conditions must consist of direct/literal BOOLEAN values, logical/null predicates or
comparisons of recursively verified integer operands. The value uses the same verified
integer producer tree as literal-position ROUND. Each row retains its original integer
width, HALF_UP ties, wrapping overflow and selected NULL position.

The planner proves every condition and result failure-free before emitting a typed position
selector. It picks the first TRUE condition in one Arrow pass; NULL conditions behave as
false. ROUND consumes that INT column without the generic CASE filter/scatter machinery
or a JVM evaluator. Empty filtered batches remain empty. No configuration switch or JNI
contract changes are needed. Arbitrary position columns, out-of-range results,
division/remainder, narrowing, stateful or other unverified conditions/producers retain the
complete generated expression. TRUNCATE has the separate verified subset below.
The [runtime-position measurements](../benchmarks/scalar-functions.md#integer-round-with-a-bounded-runtime-position)
retain the initial slower prototype, batch qualification and conversion-inclusive jobs.

Integer ROUND also admits total runtime INT positions bounded by the built-in GREATEST
and LEAST, for example `ROUND(value, GREATEST(-38, LEAST(position, 38)))`.
The planner computes conservative intervals through both extrema and CASE, and requires
every non-NULL result to stay in -38..38. NULL-only branches do not enlarge an interval;
NULL arguments still propagate through extrema. Every argument must be verified total,
even when another argument guarantees NULL. Wrapping integer arithmetic is conservatively
treated as the full INT range before clamping, rather than assuming mathematical bounds.
CASE branches may contain these clamped arrays and use the existing typed selector.
Same-name catalog functions, unbounded/one-sided ranges and failing or unverified producers
keep their preceding routes. Epoch precision keeps its separate 0/3 proof.
This extension changes only planning and reuses existing Arrow extrema, selection and
rounding kernels. Its [separate qualification](../benchmarks/scalar-functions.md#integer-round-with-clamped-runtime-positions)
compares stock with the actual preceding encoder on the identical release runtime.

### Single-argument integer TRUNCATE {#single-argument-integer-truncate}

`TRUNCATE(value)` over INT and BIGINT preserves the integer, its resolved width and NULL.
Verified direct inputs, typed literals, recursive lossless widening and nested admitted
integer rounding reuse the native operand expression without a JVM evaluator or a new
values buffer. Recursively verified integer addition/subtraction/multiplication also run
natively, preserving every intermediate width and Java wrapping overflow.
This removes released Flink's DECIMAL(38,18) intermediate only when the
default zero scale provably leaves every input unchanged, including signed minima and
exact BIGINTs above 2^53. Explicit positions use the bounded native path below;
unverified positions and producers retain the complete generated program, including
its overflow and exception behavior. DECIMAL and floating
TRUNCATE retain their separate paths. TINYINT/SMALLINT TRUNCATE still preserves released
Flink's generated-compilation failure; explicitly widening them to INT/BIGINT is supported.
[Release measurements](../benchmarks/scalar-functions.md#single-argument-integer-truncate)
compare stock, the previous JVM route and candidate at both batch and complete-job boundaries.
The [computed-operand controls](../benchmarks/scalar-functions.md#integer-truncate-over-arithmetic)
also cover wrapping arithmetic and jobs with both transposes.

### Integer TRUNCATE with a literal position {#integer-truncate-with-a-literal-position}

`TRUNCATE(value, literal_position)` over INT and BIGINT executes an Arrow primitive kernel
for INT positions -38..38, including a typed NULL. Its verified producers are direct
inputs/literals, lossless widening, nested admitted integer rounding and recursively verified
integer addition/subtraction/multiplication.
Negative positions truncate toward zero using integer remainder; a power larger than the
input width yields zero. Nonnegative positions share the input buffers, NULL positions
fold to typed NULLs after the operand proof excludes failures, and other results allocate
values while retaining sliced validity.
This removes Flink's DECIMAL(38,18) intermediate only in the verified domain, preserving
resolved widths, signed minima and exact large BIGINTs without JVM computation.

Unverified runtime/extreme positions, division, remainder, narrowing and other unverified producers keep their
complete generated expression. In particular, nonzero input with position -2147483648
keeps Flink's Underflow exception. Direct TINYINT/SMALLINT signatures still preserve
released Flink's generated-compilation failure; explicit widening is supported. Conditional
branches and empty filtered batches retain their guards. No configuration switch is added.
[Release measurements](../benchmarks/scalar-functions.md#integer-truncate-with-a-literal-position)
compare stock and the actual preceding scalar callback, including both transposes in
complete jobs.
The [arithmetic controls](../benchmarks/scalar-functions.md#integer-truncate-over-arithmetic)
add computed operands, bounded positions and failure-safe NULL folding.

### Integer TRUNCATE with a bounded runtime position {#integer-truncate-with-a-bounded-runtime-position}

INT/BIGINT `TRUNCATE(value, position)` reuses the
[bounded CASE proof](#integer-round-with-a-bounded-runtime-position), typed position selector
and exact integer kernel. On the literal-branch CASE path, every position result is a
literal in -38..38 or NULL, including nested CASE; every condition and value producer
must satisfy the failure-free proof. Negative positions truncate toward zero without a
DECIMAL(38,18) JVM intermediate.
The selected NULL, input NULL, resolved width, integer overflow in verified producers,
STRING/epoch consumers and empty-batch contracts remain exact.

Arbitrary columns, out-of-range results, failing/stateful conditions and other unverified
producers retain their complete generated expression. Direct TINYINT/SMALLINT TRUNCATE
retains released Flink's compilation failure; explicit widening to INT/BIGINT is supported.
No Rust implementation, opcode, JNI, transport or switch is added by this extension.
The [TRUNCATE qualification](../benchmarks/scalar-functions.md#integer-truncate-with-a-bounded-runtime-position)
compares both releases with stock and the actual preceding encoder, retaining tiny and
conversion-inclusive complete-job controls.

INT/BIGINT TRUNCATE also accepts positions conservatively bounded through built-in
GREATEST/LEAST and CASE, for example `TRUNCATE(value, GREATEST(-38, LEAST(position, 38)))`.
It reuses ROUND's interval/totality proof and existing extrema/selection kernels; the
rounding kernel retains its toward-zero branch. NULL-only branches and independent value/
position NULLs preserve their existing behavior. One-sided/unbounded/out-of-range positions,
failing/unverified producers and same-name catalog functions keep their preceding routes.
Direct TINYINT/SMALLINT signatures retain their released failure. The
[clamped TRUNCATE qualification](../benchmarks/scalar-functions.md#integer-truncate-with-clamped-runtime-positions)
uses its own actual preceding encoder, stock/native measurements and complete-job controls.

### Exact unary and integral functions {#exact-unary-and-integral-functions}

TINYINT, SMALLINT, INT and BIGINT ABS use a Rust Arrow unary kernel for direct inputs,
typed literals, recursive lossless integer widening, nested admitted ABS calls and
recursively verified integer addition, subtraction and multiplication of those producers.
Each arithmetic node retains its resolved integer width and Java overflow wrapping;
no floating conversion or JVM arithmetic callback is introduced. Same-width integer
result casts share their Arrow array instead of widening and reconstructing it.
Both nullable and NOT NULL inputs retain their width. NULL propagates; the minimum negative
value remains negative, matching Flink's `SqlFunctionUtils.abs` and Java narrowing/wrapping
rather than DataFusion's checked integer ABS. Input buffers and sliced validity remain owned
by the input; the kernel allocates the result values and shares validity without a JVM callback.
Division, remainder, casts outside that whitelist, DECIMAL arithmetic and other unverified
ABS producers retain the enclosing generated Flink evaluation. This replaces
computation inside an already-admitted Calc and introduces no configuration switch.
See the [release controls and small-batch limits](../benchmarks/scalar-functions.md#integral-abs).
The [arithmetic-producer controls](../benchmarks/scalar-functions.md#integer-abs-over-arithmetic)
also retain the initially slower native implementation and the cost of the removed casts.

Integer unary minus on direct TINYINT/SMALLINT/INT/BIGINT inputs, typed literals, lossless
integer widening, nested admitted negation and recursively verified integer addition,
subtraction and multiplication executes a Rust Arrow kernel without a JVM
evaluator. It retains the resolved width and wraps each signed minimum to itself, including
scalar literals: verified expressions containing input references reuse DataFusion's physical negative
expression, while literal-rooted chains use wrapping scalar dispatch. DataFusion 54's ordinary
negative expression checks scalar overflow, so it cannot represent both contracts alone.
NULLs, sliced validity and ownership
are preserved. The function route diagnostic includes unary minus; runtime witnesses require
its native route and consumed batches, rather than interpreting an empty function list as
evidence. Division, remainder, narrowing, DECIMAL arithmetic and other unverified producers retain generated evaluation
with their enclosing negation. [Release measurements](../benchmarks/scalar-functions.md#integral-unary-minus)
compare prepared inputs with stock and the previous callback, and separately report the
complete row-source job with both transposes.

The [arithmetic-producer controls](../benchmarks/scalar-functions.md#integer-unary-minus-over-arithmetic)
also cover both constant operand orders through production JNI on JDK 11/17, including
empty batches, broadcast cardinality and output ownership after the input closes.

DECIMAL ABS also uses the exact Rust kernel for direct input references, typed literals,
nested admitted ABS, recursive precision widening at the same scale and recursively
verified DECIMAL-only addition, subtraction and multiplication of those producers. It preserves
the declared precision (1 through 38), scale, NULLs and sliced validity; it never converts
through floating point. Like Comet's decimal ABS, the kernel transforms the unscaled i128
and explicitly retains its Arrow decimal type. Each arithmetic node retains its own
resolved precision/scale and overflow NULL. Division, remainder, mixed numeric operands,
rescaling, precision narrowing and other unverified producers
retain the enclosing generated expression. Existing
value-dependent FLOOR/CEIL and runtime-scale rounding guards still apply before this matcher.
The [DECIMAL ABS measurements](../benchmarks/scalar-functions.md#decimal-abs) compare
stock, previous callback and native execution at prepared and complete-job boundaries.
The [arithmetic-producer controls](../benchmarks/scalar-functions.md#decimal-abs-over-arithmetic)
measure ABS through a nullable BOOLEAN consumer. NOT NULL basic-type final projections
still retain their generated default/NULL-flag contract described above; direct DECIMAL
overflow retains ERROR/DROP enforcement.
Character CAST/TRY_CAST keep their separate producer whitelist: admitting arithmetic
ABS does not by itself make every enclosing character conversion JVM-free. Remaining
conversion signatures are tracked in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

ABS also admits verified negative DECIMAL producers, including `ABS(-d)`, negative
subtrees of the existing arithmetic proof and same-scale widening. Leading verified
negations are omitted: every valid DECIMAL coefficient has an exact negation, so ABS
retains the same value, precision, scale and NULL without the intermediate array.
The same verified proof also admits negation over ABS, such as `-ABS(d)` and
`ABS(-ABS(d))`, and alternating trees of both operators. Fallible/unverified producers
retain generated evaluation. STRING and numeric-epoch
consumers retain their own separate proofs; the latter accepts total unary/widening
trees but excludes arithmetic. The [release qualification](../benchmarks/scalar-functions.md#decimal-abs-over-negative-producers-2026-11-05)
records prepared and complete-job comparisons against stock and the preceding evaluator.
The separate [negation-over-ABS qualification](../benchmarks/scalar-functions.md#decimal-negation-over-absolute-values-2026-11-05)
records the later alternating-tree admission. Character-consumer guards remain separate:
`TRY_CAST(-ABS(d*d) AS CHAR(48))` now uses the verified arithmetic formatter, while
ordinary CAST over unverified unary/arithmetic producers retains generated evaluation.
The separate [TRY_CAST qualification](../benchmarks/scalar-functions.md#decimal-unary-arithmetic-try_cast-to-character-2026-11-05)
records the nullable-result boundary and its stock/preceding fallback comparisons.

DECIMAL unary minus admits direct inputs/literals, recursive same-scale precision widening,
nested admitted negation/ABS and DECIMAL-only addition/subtraction/multiplication over those
producers. Every arithmetic node retains its own resolved precision/scale and overflow NULL.
It uses Arrow's decimal numeric negation for arrays and direct i128 scalar dispatch,
retaining precision, scale and NULL without a JVM evaluator. All legal DECIMAL extrema
have a representable negation. Division, remainder, mixed numeric operands, rescaling,
narrowing and other unverified producers retain their whole generated expression; existing rounding
metadata guards remain unchanged. See the
[DECIMAL unary-minus controls](../benchmarks/scalar-functions.md#decimal-unary-minus).
The [arithmetic-producer controls](../benchmarks/scalar-functions.md#decimal-negation-over-arithmetic)
measure negation through a nullable BOOLEAN consumer. Final NOT NULL basic-type defaults,
direct DECIMAL ERROR/DROP enforcement and the separate character CAST/TRY_CAST producer
whitelist retain their contracts; this arithmetic proof does not extend every enclosing cast.

DECIMAL SIGN uses the bounded native coverage described above. Unverified integer rounding
producers and positions use the same path, including per-row ROUND/TRUNCATE positions.
TINYINT/SMALLINT TRUNCATE
retains the host compilation
failure described under exact floating math below. This retains resolved widths, decimal precision/scale, NULL handling,
and Java overflow behavior. Adjacent supported
host expressions fuse before crossing the Arrow boundary. These are host-evaluated functions
inside a columnar native Calc, not pure-Rust kernels.

Compact `DECIMAL(p,p)` SIGN and its affected character CAST stay in one generated evaluator,
preserving the intermediate sign before an Arrow boundary can revalidate its precision.
This is the preserving boundary shipped in [#542](https://github.com/datafusion-contrib/StreamFusion/pull/542)
for [#536](https://github.com/datafusion-contrib/StreamFusion/issues/536), described under
[SIGN](#sign). Noncompact DECIMAL(19,19)/(38,38) retain released Flink's NULL sign
results for nonzero values. The native ABS/negation producer whitelist does not admit SIGN;
its character-consumer fusion and rounding guards remain in force.

DECIMAL FLOOR/CEIL/CEILING run inside verified scalar consumers using one fused generated
expression, so the intermediate decimal does not cross Arrow before its consumer executes.
This covers STRING and integral casts, predicates, nested selections, and DECIMAL results whose
precision is normalized by a non-identity cast, exact arithmetic or Flink's IF branch casts.
Precision 12/18/19/20/38, positive/negative/exact/NULL values, large precision-38 values,
selected and unselected failures, empty filtered batches and native work over 5,003 rows
are covered by the runtime regressions.

Direct rounded DECIMAL results and decimal-preserving consumers such as ABS or COALESCE retain
fallback: Flink's FLOOR/CEIL return a decimal with value-dependent precision, which can fail the
released host's binary-writer precision assertion. Normalizing it at an Arrow boundary would hide
that host behavior. The same gate applies to whole-Calc generated execution, including JSON
compositions. Collection and ROW consumers of DECIMAL FLOOR/CEIL remain outside the verified
scalar boundary. Existing floating-point gates and decimal TRUNCATE/ROUND kernels are unchanged.

Release+mimalloc measurements on Linux x86-64 (Intel i7-12650H, JDK 17, Flink 2.2.1),
with two million runtime rows, NULL every seventh row, parallelism 1, two warmups and
five interleaved measurements per engine, include the row source, blackhole sink and
both asserted transposes:

| DECIMAL(38,9) workload | Flink median (s) | Native median (s) | Flink/native |
| --- | ---: | ---: | ---: |
| Identity control | 0.276592 | 0.578956 | 0.478x |
| `CAST(FLOOR(n) AS STRING)` | 0.600918 | 1.028703 | 0.584x |
| `CAST(CEIL(n) AS STRING)` | 0.621295 | 1.035432 | 0.600x |

These standalone projections are slower than Flink. The extension retains columnar pipeline
composition using the existing JVM evaluator; no standalone speedup is claimed. Reproduce with
`ScalarFunctionBenchmark`, `SF_BENCHMARK=true`, `-Pbench`,
`-Dscalar.functions=DECIMAL_FLOOR_STRING,DECIMAL_CEIL_STRING`, `-Dscalar.rows=2000000`,
`-Dscalar.nullEvery=7`, `-Dscalar.warmup=2`, and `-Dscalar.runs=5`.

### Decimal ROUND, TRUNCATE and literals {#decimal-round-truncate-and-literals}

`ROUND(decimal_column[, literal_integer_scale])` runs with compatibility overrides disabled.
It rounds ties away from zero (HALF_UP), preserves NULLs and Flink's inferred result precision,
scale and nullability, and returns NULL when rounding exceeds the result precision. Negative
positions round the integral part. A position at or above the source scale preserves its value
and scale rather than appending zeroes. A NULL position returns a typed NULL.

Positions from -38 upward use a prepared native decimal kernel. More negative literal positions
use Flink's own decimal rounding through the existing columnar JVM upcall, so extreme BigDecimal
scale/range exceptions match the host instead of being silently clamped to zero. These calls
remain inside native Calc, but fall back when nested under AND/OR to preserve row short-circuiting.
CASE can skip an unselected failing branch. BIGINT scale arguments retain an explicit
planner fallback; float/double ROUND uses exact generated evaluation by default, with the existing approximate Rust opt-in.

Runtime INT scales for ROUND and TRUNCATE run inside native Calc when a verified scalar
consumer encloses the rounding. A standalone STRING cast on direct decimal and INT columns
uses native batch rounding and per-row decimal formatting; a batch with non-NULL positions
below -38 uses the existing Flink evaluator. Other complete expressions use Flink-generated
evaluation, so integer casts, comparisons, predicates, nested rounding, CASE/COALESCE and
explicit decimal normalization see the original runtime precision and scale. Arithmetic,
nonidentity DECIMAL casts and IF can normalize the result before it crosses Arrow.
Negative/NULL scales, scale expansion, precision 38, input errors and AND/OR/CASE
short-circuiting retain released Flink behavior. Positions at or above the input scale
leave the input unchanged; extreme negative positions can throw BigDecimal's scale errors.

Direct runtime-scale DECIMAL outputs, decimal-preserving selections/sign operations, and
complex outputs remain on Flink. Its serializer can reinterpret the internal unscaled
integer using the declared scale: for DECIMAL(20,3) `123.456` and scale 2, direct ROUND and
TRUNCATE collect as `12.346` and `12.345`, while their STRING consumers yield `123.46` and
`123.45`. Scale 1 changes the internal precision and triggers the host binary-writer
assertion with assertions enabled. Tests preserve both behaviors through explicit fallback,
including when a JSON expression would otherwise select complete-row generation.

A release+mimalloc comparison of the direct STRING consumers on Flink 2.2.1/JDK 17,
with two million row-fed records, measured native ROUND at 0.634 s versus Flink's 0.690 s,
and native TRUNCATE at 0.635 s versus 0.694 s. At five million rows, ROUND measured
1.458 s versus 1.605 s and TRUNCATE 1.463 s versus 1.576 s. Both transposes and the row sink
remain in these measurements. These results apply to the direct decimal/INT column shape; composed
expressions retain generated evaluation. See the [method, variability and prior implementation
comparison](../optimizations/udf-columnar-upcall.md#decimal-runtime-scale-consumers).

`TRUNCATE(decimal_column[, literal_integer_scale])` uses the same native fixed-width
kernel with rounding toward zero. Positive, zero and negative positions preserve Flink's
resolved precision/scale and NULL behavior; a position at or above the source scale retains
the input value. Positions below -38 use Flink's generated expression through the columnar
callback, including its extreme-scale errors. They retain the same AND/OR short-circuit
restriction as ROUND. Runtime INT scales use the generated scalar-consumer path above. A standalone STRING cast of ROUND/TRUNCATE on direct decimal and INT columns uses native batch rounding and formatting, with released-function fallback for batches containing extreme negative positions. Other compositions retain generated evaluation. The direct STRING consumer measurements above include the full native island.
Integer TRUNCATE inputs outside the verified single-argument INT/BIGINT path use the
generated path described under exact numeric functions;
floating-point inputs use the exact generated path below. SQL tests cover
precision 38, multiple batches, filters, CASE, COALESCE and aggregate consumers, including
projections that combine and nest truncation with DECIMAL-to-FLOAT/DOUBLE casts.

Decimal planner literals are rescaled HALF_UP to their declared scale before encoding their
unscaled integer. A precision overflow becomes a typed decimal NULL. This handles constant-folded
casts whose stored value retains more fractional digits than its resolved DECIMAL type.

### Arithmetic operators {#arithmetic-operators}

**Native and byte-exact by default, with the boolean short-circuit restriction below.**

- `+`/`-`/`*` whose result type is `DECIMAL` (e.g. Nexmark q1's `0.908 * price`) use fused native
  kernels with Flink's resolved result precision and scale. Operands stay `Decimal128` or signed
  integers; intermediates use checked i128 arithmetic and widen to i256 when needed. The result
  is rounded `HALF_UP` before checking precision and writing one `Decimal128` column. Overflow
  produces SQL `NULL`, including in `IS NULL`, CASE, filters, and group keys. A wide intermediate
  does not discard a result that fits after rounding.
- **Division** (`/`) uses Flink's decimal rounding rather than Arrow's: the quotient is
  computed to 38 *significant* digits with `HALF_UP` rounding (matching `BigDecimal`'s
  `MathContext(38, HALF_UP)`), then rescaled to the declared `DECIMAL(p, s)` with `HALF_UP` again —
  producing `NULL` when the result would exceed `p` digits, and failing the job on division by zero,
  all exactly as the host does.
- **Modulo** (`MOD`, `%`) computes the exact signed remainder with Flink's `MathContext(38)`
  constraint on the integral quotient. A quotient that needs more than 38 significant digits
  after removing trailing zeroes fails the job with `Division impossible`; for example,
  `DECIMAL(38,0)` value `10^37` modulo `DECIMAL(38,38)` value `3 * 10^-38`. A large quotient
  consisting of a power of ten remains valid. The remainder is rescaled `HALF_UP`, with NULL on
  result-precision overflow and job failure on a zero divisor.

Decimal `/`, `MOD` and `%` nested under `AND` or `OR` fall back at planning time.
DataFusion 54 can evaluate an operand for rows Flink skips in a mixed boolean batch,
exposing division-by-zero or `Division impossible` errors on unevaluated rows.
For example, `guard_value OR MOD(a, b) = 0` must skip MOD on rows whose guard is TRUE.
This restriction applies to projections and filters, including nested expressions.
Direct arithmetic and CASE result branches remain native; CASE selects the rows to
evaluate before running the decimal kernel. Evaluated invalid operands still fail the job.

Flink can retain a `NOT NULL` result declaration from non-nullable operands even
when decimal arithmetic overflows. The physical Arrow result of a generated Calc permits
those runtime NULLs while the SQL result type stays unchanged. Flink's downstream constraint
enforcer remains responsible for its configured policy: ERROR fails the job and DROP discards
only the violating row. Arrow validation does not preempt that policy, including in a Calc
whose multiple divisions select complete-row evaluation.

The old `decimalArithmetic.approximate` flag is retired entirely: the float/double→`DECIMAL` cast it
used to gate now runs host-exact through the cast upcall above.


## POWER and SQRT {#power-and-sqrt}

`POWER` (including Flink's lowering of `SQRT`) runs inside native Calc by default through
Flink-generated JVM code in the existing batch UDF bridge. The resolved primitive/DECIMAL
overload uses Flink's own conversion and `Math.pow`, preserving exact deterministic results,
signed zeros, NaN, infinities, overflow/underflow and NULL propagation. Nested powers can fuse
into one generated expression. The surrounding operator remains columnar; the power operation
itself executes on the JVM. The Rust alternative remains available under
`streamfusion.expression.POWER.allowIncompatible=true` or the blanket flag.


## Exact floating math {#exact-floating-math}

EXP, LN, LOG10, SIN, COS, TAN, ASIN, ACOS, ATAN, COSH, SINH, TANH, COT, ATAN2,
DEGREES, RADIANS, LOG (one or two arguments) and LOG2 run inside native Calc by default
through Flink-generated evaluation in the existing batch JVM bridge. Resolved numeric
overloads, including integer and DECIMAL operands, use the selected released Flink line's
own conversions and math functions. This preserves deterministic last-bit results, signed
zero, NaN, infinities, domain results and NULL behavior.

Non-DECIMAL ROUND and INT/BIGINT TRUNCATE use generated evaluation outside the verified
single-argument integer and bounded literal-position ROUND/TRUNCATE paths above. FLOAT TRUNCATE also uses generated evaluation,
including runtime INT scales.
Independently emitted DOUBLE TRUNCATE uses an exact
[bounded shortcut](../optimizations/host-exact-builtins-upcall.md#bounded-double-truncate):
for magnitudes from 1 through 1e9 and scales -6 through 6, outward-rounded bounds must
prove the same truncated decimal integer before arithmetic replaces decimal conversion.
Ambiguous boundaries, other values/scales, and row-fused expressions retain Flink's
implementation. NULLs, conditional short-circuiting and declared types are unchanged. FLOAT/DOUBLE
ROUND retains Flink's BigDecimal-based HALF_UP semantics; every TRUNCATE path preserves
Flink's decimal rounding-toward-zero result.
TINYINT/SMALLINT TRUNCATE falls back: released Flink generates an invalid narrowing
assignment for these signatures, and the stock job also fails during initialization.
Non-finite values that fail in Flink fail on the native route with the same root exception.
Supported surrounding expressions fuse into the generated evaluator, preserving CASE and
filter short-circuiting for unselected failing calls. DECIMAL ROUND/TRUNCATE keep their
separate admission rules above.

These are JVM-evaluated functions inside a columnar operator. Existing Rust alternatives for
EXP, LN, SIN, COS, TAN, ASIN, ACOS, ATAN, LOG10 and floating ROUND remain opt-in through
`-Dstreamfusion.expression.<NAME>.allowIncompatible=true` or the blanket flag. Those paths
can differ from Java Math at the last bit, or from Flink's decimal-based floating rounding;
the flags are unnecessary for exact default execution. The new COSH/SINH/TANH/COT/ATAN2,
DEGREES/RADIANS/LOG/LOG2 paths always use generated evaluation.

Release+mimalloc diagnostic on an Intel Core i7-12650H under Linux/WSL, JDK 17,
Flink 2.2.1 (2026-09-27): two million numeric runtime rows, one-seventh NULLs, parallelism 1,
two warmups and five measured runs in alternating engine order. The matched numeric identity
control, both Arrow transposes and a rowwise blackhole sink remain in the measured path.

A [grouped integer-SUM benchmark](../optimizations/host-exact-builtins-upcall.md#composition-with-grouped-integer-sums) shows 16–24% less native elapsed time than Flink at 2M and 5M rows for TAN, COSH and floating TRUNCATE, retaining both transposes. This gain applies to those composed queries; standalone math remains slower. A separate admission ablation confirms that these compositions previously fell back entirely to Flink and were slower than the current native plans. Grouped nullable/extreme-value parity cases pass on Flink 1.18 and 2.2.

The original measurements below are historical. A subsequent [CPU-profile investigation](../optimizations/host-exact-builtins-upcall.md#exact-floating-math-remaining-performance-limits) tested borrowed inputs, primitive batch evaluation and simplified rounding. None established a whole-job improvement; the prototypes were removed. The current default
retains released Flink evaluation inside native Calc, with standalone costs reported separately
from the composed-query improvements above.


| Expression | Flink median (s) | Native median (s) | Flink/native ratio |
|---|---:|---:|---:|
| Numeric identity | 0.293 | 0.429 | 0.685x |
| Exact TAN | 0.367 | 0.643 | 0.571x |
| Exact COSH | 0.337 | 0.534 | 0.631x |
| DOUBLE TRUNCATE with runtime INT scale | 0.782 | 0.939 | 0.833x |

These standalone queries are slower than Flink. The generated path is retained to keep exact
math inside larger native pipelines using the existing bridge; it introduces no new operator
or boundary. It does not establish a floating-math speedup. Reproduce with:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=ScalarFunctionBenchmark \
  -Dscalar.functions=TAN_EXACT,COSH_EXACT,FLOAT_TRUNCATE_EXACT \
  -Dscalar.rows=2000000 -Dscalar.nullEvery=7 -Dscalar.warmup=2 -Dscalar.runs=5
```


## Literal/arity guards {#literalarity-guards}

A number of otherwise-admitted functions decline when called with an argument shape the native
implementation can't handle, even though the function itself is supported:

- An **unsupported literal type** anywhere in the expression.
- **`TRIM`, `BTRIM`, `LTRIM`, `RTRIM`** — per-row sets require the documented
  external-Java-string admission; unknown or binary-backed representations retain fallback.
- **`POSITION`** — a `FROM` start offset.
- **`CURRENT_WATERMARK`** — requires a Calc watermark context; unsupported in standalone join or UNNEST residuals.
- **Collection subscripts:** non-INT ARRAY indexes and literal indexes below one; runtime MAP keys
  of floating, collection or mismatched types; nullable non-compact decimal/timestamp MAP keys.
  See the [collection contract](calc-filter-jvm-collections.md#collection-subscripts).
- **Wrong arity** for any otherwise-admitted function.

See [Configuration](../configuration.md) for the full `allowIncompatible` flag surface referenced
throughout this page.

