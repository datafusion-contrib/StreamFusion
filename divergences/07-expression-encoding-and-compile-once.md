# Expression IR: hand-encoded, compiled once, no Substrait

**Kind:** structural — how `RexNode` predicates/projections cross the JNI boundary.
**Diverges from:** nothing (it *follows* Comet); records why we reject Substrait.
**Forced by parity:** the encoding choice is; the compile-once lifecycle is performance.

## The options
Three ways to get an expression tree from the planner to the native engine:

1. **Substrait** — a cross-engine serialization standard (what Gluten uses to
   feed Velox/ClickHouse).
2. **A proprietary encoding** — hand-translate each node to a compact format and
   rebuild the engine's own expression natively (what Comet does, with Protobuf).
3. **No boundary** — plan with DataFusion directly and walk its `Expr` tree (what
   Arroyo does; it is pure Rust with no JVM, so it has no encoding problem at all
   and offers nothing to copy here).

## What we do, and why
We follow **Comet (option 2)**. We hand-translate `RexNode` to a compact pre-order
encoding — parallel primitive arrays (`kinds`, `payload`, `childCounts`) plus typed
literal pools — and rebuild a DataFusion `Expr` natively. Comet uses Protobuf for
the same job; we start with parallel arrays because the op set is small and they
cross JNI as cheap primitive arrays with no new dependency. If the admitted op set
grows toward Comet's scale (nested structs, N-ary functions, casts), switching the
*wire format* to Protobuf is the natural upgrade — it is a maintainability change,
not a performance one, since encoding happens once per operator, not per batch.

We **reject Substrait**, the same call Comet made. Substrait describes expressions
generically; we would still have to map every node onto Flink's exact semantics
(integer division, decimal scale, null and collation rules), which is the entire
value of the project. Its generality buys nothing toward parity and costs a heavy
dependency and a semantic-mapping layer we would own anyway. Parity-first means we
admit ops one at a time behind a matcher gate — the opposite of accepting an open
vocabulary we then have to defend against.

## Compile once, evaluate directly (performance)
Independent of the wire format: the native side compiles the decoded `Expr` into a
physical expression **once**, against the first batch's schema, and caches it on the
operator handle (`createFilterExpression` → handle; `filterExpression` per batch).
Each batch then evaluates that physical expression directly and filters with an
Arrow kernel — no per-batch `SessionContext`, logical→physical planning, or async
stream. This mirrors Comet, whose `PhysicalExpr` is built at plan time and reused.

The earlier stateless filters (`filterBatch`, `filterGreaterThan`) re-planned a full
DataFusion query *per batch*; that path is superseded by the compiled handle. The
cost it removed was the first confirmed hot-path finding of the benchmark sweep
(see `docs/optimizations.md`).

## Scope / consequences
- The matcher gates to the admitted op codes and operand types; any un-admitted node
  anywhere in the expression makes the whole Calc fall back. Every admitted op gets a
  parity test before it is turned on.
- The handle holds compiled native state and must be released on operator close, like
  the aggregator handles ([04](04-synchronous-stateful-execution.md)).

## Admitted-op semantics notes (parity edges)
Some functions diverge from the host only at precision/locale edges, not in value. Those fall back
**by default** but are opt-in via `NativeConfig` — `-Dstreamfusion.expression.<NAME>.allowIncompatible`
(or the blanket `-Dstreamfusion.expression.allowIncompatible`), mirroring DataFusion Comet's
`spark.comet.expression.<EXPR>.allowIncompatible`. This covers `UPPER`/`LOWER`, `ROUND`, and the
transcendental math below. A true value divergence (`CONCAT`'s NULL handling) is never opt-in — it
would just be wrong.

- **Integer `/` and `%`:** DataFusion and Flink (Java) agree for all finite operands —
  division truncates toward zero and modulo takes the sign of the dividend (verified with
  negative dividends, not just positives). Two edges are *not* silent divergences:
  divide-by-zero fails the job on both sides (Flink throws, DataFusion's kernel errors and
  the operator surfaces it — a query that divides by zero fails either way, never a wrong
  answer); and the single pathological `INT_MIN / -1` (and `LONG_MIN / -1`) overflow, where
  Java wraps to `MIN` but DataFusion's checked kernel errors. The latter is the one input we
  do not reproduce bit-for-bit; it is astronomically rare and fails loudly rather than
  silently, so we admit `/`/`%` and flag it here rather than forcing a fallback. (Contrast
  `+ - *`, which use wrapping kernels on both sides and match even on overflow.)
- **`COALESCE`/`NULLIF`:** lowered on the encoder side to the searched `CASE` the host defines
  them as, so they inherit `CASE`'s parity exactly rather than relying on a separate native
  function.
- **`SUBSTRING`:** `SUBSTRING(s FROM pos [FOR len])` maps to DataFusion `substr`/`substring`, with the
  result cast back to `Utf8` (DataFusion returns a `Utf8View` the JVM converter doesn't read).
  Admitted only when `pos` is an integer literal ≥ 1 and `len` (if present) ≥ 0: at `pos < 1` Flink
  clamps the start to 1 while DataFusion counts the out-of-range prefix against the length (e.g.
  `SUBSTRING('  pad  ' FROM 0 FOR 3)` → `"  p"` on Flink, `"  "` on DataFusion). A runtime (non-literal)
  position can't be range-checked, so it falls back too — both asserted by tests.
- **`TRIM`:** only the default `TRIM(BOTH ' ' FROM s)` (whitespace, both sides) is admitted, mapped
  to DataFusion's `btrim`; `LEADING`/`TRAILING` and custom trim characters fall back (asserted by a
  test). The encoder reads Calcite's three-operand TRIM (flag, trim-chars, source) and only proceeds
  for the `BOTH` + single-space case.
- **`LPAD`/`RPAD`/`CHR`:** `LPAD`/`RPAD` → `lpad`/`rpad` (cast `Utf8View`→`Utf8`), length a literal
  ≥ 0 and pad a literal (Comet's scalar-pad constraint); `CHR` → `chr`. Exact, no rounding/case/locale.
- **`LTRIM`/`RTRIM`/`POSITION`/`REPEAT`/`ABS`/`FLOOR`/`CEIL`/`SIGN`:** `LTRIM`/`RTRIM` default-
  whitespace only (a 2-arg custom-char form falls back); `POSITION(sub IN s)` → `strpos(s, sub)`
  (returns Int32, matching Flink's INT); `REPEAT(s, n)` → `repeat`; `ABS`/`FLOOR`/`CEIL`/`SIGN`
  admitted **only over float/double** — integer `ABS(INT_MIN)` overflows differently, integer
  `FLOOR`/`CEIL` is an identity Flink keeps in the int type, and integer `SIGN` would return int where
  DataFusion's `signum` returns float, so the integer forms fall back. `25.5` is a SQL `DECIMAL`
  literal, so the operand must be a true double (e.g. `v - 25.5E0`) to route.
- **`LIKE`/`REPLACE`/`REVERSE`:** `LIKE` maps to DataFusion's `Expr::Like` (case-sensitive, no
  explicit `ESCAPE` — a 3-operand `LIKE … ESCAPE` falls back); `REPLACE(s, from, to)` to `replace`;
  `REVERSE` to `reverse` (cast `Utf8View`→`Utf8` like `SUBSTRING`). ASCII-identical to the host.
- **`CHAR_LENGTH`** maps to DataFusion's `character_length` (Comet marks `Length` compatible). It
  counts Unicode code points; a supplementary character (e.g. an emoji) is one code point on both
  sides, so ASCII and BMP text are bit-identical.
- **`UPPER`/`LOWER` fall back by default** (opt-in via the flag above; asserted by a test). Native (Rust) case
  folding is locale-independent Unicode, but the JVM's `String.toUpperCase()/toLowerCase()` is
  locale-sensitive (e.g. Turkish dotless-i), so non-ASCII results can silently differ. DataFusion
  Comet reached the same conclusion — it routes case conversion through the JVM by default and only
  uses the native path under an opt-in flag. We do the same: fall back by default, native under the
  `allowIncompatible` flag, rather than ship a silent non-ASCII divergence.
- **Transcendental math falls back by default** (opt-in via the flag above): `EXP`/`LN`/`LOG10`/`SIN`/`COS`/`TAN`/`ASIN`/`ACOS`/
  `ATAN`/`POWER`/`SQRT`, which Calcite lowers to `POWER`). These are not IEEE-correctly-rounded, so
  the JVM's `java.lang.Math` (Flink) and DataFusion's Rust libm differ at the last ULP — verified:
  `TAN`/`ATAN`/`ASIN`/`ACOS` mismatch on sampled values (e.g. `tan` `…2386603` vs `…2386602`).
  `SIN`/`COS`/`EXP`/`LN`/`POWER` happened to match those samples, but a passing sample is not parity
  for a last-ULP-divergent family, so the whole family falls back. (Comet ships them as Compatible —
  it tolerates last-ULP; our byte-exact harness does not. The IEEE-exact ops `+ - * /`, `ABS`,
  `FLOOR`, `CEIL`, `SIGN` are admitted.)
- **`ROUND` falls back by default** (opt-in via the flag above; asserted by a test). Flink rounds float/double via
  `BigDecimal` (HALF_UP), which operates on the `Double.toString` decimal representation; DataFusion
  rounds with a binary float multiply (`(x·10^n).round()/10^n`). They agree on sampled values but
  differ on input-dependent precision edges, so a sample passing does not prove parity. Comet
  likewise falls back float/double `ROUND` ("does not support Spark's BigDecimal rounding").
- **`CONCAT` is *not* admitted:** Flink's `CONCAT` propagates NULL (`CONCAT(null, x) = null`) but
  DataFusion's `concat` ignores NULL args — a value divergence, so it falls back (asserted by a test)
  rather than ship a wrong answer.
- **`CAST`:** widening numeric (`integer→wider int`, `integer→float/double`, `float→double`) is a plain
  Arrow cast — lossless/IEEE-identical. **Narrowing to an integer type** (a wider int, or a float/double,
  → `TINYINT`/`SMALLINT`/`INTEGER`/`BIGINT`) is *not* a plain Arrow cast — arrow's kernel errors on
  overflow, whereas Flink emits the primitive Java cast, which **wraps** (an integer source truncates to
  the low bits, two's-complement) or **saturates** (a float/double source rounds toward zero and clamps
  to the target range, `NaN`→0). A dedicated `NarrowingCast` kernel uses Rust's `as`, which reproduces
  both exactly (Rust's float→int `as` is saturating with `NaN`→0, matching Java since 1.45); parity is
  tested at the `2³¹`/`2³²+1` integer boundaries and the `NaN`/`±∞`/`±1e20` float boundaries. **String
  casts still fall back:** number→string / string→number (formatting/parsing diverges from Arrow),
  narrowing a `VARCHAR` (truncation), and casting *to* `CHAR(n)` (space-padding). A **`CHAR`/`VARCHAR`→
  `VARCHAR`** cast with target length ≥ source is admitted as an unpadded passthrough (Flink stores both
  unpadded and neither pads nor truncates a widening string cast), which is what lets `COALESCE(s,'x')`
  (its `CHAR` literal branch unified up to `VARCHAR`) route.
- **Decimal `+`/`-`/`*`/`/`/`%` are all exact and admitted by default.** Operands reach the native
  side as `Decimal128` (columns already are; a literal emits as an exact `Decimal128`, not via double).
  Arrow's `Decimal128` add/sub/mul carry Flink's derived scales, and the wrapping cast to the declared
  `DECIMAL(p, s)` rounds HALF_UP — the same mode Flink uses. Division/modulo derive a rounded quotient
  scale that Arrow's own kernel derives differently from Flink, so they instead run through a dedicated
  fused kernel (`DecimalDivide`) that reproduces Flink's own two-step `BigDecimal` algorithm on
  arbitrary-precision integers (`num-bigint`) — an exact quotient rounded to 38 significant digits with
  HALF_UP, then a HALF_UP rescale to the declared `DECIMAL(p, s)` (NULL on overflow) — since that
  intermediate quotient can need more digits than fit in a fixed-width `i128` before it is rescaled back
  down. A `CAST` to `DECIMAL` from an exact source (another decimal or an integer) is likewise
  byte-exact; from a float/double source it is approximate (flag-gated).
