# Calc / filter

**Status:** native, with gaps — the largest admission surface in the project.

`Calc` (Flink's fused projection + filter node) and standalone filters are where per-row expression
evaluation happens, so their admission is expression-level rather than shape-level: the operator
itself always has a native form, but it only runs natively if **every** expression node inside it —
every function call, cast, and operator in the projection and the predicate — is one the native
expression engine admits. One un-admitted node anywhere in the `Calc` declines the whole node,
which (per the [all-or-nothing island rule](index.md#the-all-or-nothing-island)) drags the entire
query back to Flink, not just that node.

`Calc` is also one of the changelog-aware operators (alongside `GROUP BY`, the regular join, a CDC
source, `UNION ALL`, `Expand`, and changelog normalize) exempt from the insert-only guard — a
retracting/updating input doesn't disqualify it by itself.

The rest of this page is the exact admission list: what's unconditionally native, what's native by
default via a JVM upcall (and why that's not a fallback), what's opt-in, and what's a straight
fallback.

- **Unsupported function/operator** outside the admitted set (e.g. `MD5`; `CONCAT`, for a
  NULL-semantics divergence from Flink) is a plain fallback — there's no partial evaluation of an
  expression tree, so one unknown function anywhere in it declines the whole `Calc`.

## Declared-type guard

Admitting every node in a tree is not the same as producing the type Flink declared for the tree:
the result type of an arithmetic or function call is decided by DataFusion's coercion rules when
the expression is compiled, and those do not always agree with Calcite's. `FLOAT * DECIMAL`, for
example, is `DOUBLE` to Flink but single precision to DataFusion. The columnar boundary builds
Flink's column vectors from the Arrow batch it is handed, so a disagreement used to surface as a
`ClassCastException` on the first row read, after the job had started.

The planner therefore compiles the encoded `Calc` at planning time — types only, no data — against
the Arrow schema of its input and checks that the boundary would read each projection's inferred
Arrow type as its declared column (and that the condition is `BOOLEAN`). "Read as" is the reader's
own rule, not byte-equality of Arrow types: timestamps and times may carry any unit or zone, since
the column vectors convert on read (`PROCTIME()` is stamped as millisecond UTC where the row type
converts to nanoseconds), but every other type — width, decimal precision and scale, string
encoding, nested element types — must match exactly. Any disagreement, or a tree DataFusion cannot
coerce at all, is a plain fallback whose recorded reason names the column and both types, e.g.
`projection `EXPR$0` evaluates natively as FloatingPoint(SINGLE) but the plan declares DOUBLE`. Such
a reason is a real gap to close (either the encoder should carry the width Flink uses, as it does
for narrow integer literals, or the tree should be cast to the declared type), not a query to
rewrite. This check requires the native library in the planning JVM, which the standard deployment
already provides (see [Deployment](../deployment.md)).

## Casts

Native, unconditionally, with no host involvement:

- **Widening numeric** — integer→wider integer, integer→float/double, float→double.
- **Narrowing integer→integer and float/double→integer** — a purpose-built `NarrowingCast` kernel
  reproduces Flink's primitive Java cast semantics exactly: two's-complement wraparound for an
  integer source, and round-toward-zero-with-saturation (`NaN`→0) for a float source. Arrow's own
  cast kernel can't do this — it errors on overflow instead of wrapping/saturating.
- **`CHAR`/`VARCHAR` → `VARCHAR`** when the target length is ≥ the source length — an unpadded
  no-op (e.g. the common `COALESCE(s, 'x')` pattern).
- **Widening timestamp precision** within `TIMESTAMP` or within `TIMESTAMP_LTZ` — Arrow stores both
  at nanosecond precision at the columnar boundary, so widening the Flink declaration is a no-op.
- **`→ DECIMAL` from an exact source** — a `DECIMAL` or integer input, rescaled `HALF_UP`.

### The host-exact JVM upcall

A second group of casts is **native by default, and this is not a fallback** — it's a real JNI call
back into Flink's own cast machinery (`CastExecutor`/`CastRuleProvider`) for the one column being
cast, with the rest of the expression tree still evaluated natively around it:

- **Number ↔ string, both directions** — `CAST(x AS VARCHAR)`, `CAST(s AS INT)`, decimals
  included.
- **Narrowing a `VARCHAR`** (truncation).
- **Casting to `CHAR(n)`** (space-padding).
- **`→ DECIMAL` from a `float`/`double`.**

These four are deliberately routed through the host rather than reimplemented, because the host's
output isn't just "a reasonable float-to-string conversion" — it's a specific, JDK-version-dependent
rendering (trailing-zero handling, the scientific-notation threshold, trim semantics), and an
unparsable string input must fail the job exactly the way the host's default cast does. Running the
upcall makes the result byte-identical to Flink by construction instead of by reimplementation, at
the cost of one JNI round-trip per cast column rather than per row family. (The Kafka text sinks
already carry a probed native port of the legacy `Double.toString` spelling; moving the
float-to-string `CAST` onto that port instead of the upcall is a separate, tracked follow-up.)

The upcall casts **decline** — i.e. fall back to Flink entirely — when the deprecated
`table.exec.legacy-cast-behaviour` is enabled, since its null-on-failure semantics differ from the
default cast the upcall reproduces.

### Still falling back

Casts between strings and the non-numeric types (`boolean`/`date`/`time`/`timestamp` ↔ string), and
any other pair not listed above.

## Decimal arithmetic

**All native and byte-exact by default — not a fallback.**

- `+`/`-`/`*` whose result type is `DECIMAL` (e.g. Nexmark q1's `0.908 * price`) run entirely in
  Arrow: operands are `Decimal128` (columns already are; literals emit as an exact `Decimal128`),
  Arrow's `Decimal128` add/sub/mul carry Flink's scales, and the wrapping cast to the declared
  `DECIMAL(p, s)` rounds `HALF_UP`, exactly as Flink does.
- **Division and modulo** (`/`, `%`) go through a fused native kernel that reproduces Flink's exact
  runtime (`DecimalDataUtils.divide`/`mod`) rather than Arrow's own decimal division: the quotient is
  computed to 38 *significant* digits with `HALF_UP` rounding (matching `BigDecimal`'s
  `MathContext(38, HALF_UP)`), then rescaled to the declared `DECIMAL(p, s)` with `HALF_UP` again —
  producing `NULL` when the result would exceed `p` digits, and failing the job on division by zero,
  all exactly as the host does.

The old `decimalArithmetic.approximate` flag is retired entirely: the float/double→`DECIMAL` cast it
used to gate now runs host-exact through the cast upcall above.

## Case folding & regex

**Native by default — not a fallback.** `UPPER`/`LOWER` and `REGEXP_EXTRACT` run natively by default
via a columnar JVM upcall to Flink's own string routines — `BinaryStringData` case folding and
`SqlFunctionUtils.regexpExtract` — so the result is byte-identical to the host, and the rest of the
containing expression still evaluates natively around the upcalled function.

Each of these also has a faster **pure-Rust** alternative — Rust's own case folding, and the `regex`
crate — that is **opt-in** under
`-Dstreamfusion.expression.<NAME>.allowIncompatible=true` (or the blanket flag; see
[Configuration](../configuration.md)). It's opt-in rather than default because it can diverge from
the JVM behavior on non-ASCII case folding and on advanced regex features (backreferences,
lookaround, some Unicode character classes) — real correctness differences, not just a performance
trade-off, which is why parity comes first by default.

Neither path falls back to the host for a supported argument type. What *does* fall back: a
non-string argument, or — specifically on the pure-native (opt-in) `REGEXP_EXTRACT` — a non-literal
pattern or index.

## Date/time

**`DATE_FORMAT`/`EXTRACT` over `TIMESTAMP_LTZ` — native by default, not a fallback.** A local-zoned
timestamp's calendar fields (year, hour, day-of-week, …) depend on the session time zone
(`table.local-time-zone`), which a naive native formatter working in UTC wall-clock time can't
reproduce correctly. So, exactly like case folding and regex above, the **default** path routes the
`TIMESTAMP_LTZ` case through Flink's own zone-aware `DateTimeUtils.formatTimestamp`/
`extractFromTimestamp` via the columnar JVM upcall — byte-identical to the host.

A **pure-Rust `chrono-tz`** path is opt-in under
`-Dstreamfusion.expression.<DATE_FORMAT|EXTRACT>.allowIncompatible=true` (or the blanket flag). It
can diverge from the JVM at time-zone-database edges — bundled-tzdb-version skew, DST transitions
beyond roughly 2100, and deep historical dates.

A **legacy zone spelling** the native parser can't read (`GMT+1`, `PST`) makes the opt-in path fall
back; the default upcall path handles any zone Flink itself accepts. A plain `TIMESTAMP` argument
(no zone) stays on the pure-native path either way — there's nothing zone-dependent to upcall.

## Opt-in math

**Off by default, native only under `-Dstreamfusion.expression.<NAME>.allowIncompatible=true`** (or
the blanket flag): `EXP`, `LN`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `LOG10`, `POWER`/`SQRT`
(last-ULP libm divergence from Java's `StrictMath`), and float/double `ROUND` (`BigDecimal`-based
rounding in Flink vs. binary-float rounding natively).

Unlike case folding/regex/datetime above, there is no cheap byte-exact upcall available for these —
so, unlike those, **these fall back to Flink by default** and only run natively once you've opted in
and accepted the (typically last-bit) divergence.

## Literal/arity guards

A number of otherwise-admitted functions decline when called with an argument shape the native
implementation can't handle, even though the function itself is supported:

- An **unsupported literal type** anywhere in the expression.
- **`SUBSTRING`** — a non-literal or out-of-range start/length.
- **`LEFT`/`RIGHT`/`REPEAT`/`LPAD`/`RPAD`** — a non-literal or negative count.
- **`TRIM`** — anything other than the default `BOTH`-whitespace form.
- **`POSITION`** — a `FROM` start offset.
- **`SPLIT_INDEX`** — an empty or non-literal separator.
- **`DATE_FORMAT`** — a non-literal pattern, or (on the pure-native path only) a
  non-translatable pattern (text, fraction, or zone fields) — the JVM-upcall `TIMESTAMP_LTZ` path
  accepts any pattern Flink's own formatter does.
- **`EXTRACT`** — a fractional or convention-divergent field (`SECOND`, `DOW`, `WEEK`, `QUARTER`). A
  `TIMESTAMP_LTZ` argument to either `DATE_FORMAT` or `EXTRACT` now runs natively regardless — see
  Date/time above.
- **`TO_TIMESTAMP_LTZ`** — a precision other than 3.
- **A non-literal subscript** in `array[i]`/`map[key]` — at runtime a negative index counts from the
  end in DataFusion but is `NULL` in Flink, and the native map lookup binds its key at compile time,
  so only a literal subscript is safe to run natively (`array[i]` additionally requires the literal
  to be ≥ 1).
- **Wrong arity** for any otherwise-admitted function.

See [Configuration](../configuration.md) for the full `allowIncompatible` flag surface referenced
throughout this page.
