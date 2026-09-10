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

- **Unsupported function/operator** outside the admitted set (e.g. `PARSE_URL`) is a plain fallback —
  there's no partial evaluation of an expression tree, so one unknown function anywhere in it
  declines the whole `Calc`.

## String concatenation and hashes

These functions run entirely in Rust by default, in projections, predicates, and nested expressions:

| Function | Native behavior and admission |
|---|---|
| `CONCAT(s, ...)`, `s \|\| t` | Character-string arguments; any NULL argument makes the result NULL. Empty strings are preserved. |
| `CONCAT_WS(separator, ...)` | A literal or column separator; a NULL separator makes the result NULL. NULL values are skipped, empty strings are preserved, and no values or all-NULL values produce an empty string. |
| `MD5(s)` | Lowercase hexadecimal MD5 of the string's UTF-8 bytes; NULL input produces NULL. |
| `SHA1(s)` | Lowercase hexadecimal SHA-1 of the character string's UTF-8 bytes; NULL input produces NULL. |
| `SHA224(s)`, `SHA256(s)`, `SHA384(s)`, `SHA512(s)` | Lowercase hexadecimal SHA-2 of the UTF-8 bytes; NULL input produces NULL. |
| `SHA2(s, bit_length)` | The two-argument form with a literal bit length of 224, 256, 384, or 512; equivalent to the corresponding fixed-width function. |

A non-literal or NULL `SHA2` bit length, and other bit lengths, are not admitted. A dynamic bit length
falls back; literal values are checked exactly, including `BIGINT`, without truncating to 32 bits.
For example, `SHA2(s, CAST(4294967520 AS BIGINT))` falls back and retains Flink's unsupported-algorithm
failure instead of being treated as SHA-224. Flink 2.2 does not expose hash
overloads with an explicit character set. Binary/collection concatenation is outside this
character-string admission.

`CONCAT` computes Flink's strict NULL propagation from the input validity bitmaps, without
re-evaluating its arguments. Batches without NULL results use DataFusion's kernel directly; batches
with NULL results append only surviving rows. Neither path revalidates the concatenated UTF-8
payload. `CONCAT_WS` delegates to DataFusion. Hashes use the released RustCrypto libraries used by
DataFusion and Comet, writing lowercase hex directly into presized UTF-8 Arrow buffers without intermediate
binary or string-view columns or per-row heap allocations. See [string copy reduction](../optimizations/string-copy-reduction.md).
SQL parity tests cover NULLs, empty strings, embedded zero bytes, Unicode, long inputs, nested
calls, filters, valid BIGINT widths, and dynamic or oversized bit-length fallback.

The corresponding throughput diagnostics compare stock Flink against native execution with a
rowwise generated source, both row/Arrow transposes, and a blackhole sink. They assert those native
plan boundaries and use one warmup followed by the best of three complete job runs at parallelism 1.
They are function diagnostics, separate from the Nexmark headline benchmark:

```sh
SF_BENCHMARK=true SF_ROWS=2000000 mvn -pl :streamfusion-runtime test -Pbench \
  '-Dnative.cargo.args=build --release --features mimalloc' \
  '-Dtest=ThroughputBenchmark#stringConcatThroughput+stringHashThroughput'

SF_BENCHMARK=true SF_ROWS=2000000 mvn -pl :streamfusion-runtime test -Pbench \
  '-Dnative.cargo.args=build --release --features mimalloc' \
  '-Dtest=ThroughputBenchmark#nullableStringConcatThroughput'
```

Measured on Apple M4 Pro with JDK 17 on 2026-09-08, using the core-only release build above and
2 million rows whose strings cycle through `s0` to `s7`. The nullable diagnostic adds a 1 KiB
literal prefix and a CASE argument that makes 75% of results NULL. The two commands run in
separate JVMs:

| Diagnostic | Flink | StreamFusion | Throughput ratio |
|---|---:|---:|---:|
| `CONCAT` and `CONCAT_WS` projections | 0.566 s | 0.733 s | 0.77x |
| MD5 and SHA-2 projections | 4.141 s | 2.211 s | 1.87x |
| Nullable `CONCAT`, 1 KiB prefix | 0.390 s | 0.613 s | 0.64x |

The short-concatenation ratio changed from 0.94x in the first patchset to 0.77x here,
but native time was effectively unchanged (0.736 s to 0.733 s). Flink's baseline moved
from 0.691 s to 0.566 s, an approximately 18% decrease. The ratio change is within
the observed run-to-run baseline variance, not evidence of a native regression;
these separate runs do not establish a precise change in relative performance.

Both standalone concatenation cases remain slower than Flink. Their admission is useful for keeping
larger queries fully native; these results retain the regressions alongside the hashing gain.
Against the previous native implementation on the same upstream revision, hashing throughput
improved by about 47% and nullable concatenation by about 16%; short concatenation was essentially
unchanged. The [optimization ledger](../optimizations/string-copy-reduction.md#calc-construct-only-the-final-string-buffers)
records the before/after native times.
Both engines are timed through planning, source generation, and job completion; the native path
also includes both transposes. These are end-to-end diagnostics on this machine rather than
isolated kernel timings.

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
converts to nanoseconds). A top-level day-time interval projection may infer either `Int(64)`
milliseconds or `Interval(DAY_TIME)`. Calc converts the latter to nullable `Int(64)` milliseconds
before emitting its batch, so native exchanges and keyed operators receive the same representation
as a Flink-fed interval column. Inference retains the expression's interval type so this exemption
cannot admit an interval expression for a declared `BIGINT`. The Java reader also supports an
`Interval(DAY_TIME)` at the host boundary. Nested interval outputs must already use their canonical
integer encoding; they do not receive this exemption. Every other type — width, decimal precision
and scale, string encoding, nested element types — must match exactly. Any disagreement, or a tree
DataFusion cannot
coerce at all, is a plain fallback whose recorded reason names the column and both types, e.g.
`projection `EXPR$0` evaluates natively as FloatingPoint(SINGLE) but the plan declares DOUBLE`. Such
a reason is a real gap to close (either the encoder should carry the width Flink uses, as it does
for narrow integer literals, or the tree should be cast to the declared type), not a query to
rewrite. This check requires the native library in the planning JVM, which the standard deployment
already provides (see [Deployment](../deployment.md)).

One such gap is deliberate. A difference between two timestamps evaluates natively as a duration,
reported as `evaluates natively as Duration(NANOSECOND)`, and the Calc falls back. Flink floors each
operand to milliseconds and subtracts those, so narrowing the finished difference would disagree
whenever the operands carry sub-millisecond precision — `TIMESTAMP(6)` values a millisecond apart in
Flink's arithmetic can round to the same millisecond in ours. Reading it here is not parity, so it
stays on the host until the subtraction itself is done on floored operands.

Year-month interval literals remain unsupported by the expression encoder and fall back with
`unsupported literal type: INTERVAL_YEAR_MONTH` (or the literal's specific year/month qualifier).
Arrow `Interval(YEAR_MONTH)` and `Interval(MONTH_DAY_NANO)` outputs are not accepted by this
read contract. This change does not add native calendar-month interval expressions.

Filter-only Calcs use the same plan-time compilation check. An encodable predicate is not enough:
for example, a materialized interval column is an `Int64`, but comparing it with an interval literal
requires coercion DataFusion does not support. This shape falls back with
`condition does not compile natively`, including when a Top-N separates the producing projection
from the filter. It must not be admitted only to fail when the first batch arrives.

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

## String and calendar functions

The following list describes admission and fallback. Per-function Flink/native timings and
workload details are on the [scalar function benchmark page](../benchmarks/scalar-functions.md).

Admission requires verified semantics; it does not promise a speedup for every isolated query.
Retained coverage with slower standalone results is a precursor to the concrete optimizations
in this PR: scalar parameter reuse, direct output construction, primitive extrema, and shared
text scans. Cheap expressions can also remain inside a larger native Calc without another
host boundary. This is not a measured whole-query speedup claim. STARTSWITH/ENDSWITH remain
native for this composition benefit; their standalone regressions are documented in the
benchmark results and do not disable otherwise verified expressions.

### STARTSWITH

Two character arguments, literal or column. Matches a literal prefix, including Unicode and
empty strings; any NULL argument returns NULL. Wildcard characters have no special meaning.
Binary operands fall back.

### ENDSWITH

Two character arguments with the same NULL and type rules as STARTSWITH. Matches a literal
suffix; an empty suffix matches every non-NULL string. Wildcards have no special meaning.

### INSTR

Two character arguments only. Returns the first match as a 1-based Unicode codepoint position, or zero if absent. An empty needle returns 1; any NULL returns NULL. Three/four-argument INSTR falls back.

### LOCATE

Both LOCATE(needle, s) and LOCATE(needle, s, start) are native. Character inputs and TINYINT/SMALLINT/INTEGER starts are admitted; BIGINT starts fall back without narrowing. Positions count Unicode codepoints. Empty needles return 1 for every non-NULL start. Zero/negative starts search from the beginning, except INTEGER minimum: start - 1 wraps to maximum, matching Flink. Out-of-range starts return zero; any NULL argument returns NULL.

### BIN

TINYINT, SMALLINT, INTEGER, and BIGINT inputs are admitted. Returns binary digits without leading zeros; zero is `0`. Negative values have 64 two's-complement digits even for narrow input types. NULL returns NULL. Folded string NULL literals retain their declared type.

### HEX

Integer and character inputs are admitted. All four signed integer widths preserve Long.toHexString behavior: no leading zeros, uppercase digits, and 16 digits for negative values. Character strings encode their UTF-8 bytes as uppercase hex. NULL returns NULL.

### TO_BASE64

Character strings and binary columns are encoded as padded RFC 4648 Base64 without line wrapping.
Strings use their UTF-8 bytes; binary inputs preserve every byte, including invalid UTF-8.
Both overloads share the direct-output encoder. Empty input stays empty and NULL propagates.
VARBINARY literals are native; fixed-size BINARY literals retain the literal encoder's fallback.
FROM_BASE64 falls back.

### UNHEX

Character inputs produce BYTES. Either hex letter case is accepted; invalid bytes, whitespace, `0x` prefixes, and non-ASCII digits return NULL. Empty input produces empty bytes. Flink validates but discards an odd leading digit, emitting zero: `UNHEX('A') = 00`, `UNHEX('ABC') = 00 BC`. Folded VARBINARY constants carry bytes directly as typed binary literals, including empty values and NULLs; fixed-size BINARY literals retain the existing fallback.

### GREATEST

Integers, BOOLEAN and matching-precision/scale DECIMAL are native, with strict NULL propagation. Strings require ASCII literals or CASE results composed entirely of ASCII literals. Unrestricted string columns fall back: Flink uses UTF-16 order for Java-backed strings and byte order after binary materialization. Floating point and mixed decimal scales fall back.

### LEAST

Uses the same type and ASCII-proof gates as GREATEST, with strict NULL propagation and minimum comparison.

### INITCAP

Character strings only. Only ASCII letters and digits form words; every other character separates words and is preserved. NULL returns NULL. This follows Flink rather than DataFusion word boundaries.

### TRANSLATE

Three character arguments. Mappings use Unicode codepoints, not graphemes. The first duplicate mapping wins, but duplicates consume target positions. Missing target characters delete; a NULL target acts as empty. NULL/empty `from` leaves the source unchanged. A NULL source returns NULL.

### BTRIM

One-argument space trimming and two-argument character-set trimming with a literal set are native. Empty sets preserve the input and NULL propagates. Column trim sets fall back because Flink can change their meaning after an exchange when the first set character is a space.

### TRIM

The SQL `TRIM([BOTH | LEADING | TRAILING] [characters] FROM s)` forms are native with
a literal trim set, including Unicode, empty and NULL sets. The default set is the ASCII
space, not all whitespace. These forms reuse BTRIM/LTRIM/RTRIM's character-set kernels.
Column trim sets fall back for the same Flink representation-dependent behavior as BTRIM.

### ELT

An INTEGER index and character alternatives are admitted. The index is 1-based; out-of-range and NULL indices return NULL. Only the selected alternative's NULL matters. Other index types and binary alternatives fall back: Flink casts its boxed index to Integer after its bounds check. Explicit casts to INTEGER follow the existing cast rules.

### URL_ENCODE

Character strings use Java form encoding: space becomes `+`, ASCII alphanumerics and `-_. *` are preserved apart from space, and other UTF-8 bytes use uppercase percent escapes. NULL returns NULL.

### OVERLAY

Character strings and integer positions, widened to BIGINT without losing bits. Preserves Java UTF-16 positions, length narrowing/overflow, and substring errors. Non-positive or beyond-end starts return the source; zero/negative lengths omit the suffix. Split surrogate pairs encode as `?`, like Flink. Any NULL argument returns NULL.

### URL_DECODE

One character argument is native. Form decoding preserves JDK UTF-8 replacement grouping and returns NULL for malformed escapes. The planner selects the runtime JDK rule: JDK 17/21 (and pre-25 runtimes) use Integer.parseInt, accepting signed one-digit escapes and BMP Unicode hex digits; JDK 25+ uses ASCII-only HexFormat rules.

The JDK rule is selected on the JobManager during planning, so the JobManager and TaskManagers must use the same URL-decoding rule (pre-25 or 25+); mixed JDK groups can produce results that differ from Flink on the TaskManager.

### ENCODE

Character input and a literal UTF-8, US-ASCII, ISO-8859-1, UTF-16, UTF-16BE, or UTF-16LE
charset (including JDK aliases). Returns BYTES, preserves NULL, and replaces unmappable
characters with `?` in ASCII/Latin-1. UTF-16 emits a big-endian BOM for non-empty strings;
UTF-16BE/LE emit no BOM. Empty strings produce empty bytes in all six charsets.
Other or dynamic charsets fall back.

### DECODE

Binary input and the same six literal charsets as ENCODE. UTF-8 uses the JDK's replacement
grouping for malformed sequences; ASCII replaces each non-ASCII byte; Latin-1 maps all bytes.
UTF-16 detects and consumes an initial BOM, defaulting to big-endian without one. UTF-16BE/LE
use their fixed byte order and retain the BOM as a character. Malformed surrogate pairs and
odd trailing bytes follow JDK UnicodeDecoder grouping, including consuming a high surrogate
and a following non-low code unit together. NULL stays NULL. Other or dynamic charsets fall back.

### JSON_QUOTE

Character input, including NULL. Matches Flink 2.2.1's actual spelling: slash is escaped, non-ASCII values use lowercase Unicode escapes, and supplementary characters emit a full code-point escape followed by a low-surrogate escape. Unlisted ASCII controls are retained.

### JSON_UNQUOTE

One character argument is native. Valid quoted values are unescaped with Flink/Jackson first-token validation; invalid input is preserved and NULL propagates. A truncated Unicode escape after a valid first token fails the job, matching Flink 2.2.1's uncaught bounds exception. A truncated escape inside the first token is invalid JSON and is preserved.

### JSON_STRING

One character, BOOLEAN, TINYINT, SMALLINT, INTEGER, or BIGINT scalar is native by default.
SQL NULL returns SQL NULL; other scalars serialize to JSON text. Strings use Jackson's
escaping: quote/backslash and ASCII controls are escaped, other controls use uppercase
`\u00XX`, and slashes and Unicode remain unescaped. Integer widths retain their exact
decimal spelling. Output is written directly into the Arrow string builder.

Floating point, DECIMAL, binary, temporal, and collection inputs fall back. Direct nested
JSON_OBJECT, JSON_ARRAY, and JSON(value) calls also fall back: Flink treats those as raw JSON,
which is outside this scalar admission. JSON_STRING applied to an ordinary string column
containing JSON text quotes it normally. No compatibility opt-in is needed.

NULL values must have a supported scalar type, for example `CAST(NULL AS STRING)`.
An operand whose type remains SQL NULL falls back: Flink's JSON node generator cannot
serialize that type. The same typed-NULL rule applies to JSON_OBJECT values.

### JSON_OBJECT

Literal, non-null character keys with character, BOOLEAN, TINYINT, SMALLINT, INTEGER,
or BIGINT scalar values are native. Keys must contain well-formed Unicode. The default
NULL ON NULL writes JSON null values; ABSENT ON NULL skips them. Duplicate keys retain
the last inserted value, so an absent NULL does not overwrite an earlier non-null value.
Objects with no surviving entries produce `{}`, never SQL NULL.

Keys are sorted in Java UTF-16 order, matching Flink's Jackson serializer even when BMP
and supplementary characters mix. Keys and values use the same escaping as JSON_STRING.
Each batch reuses escaped keys and scalar parameters while writing directly to Arrow.

Dynamic/NULL keys, other value types, and direct nested JSON_OBJECT, JSON_ARRAY or
JSON(value) inputs fall back. An ordinary string containing JSON text is quoted.
No compatibility opt-in is needed.

### IS JSON

`s IS JSON [VALUE | OBJECT | ARRAY | SCALAR]` and their `IS NOT JSON` forms are native
for character input. Omitting the type means VALUE. Results are non-nullable: SQL NULL
and invalid JSON return FALSE (TRUE for the negated form). The JSON literal `null` is
a valid VALUE and SCALAR, but is neither an OBJECT nor an ARRAY.

Parsing matches Flink 2.2.1's Jackson first-document validation, including trailing
content, token boundaries, escaped surrogates, and limits in every nested field.
It shares the streaming/SIMD reader and JDK profiles described below; no compatibility
opt-in is needed. This validates the document directly, without applying JSON path policies.

### JSON_VALUE

Enabled by default for the following verified shapes; no compatibility opt-in is needed.

Character input with a non-null literal definite path is native. Supported paths are `$`,
dot members such as `$.user.name`, bracket members such as `$['user name']`, and nonnegative
32-bit array indexes such as `$.users[0].name`. Dot names use ASCII letters, digits and
underscores, with a letter/underscore first; bracket names additionally allow spaces and
hyphens. Member names are case-sensitive. Wildcards, recursive descent, filters, slices,
negative indexes, escapes/Unicode in path member names and dynamic paths fall back.

The default return type and explicit `RETURNING VARCHAR(n)` are native; Flink 2.2.1 does
not truncate this function's result to `n`. `RETURNING BOOLEAN`, `INTEGER` and `DOUBLE`
are also native with the following exact Flink object-type rules:

| RETURNING | Accepted selected scalar | Supported literal DEFAULT |
|---|---|---|
| VARCHAR(n) | String, boolean or number converted to Jackson's text | Non-null character literal |
| BOOLEAN | JSON boolean | Non-null BOOLEAN literal |
| INTEGER | JSON integer token within signed 32-bit range | Non-null INTEGER literal |
| DOUBLE | JSON number with a decimal point or exponent (Jackson BigDecimal) | Not admitted |

`NULL` and `ERROR` behaviors are supported independently for ON EMPTY and ON ERROR.
Other default types, NULL defaults and non-literal defaults fall back. DOUBLE defaults stay
on Flink because generated Double/DecimalData defaults do not match its BigDecimal cast.
Selected scalar type mismatches fail the job **outside ON ERROR**, matching Flink: a quoted
`"12"` is not an INTEGER, `1.0` is not an INTEGER, and `1` is not a DOUBLE. Decimal-to-double
conversion preserves rounding, infinity and underflow; a decimal zero has no negative sign.

A BOOLEAN form with either NULL policy is admitted only as a direct projection. Flink 2.2.1
can unbox its boxed NULL result without checking the null flag in a bare WHERE condition,
truth predicate or CASE condition, failing the job. Such compositions stay on Flink.
BOOLEAN forms with non-null DEFAULT or ERROR for both policies can compose natively.
Typed JSON_VALUE calls nested under AND/OR stay on Flink: DataFusion may evaluate the
unneeded side on some rows, exposing a scalar conversion failure that Flink short-circuits.
CASE result branches retain native admission and evaluate only selected conversions.
VARCHAR calls with an ERROR policy also stay on Flink when nested under AND/OR.

The default path mode is **strict**. Missing members, selected JSON nulls, malformed JSON,
and selected containers invoke ON ERROR in strict mode. In lax mode these invoke ON EMPTY,
except a document containing the JSON literal `null`, which invokes ON ERROR in either mode.
SQL NULL input always returns SQL NULL. ERROR ON EMPTY fails directly, even with a default
ON ERROR. Duplicate members keep the last value, decimal text retains Jackson's BigDecimal
scale/exponent spelling, and unpaired escaped surrogates become `?` in UTF-8 output.

### JSON_EXISTS

Enabled by default for the following verified shapes; no compatibility opt-in is needed.

Character input and the same literal path grammar as JSON_VALUE are native. Supports FALSE
(the default), TRUE, UNKNOWN and ERROR ON ERROR. A selected scalar or container, including an
empty object/array, returns TRUE. Lax missing paths and selected JSON nulls return FALSE;
strict missing/null paths invoke ON ERROR. Malformed JSON invokes ON ERROR in strict mode
and returns FALSE in lax mode. A document containing the JSON literal `null` invokes ON ERROR
in both modes. SQL NULL input returns SQL NULL.

UNKNOWN ON ERROR is admitted only as a direct projection, preserving the same Flink
boxed-null behavior described for BOOLEAN JSON_VALUE. ERROR ON ERROR under AND/OR stays
on Flink to preserve row short-circuiting. The default FALSE policy and TRUE ON ERROR
remain native in predicates and nested expressions.

These JSON functions use native first-document parsing and validate unselected fields too.
Admission first probes the shaded Jackson runtime once per class loader: version 2.18.2,
the default thread-local recycler pool, and successful buffer acquisition, cross-factory
reuse and release are required. Missing methods/classes, a different version or pool,
or probe failure cause planning-time fallback for JSON_VALUE, JSON_EXISTS and IS JSON.
JobManagers and TaskManagers must use the same verified shaded Jackson runtime.
They currently admit JDK 17, 21, 24 and 25, selecting the corresponding Unicode version for
Jackson's token-termination rules; other JDKs fall back. The profile is selected on the
JobManager, so TaskManagers must use the same JSON parsing rules. Jackson's resource limits
(1000 nesting levels, 1000 number digits, 20 million UTF-16 string units, 50,000 member-name
units) also apply to unselected values. Its numeric boundary has a buffer-dependent exception:
the slow parser can accept an extra digit. Native evaluation uses the task thread's actual
Jackson input-buffer capacity and preserves its growth, including invalid input and SIMD
parsing. A batch exchanges this capacity through JNI; documents and results remain native.
See the [SQL/JSON parser note](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/32-sql-json-definite-paths.md)
and [per-function benchmarks](../benchmarks/scalar-functions.md).

### SPLIT

Character input and a literal non-empty separator. The separator is literal text, including regex metacharacters. NULL input returns NULL, empty input returns an empty array, and leading/repeated/trailing separators retain empty tokens. Empty or dynamic separators fall back; the empty form splits UTF-16 surrogate units in Flink.

### SUBSTRING

SUBSTRING/SUBSTR accepts dynamic TINYINT, SMALLINT, or INT starts and optional lengths. Positive positions are one-based, zero starts at the first character, negative positions count from the end, and a position before the beginning returns empty. Negative length returns NULL; input NULLs propagate.

### LEFT

Character input with a dynamic TINYINT, SMALLINT, or INT count. Non-positive counts return empty, large counts return the full string, and NULL propagates. Counts measure Unicode code points.

### RIGHT

Character input with a dynamic TINYINT, SMALLINT, or INT count. Non-positive counts return empty, large counts return the full string, and NULL propagates. Counts measure Unicode code points.

### LPAD

STRING, dynamic INT-width length, and literal or dynamic STRING padding. NULL, negative length, or empty padding returns NULL; zero length otherwise returns empty. Flink 2.2.1 counts UTF-16 units, including truncation through surrogate pairs.

### RPAD

Same input and boundary rules as LPAD, with padding appended on the right. Dynamic lengths and padding are admitted, with Flink 2.2.1 UTF-16 counting.

### SPLIT_INDEX

Character separators and TINYINT/SMALLINT/INTEGER indices may be dynamic. Indices are zero-based; negative/out-of-range indices, empty input, or any NULL produce NULL. Whole separators preserve empty tokens. An empty separator uses Java Character.isWhitespace, including tabs and line separators but excluding non-breaking spaces. Numeric separators and BIGINT indices fall back.

### TO_DATE

One character argument. Accepts Flink's partial year/year-month forms, field trimming, and a timestamp suffix after the first ASCII space. Impossible dates return NULL; an all-digit field overflowing INTEGER fails the job. Formatted two-argument calls fall back.

### TO_TIMESTAMP

Falls back to Flink for both the default and explicit-format forms. Native support is
deferred: parsed timestamps can exceed the nanosecond range, while downstream native
operators require nanosecond columns. A millisecond result is therefore unsafe even when
standalone parsing succeeds. Computed-rowtime windows and parsed timestamp group keys
also remain on Flink.

### QUARTER

QUARTER and EXTRACT(QUARTER) over DATE or plain TIMESTAMP return, for ordinary calendar dates, 1 through 4 as BIGINT. NULL propagates. TIMESTAMP_LTZ falls back for this newly admitted field.

### WEEK

WEEK and EXTRACT(WEEK) over DATE or plain TIMESTAMP use ISO week numbers, including weeks spanning calendar years. NULL propagates. TIMESTAMP_LTZ falls back for this field.

### DAYOFYEAR

DAYOFYEAR/EXTRACT(DOY) over DATE or plain TIMESTAMP return, for ordinary calendar dates, 1 through 365/366 as BIGINT. NULL propagates. TIMESTAMP_LTZ falls back for this field.

### DAYOFWEEK

DAYOFWEEK/EXTRACT(DOW) over DATE or plain TIMESTAMP return Sunday=1 through Saturday=7 as BIGINT. NULL propagates. TIMESTAMP_LTZ falls back for this field.

### FLOOR (timestamp)

Temporal FLOOR falls back to Flink for every unit and timestamp precision, including
TIMESTAMP(3) and TIMESTAMP(9). Its former millisecond Arrow output is incompatible with
downstream native timestamp columns. The one-argument numeric FLOOR admission is unchanged.

### CEIL (timestamp)

Temporal CEIL/CEILING falls back to Flink for every unit and timestamp precision, for the
same timestamp-unit incompatibility as temporal FLOOR. One-argument numeric CEIL/CEILING
keeps its existing native admission. No millisecond timestamp rounding kernel is registered.

### LTRIM

One-argument space trimming and two-argument trimming with a literal Unicode character set are native. Empty sets preserve the input; NULL propagates. Dynamic trim sets fall back because Flink semantics depend on whether strings are Java-backed or binary-backed.

### RTRIM

Uses the same literal-set gate as LTRIM, trimming from the right. Dynamic trim sets fall back; one-argument space trimming is native.

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
(no zone) uses the pure-native path when its Arrow representation is nanoseconds. Parsed or rounded
millisecond results have a wider range than the legacy chrono formatting/extraction kernels and
fall back for those consumers. The new QUARTER/WEEK/DAYOFYEAR/DAYOFWEEK kernels accept them.

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
- **`TRIM`** — dynamic trim sets; all directions with literal sets are native.
- **`POSITION`** — a `FROM` start offset.
- **`SPLIT_INDEX`** — the numeric separator overload.
- **`DATE_FORMAT`** — a non-literal pattern, or (on the pure-native path only) a
  non-translatable pattern (text, fraction, or zone fields) — the JVM-upcall `TIMESTAMP_LTZ` path
  accepts any pattern Flink's own formatter does.
- **`EXTRACT`** — a fractional result or a field outside the admitted set. The added
  `QUARTER`/`WEEK`/`DOY`/`DOW` fields admit DATE and plain TIMESTAMP; their LTZ forms fall back.
  Existing YEAR/MONTH/DAY/HOUR/MINUTE/SECOND LTZ extraction uses the host-exact upcall described above.
- **`TO_TIMESTAMP_LTZ`** — a precision other than 3.
- **A non-literal subscript** in `array[i]`/`map[key]` — at runtime a negative index counts from the
  end in DataFusion but is `NULL` in Flink, and the native map lookup binds its key at compile time,
  so only a literal subscript is safe to run natively (`array[i]` additionally requires the literal
  to be ≥ 1).
- **Wrong arity** for any otherwise-admitted function.

See [Configuration](../configuration.md) for the full `allowIncompatible` flag surface referenced
throughout this page.
