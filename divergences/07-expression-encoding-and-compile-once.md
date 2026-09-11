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

## Plan-time type verification (diverges from Comet)
The encoder admits nodes; DataFusion decides result types when the tree is compiled,
and its coercion rules are not Calcite's (`FLOAT * DECIMAL` is `DOUBLE` to Flink, `Float32`
to DataFusion). Comet resolves this on the JVM side: its serde mirrors Spark's type rules
and the native plan is trusted to match. We do not duplicate DataFusion's coercion table in
Java — it is the very thing that drifts. Instead the planner asks the native library: it
compiles the encoded Calc against the input's Arrow schema at planning time (types only, no
batch) and hands back the inferred output schema. The comparison then happens on the JVM
side, next to the column-vector reader whose acceptance it encodes (`ArrowConversion.readsAs`):
exact Arrow type, except the unit/zone tolerance the timestamp and time vectors already have
on read. A projection the boundary would not read as its declared column, or a tree that does
not coerce at all, declines the Calc with the disagreement as the recorded fallback reason.
The consequence is that planning itself loads the native library — on the client or
JobManager — which the deployment already requires to be present everywhere.

## Scope / consequences
- The matcher gates to the admitted op codes and operand types; any un-admitted node
  anywhere in the expression makes the whole Calc fall back. Every admitted op gets a
  parity test before it is turned on.
- Admission is also gated on the compiled tree's type matching the declared output; see
  the section above and `docs/operators/calc-filter.md` ("Declared-type guard").
- The handle holds compiled native state and must be released on operator close, like
  the aggregator handles ([04](04-synchronous-stateful-execution.md)).

## Admitted-op semantics notes (parity edges)
Some functions diverge from the host only at precision/locale edges, not in value. Those fall back
**by default** but are opt-in via `NativeConfig` — `-Dstreamfusion.expression.<NAME>.allowIncompatible`
(or the blanket `-Dstreamfusion.expression.allowIncompatible`), mirroring DataFusion Comet's
`spark.comet.expression.<EXPR>.allowIncompatible`. This covers `UPPER`/`LOWER`, `ROUND`, and the
transcendental math below. A true value divergence must be corrected before admission, as with the
strict NULL propagation applied to `CONCAT` below.

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
- **`SUBSTRING`:** A native borrowed-slice kernel admits dynamic starts/lengths and preserves Flink's
  zero/negative-position and negative-length behavior. The DataFusion substring semantics differ at
  these boundaries; the result is a plain Utf8 array. See the Calc page for exact admission.
- **`TRIM`:** SQL's `BOTH`, `LEADING`, and `TRAILING` forms reuse the same DataFusion kernels as
  `BTRIM`, `LTRIM`, and `RTRIM`. Custom character sets must be literal, avoiding Flink's
  representation-dependent treatment of binary-backed sets beginning with a space.
- **`LPAD`:** The native kernel admits dynamic length/padding and counts UTF-16 units, matching
  released Flink 2.2.1. Empty padding and negative length return NULL. DataFusion and newer Flink
  source count code points, so their kernels cannot reproduce supplementary-character truncation.
- **`RPAD`:** Uses the same verified padding machinery on the right, including UTF-16 truncation.
- **`CHR`:** Delegates to DataFusion chr.
- **`LTRIM`:** The one- and two-argument forms delegate to DataFusion's space/character-set kernel;
  two-argument trim sets must be literal, with strict NULL propagation.
- **`RTRIM`:** Likewise delegates right trimming, with literal character sets.
- **`POSITION`/`REPEAT`/`ABS`/`FLOOR`/`CEIL`/`SIGN`:** `POSITION(sub IN s)` uses `strpos(s, sub)`
  (Int32, matching Flink's INT); `REPEAT(s, n)` uses `repeat`. The numeric `ABS`/`FLOOR`/`CEIL`/`SIGN`
  forms admit float/double; incompatible integer behavior still falls back. Temporal FLOOR/CEIL
  remain on Flink; their millisecond-producing kernels are removed from this PR.
- **`LIKE`/`REPLACE`/`REVERSE`:** `LIKE` maps to DataFusion's `Expr::Like` (case-sensitive, no
  explicit `ESCAPE` — a 3-operand `LIKE … ESCAPE` falls back); `REPLACE(s, from, to)` to `replace`;
  `REVERSE` to `reverse` (cast `Utf8View`→`Utf8`). ASCII-identical to the host.
- **`CHAR_LENGTH`** maps to DataFusion's `character_length` (Comet marks `Length` compatible). It
  counts Unicode code points; a supplementary character (e.g. an emoji) is one code point on both
  sides, so ASCII and BMP text are bit-identical.
- **`STARTSWITH`:** Character operands delegate to DataFusion's starts_with through the shared scalar registry. Flink materializes both operands before comparing UTF-8 bytes, so prefix matching does not have the representation-dependent ordering problem of string extrema. Scalar needles stay scalar. Standalone regressions remain documented; verified expressions can stay in a composed native Calc.
- **`ENDSWITH`:** Character operands similarly delegate to DataFusion's ends_with, preserving strict NULLs and empty-suffix matching without a custom kernel. Admission follows semantic compatibility, including when standalone execution is slower than Flink.
- **`INSTR`:** Delegates to DataFusion 54's strpos, retaining its scalar-needle search path and Unicode positions. See the Calc page for admission, semantics, and individual measurements.
- **`LOCATE`:** The two-argument form reverses operands into DataFusion strpos. The three-argument Rust kernel follows DataFusion's batch ASCII detection, one reusable memmem::Finder for literal needles, and memmem searches for column needles. Constant needles/starts stay scalar, following Comet's contains pattern. Shared positioning code preserves Flink's empty-needle and signed-start rules; no JVM callback is added. DataFusion has no start operand; slicing and adding an offset would violate Flink's empty-needle and signed-int arithmetic. Comet routes Spark StringLocate through host codegen; these narrower Flink forms have a verified Rust implementation. See the Calc page for admission, semantics, and individual measurements.
- **`BIN`:** Uses an Arrow string builder and Rust binary formatting of the widened unsigned bit pattern to match Long.toBinaryString. See the Calc page for admission, semantics, and individual measurements.
- **`HEX`:** Integer HEX writes uppercase digits directly into the Arrow string builder in one pass. String HEX checks output sizes and writes uppercase digits directly into the final Arrow buffer, avoiding temporary strings and a separate uppercase array. Arrow's safe constructor validates the result. See the Calc page for admission, semantics, and individual measurements.
- **`TO_BASE64`:** Uses DataFusion's standard padded base64 codec, computes output offsets with checked sizes, and writes directly into the final Arrow buffer. Unlike Spark's MIME form modeled by Comet, Flink does not wrap lines. Arrow's safe constructor validates the result. See the Calc page for admission, semantics, and individual measurements.
- **`UNHEX`:** Follows Comet's nibble lookup table and combined invalid-digit check, while retaining Flink's odd-length rule. Validation and decoding share a pass into the final Arrow binary buffer. Invalid rows roll back partial output. Capacity uses the active slice, offsets stay checked, and the constructor remains safe; no per-row temporary output copy is needed. DataFusion's strict decoder errors on invalid input and does not implement Flink's odd-leading-zero rule. VARBINARY literals use a dedicated literal kind, carrying length and byte values in the existing long pool; -1 denotes NULL. Like Comet BytesVal literals, they become ScalarValue::Binary directly, without a synthetic function call or a new JNI payload. See the Calc page for admission, semantics, and individual measurements.
- **`GREATEST`:** Integer and matching-scale Decimal extrema use primitive Arrow comparison loops, folding constants once and combining validity masks. Boolean and ASCII-provable string extrema reuse DataFusion with Flink's strict NULL mask. This avoids intermediate Boolean selection arrays and expanded scalar arrays. Arroyo's registry structure is retained. DataFusion skips NULLs; Flink requires strict NULL propagation. Floating point and mixed decimal scales remain unverified. See the Calc page for admission, semantics, and individual measurements.
- **`LEAST`:** Reuses the shared extremum kernel with minimum comparison, including primitive Arrow loops, folded constants, and strict NULL masking. Matching decimal scale is preserved on the output. DataFusion skips NULLs; Flink requires strict NULL propagation, with floating point and mixed decimal scales still unverified. See the Calc page for admission, semantics, and individual measurements.
- **`INITCAP`:** A Flink-specific column kernel changes only ASCII case bits, preserving other UTF-8 bytes and word boundaries. See the Calc page for admission, semantics, and individual measurements.
- **`TRANSLATE`:** ASCII mappings use direct lookup; non-ASCII codepoints use the project's ahash map. Consecutive equal alphabets reuse the mapping. Duplicate source positions retain their first mapping, including deletion mappings. Comet explicitly marks DataFusion's grapheme-based translation as incompatible. Flink requires codepoint mappings and its own duplicate/NULL rules. See the Calc page for admission, semantics, and individual measurements.
- **`BTRIM`:** Both default and literal character-set trimming delegate to DataFusion's btrim kernel; no duplicate space-only kernel is maintained. See the Calc page for admission, semantics, and individual measurements.
- **`ELT`:** A scalar index returns the selected array with its buffers/NULL mask intact. Longer dynamic outputs are sized before writing; short strings keep a single writing pass to avoid a selection vector larger than their payload. Comet uses host codegen. The narrower Flink INTEGER gate avoids admitting other boxed index types that Flink rejects. See the Calc page for admission, semantics, and individual measurements.
- **`URL_ENCODE`:** A Rust column kernel emits JDK-compatible form encoding into an Arrow string builder without a JVM callback. See the Calc page for admission, semantics, and individual measurements.
- **`OVERLAY`:** Copies intact UTF-8 prefix/replacement/suffix slices when UTF-16 cuts align with codepoints; ASCII needs no UTF-16 conversion. Cuts inside surrogate pairs retain full UTF-16 reconstruction, recombined pairs, and Java's encoding of lone surrogates. Comet uses host codegen. Flink's verified UTF-16 boundaries require preserving Java narrowing, overflow, substring errors, and split surrogates. See the Calc page for admission, semantics, and individual measurements.
- **`URL_DECODE`:** A Rust column kernel mirrors the JDK's escape parser and UTF-8 replacement grouping instead of substituting Rust from_utf8_lossy. Flink SQL parity enumerates roughly 100,000 byte/Unicode-digit boundary probes. Rust from_utf8_lossy does not preserve Java replacement grouping. The planner chooses pre-JDK-25 Integer.parseInt escape rules or JDK 25+ ASCII-only HexFormat rules; PARSE_URL remains unadmitted. See the Calc page for admission, semantics, and individual measurements.
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
- **`CONCAT` is admitted with strict NULL propagation:** Flink propagates NULL
  (`CONCAT(null, x) = null`), whereas DataFusion's general-purpose kernel skips NULL arguments.
  Following the wrapper pattern Comet uses through DataFusion's Spark `concat`, we union the input
  null bitmaps after evaluating each argument once. Unlike that wrapper, we return DataFusion's
  result directly when no rows are NULL, and append only valid rows when a mask is required. This
  avoids copying bytes for NULL results and rebuilding the array through a full UTF-8 validation
  pass. `CONCAT_WS` already matches Flink's separator and NULL-value semantics and delegates directly.
- **MD5, SHA-1 and SHA-2 fuse digest and hex output:** Arroyo delegates hashes to DataFusion; Comet's Spark
  SHA-2 wrapper also reuses the released Rust digest implementations. We use the same `md-5` and
  `sha2` crates, plus the released `sha1` crate used by Comet's `SparkSha1`, but write lowercase hex
  directly into the final UTF-8 Arrow buffers. This removes
  intermediate binary columns, per-row hex strings, and MD5's string-view-to-UTF-8 copy. The digest
  algorithms are unchanged; the deviation is allocation and output construction, measured in
  `docs/optimizations/string-copy-reduction.md`. `SHA2` literal widths are compared exactly, without
  narrowing BIGINT values. Exact admission and remaining gaps live in `docs/operators/calc-filter.md`.
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

## Further text and calendar scalar functions

The registration follows Arroyo's DataFusion ScalarUDF structure. Kernels follow Comet's
scalar/array and Arrow-buffer patterns; the released Flink 2.2.1 runtime determines semantics.
Relevant references include Comet's `string_funcs/split.rs`, `string_funcs/unbase64.rs`,
`datetime_funcs/extract_date_part.rs`, and `datetime_funcs/timestamp_trunc.rs`. These references
are architectural guidance; dependencies remain released crates and Maven artifacts. No new
JNI callback or row/Arrow conversion is introduced. See the Calc coverage page for the admitted
shapes, differential tests, and individual end-to-end measurements.

### ENCODE

UTF-8 reuses the Arrow offsets, bytes, and validity; the single-byte encodings reuse one batch scratch buffer. This follows Comet's scalar/array kernel boundary without a JVM callback.

### DECODE

Valid UTF-8 is validated once and reuses the BinaryArray buffers as Utf8. Invalid input uses the already parity-tested JDK UTF-8 replacement routine. No per-row byte or String allocation is needed.

### JSON_QUOTE

A reusable Rust string buffer mirrors Flink's UTF-16 iteration. A general JSON serializer would produce different output for these inputs.

### JSON_UNQUOTE

Combines Jackson's first-token validation and unescaping in one scan, with one reusable UTF-8 buffer and a pending surrogate. It deliberately preserves Flink's treatment of trailing text after that first value.

### SPLIT

Writes one Arrow `List<Utf8>` column with batch-level builders. Single-byte separators use Rust's character searcher (memchr); other separators use literal substring search. Comet's SPLIT uses regex semantics, so only its Arrow output structure is applicable here.

### SUBSTRING

Copies a borrowed UTF-8 slice into Arrow. ASCII positions use byte offsets; negative Unicode positions scan backward only as far as the requested start. This removes a full character-count pass while preserving Flink's boundaries.

### LEFT

Copies a borrowed prefix into the Arrow output and avoids DataFusion's different negative-count semantics. Constant and dynamic counts use the same verified kernel.

### RIGHT

Locates the suffix by traversing character boundaries from the end, without an initial full-string character count.

### LPAD

Copies borrowed UTF-8 prefixes and whole padding spans, following Comet's padding structure. A prefix counter measures Flink 2.2.1 UTF-16 units; a cut through a surrogate pair appends the JDK `?` replacement. Intact spans write directly into the Arrow output builder, without a per-row scratch string or whole-row UTF-16 buffers. Newer Flink source counts code points and cannot be substituted for the released runtime.

### RPAD

Reuses the same UTF-16 prefix accounting and UTF-8 span-copying kernel, placing the padding on the right.

### SPLIT_INDEX

Scans only as far as the requested token, retaining empty tokens, instead of constructing the complete split array. An empty separator uses the exact Java Character.isWhitespace set, not Rust is_whitespace or ASCII space alone.

### TO_DATE

A native parser mirrors DateTimeUtils.parseDate rather than the stricter Arrow ISO parser. Calendar validation includes year 0 through 9999 and leap-year rules.

### TO_TIMESTAMP (deferred)

Native parsing is withdrawn. Its millisecond timestamp result can represent expanded years,
but downstream native windows and keyed operators assume nanoseconds. A standalone parsing
benchmark does not establish safe composition. The expression encoder declines the function,
so the existing all-or-nothing admission rule keeps its consumers on Flink as well.

### QUARTER

Fuses Flink's timestamp-to-day conversion and integer Julian-calendar extraction in one Arrow primitive-array pass. Epoch milliseconds divide towards zero, including before 1970. The integer algorithm also preserves Flink's overflow behavior across the full DATE range.

### WEEK

Uses Flink's integer ISO-week calculation in the shared single-pass calendar kernel. Tests span leap days, year boundaries, and pre-epoch fractions. DATE inputs retain Flink's integer arithmetic.

### DAYOFYEAR

Subtracts the first Julian day of the year using Flink's integer arithmetic in the shared calendar kernel. Expanded years retain Flink's overflow results rather than becoming NULL outside chrono's range.

### DAYOFWEEK

Computes the Julian-day remainder with Sunday=1, directly in the shared primitive-array mapping. Preserves Flink's timestamp-day conversion and integer overflow on expanded years.

### FLOOR (timestamp, deferred)

Temporal FLOOR is withdrawn: its millisecond output does not satisfy downstream native
operators' nanosecond contract. Rejecting the temporal expression keeps projection, grouping,
formatting, comparison, and computed-rowtime window consumers on Flink. Numeric FLOOR retains
its existing native implementation.

### CEIL (timestamp, deferred)

Temporal CEIL/CEILING is withdrawn for the same timestamp-unit mismatch as FLOOR. The shared
rounding kernel, resolution encoder, and millisecond-only extraction/coercion guards are
removed with the last producer that required them. Existing numeric rounding is unaffected.
The change stays within expression admission; it adds no timestamp metadata or checks to
physical Calc operators or row/Arrow conversion.

### LTRIM

Delegates literal character sets to the released DataFusion Unicode trim kernel; dynamic sets are declined.

### RTRIM

Delegates literal sets to DataFusion's right-trim kernel and scalar-pattern reuse; dynamic sets are declined.

## Review-driven admission and organization

All scalar registrations added here live in `native/engine/src/flink_functions/mod.rs`, following
Arroyo's registry pattern. The expression decoder consults that registry once, and all local
kernels live beneath the same module. Unknown registrations return None; they never select an
unrelated function. Retired opcodes are not reused. Numeric/date-time operations that predate
this PR retain their existing decoder behavior.

Flink's `BinaryStringData.compareTo` uses UTF-16 String.compareTo for Java-backed strings and
byte order for binary-backed strings. There is no single Unicode order that reproduces both
plan shapes. GREATEST/LEAST therefore admit only ASCII-provable string literals and CASE results;
unrestricted string columns fall back. The existing string comparison operators predate this
change and share that limitation; this PR does not claim to fix their Unicode ordering.

Flink's binary-backed `isSpaceString` can treat a trim set beginning with a space as space-only.
BTRIM/LTRIM/RTRIM consequently admit literal sets only. The benchmarks use literal trim sets
that match this admission rule. Both of these gates avoid promising exact results
from a kernel whose semantics depend on Flink's runtime string representation.

ENCODE/DECODE keep the charset scalar, matching Comet's scalar-parameter approach. Integer HEX uses Comet's bounded stack-digit buffer, avoiding general formatting and the
old to_hex/uppercase array pipeline. LPAD/RPAD write directly to Arrow builders rather than materializing intermediate arrays or
copying a per-row scratch string. The shared codepoint reverse scan serves SUBSTRING and RIGHT;
LOCATE reuses memmem for both scalar and column needles.
