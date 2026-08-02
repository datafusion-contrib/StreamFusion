# The ingest text envelope: Flink-exact parsers, and the residual leniencies

## Context

Flink's JSON and CSV formats funnel string-positioned values through one converter family
(`JsonToRowDataConverters` / `CsvToRowDataConverters`): Java `parseLong`/`parseDouble`/
`parseBoolean` over trimmed text, `new BigDecimal(String)` + `DecimalData.fromBigDecimal`
(HALF_UP rescale, **null** — not an error — on precision overflow) for decimals, and the
`TimeFormats` formatters for temporals. The native decode originally leaned on arrow-csv /
arrow-cast for these conversions, whose envelope differs from Flink's **on valid data**: arrow-csv
turns an empty string field into NULL where Flink produces `""`, truncates extra decimal fraction
digits where Flink rounds HALF_UP, and errors on the padded numbers (`" 12 "`), `Infinity`
spellings, and `1.5d` suffixes Java accepts. None of that is configurable from the outside.

## Decision

The decoders parse text with our own Flink-exact parsers (`native/src/flink_text.rs`), and the CSV
decode splits records with `csv-core` configured like Flink's Jackson `CsvSchema`
(`native/src/csv.rs`) instead of using arrow-csv. The JSON simd-path appenders follow the same
converters — string-encoded numbers with a trim, floats truncating toward zero into INT/BIGINT
columns (TINYINT/SMALLINT reject float tokens: their converters fall through to `parseByte` over
the raw literal), never-failing booleans, the strict `ISO_LOCAL_DATE`, and the table's
`timestamp-format.standard` (SQL or ISO-8601) — and the JSON `ignore-parse-errors` reproduces
Flink's per-FIELD granularity at every nesting level (a bad value nulls just that value; only a
structurally bad document drops the message). DECIMAL-bearing JSON schemas keep the arrow-json
path for its raw number literals, but the decimal columns decode as raw *text*
(`coerce_primitive`) and convert through the same `BigDecimal` + HALF_UP-or-NULL — arrow-json's
own decimal parse truncates extra fraction digits and errors on precision overflow, which silently
diverged from Flink on valid data.

The same reasoning holds on the way OUT: the Kafka sink's CSV encode (`native/src/csv_encode.rs`)
is hand-rolled against Jackson's `CsvEncoder` semantics rather than arrow-csv's writer, whose
envelope cannot be configured into Jackson's — Jackson's "loose" quote decision (25+ UTF-16 units
always quote; anything at or below `max(delimiter, quote)`, the escape char, or a bare backslash
quotes), raw never-quoted numbers/booleans/null-literals, doubled quote and escape characters,
and the joined-array single-field form have no arrow-csv counterparts. Pinned byte-for-byte
against `CsvRowDataSerializationSchema` in `NativeKafkaCsvEncoderTest`. FLOAT/DOUBLE columns
spell through a byte-exact port of the legacy (JDK ≤ 18) `Double.toString`/`Float.toString`
algorithm (`native/src/jdk_double.rs`) on both the CSV and JSON sinks. The spelling is
JDK-version-dependent — JDK 19 switched to shortest-representation digits, which differ from the
legacy output on ~0.3% of random doubles and ~11% of random floats — so a runtime probe spells a
fixed corpus (seeded with values where the two algorithms disagree) on both sides at plan time
and declines the column on mismatch; the parity target is JDK 17. The float-to-string CAST still
stays on the host — a separate follow-up.

The envelope — what parses, what fails, and the produced value — is pinned message-by-message
against Flink's own deserializers (`CsvDecodeParityTest` / `JsonDecodeParityTest`, no containers
needed: Flink's format classes are on the test classpath and referee every scenario). Those tests
are how the non-obvious behaviors were settled: Java's `appendFraction(…, 0, 9, true)` accepts a
bare trailing decimal point in a timestamp, `java.sql.Date.valueOf` leniently normalizes a day past
the month's end (`2020-02-31` → `2020-03-02`), java.time's SMART resolver reads hour 24 as
midnight (rolling a timestamp to the next day, leaving a bare TIME at 00:00), TIME silently
discards a parsed fraction (`toSecondOfDay() * 1000`), and JSON's skip mode keeps a row whose
field failed — all reproduced.

## Deliberate residual divergences

All are **accept-where-Flink-rejects** (or the reverse) on data that never decodes to a different
value — a job that runs on both engines produces identical results.

- **A trailing `Z` is tolerated on any timestamp column.** Flink's `*_WITH_LOCAL_TIMEZONE` formats
  *require* the literal `Z` and the plain-timestamp formats *forbid* it, but the Arrow boundary
  schema maps `TIMESTAMP` and `TIMESTAMP_LTZ` to the same nanosecond type, so the decoder cannot
  tell the columns apart. The parsed value is identical with or without the `Z`, so the union of
  both shapes is accepted rather than plumbing an LTZ marker through the boundary for a pure
  strictness gain.
- **Java-only numeric exotica are rejected**: hex float literals (`0x1.8p1`) and expanded ISO years
  beyond four digits (`+10000-01-01`) fail natively where Java parses them.
- **Whitespace trimming is Unicode.** Java's `String.trim` strips only chars ≤ U+0020; Rust's
  `trim` also strips exotic Unicode whitespace, so a number padded with e.g. a non-breaking space
  parses natively where Flink fails.
- **An unterminated quote parses as field content** (csv-core prefers *a* parse over *no* parse)
  where Jackson throws on EOF inside a quote.
- **A message holding several CSV records emits only the first** — same as Flink (Jackson's
  `readValue` reads one), noted here because the pre-rewrite native decode emitted them all.
- **A float token under a JSON STRING column fails loudly instead of echoing.** Flink echoes the
  producer's raw literal (`1.50` stays `"1.50"`), but the tape parse discards it and Flink's own
  two decode paths already disagree on the rendering (the parser path echoes raw, the tree path
  re-renders via `Double.toString`), so the native decode fails the job rather than silently pick
  one. Integer, boolean, and float-free container values echo exactly (containers serialize to
  Jackson's compact form, duplicate keys collapsing last-value-first-position). Under
  `ignore-parse-errors` the field nulls — a null where Flink keeps a value, the one lenient-mode
  value divergence. On the Jackson-faithful retry walk (below) the raw literal IS available, so a
  float token at a scalar STRING position echoes exactly there; a float inside an echoed
  *container* keeps this rule on both paths (the tree rendering re-spells it).
- **The Jackson-faithful retry covers the plain `json` format only.** Messages simd-json rejects
  but Jackson tokenizes — out-of-range number literals (converted per field from the raw text),
  raw control characters inside strings (`ALLOW_UNESCAPED_CONTROL_CHARS`), content trailing the
  root document (never read) — re-decode through a token walk that ports Flink's converters
  (`native/src/json_retry.rs`) and rewrite into sanitized rows for the fast-path appenders. The
  CDC envelope dialects keep the spec-strict parse (their `old`-presence pre-scans mirror the
  skip conditions row for row), so a Jackson-only CDC message still fails/drops as before. Two
  tokenizer residuals, both message-level failures exactly like before the retry existed: an
  unpaired `\u` surrogate escape is rejected (Jackson carries the lone surrogate in its UTF-16
  text, which Rust strings cannot hold), and invalid UTF-8 stays rejected.
- **A converter walk that runs past end-of-input fails/drops the message instead of hanging.**
  Certain cursor drifts make Flink's object loop poll a null token forever (`nextToken()` at
  end-of-input never yields the END_OBJECT the loop exits on) — a hung job. The native walk
  detects the overrun and fails the message in strict mode / drops it in skip mode; any output
  at all diverges from a hang, and a loud failure is the sane floor.
- **Top-level JSON arrays fan out with the tree walk's element granularity.** Flink's `json`
  format turns an array-rooted message into one row per element, and the native decode does the
  same (both subpaths — the simd tape walk fans elements directly, the decimal-bearing arrow-json
  path splits the validated document into raw element slices so decimal literals survive). On a
  bad ELEMENT the two Flink paths disagree: in strict mode both fail the message (reproduced — a
  non-object element or a failing value fails the job); under `ignore-parse-errors` the tree path
  drops the element alone (its `result != null` filter) while the parser path hands the collector
  a null row whose fate is pipeline wiring — the standard non-upsert Kafka collector throws, and
  the deserializer's catch then swallows the REST of the message, keeping only the prefix. The
  native decode pins the tree path's deterministic per-element drop (a bad value inside an
  element stays the usual per-field null). Two parser-path artifacts are deliberately not
  reproduced: a malformed array document keeps its already-collected prefix elements in Flink
  (natively the whole message drops, like any structurally bad document), and a NESTED-array
  element garbles the parser's element loop (Flink misparses the message tail
  nondeterministically; natively the element drops alone).
- **Skip granularity on a decimal-bearing JSON schema is per message/element for non-decimal
  errors.** arrow-json is all-or-nothing per document, so a bad non-decimal value drops the whole
  message (or the whole fanned-out array element) where Flink nulls the field; the decimal cells
  themselves skip per field. The simd path (every schema without a DECIMAL) has Flink's exact
  per-field granularity. On a decimal-bearing **Maxwell/Canal** table the same all-or-nothing
  behavior can trip the `old`-presence alignment check under skip mode — a loud failure where
  Flink skips, never a silent divergence.
- **TIME/VARBINARY leaves on a decimal-bearing JSON schema ride arrow-json as coerced text**, and
  the coercion erases the token's type: a number or boolean token under such a column converts
  from its rendered literal, so a base64-shaped literal (`42`, `true`) decodes into a VARBINARY
  where Flink's `getBinaryValue` fails the job — accept-where-Flink-rejects, decimal-path only
  (the simd path sees the token type and fails like Flink). Likewise, the quote-consuming base64
  shapes (below) null the field on this path instead of dropping the message — the columns are
  already built when the text converts.
- **Jackson's quote-consuming base64 shapes drop the whole message under `ignore-parse-errors`.**
  A base64 group cut after one character or after a single `=` makes Jackson's streaming decoder
  read — and consume — the string's closing quote before throwing; the corrupted parser then
  fails outside Flink's per-field catch and the deserializer's message-level catch swallows the
  document. Parity-pinned and reproduced on the simd path by a pre-scan (lenient, BINARY-bearing
  schemas only) that drops such a message before anything is appended.

## Options gated to fallback (not divergences — the query runs on Flink)

- `csv.escape-character`: Jackson unescapes in *unquoted* fields too (parity-pinned); csv-core's
  escape is quoted-context only, so the option is refused rather than half-reproduced.
- A non-ASCII `field-delimiter`/`quote-character` (csv-core splits on bytes), a `null-literal`
  containing a newline, and ARRAY/ROW CSV columns (Jackson's `array-element-delimiter` layer).
- `json.fail-on-missing-field = true`: a missing field is null natively (Flink's default mode);
  the fail mode isn't modeled.
