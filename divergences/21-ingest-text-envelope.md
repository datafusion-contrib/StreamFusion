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
(`native/src/csv.rs`) instead of using arrow-csv. The envelope — what parses, what fails, and the
produced value — is pinned message-by-message against Flink's own deserializer
(`CsvDecodeParityTest`, no containers needed: Flink's format classes are on the test classpath and
referee every scenario). That test is how two non-obvious behaviors were settled: Java's
`appendFraction(…, 0, 9, true)` accepts a bare trailing decimal point in a timestamp, and
`java.sql.Date.valueOf` leniently normalizes a day past the month's end (`2020-02-31` →
`2020-03-02`) — both reproduced.

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

## Options gated to fallback (not divergences — the query runs on Flink)

- `csv.escape-character`: Jackson unescapes in *unquoted* fields too (parity-pinned); csv-core's
  escape is quoted-context only, so the option is refused rather than half-reproduced.
- A non-ASCII `field-delimiter`/`quote-character` (csv-core splits on bytes), a `null-literal`
  containing a newline, and ARRAY/ROW CSV columns (Jackson's `array-element-delimiter` layer).
