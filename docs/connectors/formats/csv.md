# CSV

**Status:** Native for both decode and encode, parity-pinned against Flink's own converters
(`CsvDecodeParityTest` for decode), with the gaps enumerated below. See
[Kafka](../kafka.md) for the connector-level table shapes (`kafka`, `upsert-kafka`) CSV can be
used in, and [Configuration](../../configuration.md) for the global native switches.

## Decode (Kafka source)

Records are split with csv-core and fields converted with Flink-exact text parsers, honoring
`field-delimiter` (including `\t`/`\uXXXX` escaped forms), `quote-character`,
`disable-quote-character`, `allow-comments`, and `null-literal` natively.

Numeric and boolean trimming follows Java `String.trim()` (characters at or below U+0020);
non-breaking spaces remain invalid numeric input. Integer and decimal strings accept Java's
BMP decimal digits with exact width and overflow checks; CSV decimals preserve Flink's
untrimmed input semantics. Float suffixes apply only to decimal literals, so `NaNf` and
`InfinityD` fail conversion. Malformed Unicode numeric or timestamp fields fail normally;
`ignore-parse-errors` nulls the affected field and preserves the rest of the row.

Decimal zero accepts the full Java exponent range without constructing exponent-sized powers
of ten. Nonzero rescaling detects precision overflow and complete fractional discard before
allocating a divisor, while retaining Java's scale and supported-integer-range errors.

Decode supports BOOLEAN, integer and floating-point types, CHAR/VARCHAR, DATE, TIMESTAMP,
TIMESTAMP_LTZ, and DECIMAL. It does not admit TIME, binary, or container types.

| Fallback condition | Why |
|---|---|
| `escape-character` | Jackson unescapes an escape character in unquoted fields; csv-core can't. |
| A non-ASCII delimiter or quote character | Not representable in the native decode. |
| `null-literal` containing a newline | Not representable in the native decode. |
| A **TIME**, **BINARY**, or **VARBINARY** column | Outside the native CSV decode converter set. |
| An **ARRAY/ROW/MAP/MULTISET** column | Needs conversion or Jackson's nested `array-element-delimiter` layer, which decode doesn't implement. |
| Any other type outside the list above | Outside the natively-converted set. |

## Encode (Kafka sink)

Serialization is byte-identical to `CsvRowDataSerializationSchema` (Jackson CSV underneath): one
record per row, no trailing line separator, and Jackson's default "loose" quote decision — a value
longer than 24 UTF-16 units always quotes; otherwise any character at or below
`max(delimiter, quote)`, the configured escape character, or (with no escape configured) a
backslash triggers quotes. Quote characters double inside quotes, and a configured escape
character doubles itself. Numbers, booleans, and the null literal are always written raw (never
quoted).

Nested ROW/ARRAY join into a single CSV field via `csv.array-element-delimiter`, with raw
elements. BINARY is base64. DATE/TIME use Flink's ISO spellings (seconds always present, fraction
trimmed); TIMESTAMP uses the SQL spelling; TIMESTAMP_LTZ uses Flink's `'Z'` suffix. DECIMAL
defaults to the plain, exact-scale spelling — `csv.write-bigdecimal-in-scientific-notation`'s
documented default of `true` is dead in Flink's own factory (it reads the option through
`getOptional`, which never yields the declared default), so only an **explicit** `true` selects
`stripTrailingZeros().toString()`.

Encode covers scalars plus **depth-one ARRAY and ROW** (Flink's own schema converter refuses
deeper nesting) — minus TIME, which falls back (see below). This is broader than decode's
scalars-only coverage.

| Option | Effect |
|---|---|
| `csv.field-delimiter` | First character, Java-unescaped. |
| `csv.quote-character` | Quote character. |
| `csv.disable-quote-character` | Disables quoting entirely. |
| `csv.array-element-delimiter` | Joins nested ROW/ARRAY elements into one field. |
| `csv.escape-character` | Escape character. |
| `csv.null-literal` | Literal written for null values. |
| `csv.write-bigdecimal-in-scientific-notation` | Only an explicit `true` takes effect — see above. |
| `csv.allow-comments`, `csv.ignore-parse-errors` | Accepted and ignored (deserialization-only, as in Flink's own serializer). |

### Sink fallbacks specific to CSV

| Fallback condition | Why |
|---|---|
| A **TIME** column | SQL DDL resolves every TIME precision to TIME(0), whose Arrow boundary is second-granular, while Flink's CSV converter prints whatever milliseconds the value carries. Millisecond-preserving precisions ≥ 1 would run, but the SQL planner never produces them. |
| MAP/MULTISET, RAW, second-level ARRAY/ROW nesting, or TIMESTAMP_WITH_TIME_ZONE | Flink's own converter can't serialize these either. |
| An option value Flink's factory refuses (a quote character alongside `csv.disable-quote-character`, a multi-character or malformed-escape delimiter, a malformed boolean) | Flink then raises its own validation error. |
| A non-ASCII delimiter, quote, array-delimiter, or escape character | Not representable in the native encoder. |
| `csv.null-literal` containing a line break | Not representable in the native encoder. |
| A FLOAT/DOUBLE column when the runtime JDK float-spelling probe fails | Shared with JSON — see [JSON](json.md#sink-fallbacks-specific-to-json). FLOAT/DOUBLE otherwise spells raw and unquoted (NaN/Infinity included) under the same probe. |

General sink-shape fallbacks that apply to every value format (an upsert-materialized sink, a
keyed ordinary `kafka` table, `sink.parallelism` on a changelog input, and so on) are covered on the
[Kafka](../kafka.md) page, not repeated here.

Timestamp decode and encode retain years 0001–9999 and all runtime fractional nanos, including
fractions present below the declared precision. The columnar timestamp uses separate millisecond
and fractional buffers; no signed 64-bit nanosecond conversion sits on the format boundary.

## Flink 1.18 decimal encoding

The development profile reproduces the released host's older Jackson decimal-node normalization,
including trailing-zero removal before plain output and nested CSV decimal text. The current-line
encoding remains unchanged. See [Flink line compatibility](../../flink-compatibility.md) for the
per-line admission and validation status.
