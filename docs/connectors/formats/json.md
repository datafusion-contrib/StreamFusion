# JSON

**Status:** Native for both decode and encode, parity-pinned against Flink's own converters
(`JsonDecodeParityTest` for decode), with the gaps enumerated below. Covers plain `json` and,
on decode, the four CDC envelope dialects (`debezium-json`, `canal-json`, `maxwell-json`,
`ogg-json`), which decode through the same column appenders as plain JSON. See
[Kafka](../kafka.md) for the connector-level table shapes (`kafka`, `upsert-kafka`) each format
can be used in, and [Configuration](../../configuration.md) for the global native switches.

## Decode (Kafka source)

Both `timestamp-format.standard` modes (`SQL` and `ISO-8601`) are native on every JSON-decoded
path. Scalar coercion follows Flink's converters exactly: string-encoded numbers with trimming,
`Infinity`/`NaN`/suffix floats, never-failing booleans, strict `ISO_LOCAL_DATE` dates, and
integer/boolean/container echo under `STRING`. DECIMAL parses the exact raw literal with
`BigDecimal`'s HALF_UP-or-NULL rounding. `ignore-parse-errors` skips at Flink's own per-field
granularity. A top-level JSON array fans out into one row per element, matching Flink's
`processArray` (an empty array yields zero rows; a bad element fails the message in strict mode
and drops alone under `ignore-parse-errors`). TIME columns parse `SQL_TIME_FORMAT` and reproduce
Flink's silent sub-second discard (`toSecondOfDay() * 1000`, regardless of declared precision),
including java.time's SMART hour-24-is-midnight resolution. VARBINARY reproduces Jackson's exact
base64 read (whitespace between four-char groups, padding required, declared length not
enforced), down to its corrupted-input drop granularity. The CDC dialects treat an array-rooted
envelope as a corrupt message, as Flink does.

Supported column types (recursively over ROW/ARRAY/MAP/MULTISET): BOOLEAN, TINYINT, SMALLINT,
INT, BIGINT, FLOAT, DOUBLE, CHAR/VARCHAR, DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ, DECIMAL,
VARBINARY.

| Fallback condition | Why |
|---|---|
| `fail-on-missing-field = true` | Not modeled — a missing field decodes as null natively (Flink's default mode). |
| `decode.json-parser.enabled = false` | Switches Flink to its tree deserializer, whose coercion envelope differs from the parser path the native decode mirrors. |
| A column (or nested leaf) of type **BINARY** | Its fixed-size Arrow carriage can't hold arbitrary-length base64 without the length enforcement Flink's decode doesn't apply. |
| A column (or nested leaf) of an **INTERVAL** type | Outside the natively-converted type set. |
| A MAP/MULTISET key type outside CHAR/VARCHAR | Defensive only — Flink's own JSON format rejects a non-string map key at planning, so this can't reach substitution. |

Kafka discovery, startup offsets, and boundedness remain connector-owned; see
[Kafka](../kafka.md#source-flink-consumption-native-decode). Native JSON decoding runs after
Flink's partition split reader for every admitted source shape.

## Encode (Kafka sink)

Supported column types, recursively over ROW/ARRAY/MAP/MULTISET: BOOLEAN, TINYINT/SMALLINT/
INT/BIGINT, FLOAT/DOUBLE (see the spelling note below), CHAR/VARCHAR, BINARY/VARBINARY, DECIMAL,
DATE (ISO_LOCAL_DATE, with `+`/`-` EXCEEDS_PAD years past 9999/below 0), TIME, TIMESTAMP, and
TIMESTAMP_LTZ (SQL or ISO-8601). A null field inside a nested ROW follows
`encode.ignore-null-fields` exactly as Flink's recursive converter does; array elements and map
values keep explicit nulls regardless.

Map keys must be in the CHARACTER_STRING family (a MULTISET's element is its key) — Flink's own
converter throws for anything else, so a non-string-keyed column declines and Flink raises its own
error. Null map keys follow `json.map-null-key.mode`: `DROP` and `LITERAL` (with
`json.map-null-key.literal`) reproduce Flink's bytes exactly; the default `FAIL` mode fails the
record at runtime pointing at the option, as Flink's does, since a data-dependent failure can't
gate at plan time. Duplicate map keys collapse the way Jackson's `ObjectNode` does — first
position, last value — with one documented corner: a duplicate key whose value is a nested ROW
under `encode.ignore-null-fields` merges field-by-field in Flink (an `ObjectNode`-reuse artifact)
but takes the whole last value natively.

| Option | Effect |
|---|---|
| `encode.ignore-null-fields` | Drops null fields (including a CDC envelope's null `before`/`after` key). |
| `encode.decimal-as-plain-number` | Keeps the column's declared scale; the default reproduces Jackson's `stripTrailingZeros().toString()`, scientific notation included. |
| `json.map-null-key.mode` | `DROP` / `LITERAL` / `FAIL` (default) — see above. |
| `json.map-null-key.literal` | The literal string substituted under `LITERAL` mode. |

Each option set configures the format instance it belongs to, as in Flink: value options come
from `json.*`/`value.json.*`, and upsert key options only from `key.json.*` — never inherited from
the value format's settings.

### Sink fallbacks specific to JSON

| Fallback condition | Why |
|---|---|
| An out-of-range `json.*` option value | Flink's format factory raises its own validation error. |
| A field name that would need a JSON control-character escape | arrow-json spells field-name escapes lowercase where Jackson's are uppercase; value and map-key strings already escape natively in Jackson's exact form. |
| `json.map-null-key.literal` containing a line break | Not representable in the native encoder. |
| `debezium-json.schema-include = true` | Rejected by Flink's own sink factory. |
| A FLOAT/DOUBLE column when the runtime JDK float-spelling probe fails | See below. |

**The FLOAT/DOUBLE spelling probe.** The native library ports the legacy (JDK ≤ 18)
`Double.toString`/`Float.toString` algorithm — the parity target is JDK 17's spelling — but JDK 19
changed `Double.toString` to shortest-representation digits, which differ on roughly 0.3% of
random doubles and 11% of random floats. At plan time the JVM spells a fixed corpus (deliberately
containing values where the two algorithms disagree) and compares it against the native spelling;
a mismatch keeps the column on the host rather than silently diverging. Decode is unaffected
(parsing a JSON number is exact), and the reported reason is `jdk float spelling mismatch (JDK
19+)`.

General sink-shape fallbacks that apply to every value format (an upsert-materialized sink, a
keyed ordinary `kafka` table, `sink.parallelism` on a changelog input, and so on) are covered on the
[Kafka](../kafka.md) page, not repeated here.
