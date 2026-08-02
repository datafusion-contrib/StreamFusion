# Avro Confluent

**Status:** Native for both decode and encode, backed by Confluent Schema Registry lookup and
registration. The same row-type gates as [Avro](avro.md) apply, plus the registry-option limits
below. The combined `debezium-avro-confluent` format — Avro Confluent framing inside a Debezium
changelog envelope — is native too; see [Debezium Avro Confluent](#debezium-avro-confluent) below.

This page covers Confluent Schema Registry-framed Avro: frames carry a registry-assigned schema id
instead of an inline schema. The row-type gates, timestamp/map-encoding quirks, and `BINARY(n)`/
`TIME(0)` gaps are shared with bare [Avro](avro.md) — this page covers what's specific to the
registry and to the encoding it hard-wires.

## Decode

Each frame's writer schema is fetched from the registry by id at runtime — the same lazy per-id
lookup Flink's own deserializer makes — following mid-stream schema evolution. Because that lookup
is per-batch JVM work, Avro Confluent decode runs on the shallow native decode operator and on the
native [Kafka](../kafka.md) source's split reader routed through the JVM (see
[Avro's decode section](avro.md#decode) for the general split-reader/decode-operator split).

A secured registry is native for two header-only auth schemes, sent on every fetch:

- `basic-auth.credentials-source = 'USER_INFO'` (with `basic-auth.user-info`)
- `bearer-auth.credentials-source = 'STATIC_TOKEN'` (with `bearer-auth.token`)

Both send the Confluent client's exact Authorization header.

Row-type gates, `TIME(0)`, and `BINARY(n)` are identical to bare Avro decode. Unlike bare Avro,
`avro-confluent` has neither an `avro.encoding` option nor `avro.timestamp_mapping.legacy`: Flink
hard-wires it to binary encoding and the legacy timestamp mapping, an asymmetry the native gate
mirrors exactly.

### Decode fallback conditions

| Condition | Why |
|---|---|
| A row type Flink's own avro-confluent factory rejects at job submission (RAW and other unmapped types, TIMESTAMP_LTZ under the always-active legacy mapping, TIMESTAMP/TIME precision beyond the mapping's limit, a non-string map key) | Declined at plan time, reproducing Flink's own submission failure — the same derivation as bare [Avro](avro.md#decode-fallback-conditions). |
| `TIME(0)` column | Same second-vs-millisecond gap as bare Avro. |
| `BINARY(n)` column | Same fixed-size boundary gap as bare Avro (decode only — encode is fine, below). |
| An explicit reader `schema` option | Not translated to the native registry client. |
| Any `ssl.*` registry option | Not translated. |
| Pass-through client `properties` | Not translated. |
| A credentials source beyond `USER_INFO`/`STATIC_TOKEN` (`URL`, `SASL_INHERIT`, OAuth/`CUSTOM` bearer) | Needs the registry URL's userinfo, the Kafka JAAS login, or a token flow — not a header the format options alone supply. |
| A translated source missing its credential | No credential to build the header from. |
| Basic and bearer auth configured together | The Confluent client's own `ConfigException`; the fallback lets Flink raise it. |

## Encode

The writer schema is derived the same way as bare Avro — Flink's own converter, shipped verbatim.
Registration and framing are what's specific here.

- **Subject** — auto-completed from the single fixed topic (`<topic>-value`/`<topic>-key`), the way
  Flink's Kafka factories do; never overrides an explicit subject.
- **Registration** — the schema is registered once at sink open
  (`POST /subjects/<subject>/versions`), and the returned id frames every message. A rejected
  registration (an incompatible schema, an unreachable registry) fails the job, the same way Flink's
  serializer fails on its first record.
- **Encoding** — always binary framing under the legacy timestamp mapping. `avro-confluent` has
  neither an `avro.encoding` option nor `avro.timestamp_mapping.legacy`; Flink hard-wires both, and
  the native gate mirrors that (contrast [Avro](avro.md#encode), which honors the legacy-mapping
  option both ways).

The same converter quirks as bare Avro apply: epoch-millisecond timestamps regardless of the
schema's declared unit, `java.util.HashMap`-iteration-order map encoding (including the
nine-keys-per-bucket treeification edge case), and a NULL map key failing the record at runtime. See
[Avro's encode section](avro.md#encode) for the full detail. `BINARY(n)` columns encode fine even
though decode declines them.

Auth on registration is native for the same two header-only schemes as decode (`USER_INFO` basic
auth, `STATIC_TOKEN` bearer auth).

### Encode fallback conditions

| Condition | Why |
|---|---|
| A row type the writer-schema derivation rejects (RAW, intervals, TIME(p) with p>3, TIMESTAMP beyond the legacy mapping's precision, TIMESTAMP_LTZ, a non-string map key) | Declined at plan time; Flink raises its own submission error. |
| `TIME(0)` column | Same gap as decode. |
| An explicit `schema`, any `ssl.*` option, or pass-through `properties` | Not translated to the native registry client. |
| A credentials source beyond `USER_INFO`/`STATIC_TOKEN` | Same registry-auth limit as decode. |
| A translated source missing its credential, or basic and bearer auth together | Same as decode. |
| A NULL map key | Runtime failure, matching Flink. |
| Missing `streamfusion-avro-confluent-registry` JAR | The provider seam treats a missing optional format module as an absent native format, not a linkage failure. |

Both `avro` and `avro-confluent` are legal `upsert-kafka` key and value formats (Avro is
insert-only); the key format serializes the primary-key projection as its own format instance.

## Debezium Avro Confluent

`debezium-avro-confluent` wraps Avro Confluent framing inside Flink's Debezium changelog envelope —
the Avro-framed counterpart to the four JSON CDC dialects (`debezium-json`, `canal-json`,
`maxwell-json`, `ogg-json`; see [CDC JSON](cdc-json.md)). It's the one non-upsert case where
changelog input — including UPDATE_BEFORE — is admitted to an ordinary `kafka` sink, since the CDC
encoding format itself requests the full changelog from the planner.

### Encode

Each changelog row wraps in Flink's exact envelope record — before/after images by row kind, op
`c`/`d` — Confluent-framed against the registered ENVELOPE schema. Flink derives the envelope with a
nullable root, so the registered schema is a `[null, record]` union, and every frame carries the
union's branch marker exactly as Flink's datum writer emits it.

Subject auto-completion, open-time registration, plain-URL registry, and the hard-wired legacy
timestamp mapping all follow the plain `avro-confluent` sink (above). A PRIMARY KEY on such a table
is allowed (Flink permits a PK only alongside a CDC value format) and, as in Flink, produces no key
output without `key.format`.

Byte parity is pinned per row kind × option mode against Flink's envelope serializer, and a broker
test diffs a native updating aggregate's debezium topic against stock Flink's, record for record.

### Decode

The reader schema is derived from the envelope row type exactly as Flink derives it. Each frame's
writer schema is fetched from the registry by id — following mid-topic schema evolution — and
rebuilt onto the reader's record names: one record copy per image position, since Debezium's schema
references a single `Value` record for both `before` and `after`. The envelope fans out to
changelog rows with the image payloads reconciled like plain `avro-confluent` columns
(parity-pinned against Flink's own deserializer — `DebeziumAvroDecodeParityTest`).

Null and empty messages are tombstones and are skipped. Corrupt messages (an unknown `op`, a null
pre-image on update/delete) fail the job, as Flink does — the format defines no
`ignore-parse-errors`.

It shares plain `avro-confluent`'s registry-option coverage and fallback causes (`USER_INFO`/
`STATIC_TOKEN` native; explicit `schema`, SSL stores, client `properties`, and the other credential
sources fall back), and the avro type gates run over the envelope, so TIMESTAMP_LTZ/TIME(0)/
BINARY(n) and the other legacy-mapping exclusions apply to the physical columns.

### Debezium Avro Confluent fallback conditions

| Condition | Why |
|---|---|
| Row-type and registry-option gates | Same as plain `avro-confluent` — see the tables above. |
| The `schema-include` envelope wrapper | Not supported. |
| A metadata or computed column | Not supported on the CDC decode path. |
| Nested Maxwell/Canal columns, Canal's `database.include`/`table.include` filters | Gaps in the sibling JSON dialects, not this format — see [CDC JSON](cdc-json.md). |
