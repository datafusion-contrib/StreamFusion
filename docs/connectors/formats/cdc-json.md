# CDC JSON

**Status:** All four dialects encode and decode natively; decode support narrows for two of them —
see the table below.

Four Kafka value formats carry a Flink changelog as a JSON envelope: `debezium-json`, `ogg-json`,
`maxwell-json`, and `canal-json`. They share one mechanism — an envelope wrapping before/after row
images plus an operation marker — and differ in field names and in how much of the row image
survives. `debezium-avro-confluent` (the Debezium envelope with Confluent-framed Avro bodies,
rather than plain JSON) is documented on the [Avro Confluent](avro-confluent.md) page, not here.

| Dialect | Encode | Decode | Image scope |
|---|---|---|---|
| `debezium-json` | Native | Native | Full pre/post images, including nested columns |
| `ogg-json` | Native | Native | Full pre/post images, including nested columns |
| `maxwell-json` | Native | Native, flat scalar schemas only (≤ 128 columns) | Post-image plus a partial `old` (key-presence scan) |
| `canal-json` | Native | Native, flat scalar schemas only (≤ 128 columns) | Post-image plus a partial `old`; presence spans the whole `old` array |

## Encode

A CDC JSON value format is the one case where a [Kafka](../kafka.md) sink admits a full changelog
— UPDATE_BEFORE included — rather than requiring an upsert or insert-only stream, because the CDC
encoding itself requests the full changelog from the planner.

Each row is spliced into its dialect's envelope in Flink's own field order:

- INSERT and UPDATE_AFTER rows become the envelope's **post-image**.
- UPDATE_BEFORE and DELETE rows become the envelope's **pre-image**.

The shared `json.*` options forward to the nested row serializer exactly as in Flink; in
particular, `json.encode.ignore-null-fields` also drops the envelope's own null `before`/`after`
key. A `PRIMARY KEY` is allowed on such a table (Flink permits one only alongside a CDC value
format) and, as in Flink, produces no key output unless `key.format` is also set.

Fallbacks on write:

- **`debezium-json.schema-include = 'true'`** declines, so Flink raises its own sink-side
  `ValidationException` for the schema wrapper rather than StreamFusion diverging from it.
- Canal's `database.include`/`table.include` are decode-only options in Flink; on write they're
  simply ignored, by both Flink and the native sink — not a fallback.
- `upsert-kafka` keeps Flink's own rejection of CDC value formats — parity, not a gap.

Byte parity is pinned per dialect × row kind × null-field × option combination against Flink's own
envelope serializers, and a broker test diffs a native updating aggregate's `debezium-json` topic
against stock Flink's output record for record.

## Decode

Debezium and OGG carry the **full** pre/post row image, nested columns included, straight through.

Maxwell and Canal only ever carry the full **post**-image; their `old` object is a partial diff of
just the changed fields. To reconstruct UPDATE_BEFORE, the native decoder follows Flink's own
`findValue` key-presence rule with a per-message key scan of the raw `old` value — recursive like
Jackson's `findValue`, descending nested objects and arrays, with duplicate keys collapsing to
their last occurrence:

- an explicit `null` in `old` is kept as `null`;
- a key found only inside a nested container leaves the top-level field `null`;
- a key absent from `old` altogether copies the post-image value;
- for Canal specifically, presence is checked against the whole `old` **array**, not a single
  object.

This reconstruction is parity-pinned against Flink (`CdcDecodeParityTest`).

`ignore-parse-errors` is native across all four dialects, with Flink's exact granularity: a
structurally malformed message drops whole, a bad value inside an image nulls just that one field,
and a failure partway through a fan-out (a JSON array carrying multiple logical records) keeps the
rows already emitted before the failure.

`debezium-avro-confluent` also decodes natively — see [Avro Confluent](avro-confluent.md) for its
registry, schema-evolution, and type-coverage details, which layer the Debezium envelope onto the
same machinery as the plain `avro-confluent` source.

Fallbacks on read:

- the `schema-include` envelope wrapper;
- metadata/computed columns;
- **nested Maxwell/Canal columns** — `findValue`'s recursive search could false-match a column
  name nested inside another field, so these two dialects are native only for **flat scalar
  schemas, up to 128 columns**;
- Canal's `database.include`/`table.include` regex filters — Flink honors them at deserialization
  time, but the native decoder doesn't implement the filter, so a table using either falls back.
