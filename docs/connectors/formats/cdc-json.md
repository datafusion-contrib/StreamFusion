# CDC JSON

**Status:** All four dialects encode and decode natively, nested columns and wide schemas
included; the decode fallbacks are listed below.

Four Kafka value formats carry a Flink changelog as a JSON envelope: `debezium-json`, `ogg-json`,
`maxwell-json`, and `canal-json`. They share one mechanism — an envelope wrapping before/after row
images plus an operation marker — and differ in field names and in how much of the row image
survives. `debezium-avro-confluent` (the Debezium envelope with Confluent-framed Avro bodies,
rather than plain JSON) is documented on the [Avro Confluent](avro-confluent.md) page, not here.

| Dialect | Encode | Decode | Image scope |
|---|---|---|---|
| `debezium-json` | Native | Native | Full pre/post images, including nested columns |
| `ogg-json` | Native | Native | Full pre/post images, including nested columns |
| `maxwell-json` | Native | Native | Post-image plus a partial `old` (key-presence scan) |
| `canal-json` | Native | Native | Post-image plus a partial `old`; presence spans the whole `old` array |

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
- a key found only inside a nested container leaves the top-level field `null` — including a
  column name that also names a nested ROW field, an element row's field, a MAP key or a MULTISET
  element under `old`;
- a key absent from `old` altogether copies the post-image value;
- a nested value in `old` is the pre-image value as written: a partial ROW is not merged with the
  post-image's sub-fields;
- for Canal specifically, presence is checked against the whole `old` **array**, not a single
  object.

Presence is tracked per column, so the schema width is not limited. This reconstruction is
parity-pinned against Flink (`CdcDecodeParityTest`), for flat, nested and wider-than-128-column
schemas.

On ten million Maxwell messages with nested columns, a mini-batch aggregate ran 1.30× faster than
the previous route (Flink's decode feeding native operators) and 2.49× faster than stock Flink; on
Canal, 1.36× and 2.72×. See the [benchmark archive](../../benchmarks/kafka-nested-cdc-20261027/README.md).

`ignore-parse-errors` is native across all four dialects, with Flink's exact granularity: a
structurally malformed message drops whole, a bad value inside an image nulls just that one field,
and a failure partway through a fan-out (a JSON array carrying multiple logical records) keeps the
rows already emitted before the failure.

Canal's `canal-json.database.include` and `canal-json.table.include` filters are native. Flink
reads each message's top-level `database` and `table` values as Jackson text and drops the
message unless a value fully matches its Java regular expression. The database check runs first,
and both run before row conversion, so a dropped message is never corrupt for a bad image. The
native decoder validates each message and reads the same values. It caches up to 4,096 verdicts
per filter, and sends a batch's undecided values to the JVM in one call, so neither a steady set
of tables nor many sharded table names costs a JVM call per message. The JVM decides with the
pattern Flink compiles and Flink's Jackson settings, so the same edge cases apply:

- a string value is matched as written; a JSON `null` is matched as the text `null`; an object or
  array is matched as the empty string;
- a number is matched as Jackson spells it, including the `BigDecimal` spelling Flink switches to
  when the schema has a `DECIMAL` anywhere;
- a duplicated top-level key uses its last value;
- a missing `database`/`table` key or a non-object message fails the way Flink's
  `NullPointerException` does — the job fails, or the message is skipped under
  `ignore-parse-errors`.

Messages the filter keeps decode as above. TiCDC's watermark events (`TIDB_WATERMARK`) carry an
empty database and table, and its DDL events (`QUERY`) carry their database and, for table-level
DDL, their table. A filter drops whichever of them it rejects, exactly as Flink does; one that
passes with a `type` Flink doesn't decode is a corrupt message in both.

On ten million Canal messages spread over ten tables, a filtered table's mini-batch aggregate ran
1.47× faster than the previous route (Flink's decode feeding native operators) when it admitted
one table and 1.15× when it admitted all ten. On ten million messages over a million sharded table
names, where the verdict cache stays cold, it ran 1.26× faster. Against stock Flink: 1.55×, 1.35×
and 1.31×. See the [benchmark archive](../../benchmarks/kafka-canal-include-20261027/README.md).

`debezium-avro-confluent` also decodes natively — see [Avro Confluent](avro-confluent.md) for its
registry, schema-evolution, and type-coverage details, which layer the Debezium envelope onto the
same machinery as the plain `avro-confluent` source.

Fallbacks on read:

- the `schema-include` envelope wrapper;
- computed columns, and the format's own metadata (`value.*` keys such as Canal's
  `value.database`); the Kafka connector's metadata decodes natively — see
  [Kafka](../kafka.md#connector-metadata);
- a Canal include pattern Java can't compile — the scan stays on Flink, whose planner fails with
  its own pattern error.
