# Avro

**Status:** Native for both decode and encode. A short list of row-type and option gaps falls back
to Flink's own Avro (de)serializer, listed below.

This page covers the bare `avro` format. Flink derives its schema from the table row type; records
carry neither a Confluent schema id nor registry framing. For the Confluent Schema Registry-framed
variant, see [Avro Confluent](avro-confluent.md). For changelog envelopes
(Debezium/OGG/Maxwell/Canal JSON), see [CDC JSON](cdc-json.md).

## Decode

Avro decode runs on the shallow native decode operator and on the native [Kafka](../kafka.md)
source's split reader, over the same row type Flink's own Avro factory accepts. The split reader
routes decode through the JVM only when a format needs per-batch JVM work — as the registry lookup
does for [Avro Confluent](avro-confluent.md#decode).

Parity behaviors, pinned against Flink's converter by `AvroDecodeParityTest`:

- Every avro timestamp long decodes as epoch millis regardless of the schema's declared unit —
  Flink's converter has no micros path, so a `timestamp-micros` schema resolves through the same
  millis read in both engines.
- TINYINT/SMALLINT narrow with Java's wrapping `byteValue()`/`shortValue()`.
- A decimal whose digits exceed the declared precision decodes to NULL.
- A null Kafka value (a tombstone) is dropped silently.

`avro.timestamp_mapping.legacy` is honored in both directions: the corrected (`false`) mapping
derives `local-timestamp-millis`/`micros` for TIMESTAMP and unlocks TIMESTAMP_LTZ up to precision 6
natively — but only at the top level. Flink itself rejects a nested TIMESTAMP_LTZ under the
corrected mapping (its converter factory drops the flag for nested rows, and its schema derivation
drops it inside collections), so those shapes fall back to reproduce Flink's own submission
failure.

### Decode fallback conditions

| Condition | Why |
|---|---|
| A row type Flink's own Avro factory rejects at job submission (RAW and other unmapped types, TIMESTAMP_LTZ under the legacy mapping, TIMESTAMP/TIME precision beyond the mapping's limit, a non-string map key) | Declined at plan time — the provider runs the same schema/converter derivation Flink's factory runs, so the fallback reproduces Flink's exact submission failure instead of a native-planner abort. |
| `TIME(0)` column | Flink keeps an avro `time-millis` value's full milliseconds in a TIME(0) column, while the Arrow boundary's second-precision form would truncate them. TIME(1..3) is native and exact — but SQL DDL resolves every `TIME(p)` column to TIME(0) in the catalog, so a TIME column in a SQL-defined table always stays on Flink; TIME(1..3) is reachable only through Table-API-defined schemas. |
| `BINARY(n)` column | Flink accepts avro `bytes` of any length into a BINARY(n) column, which the boundary's fixed-size form can't hold. (VARBINARY is native; BINARY(n) *encodes* fine — see below.) |
| `avro.encoding = 'json'` | Avro's JSON encoding is a different wire format the native decode doesn't read. |
| A nested TIMESTAMP_LTZ column under `avro.timestamp_mapping.legacy = false` | Flink's own converter factory and schema derivation both reject this shape; the native gate mirrors the submission failure. |

## Encode

The writer schema is derived from the sink row type with Flink's own converter and shipped to the
native encoder verbatim, so record names, null-first unions, and logical types match Flink's bytes
exactly.

Two Flink converter behaviors are reproduced deliberately, not "fixed":

- Every timestamp flavor is written as an epoch-millisecond long even into a `*-timestamp-micros`
  schema (Flink calls `toEpochMilli` unconditionally, so micros values read 1000x small and
  sub-millisecond digits are dropped).
- Map/multiset entries serialize in `java.util.HashMap` iteration order (Flink copies each map
  through a HashMap before Avro walks its `entrySet`), including first-position/last-value
  duplicate-key collapse. A NULL map key fails the record at runtime, as Flink's converter does — a
  data-dependent failure that can't gate at plan time.

`avro.timestamp_mapping.legacy` is honored both ways (contrast
[Avro Confluent](avro-confluent.md#encode), which hard-wires it).

`BINARY(n)` columns, which decode declines (above), encode fine: the schema says `bytes` and the
fixed-size boundary value widens losslessly.

One loud native-only failure mode: nine map keys sharing one hash bucket of a 64-slot-or-larger
table would make Java treeify the bin and iterate in red-black-tree order, which the native encode
does not reproduce — it fails the record with an explicit error instead of silently reordering.

Both `avro` and `avro-confluent` are legal `upsert-kafka` key and value formats (Avro is
insert-only), with the key format serializing the primary-key projection as its own format
instance.

### Encode fallback conditions

| Condition | Why |
|---|---|
| A row type the writer-schema derivation rejects (RAW, intervals, TIME(p) with p>3, TIMESTAMP beyond the active mapping's precision, TIMESTAMP_LTZ under the legacy mapping, a non-string map key) | Declined at plan time; Flink raises its own submission error. |
| `TIME(0)` column | Same second-vs-millisecond precision gap as decode. |
| A NULL map key | Fails the record at runtime, matching Flink's converter — a data-dependent failure that can't gate at plan time. |
| Missing `streamfusion-avro` JAR | The provider seam treats a missing optional format module as an absent native format, not a linkage failure. |
