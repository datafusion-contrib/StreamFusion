# Kafka

**Status:** Partial. StreamFusion accelerates Kafka value/key serialization and deserialization;
Flink's Kafka enumerator, partition split reader/client, and sink own every broker interaction.

This boundary is intentional. It gives an accelerated job the same Kafka client, defaults,
properties, security plugins, partition discovery, offset/checkpoint behavior, producer batching,
metrics, and transaction implementation as the equivalent Flink job. StreamFusion does not
translate kafka-clients settings to librdkafka and does not create native Kafka consumers or
producers.

The `streamfusion-kafka` connector extension and the matching `streamfusion-*` format extension
must both be installed. A missing extension is a planner fallback, never a linkage failure. See
[Deployment](../deployment.md).

Nested ROW transport metadata is excluded from format input. The annotation plan removes only
marked RowKind and binary-row sidecars, recursively through containers, while retaining the
logical field names, parent nulls and existing Arrow data buffers. A user field with a similar
name remains a SQL field. The same logical view feeds JSON, CSV, Avro and Protobuf encoders;
it does not change broker ownership or the format-specific admission limits.

## Flink release lines

Each build line pins the released Kafka connector and the client used with it:

| | Default build | `flink-1.18` build |
| --- | --- | --- |
| Flink | 2.2.1 | 1.18.1 |
| Kafka connector | `flink-connector-kafka:5.0.0-2.2` | `flink-connector-kafka:3.2.0-1.18` |
| Kafka client | 4.2.0 | 3.4.0 |

The direct Kafka client dependency is needed for source compilation. Keep it aligned with
its connector: overriding the transitive client also changes the broker-facing implementation.
StreamFusion retains Flink's connector for broker interactions, so its native format paths do
not supply connector or client fixes absent from the selected releases.

The `streamfusion-kafka` module tests exercise the 1.18 pairing under `-Pflink-1.18`.
The unchanged upstream Kafka 3.2 suite exercises `KafkaChangelogTableITCase`,
`KafkaTableITCase` and `UpsertKafkaTableITCase` against real brokers on that line.
`DynamicKafkaTableITCase` belongs to the newer connector suite. Upstream Kafka invocations do
not yet have individual native-route contracts: passing these integration tests establishes
compatibility, not native execution for every invocation. See the
[upstream suite's validation evidence](../upstream-flink-suite.md) for the distinction between
suite results and native coverage.

## Source: Flink consumption, native decode

Flink continues to own topic enumeration, split assignment, offsets, checkpoints, authentication,
and the Kafka client. StreamFusion wraps Flink's source and delegates enumeration and split-state
serialization to it. At the task, Flink's partition split reader groups raw key/value bytes into
batches that StreamFusion decodes directly to Arrow in Rust, avoiding Flink's
format-to-`RowData` materialization without losing the Kafka split identity.

For JSON, the split reader copies one poll into a reusable direct `[keys][values]` byte slab and
passes only its address plus row lengths to the task-local native decoder. It does not construct,
export, or re-import Arrow binary input vectors. The decoded output still crosses the Arrow C Data
Interface, while the parser buffers, schema lookup plan, and recursive appenders remain attached to
the decoder handle across polls.

Each native source reader reports its decode work beside Flink's own source metrics, the source-side
twin of the sink's `nativeKafkaSerialization*` counters: `nativeKafkaDecodeBatches`,
`nativeKafkaDecodeRecords` (the Kafka records decoded), `nativeKafkaDecodeRows` (the rows they
produced) and `nativeKafkaDecodeNanos`. The end-to-end Kafka tests require them to account for every
record read, so a job that substituted only downstream operators can't pass as a native decode.

Native value decoding covers insert-only [JSON](formats/json.md), [CSV](formats/csv.md),
[raw](formats/raw.md), bare [Avro](formats/avro.md), [Avro Confluent](formats/avro-confluent.md),
and [Protobuf](formats/protobuf.md), plus the supported [CDC JSON](formats/cdc-json.md) envelopes
and `debezium-avro-confluent`. Format-specific type and option gaps are listed on those pages.

All Flink Kafka startup modes, consumer properties, authentication schemes, interceptors,
partition discovery, group-offset behavior, and checkpoint offset commits remain available
because Flink constructs and runs the source. The native source wrapper admits unbounded input and
`scan.bounded.mode = 'latest-offset'`; other bounded modes use Flink's stock source path. No Kafka
property translation gate exists.

JSON, bare Avro, Avro Confluent, and Protobuf can push a physical-column projection into native
decode. CSV and raw decode their complete positional record. A keyed table currently also decodes
the full value record.

### Connector metadata

Every metadata key of Flink's Kafka connector decodes natively over JSON, CSV and raw values and
the CDC JSON dialects: `topic`, `partition`, `headers`, `leader-epoch`, `offset`, `timestamp` and
`timestamp-type`. Each decoder reports which record every row came from, and the record's
metadata is gathered through that, so a record that fans out into several rows (a Canal data list,
a JSON array, an update's before and after images) repeats its metadata, and a record that yields
no row (a tombstone, a filtered or skipped message) contributes none. The split reader writes the
requested metadata into the same reusable direct buffer as the record values; the topic and
partition are the split's.

The values are Flink's exactly:

- `headers` is the `HashMap` Flink builds from the record's headers, in that map's entry order: a
  repeated key keeps its first position and its last value, and a NULL value stays NULL. The
  order is observable downstream (map functions, casts to STRING, JSON sinks), so it is not left to
  chance.
- `leader-epoch` is NULL for a record without one.
- `timestamp` is the record's timestamp as epoch milliseconds, `-1` included, and `timestamp-type`
  is `NoTimestampType`, `CreateTime` or `LogAppendTime`.

Reading a header by key (`hdrs['trace']`) stays native. Casting a header's bytes to `STRING` is a
VARBINARY-to-VARCHAR cast native Calc doesn't run yet, so such a query stays on Flink. Flink casts
the connector's `NOT NULL` headers map into a nullable `headers` column; Flink 2.2 runs that cast as
the identity, but 1.18 rebuilds the map, so on 1.18 a query that emits the whole map from a nullable
column stays on Flink. Declaring the column `MAP<STRING, BYTES> NOT NULL` needs no cast.

A watermark over a `TIMESTAMP_LTZ(3)` metadata rowtime is regenerated natively, like a physical
rowtime. `KafkaMetadataParityTest` pins every key against Flink's own record deserializer, including
records no modern broker produces (no timestamp type, no leader epoch).

On ten million JSON records reading `partition`, `offset`, `timestamp` and `headers`, a mini-batch
aggregate ran 1.68× faster than the previous route (Flink's decode feeding native operators) and
1.93× faster than stock Flink. See the
[benchmark archive](../benchmarks/kafka-metadata-20261027/README.md).

### Source admission and fallbacks

- Connector metadata decodes natively (above). A value format's own metadata (a CDC format's
  `value.*` keys, such as Canal's `value.database`), a keyed table that reads metadata, a table
  whose columns are all metadata, and connector metadata over Avro, Avro Confluent, Debezium Avro
  Confluent or Protobuf values (whose native decoders don't compose it yet) stay on Flink.
- A `TIMESTAMP(3)` metadata rowtime reads the record timestamp through the session time zone and
  stays on Flink; declare it `TIMESTAMP_LTZ(3)` to keep the watermark native.
- A source bounded by anything other than `latest-offset` stays on Flink's stock source path.
- Pushed periodic bounded-out-of-orderness watermarks (`rowtime` or `rowtime - INTERVAL constant`
  with non-negative day-time or year-month intervals, including chained subtractions)
  are regenerated from each partition's decoded Arrow batches, including the common
  `TO_TIMESTAMP_LTZ(epoch_millis, 3)` rowtime form and a `TIMESTAMP_LTZ(3)` metadata rowtime. Flink runs one generator per Kafka split and
  combines them with its normal minimum/idleness logic. On-event emission, watermark alignment,
  connector-defined source watermarks, CDC changelog tables, and other expressions stay on Flink.
  YEAR, MONTH and YEAR TO MONTH use Flink's calendar subtraction. The decoder computes
  `MAX(rowtime - interval)` before forwarding the batch and carries this candidate separately
  from its maximum event timestamp, so month-end clamping cannot lose the maximum candidate.
  The source serializes the shared native expression plan and creates one evaluator per split
  reader. Projection remapping and source sharing retain the expression's typed literals and
  column references; native handles remain local to the reader and are closed with it.
- Keyed native decoding currently requires a single raw key field over a supported insert-only
  value format. Other key formats/shapes stay on Flink.
- CDC values combined with `key.format` stay on Flink.
- Unsupported format options or logical types stay on Flink as documented by the format page.

## Sink: native encode, Flink production

The native serialization operator imports one Arrow batch and emits the final heap `byte[]` key
and value records. Those pre-serialized records feed Flink's unmodified `KafkaSink` for every
delivery guarantee: `none`, `at-least-once`, and `exactly-once`.

The operator creates one native encoder plan at `open`: format options, logical types, field names,
and key/value projections cross JNI and are parsed once rather than once per Arrow batch.

Consequently Flink owns producer construction, partitioning, batching, compression, callbacks,
metrics, transaction naming, checkpoint preparation, commit, abort, and restore. Producer
`properties.*` are passed to kafka-clients unchanged. Exactly-once still requires the normal
`sink.transactional-id-prefix`; both transaction naming strategies supported by the installed
Flink connector remain Flink behavior.

Native encoding covers plain insert-only Kafka values, supported CDC envelopes, and
`upsert-kafka` key/value/tombstone output using JSON, CSV, raw, Avro, Avro Confluent, or Protobuf.
Key and value formats are resolved independently. Native sink substitution requires one fixed
`topic`; topic patterns and multi-topic routing stay on Flink.

### Sink fallbacks

- A keyed ordinary `kafka` sink; use `upsert-kafka` for key/value output.
- A topic pattern, topic list, or other non-fixed-topic routing.
- Explicit key/value projection, key prefix, or `EXCEPT_KEY` projection.
- Non-default sink partitioners, sink-side buffer flushing, or writable metadata.
- A changelog/parallelism shape whose host translation inserts ordering or materialization that
  the substituted native boundary cannot preserve.
- A nullable query field assigned to a `NOT NULL` target, or a bounded `CHAR`/`VARCHAR` or
  `BINARY`/`VARBINARY` target while `table.exec.sink.type-length-enforcer` is enabled. The stock
  sink path preserves Flink's configured fail/drop and trim/pad/error behavior.
- Unsupported format options, types, or a missing format artifact.

## Copy cost

The source necessarily receives Kafka payloads as JVM byte arrays and copies them once into its
reusable direct batch slab before JNI decode, and
the sink materializes one final JVM byte array per encoded key/value because kafka-clients consumes
that representation. For structured JSON/Avro/Protobuf/CSV workloads, parsing, validation, type
conversion, and serialization are generally the larger costs; raw or very small records are the
important exception where copying and per-record object overhead can dominate. This trade keeps
Kafka semantics literally identical to Flink and can be benchmarked again if the codec work later
makes the byte-array boundary the measured bottleneck.

If a native decode fails after other partition batches from the same poll have decoded,
the source closes those un-emitted Arrow batches immediately before propagating the error.
This releases staged off-heap memory without waiting for the abandoned-batch Cleaner.
