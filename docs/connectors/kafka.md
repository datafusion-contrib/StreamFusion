# Kafka

**Status:** Partial. StreamFusion accelerates Kafka value/key serialization and deserialization;
Apache Flink's unmodified Kafka source and sink own every broker interaction.

This boundary is intentional. It gives an accelerated job the same Kafka client, defaults,
properties, security plugins, partition discovery, offset/checkpoint behavior, producer batching,
metrics, and transaction implementation as the equivalent Flink job. StreamFusion does not
translate kafka-clients settings to librdkafka and does not create native Kafka consumers or
producers.

The `streamfusion-kafka` connector extension and the matching `streamfusion-*` format extension
must both be installed. A missing extension is a planner fallback, never a linkage failure. See
[Deployment](../deployment.md).

## Source: Flink consumption, native decode

Flink continues to own topic enumeration, split assignment, offsets, checkpoints, authentication,
and the Kafka client. Its task-side split reader groups each partition's raw key/value bytes into
batches and decodes them directly to Arrow in Rust, avoiding Flink's format-to-`RowData`
materialization without losing the Kafka split identity.

For JSON, the split reader copies one poll into a reusable direct `[keys][values]` byte slab and
passes only its address plus row lengths to the task-local native decoder. It does not construct,
export, or re-import Arrow binary input vectors. The decoded output still crosses the Arrow C Data
Interface, while the parser buffers, schema lookup plan, and recursive appenders remain attached to
the decoder handle across polls.

Native value decoding covers insert-only [JSON](formats/json.md), [CSV](formats/csv.md),
[raw](formats/raw.md), bare [Avro](formats/avro.md), [Avro Confluent](formats/avro-confluent.md),
and [Protobuf](formats/protobuf.md), plus the supported [CDC JSON](formats/cdc-json.md) envelopes
and `debezium-avro-confluent`. Format-specific type and option gaps are listed on those pages.

All Flink Kafka startup modes, bounded modes, consumer properties, authentication schemes,
interceptors, partition discovery, group-offset behavior, and checkpoint offset commits remain
available because Flink constructs and runs the source. No Kafka property translation gate exists.

### Source fallbacks

- A metadata column falls back because connector metadata is not present in the message value.
- Pushed periodic bounded-out-of-orderness watermarks (`rowtime` or `rowtime - INTERVAL constant`)
  are regenerated from each partition's decoded Arrow batches, including the common
  `TO_TIMESTAMP_LTZ(epoch_millis, 3)` rowtime form. Flink runs one generator per Kafka split and
  combines them with its normal minimum/idleness logic. On-event emission, watermark alignment,
  connector-defined source watermarks, CDC changelog tables, and other expressions stay on Flink.
- Keyed native decoding currently requires a single raw key field over a supported insert-only
  value format. Other key formats/shapes stay on Flink.
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
`upsert-kafka` key/value/tombstone output for the formats listed above. Key and value formats are
resolved independently.

### Sink fallbacks

- A keyed ordinary `kafka` sink; use `upsert-kafka` for key/value output.
- Explicit key/value projection, key prefix, or `EXCEPT_KEY` projection.
- Non-default sink partitioners, sink-side buffer flushing, or writable metadata.
- A changelog/parallelism shape whose host translation inserts ordering or materialization that
  the substituted native boundary cannot preserve.
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
