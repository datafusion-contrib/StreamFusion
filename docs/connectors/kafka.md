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

## Flink release lines

Each Flink line selects the Kafka connector release published for it, together with the Kafka
client that connector was built against.

| | Default build | `flink-1.18` build |
| --- | --- | --- |
| Flink | 2.2.1 | 1.18.1 |
| Kafka connector | `flink-connector-kafka:5.0.0-2.2` | `flink-connector-kafka:3.2.0-1.18` |
| Kafka client | 4.2.0 | 3.4.0 |

Pinning the client matters because a direct dependency is needed for source compilation, and
naming another version silently overrides the connector's transitive client and changes producer
semantics.

`3.2.0-1.18` is built against Flink **1.18.0** while the StreamFusion 1.18 build targets
**1.18.1**, and it is the final Kafka connector release published for the 1.18 line. Later
connector releases target 1.19 and newer, so Kafka fixes do not reach this line. That combination
is admitted deliberately rather than by default: it is exercised on every change by the
`streamfusion-kafka` module suite under `-Pflink-1.18`, and by Flink's own unchanged
`DynamicKafkaTableITCase`, `KafkaChangelogTableITCase`, `KafkaTableITCase` and
`UpsertKafkaTableITCase` running against real brokers in the upstream `kafka` suite on the 1.18
line. Both prove native execution rather than only passing.

Because Flink owns every broker interaction, a Kafka defect on this line is fixed by the
connector, not by StreamFusion. Deployments that need a newer Kafka connector need a newer Flink
line.

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

### Source admission and fallbacks

- A metadata column falls back because connector metadata is not present in the message value.
- A source bounded by anything other than `latest-offset` stays on Flink's stock source path.
- Pushed periodic bounded-out-of-orderness watermarks (`rowtime` or `rowtime - INTERVAL constant`
  with non-negative day-time or year-month intervals, including chained subtractions)
  are regenerated from each partition's decoded Arrow batches, including the common
  `TO_TIMESTAMP_LTZ(epoch_millis, 3)` rowtime form. Flink runs one generator per Kafka split and
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
