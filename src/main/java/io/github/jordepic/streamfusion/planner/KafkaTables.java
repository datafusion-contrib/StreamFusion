package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.kafka.KafkaConfigTranslator;
import io.github.jordepic.streamfusion.kafka.NativeKafkaSource;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Maps a Flink Kafka SQL table's options to a {@link NativeKafkaSource}, and decides whether the native
 * source can run it at all. The native path is taken only for the cases it faithfully supports — a JSON
 * value format, an explicit topic list, a supported startup mode, and consumer properties that
 * {@link KafkaConfigTranslator} can render into librdkafka. Anything else returns {@code false} from
 * {@link #isNativeKafka}, so the planner leaves Flink's own Kafka source in place (the fallback).
 */
final class KafkaTables {

  private KafkaTables() {}

  private static final String PROPERTIES_PREFIX = "properties.";
  // Native batch cap per poll (Java's max.poll.records has no librdkafka analog) and poll timeout. The
  // timeout is the most a drained poll blocks before returning empty; at a bounded source's tail the
  // reader does a couple of empty polls before concluding the split is finished, so a large timeout adds
  // dead seconds there (a 1s timeout dominated a 200k-row bounded run). Keep it short — a steady stream
  // with prefetch rarely waits on it (the queue is non-empty), so it only bounds tail latency.
  private static final int MAX_RECORDS = 8192;
  private static final long POLL_TIMEOUT_MILLIS = 100;
  // MessageDecoder format codes the native source decodes in Rust (the codes decodeFormatCode emits).
  private static final int BARE_AVRO = 4;
  private static final int PROTOBUF = 5;

  /** Whether the native Kafka source can faithfully run this scan's table. */
  static boolean isNativeKafka(org.apache.calcite.rel.RelNode node) {
    if (!(node instanceof StreamPhysicalTableSourceScan)) {
      return false;
    }
    StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) node;
    // A watermarked table routes only when the pushed watermark is a shape the source regenerates
    // (per-split bounded out-of-orderness); the scan replaces the host source, so an unreproducible
    // watermark must leave the whole scan on Flink.
    if (KafkaWatermarkSpec.of(scan) == KafkaWatermarkSpec.UNSUPPORTED) {
      return false;
    }
    return supports(FilesystemTables.options(scan));
  }

  /** The shared gate, also used by {@code build} to assume a translatable, supported table. The native
   * source decodes in Rust the same insert-only value formats the shallow decode path does — JSON (0),
   * bare Avro (4), and protobuf (5) — over the consume/topic/offset prerequisites in {@link
   * #decodeCommon}, plus a librdkafka-translatable consumer config (the native source owns the consume,
   * so its config must render to librdkafka). */
  private static boolean supports(Map<String, String> options) {
    if (!decodeCommon(options)) {
      return false;
    }
    int code = decodeFormatCode(options);
    if (code == PROTOBUF) {
      String messageClass = options.get("protobuf.message-class-name");
      if (messageClass == null || !ProtobufDescriptors.isSupportedMessage(messageClass)) {
        return false;
      }
    } else if (code != 0 && code != BARE_AVRO) {
      return false; // JSON / bare Avro / protobuf only — CSV/raw/Confluent-Avro/CDC stay on the decode path
    }
    return KafkaConfigTranslator.translate(consumerProperties(options)).isTranslated();
  }

  /** Builds the native source for a table {@link #isNativeKafka} accepted. The decode runs in Rust, so
   * the format-specific schema inputs match the shallow decode path: bare Avro derives its writer schema
   * from the row type (the same converter Flink's own {@code avro} format uses), protobuf carries the
   * reflectively-extracted descriptor + message name, JSON decodes against the exported output schema.
   *
   * <p>{@code writerType} is the full table schema the decoder parses against; {@code outputType} is what
   * the source emits — narrower when a downstream Calc's projection was pushed in. They drive the
   * pruning: JSON decodes straight to {@code outputType} (arrow-json builds only those columns), bare
   * Avro keeps {@code writerType} as the writer schema and applies {@code outputType} as a reader schema
   * (Avro resolution), and protobuf prunes its descriptor to {@code outputType}'s fields natively.
   *
   * <p>{@code rowtimeIndex} is the output column whose per-batch max feeds per-split source watermarks
   * (-1 when the table declares no watermark). */
  static NativeKafkaSource build(
      Map<String, String> options, RowType writerType, RowType outputType, int rowtimeIndex) {
    Properties props = consumerProperties(options);
    Map<String, String> librdkafka =
        new java.util.HashMap<>(KafkaConfigTranslator.translate(props).config());
    // librdkafka-specific throughput tuning with no Java analog (so not produced by the translator):
    // prefetch eagerly instead of idling 1s before refetching, and keep a deep queue so the background
    // fetcher stays ahead of the reader. Measured to lift native consume throughput meaningfully.
    librdkafka.putIfAbsent("fetch.queue.backoff.ms", "2");
    librdkafka.putIfAbsent("queued.min.messages", "1000000");
    librdkafka.putIfAbsent("queued.max.messages.kbytes", "2097151");
    String[] keys = librdkafka.keySet().toArray(new String[0]);
    String[] values = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      values[i] = librdkafka.get(keys[i]);
    }
    List<String> topics = Arrays.asList(options.get("topic").split(";"));
    boolean bounded = "latest-offset".equals(options.get("scan.bounded.mode"));
    int format = decodeFormatCode(options);
    boolean pruned = !writerType.equals(outputType);
    // Bare Avro decodes against its writer schema (datums are schema-less): derive it from the full
    // writer row type, forced non-null so the row is a record, not a ["null", record] union — matching
    // Flink's `avro`. When the output is a narrowed subset, also derive a reader schema (the output) so
    // Avro resolution materializes only the read fields.
    String avroSchema =
        format == BARE_AVRO ? AvroSchemaConverter.convertToSchema(writerType.copy(false)).toString() : "";
    String readerAvroSchema =
        format == BARE_AVRO && pruned
            ? AvroSchemaConverter.convertToSchema(outputType.copy(false)).toString()
            : "";
    // Protobuf decodes against the generated message class's descriptor, extracted by reflection (no
    // compile-time protobuf-java dependency — the class + runtime come from the Flink distribution). The
    // native side prunes the descriptor to outputType's fields (a no-op when outputType is the full
    // message), so ptars builds only the read columns.
    String messageClass = options.get("protobuf.message-class-name");
    byte[] protoDescriptor =
        format == PROTOBUF ? ProtobufDescriptors.descriptorSet(messageClass) : null;
    String protoMessageName = format == PROTOBUF ? ProtobufDescriptors.messageName(messageClass) : "";
    return new NativeKafkaSource(
        KafkaSubscriber.getTopicListSubscriber(topics),
        mapStartupMode(options),
        bounded ? OffsetsInitializer.latest() : new NoStoppingOffsetsInitializer(),
        bounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED,
        props,
        keys,
        values,
        format,
        outputType,
        avroSchema,
        readerAvroSchema,
        0,
        protoDescriptor,
        protoMessageName,
        MAX_RECORDS,
        POLL_TIMEOUT_MILLIS,
        rowtimeIndex);
  }

  // --- Shallow decode path (Phase 2/3): Flink's own KafkaSource consumes raw value bytes, a native
  // operator decodes them to Arrow. Insert-only formats (JSON/CSV/raw) route via isNativeKafkaDecode;
  // CDC changelog formats (Debezium/OGG) route via isCdcDecode, gated to the cases reproduced identically
  // to Flink. Avro/protobuf decoders exist but aren't wired into this planner path yet — see ticket 32.

  /**
   * Whether this table's decoder honors a pruned output schema — decoding only the columns and nested
   * sub-fields the schema names. JSON (the decode is schema-driven and JSON self-describing, so a
   * narrowed schema skips the other keys), bare Avro (the decode carries the full writer schema and
   * resolves the narrowed output as the reader schema), and protobuf (the descriptor is pruned to the
   * read fields; ptars builds a column per descriptor field and skips unmatched wire tags) do. CSV/raw
   * are positional/scalar, and Confluent Avro's writer schema comes from the registry by id; those
   * decode in full.
   */
  static boolean decodeHonorsProjection(Map<String, String> options) {
    int code = decodeFormatCode(options);
    return code == 0 || code == 4 || code == 5; // JSON, bare Avro, protobuf
  }

  /** The {@code MessageDecoder} format code for this table's value format, or -1 if not decodable here. */
  static int decodeFormatCode(Map<String, String> options) {
    String format = options.getOrDefault("value.format", options.get("format"));
    if (format == null) {
      return -1;
    }
    switch (format) {
      case "json":
        return 0;
      case "csv":
        return 2;
      case "raw":
        return 3;
      case "avro":
        return 4; // bare Avro; the reader schema is derived from the table's RowType
      case "protobuf":
        return 5; // descriptor derived from the message-class-name's generated class
      case "debezium-json":
        return 6;
      case "ogg-json":
        return 7;
      case "maxwell-json":
        return 8;
      case "canal-json":
        return 9;
      default:
        return -1; // avro / avro-confluent / protobuf: decoder exists, planner wiring is a follow-up
    }
  }

  /** The Kafka consume/topic/offset prerequisites the decode path needs, independent of value format. */
  private static boolean decodeCommon(Map<String, String> options) {
    if (options == null || !"kafka".equals(options.get("connector"))) {
      return false;
    }
    if (options.containsKey("key.format")) {
      return false; // a key column would be a second decode the native operator doesn't produce yet
    }
    if (options.get("topic") == null || options.get(PROPERTIES_PREFIX + "bootstrap.servers") == null) {
      return false;
    }
    return mapStartupMode(options) != null && boundedModeSupported(options);
  }

  /** Whether the shallow native-decode path can run this scan for an <em>insert-only</em> value format
   * (JSON/CSV/raw/bare-Avro/protobuf — codes 0/2/3/4/5): Flink consumes bytes, the native operator decodes
   * them to Arrow. CDC changelog formats are handled separately by {@link #isCdcDecode}. */
  static boolean isNativeKafkaDecode(RelNode node) {
    if (!(node instanceof StreamPhysicalTableSourceScan)) {
      return false;
    }
    StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) node;
    // The decode path replaces the scan but regenerates no watermarks, so a watermarked table (the
    // WATERMARK clause is pushed into the Kafka scan — no assigner node remains) must stay on the
    // host; only the native source reproduces the per-split source watermarks.
    if (KafkaWatermarkSpec.of(scan) != null) {
      return false;
    }
    Map<String, String> options = FilesystemTables.options(scan);
    if (!decodeCommon(options)) {
      return false;
    }
    int code = decodeFormatCode(options);
    if (code == 5) {
      // Protobuf routes for any message whose fields (recursively — nested messages, repeated, maps) are
      // types the decode reproduces identically to Flink; enums, unsigned/fixed ints, bytes, and
      // well-known types still fall back (see ProtobufDescriptors).
      String messageClass = options.get("protobuf.message-class-name");
      return messageClass != null && ProtobufDescriptors.isSupportedMessage(messageClass);
    }
    return code == 0 || code == 2 || code == 3 || code == 4;
  }

  /** Whether this scan is a CDC changelog format the native decode reproduces <em>identically</em> to
   * Flink. Only Debezium/OGG JSON (full pre/post images) qualify, and only when the table uses the
   * options whose semantics we match exactly. Anything else — Maxwell/Canal (their partial-{@code old}
   * merge can't be reproduced from the decoded image alone), a {@code schema-include} wrapper,
   * {@code ignore-parse-errors} (Flink skips bad rows; the native decoder fails on them like Flink's
   * default), or metadata/computed columns the value decode doesn't produce — falls back to Flink. See
   * ticket 32 for the follow-ups that would lift each restriction. */
  static boolean isCdcDecode(RelNode node) {
    if (!(node instanceof StreamPhysicalTableSourceScan)) {
      return false;
    }
    StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) node;
    // Same watermark rule as isNativeKafkaDecode: the decode path regenerates no watermarks.
    if (KafkaWatermarkSpec.of(scan) != null) {
      return false;
    }
    Map<String, String> options = FilesystemTables.options(scan);
    if (!decodeCommon(options)) {
      return false;
    }
    int code = decodeFormatCode(options);
    if (code != 6 && code != 7) {
      return false; // Debezium/OGG only; Maxwell/Canal partial-old merge isn't bit-identical (ticket 32)
    }
    String format = options.getOrDefault("value.format", options.get("format"));
    if ("true".equalsIgnoreCase(options.get(format + ".schema-include"))) {
      return false; // the {schema, payload} envelope wrapper isn't handled
    }
    if ("true".equalsIgnoreCase(options.get(format + ".ignore-parse-errors"))) {
      return false; // Flink skips malformed rows; the native decoder fails, matching the default only
    }
    return FilesystemTables.allPhysicalColumns(scan); // metadata/computed columns aren't decoded natively
  }

  /**
   * Whether this scan is a decodable insert-only Kafka table kept on the host because it declares a
   * watermark (pushed into the scan) the decode path can't regenerate — the fallback reason to record.
   * Checked after the native-source branch, so it is false when the table routed there instead.
   */
  static boolean watermarkBlocksAppendDecode(RelNode node) {
    return watermarkBlocksDecode(node, false);
  }

  /** The CDC-format variant of {@link #watermarkBlocksAppendDecode} (checked above the insert-only
   * guard, where the CDC branch lives). */
  static boolean watermarkBlocksCdcDecode(RelNode node) {
    return watermarkBlocksDecode(node, true);
  }

  private static boolean watermarkBlocksDecode(RelNode node, boolean cdc) {
    if (!(node instanceof StreamPhysicalTableSourceScan)) {
      return false;
    }
    StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) node;
    if (KafkaWatermarkSpec.of(scan) == null) {
      return false;
    }
    Map<String, String> options = FilesystemTables.options(scan);
    if (!decodeCommon(options)) {
      return false;
    }
    int code = decodeFormatCode(options);
    return cdc ? code == 6 || code == 7 : code >= 0 && code <= 5;
  }

  /** Builds Flink's own {@link KafkaSource} producing each record's raw value as a {@code byte[]} (no
   * decode) — the native decode operator turns those bytes into Arrow. Flink owns consume/offsets/auth. */
  static KafkaSource<byte[]> buildBytesSource(Map<String, String> options) {
    Properties props = consumerProperties(options);
    List<String> topics = Arrays.asList(options.get("topic").split(";"));
    KafkaSourceBuilder<byte[]> builder =
        KafkaSource.<byte[]>builder()
            .setProperties(props)
            .setTopics(topics)
            .setStartingOffsets(mapStartupMode(options))
            .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class));
    if ("latest-offset".equals(options.get("scan.bounded.mode"))) {
      builder.setBounded(OffsetsInitializer.latest());
    }
    return builder.build();
  }

  /** Whether {@code scan.bounded.mode} is one the native source handles (unbounded or latest-offset). */
  private static boolean boundedModeSupported(Map<String, String> options) {
    String mode = options.get("scan.bounded.mode");
    return mode == null || "unbounded".equals(mode) || "latest-offset".equals(mode);
  }

  /** The consumer {@code Properties}: the {@code properties.*} options plus Flink's forced overrides. */
  private static Properties consumerProperties(Map<String, String> options) {
    Properties props = new Properties();
    options.forEach(
        (key, value) -> {
          if (key.startsWith(PROPERTIES_PREFIX)) {
            props.setProperty(key.substring(PROPERTIES_PREFIX.length()), value);
          }
        });
    // Offsets are checkpointed, never auto-committed; the reader assigns+seeks to concrete offsets.
    props.setProperty("enable.auto.commit", "false");
    // A group id is needed for the enumerator's committed-offset reads; synthesize one if absent.
    props.putIfAbsent("group.id", "streamfusion-" + Integer.toHexString(options.hashCode()));
    return props;
  }

  /** The {@code scan.startup.mode} as an {@link OffsetsInitializer}, or null if unsupported. */
  private static OffsetsInitializer mapStartupMode(Map<String, String> options) {
    switch (options.getOrDefault("scan.startup.mode", "group-offsets")) {
      case "earliest-offset":
        return OffsetsInitializer.earliest();
      case "latest-offset":
        return OffsetsInitializer.latest();
      case "group-offsets":
        return OffsetsInitializer.committedOffsets();
      case "timestamp":
        return OffsetsInitializer.timestamp(
            Long.parseLong(options.get("scan.startup.timestamp-millis")));
      default:
        return null; // specific-offsets and anything else
    }
  }
}
