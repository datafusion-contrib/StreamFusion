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
  // Native batch cap per poll (Java's max.poll.records has no librdkafka analog) and poll timeout.
  private static final int MAX_RECORDS = 8192;
  private static final long POLL_TIMEOUT_MILLIS = 1000;

  /** Whether the native Kafka source can faithfully run this scan's table. */
  static boolean isNativeKafka(org.apache.calcite.rel.RelNode node) {
    if (!(node
        instanceof org.apache.flink.table.planner.plan.nodes.physical.stream
            .StreamPhysicalTableSourceScan)) {
      return false;
    }
    Map<String, String> options =
        FilesystemTables.options(
            (org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan)
                node);
    return supports(options);
  }

  /** The shared gate, also used by {@code build} to assume a translatable, supported table. */
  private static boolean supports(Map<String, String> options) {
    if (options == null || !"kafka".equals(options.get("connector"))) {
      return false;
    }
    String format = options.getOrDefault("value.format", options.get("format"));
    if (!"json".equals(format) || options.containsKey("key.format")) {
      return false; // only a JSON value is decoded natively
    }
    if (options.get("topic") == null) {
      return false; // topic-pattern not supported natively yet
    }
    if (mapStartupMode(options) == null || !boundedModeSupported(options)) {
      return false; // e.g. specific-offsets startup, or an unsupported bounded mode
    }
    if (options.get(PROPERTIES_PREFIX + "bootstrap.servers") == null) {
      return false;
    }
    return KafkaConfigTranslator.translate(consumerProperties(options)).isTranslated();
  }

  /** Builds the native source for a table {@link #isNativeKafka} accepted. */
  static NativeKafkaSource build(Map<String, String> options, RowType outputType) {
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
    return new NativeKafkaSource(
        KafkaSubscriber.getTopicListSubscriber(topics),
        mapStartupMode(options),
        bounded ? OffsetsInitializer.latest() : new NoStoppingOffsetsInitializer(),
        bounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED,
        props,
        keys,
        values,
        0, // JSON (the only SQL format wired natively today)
        outputType,
        "",
        0,
        MAX_RECORDS,
        POLL_TIMEOUT_MILLIS);
  }

  // --- Shallow decode path (Phase 2/3): Flink's own KafkaSource consumes raw value bytes, a native
  // operator decodes them to Arrow. Insert-only formats (JSON/CSV/raw) route via isNativeKafkaDecode;
  // CDC changelog formats (Debezium/OGG) route via isCdcDecode, gated to the cases reproduced identically
  // to Flink. Avro/protobuf decoders exist but aren't wired into this planner path yet — see ticket 32.

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
    Map<String, String> options = FilesystemTables.options((StreamPhysicalTableSourceScan) node);
    if (!decodeCommon(options)) {
      return false;
    }
    int code = decodeFormatCode(options);
    if (code == 5) {
      return options.get("protobuf.message-class-name") != null; // needed to derive the descriptor
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
