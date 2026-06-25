package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.kafka.KafkaConfigTranslator;
import io.github.jordepic.streamfusion.kafka.NativeKafkaSource;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.table.types.logical.RowType;

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
    if (mapStartupMode(options) == null) {
      return false; // e.g. specific-offsets
    }
    if (options.get(PROPERTIES_PREFIX + "bootstrap.servers") == null) {
      return false;
    }
    return KafkaConfigTranslator.translate(consumerProperties(options)).isTranslated();
  }

  /** Builds the native source for a table {@link #isNativeKafka} accepted. */
  static NativeKafkaSource build(Map<String, String> options, RowType outputType) {
    Properties props = consumerProperties(options);
    Map<String, String> librdkafka = KafkaConfigTranslator.translate(props).config();
    String[] keys = librdkafka.keySet().toArray(new String[0]);
    String[] values = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      values[i] = librdkafka.get(keys[i]);
    }
    List<String> topics = Arrays.asList(options.get("topic").split(";"));
    return new NativeKafkaSource(
        KafkaSubscriber.getTopicListSubscriber(topics),
        mapStartupMode(options),
        OffsetsInitializer.latest(), // no stopping offset: unbounded
        Boundedness.CONTINUOUS_UNBOUNDED,
        props,
        keys,
        values,
        outputType,
        MAX_RECORDS,
        POLL_TIMEOUT_MILLIS);
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
