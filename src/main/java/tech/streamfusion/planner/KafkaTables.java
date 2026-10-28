package tech.streamfusion.planner;

import tech.streamfusion.format.FormatCodes;
import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatOptions;
import tech.streamfusion.format.NativeFormatProvider;
import tech.streamfusion.format.NativeFormatProviders;
import tech.streamfusion.kafka.KeyedKafkaBytesDeserialization;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TimeUtils;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Maps a Flink Kafka SQL table to a byte-oriented Flink source followed by native format decoding.
 * Flink's Kafka connector remains the sole owner of consumer configuration, offsets, discovery,
 * authentication, and checkpoint integration.
 */
final class KafkaTables {

  private KafkaTables() {}

  private static final String PROPERTIES_PREFIX = "properties.";

  /** Whether the table's value format sets Flink's {@code ignore-parse-errors} (skip malformed
   * messages instead of failing). */
  static boolean ignoreParseErrors(Map<String, String> options) {
    return "true".equalsIgnoreCase(NativeFormatOptions.option(options, "ignore-parse-errors"));
  }

  // Flink's own KafkaSource consumes raw value bytes, then a native
  // operator decodes them to Arrow. Insert-only formats
  // (JSON/CSV/raw/bare-Avro/Confluent-Avro/protobuf) route via isNativeKafkaDecode; CDC changelog
  // formats (the JSON dialects and debezium-avro-confluent) route via isCdcDecode, gated to the
  // cases reproduced identically to Flink.

  /**
   * Whether this table's decoder honors a pruned output schema — decoding only the columns and nested
   * sub-fields the schema names. JSON (the decode is schema-driven and JSON self-describing, so a
   * narrowed schema skips the other keys), the Avro variants (the decode resolves the narrowed output
   * as the reader schema — bare Avro against the RowType-derived writer schema, Confluent against the
   * registry-fetched one), and protobuf (the descriptor is pruned to the read fields; the decoder
   * builds a column per descriptor field and skips unmatched wire tags) do. CSV/raw are
   * positional/scalar and decode in full.
   */
  static boolean decodeHonorsProjection(Map<String, String> options) {
    // A keyed table's projection would have to split across the key and value decodes and
    // re-index both position sets; disabled until that split exists (the design doc's increment 2).
    if (options.containsKey("key.format")) {
      return false;
    }
    return formatProvider(options, null).map(NativeFormatProvider::honorsProjection).orElse(false);
  }

  /** The {@code MessageDecoder} format code for this table's value format, or
   * {@link FormatCodes#UNSUPPORTED} if not decodable here. */
  static int decodeFormatCode(Map<String, String> options) {
    return FormatCodes.forIdentifier(NativeFormatProviders.formatIdentifier(options));
  }

  /** The Kafka consume/topic/offset prerequisites the decode path needs, independent of value format. */
  private static boolean decodeCommon(Map<String, String> options) {
    if (options == null || !"kafka".equals(options.get("connector"))) {
      return false;
    }
    // Exactly one of topic / topic-pattern (the factory enforces that); discovery for a pattern is
    // Flink's enumerator owns discovery for a pattern, so both forms work.
    if (options.get("topic") == null && options.get("topic-pattern") == null) {
      return false;
    }
    if (options.get(PROPERTIES_PREFIX + "bootstrap.servers") == null) {
      return false;
    }
    return mapStartupMode(options) != null && boundedModeSupported(options);
  }

  /** Whether the native-decode path can run this scan for an <em>insert-only</em> value format
   * (JSON/CSV/raw/bare-Avro/protobuf — codes 0/2/3/4/5): Flink consumes bytes, the native operator
   * decodes them to Arrow. CDC changelog formats are handled separately by {@link #isCdcDecode}. */
  static boolean isNativeKafkaDecode(RelNode node) {
    if (!(node instanceof StreamPhysicalTableSourceScan)) {
      return false;
    }
    StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) node;
    // A supported pushed watermark is regenerated at the split-aware decoded source. Shapes outside
    // that exact contract stay on Flink; otherwise event-time timers could silently stall.
    if (ScanWatermarkSpec.of(scan) == ScanWatermarkSpec.UNSUPPORTED) {
      return false;
    }
    return decodableAppendScan(scan);
  }

  /** The format and option checks for the decode-operator path. */
  private static boolean decodableAppendScan(StreamPhysicalTableSourceScan scan) {
    Map<String, String> options = FilesystemTables.options(scan);
    if (!decodeCommon(options)) {
      return false;
    }
    // The value decode fills the physical columns and the native decode fills the connector's
    // metadata columns from each row's record; a value format's own metadata stays on Flink.
    // Computed columns are fine — the planner projects them above the scan.
    Map<String, String> metadata = connectorMetadata(scan);
    if (metadata == null) {
      return false;
    }
    // A keyed table: the key/value composition must resolve (see KeyedDecodeSpec), and the value
    // format's gate then runs against the VALUE row type — the physical schema minus the key
    // columns under EXCEPT_KEY.
    RowType valueRowType = FilesystemTables.physicalRowType(scan);
    if (options.containsKey("key.format")) {
      KeyedDecodeSpec keyed = KeyedDecodeSpec.resolve(options, valueRowType);
      if (keyed == null) {
        return false;
      }
      valueRowType = keyed.valueRowType();
    }
    NativeFormatProvider provider = formatProvider(options, valueRowType).orElse(null);
    if (provider == null) {
      // The format artifact isn't installed, or its exact options or column types are outside the
      // native decoder's Flink-faithful set (each provider owns that predicate).
      return false;
    }
    if (!metadata.isEmpty() && !provider.composesKafkaMetadata()) {
      return false;
    }
    // Flink's ignore-parse-errors drops malformed data; the JSON decode honors the per-message skip
    // and the CSV decode reproduces Flink's per-field granularity natively. A protobuf table with
    // it set would fail where Flink skips — fall back.
    if (ignoreParseErrors(options) && !provider.supportsIgnoreParseErrors()) {
      return false;
    }
    return FormatCodes.isInsertOnly(decodeFormatCode(options));
  }

  /** Whether this scan is a CDC changelog format the native decode reproduces <em>identically</em> to
   * Flink. Every dialect routes for any converter-supported schema, nested columns and any width
   * included: Debezium/OGG carry full pre/post images, and the Maxwell/Canal UPDATE_BEFORE follows
   * Flink's recursive findValue key-presence rule, reproduced by a native per-message key scan of
   * the raw {@code old}. Canal's database/table include filters route whenever their patterns
   * compile; the JVM decides each distinct value with Flink's own regex and number semantics.
   * {@code ignore-parse-errors} is supported both ways — the
   * native decoder skips an undecodable message per Flink's catch-everything-per-message semantics.
   * {@code debezium-avro-confluent} (the same envelope with registry-Avro bodies) routes under its
   * provider's registry-option and avro-type gates; it defines no error-skip or schema-include
   * options, so those clauses never engage for it.
   * Still falling back: a {@code schema-include} wrapper, metadata/computed columns the value decode
   * doesn't produce, and an include pattern Java rejects. */
  static boolean isCdcDecode(RelNode node) {
    if (!(node instanceof StreamPhysicalTableSourceScan)) {
      return false;
    }
    StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) node;
    // CDC's changelog translation has an additional host node above the scan. Keep watermarked CDC
    // on Flink until that complete path can regenerate the pushed source watermark.
    if (ScanWatermarkSpec.of(scan) != null) {
      return false;
    }
    Map<String, String> options = FilesystemTables.options(scan);
    if (!decodeCommon(options)) {
      return false;
    }
    if (options.containsKey("key.format")) {
      return false; // the keyed composition is wired for the insert-only path only, so far
    }
    NativeFormatProvider provider =
        formatProvider(options, FilesystemTables.physicalRowType(scan)).orElse(null);
    if (provider == null || (ignoreParseErrors(options) && !provider.supportsIgnoreParseErrors())) {
      return false;
    }
    int code = decodeFormatCode(options);
    if (!FormatCodes.isCdc(code)) {
      return false;
    }
    if (code == FormatCodes.CANAL_JSON && !canalIncludePatternsCompile(options)) {
      return false;
    }
    if ("true".equalsIgnoreCase(NativeFormatOptions.option(options, "schema-include"))) {
      return false; // the {schema, payload} envelope wrapper isn't handled
    }
    if (NativeFormatOptions.encode(options) == null) {
      return false; // an unreproducible format option
    }
    // Connector metadata decodes natively; a computed column or a value format's metadata doesn't.
    Map<String, String> metadata = connectorMetadata(scan);
    return FilesystemTables.noComputedColumns(scan)
        && metadata != null
        && (metadata.isEmpty() || provider.composesKafkaMetadata());
  }

  /** The metadata keys Flink's Kafka connector reads itself, as `KafkaDynamicSource` lists them. */
  private static final Set<String> CONNECTOR_METADATA_KEYS =
      Set.of(
          "topic", "partition", "headers", "leader-epoch", "offset", "timestamp", "timestamp-type");

  /**
   * The scan's metadata columns in output order, each column's name mapped to the metadata key it
   * reads; empty when it reads none. Null when the native decode can't fill them: a key a value
   * format reads from the message, or a scan that isn't its physical columns followed by metadata
   * columns. The scan names each metadata field after its column, on both Flink lines, so the key
   * is the declared column's.
   */
  static Map<String, String> connectorMetadata(StreamPhysicalTableSourceScan scan) {
    RowType physical = FilesystemTables.physicalRowType(scan);
    ResolvedSchema schema = FilesystemTables.resolvedSchema(scan);
    if (physical == null || schema == null) {
      return null;
    }
    List<String> names = scan.getRowType().getFieldNames();
    int physicalCount = physical.getFieldCount();
    if (physicalCount == 0) {
      return null; // the value decode needs a column to count its rows by
    }
    if (names.size() < physicalCount
        || !names.subList(0, physicalCount).equals(physical.getFieldNames())) {
      return null;
    }
    Map<String, String> metadata = new LinkedHashMap<>();
    for (String name : names.subList(physicalCount, names.size())) {
      Column column = schema.getColumn(name).orElse(null);
      if (!(column instanceof Column.MetadataColumn)) {
        return null;
      }
      String key = ((Column.MetadataColumn) column).getMetadataKey().orElse(name);
      if (!CONNECTOR_METADATA_KEYS.contains(key)) {
        return null;
      }
      metadata.put(name, key);
    }
    return metadata;
  }

  /**
   * Whether Canal's include patterns compile. Flink compiles them when it builds the scan's
   * deserializer and fails planning with its own error, so an invalid pattern keeps the scan there.
   */
  static boolean canalIncludePatternsCompile(Map<String, String> options) {
    for (String option : new String[] {"database.include", "table.include"}) {
      String pattern = NativeFormatOptions.option(options, option);
      if (pattern == null) {
        continue;
      }
      try {
        Pattern.compile(pattern);
      } catch (PatternSyntaxException e) {
        return false;
      }
    }
    return true;
  }

  /** Finds an installed format SPI provider without making this connector artifact depend on a format JAR. */
  private static Optional<NativeFormatProvider> formatProvider(
      Map<String, String> options, RowType rowType) {
    return NativeFormatProviders.find(
        new NativeFormatContext(rowType, rowType, options, ignoreParseErrors(options)));
  }

  /**
   * Why a watermarked, otherwise-decodable insert-only Kafka table stayed on Flink, or null when the
   * scan isn't such a table.
   */
  static String appendWatermarkFallback(RelNode node) {
    return watermarkFallback(node, false);
  }

  /** The CDC-format variant of {@link #appendWatermarkFallback}. */
  static String cdcWatermarkFallback(RelNode node) {
    return watermarkFallback(node, true);
  }

  private static String watermarkFallback(RelNode node, boolean cdc) {
    if (!(node instanceof StreamPhysicalTableSourceScan)) {
      return null;
    }
    StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) node;
    ScanWatermarkSpec watermark = ScanWatermarkSpec.of(scan);
    if (watermark == null || (!cdc && watermark != ScanWatermarkSpec.UNSUPPORTED)) {
      return null;
    }
    Map<String, String> options = FilesystemTables.options(scan);
    if (!decodeCommon(options)) {
      return null;
    }
    int code = decodeFormatCode(options);
    if (cdc ? !FormatCodes.isCdc(code) : !FormatCodes.isInsertOnly(code)) {
      return null;
    }
    if (cdc) {
      return "kafka CDC decode: pushed WATERMARK regeneration is not supported on the CDC changelog"
          + " path — the table stays on Flink";
    }
    return "kafka decode: the pushed WATERMARK expression or emission policy is outside the native"
        + " bounded-out-of-orderness contract — the table stays on Flink";
  }

  /** Builds the Flink Kafka source whose enumerator and serializers the native source delegates to. */
  static KafkaSource<byte[]> buildBytesSource(Map<String, String> options) {
    return buildBytesSource(options, sourceConsumerProperties(options));
  }

  /** Fully normalized properties shared by Flink's enumerator and the native split reader. */
  static Properties sourceConsumerProperties(Map<String, String> options) {
    Properties props = consumerProperties(options);
    props.setProperty(
        org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getName());
    props.setProperty(
        org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getName());
    OffsetsInitializer startup = mapStartupMode(options);
    props.setProperty(
        org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        startup.getAutoOffsetResetStrategy().name().toLowerCase(Locale.ROOT));
    if ("latest-offset".equals(options.get("scan.bounded.mode"))) {
      props.setProperty("partition.discovery.interval.ms", "-1");
    }
    String groupId =
        props.getProperty(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG);
    if (groupId == null) {
      props.setProperty("commit.offsets.on.checkpoint", "false");
      props.putIfAbsent("client.id.prefix", "StreamFusionKafkaSource");
    } else {
      props.putIfAbsent("client.id.prefix", groupId);
    }
    return props;
  }

  private static KafkaSource<byte[]> buildBytesSource(
      Map<String, String> options, Properties props) {
    // A keyed table's edge carries both byte arrays per record as one frame element.
    KafkaRecordDeserializationSchema<byte[]> deserializer =
        options.containsKey(NativeFormatOptions.KEYED_KEY_FORMAT)
            ? new KeyedKafkaBytesDeserialization()
            : KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class);
    KafkaSourceBuilder<byte[]> builder =
        KafkaSource.<byte[]>builder()
            .setProperties(props)
            .setStartingOffsets(mapStartupMode(options))
            .setDeserializer(deserializer);
    if (options.get("topic") != null) {
      builder.setTopics(Arrays.asList(options.get("topic").split(";")));
    } else {
      builder.setTopicPattern(Pattern.compile(options.get("topic-pattern")));
    }
    if ("latest-offset".equals(options.get("scan.bounded.mode"))) {
      builder.setBounded(OffsetsInitializer.latest());
    }
    return builder.build();
  }

  /** Whether {@code scan.bounded.mode} is one the byte-oriented source handles. */
  private static boolean boundedModeSupported(Map<String, String> options) {
    String mode = options.get("scan.bounded.mode");
    return mode == null || "unbounded".equals(mode) || "latest-offset".equals(mode);
  }

  /** The consumer {@code Properties}: the {@code properties.*} options plus Flink's forced overrides. */
  static Properties consumerProperties(Map<String, String> options) {
    Properties props = new Properties();
    options.forEach(
        (key, value) -> {
          if (key.startsWith(PROPERTIES_PREFIX)) {
            props.setProperty(key.substring(PROPERTIES_PREFIX.length()), value);
          }
        });
    // Mirror Flink's table factory: scan.topic-partition-discovery.interval (default 5 min, 0 disables)
    // becomes the enumerator's discovery property unconditionally, overriding any properties.* value.
    props.setProperty(
        "partition.discovery.interval.ms",
        Long.toString(
            TimeUtils.parseDuration(
                    options.getOrDefault("scan.topic-partition-discovery.interval", "5 min"))
                .toMillis()));
    // KafkaSourceBuilder defaults auto commit off but preserves an explicit user value. This is
    // observable for group-offset startup tests and external consumer-offset monitoring.
    props.putIfAbsent("enable.auto.commit", "false");
    return props;
  }

  /** The {@code scan.startup.mode} as an {@link OffsetsInitializer}, or null if unsupported. */
  static OffsetsInitializer mapStartupMode(Map<String, String> options) {
    switch (options.getOrDefault("scan.startup.mode", "group-offsets")) {
      case "earliest-offset":
        return OffsetsInitializer.earliest();
      case "latest-offset":
        return OffsetsInitializer.latest();
      case "group-offsets":
        String reset = options.getOrDefault(PROPERTIES_PREFIX + "auto.offset.reset", "none");
        try {
          return OffsetsInitializer.committedOffsets(
              OffsetResetStrategy.valueOf(reset.toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException ignored) {
          return null;
        }
      case "timestamp":
        return OffsetsInitializer.timestamp(
            Long.parseLong(options.get("scan.startup.timestamp-millis")));
      case "specific-offsets":
        return specificOffsets(options);
      default:
        return null;
    }
  }

  /**
   * {@code specific-offsets} startup, constructed exactly as Flink's own table source does
   * ({@link OffsetsInitializer#offsets} over the parsed partition→offset map). The connector factory
   * validated the option at DDL time (single topic, {@code partition:0,offset:42;…} format — its
   * parser is package-private, so the format is mirrored here); null (fall back) on any shape the
   * factory would have rejected anyway, defensively, rather than risk mis-reading a start position.
   */
  private static OffsetsInitializer specificOffsets(Map<String, String> options) {
    String topic = options.get("topic");
    String offsets = options.get("scan.startup.specific-offsets");
    if (topic == null || topic.contains(";") || offsets == null) {
      return null;
    }
    Map<TopicPartition, Long> byPartition = new HashMap<>();
    for (String pair : offsets.split(";")) {
      String[] kv = pair.split(",");
      if (kv.length != 2 || !kv[0].startsWith("partition:") || !kv[1].startsWith("offset:")) {
        return null;
      }
      try {
        int partition = Integer.parseInt(kv[0].substring("partition:".length()));
        long offset = Long.parseLong(kv[1].substring("offset:".length()));
        byPartition.put(new TopicPartition(topic, partition), offset);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return OffsetsInitializer.offsets(byPartition);
  }

  static RelNode substituteDecode(StreamPhysicalTableSourceScan scan, PlanContext ctx) {
    Map<String, String> options = decodeOptions(scan);
    return new StreamPhysicalNativeKafkaDecode(
        scan.getCluster(),
        scan.getTraitSet(),
        scan.getRowType(),
        options,
        connectorMetadata(scan),
        ScanWatermarkSpec.of(scan),
        ctx.repeatedSource(KafkaPlannerExtension.class.getName() + '|' + decodeSharingKey(scan)),
        NativePlanDetails.from(scan));
  }

  static String decodeSharingKey(StreamPhysicalTableSourceScan scan) {
    return StreamPhysicalNativeKafkaDecode.sharingKey(
        decodeOptions(scan),
        connectorMetadata(scan),
        scan.getRowType(),
        scan.getRowType(),
        ScanWatermarkSpec.of(scan));
  }

  private static Map<String, String> decodeOptions(StreamPhysicalTableSourceScan scan) {
    Map<String, String> options = FilesystemTables.options(scan);
    if (!options.containsKey("key.format")) {
      return options;
    }
    // The gate resolved the keyed composition; the markers ride the options into the exec node,
    // the sharing key, the format-option lines, and the native decoder.
    return KeyedDecodeSpec.resolve(options, FilesystemTables.physicalRowType(scan))
        .optionsWithMarkers();
  }

  static RelNode reportCdcWatermark(RelNode node, PlanContext ctx) {
    String fallback = KafkaTables.cdcWatermarkFallback(node);
    if (fallback != null) {
      ctx.decline(fallback);
    }
    return null;
  }

  static RelNode reportAppendWatermark(RelNode node, PlanContext ctx) {
    String fallback = KafkaTables.appendWatermarkFallback(node);
    if (fallback != null) {
      ctx.decline(fallback);
    }
    return null;
  }

}
