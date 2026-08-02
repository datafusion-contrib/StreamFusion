package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.kafka.ConsumerPrefetch;
import io.github.jordepic.streamfusion.kafka.KafkaConfigTranslator;
import io.github.jordepic.streamfusion.kafka.KeyedKafkaBytesDeserialization;
import io.github.jordepic.streamfusion.kafka.NativeKafka;
import io.github.jordepic.streamfusion.kafka.NativeKafkaSource;
import io.github.jordepic.streamfusion.format.FormatCodes;
import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatOptions;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.NativeFormatProviders;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Pattern;
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
import org.apache.flink.util.TimeUtils;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
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
  /** Whether the native Kafka source can faithfully run this scan's table. */
  static boolean isNativeKafka(org.apache.calcite.rel.RelNode node) {
    if (!(node instanceof StreamPhysicalTableSourceScan)) {
      return false;
    }
    if (!nativeKafkaAvailable()) {
      return false;
    }
    StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) node;
    // The format decode runs inside the poll, so the connector holds typed batches and the source
    // regenerates a pushed WATERMARK per split (max rowtime per batch, Flink's own min combination
    // and idleness). Only a shape outside ScanWatermarkSpec's reproducible set stays on Flink.
    if (ScanWatermarkSpec.of(scan) == ScanWatermarkSpec.UNSUPPORTED) {
      return false;
    }
    return decodableAppendScan(scan) && supports(FilesystemTables.options(scan));
  }

  /** The Kafka-consumption prerequisites of the native source (on top of the decode ones). */
  private static boolean supports(Map<String, String> options) {
    if (!decodeCommon(options)) {
      return false;
    }
    // The fused source's poll buckets carry only the value bytes; a keyed table stays on the
    // decode-operator path, whose keyed edge carries both.
    if (options.containsKey("key.format")) {
      return false;
    }
    return KafkaConfigTranslator.translate(consumerProperties(options)).fallbackReason == null;
  }

  private static boolean nativeKafkaAvailable() {
    try {
      return NativeKafka.isLoaded();
    } catch (LinkageError ignored) {
      return false;
    }
  }

  /** Whether the table's value format sets Flink's {@code ignore-parse-errors} (skip malformed
   * messages instead of failing). */
  static boolean ignoreParseErrors(Map<String, String> options) {
    return "true".equalsIgnoreCase(NativeFormatOptions.option(options, "ignore-parse-errors"));
  }

  /**
   * Builds the native rdkafka source for a table {@link #isNativeKafka} accepted. The format decoder
   * rides into the source so the split reader decodes on the fetch thread; {@code decodedType} is the
   * (possibly projection-narrowed) type the decoder emits. {@code rowtimeIndex} names the watermark's
   * rowtime column there (or -1): the reader stamps each batch's max rowtime as its record timestamp
   * for the source operator's per-split watermark generators.
   */
  static NativeKafkaSource build(
      Map<String, String> options,
      NativeMessageDecoderFactory decoderFactory,
      RowType decodedType,
      int rowtimeIndex) {
    OffsetsInitializer startingOffsets = mapStartupMode(options);
    Properties props = configuredSourceProperties(options, startingOffsets);
    Properties nativeProps = new Properties();
    nativeProps.putAll(props);
    // librdkafka requires group.id even for manual assign(), while Kafka's Java consumer does not.
    // Keep this implementation detail out of the enumerator/source properties so it cannot make
    // group-offset startup or checkpoint commits appear configured when the table omitted a group.
    nativeProps.putIfAbsent(
        "group.id", "streamfusion-native-" + UUID.randomUUID());
    Map<String, String> librdkafka =
        new HashMap<>(KafkaConfigTranslator.translate(nativeProps).config());
    ConsumerPrefetch.tune(librdkafka);
    String[] keys = librdkafka.keySet().toArray(new String[0]);
    String[] values = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      values[i] = librdkafka.get(keys[i]);
    }
    boolean bounded = "latest-offset".equals(options.get("scan.bounded.mode"));
    return new NativeKafkaSource(
        subscriber(options),
        startingOffsets,
        bounded ? OffsetsInitializer.latest() : new NoStoppingOffsetsInitializer(),
        bounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED,
        props,
        keys,
        values,
        MAX_RECORDS,
        POLL_TIMEOUT_MILLIS,
        decoderFactory,
        decodedType,
        rowtimeIndex);
  }

  // --- Shallow decode path (Phase 2/3): Flink's own KafkaSource consumes raw value bytes, a native
  // operator decodes them to Arrow. Insert-only formats (JSON/CSV/raw/bare-Avro/Confluent-Avro/protobuf)
  // route via isNativeKafkaDecode; CDC changelog formats (the JSON dialects and debezium-avro-confluent)
  // route via isCdcDecode, gated to the cases reproduced identically to Flink.

  /**
   * Whether this table's decoder honors a pruned output schema — decoding only the columns and nested
   * sub-fields the schema names. JSON (the decode is schema-driven and JSON self-describing, so a
   * narrowed schema skips the other keys), the Avro variants (the decode resolves the narrowed output
   * as the reader schema — bare Avro against the RowType-derived writer schema, Confluent against the
   * registry-fetched one), and protobuf (the descriptor is pruned to the read fields; ptars builds a
   * column per descriptor field and skips unmatched wire tags) do. CSV/raw are positional/scalar and
   * decode in full.
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
    // the reused enumerator's job, so both forms work on both native paths.
    if (options.get("topic") == null && options.get("topic-pattern") == null) {
      return false;
    }
    if (options.get(PROPERTIES_PREFIX + "bootstrap.servers") == null) {
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
    // The decode operator runs downstream of Flink's source and regenerates no watermarks, so a
    // watermarked table (the WATERMARK clause is pushed into the Kafka scan — no assigner node
    // remains) must stay on the host; only the native source reproduces per-split source watermarks.
    if (ScanWatermarkSpec.of(scan) != null) {
      return false;
    }
    return decodableAppendScan(scan);
  }

  /** The format/option checks shared by the native source and the decode-operator path (which differ
   * only in who consumes and whether a pushed watermark can be regenerated). */
  private static boolean decodableAppendScan(StreamPhysicalTableSourceScan scan) {
    Map<String, String> options = FilesystemTables.options(scan);
    if (!decodeCommon(options)) {
      return false;
    }
    // A metadata column is filled by the connector, not the message body; a value decode would
    // emit it as a (missing, so silently NULL) value field. The scan must produce exactly the
    // physical columns. Computed columns are fine — the planner projects them above the scan.
    if (!FilesystemTables.scanProducesPhysicalColumnsOnly(scan)) {
      return false;
    }
    // A keyed table: the raw-key composition must resolve (see KeyedDecodeSpec), and the value
    // format's gate then runs against the VALUE row type — the physical schema minus the key
    // column under EXCEPT_KEY.
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
    // Flink's ignore-parse-errors drops malformed data; the JSON decode honors the per-message skip
    // and the CSV decode reproduces Flink's per-field granularity natively. A protobuf table with
    // it set would fail where Flink skips — fall back.
    if (ignoreParseErrors(options) && !provider.supportsIgnoreParseErrors()) {
      return false;
    }
    return FormatCodes.isInsertOnly(decodeFormatCode(options));
  }

  /** Whether this scan is a CDC changelog format the native decode reproduces <em>identically</em> to
   * Flink. Debezium/OGG JSON (full pre/post images) route for any converter-supported schema;
   * Maxwell/Canal (post-image + partial {@code old}) route for flat scalar schemas — their
   * UPDATE_BEFORE follows Flink's findValue key-presence rule, reproduced by a native per-message
   * key scan of the raw {@code old}. {@code ignore-parse-errors} is supported both ways — the
   * native decoder skips an undecodable message per Flink's catch-everything-per-message semantics.
   * {@code debezium-avro-confluent} (the same envelope with registry-Avro bodies) routes under its
   * provider's registry-option and avro-type gates; it defines no error-skip or schema-include
   * options, so those clauses never engage for it.
   * Still falling back: a {@code schema-include} wrapper, metadata/computed columns the value decode
   * doesn't produce, Canal's database/table include regexes, and nested Maxwell/Canal schemas. See
   * https://github.com/datafusion-contrib/StreamFusion/issues/15 for the follow-ups. */
  static boolean isCdcDecode(RelNode node) {
    if (!(node instanceof StreamPhysicalTableSourceScan)) {
      return false;
    }
    StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) node;
    // Same watermark rule as isNativeKafkaDecode: the decode path regenerates no watermarks.
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
    if (code == FormatCodes.MAXWELL_JSON || code == FormatCodes.CANAL_JSON) {
      // Maxwell/Canal: the partial-`old` pre-image follows Flink's findValue KEY-presence rule,
      // reproduced natively by a per-message key scan — but findValue searches the `old` subtree
      // recursively, so a nested column's name could false-match inside another field's object.
      // Route only flat scalar schemas (capped at the presence bitmask's 128 columns); Canal's
      // database/table include filters are Java regexes the native decode doesn't run.
      if (!flatScalarColumns(scan)) {
        return false;
      }
      if (code == FormatCodes.CANAL_JSON
          && (NativeFormatOptions.option(options, "database.include") != null
              || NativeFormatOptions.option(options, "table.include") != null)) {
        return false;
      }
    }
    if ("true".equalsIgnoreCase(NativeFormatOptions.option(options, "schema-include"))) {
      return false; // the {schema, payload} envelope wrapper isn't handled
    }
    if (NativeFormatOptions.encode(options) == null) {
      return false; // an unreproducible format option
    }
    return FilesystemTables.allPhysicalColumns(scan); // metadata/computed columns aren't decoded natively
  }

  /** Finds an installed format SPI provider without making this connector artifact depend on a format JAR. */
  private static Optional<NativeFormatProvider> formatProvider(
      Map<String, String> options, RowType rowType) {
    return NativeFormatProviders.find(
        new NativeFormatContext(rowType, rowType, options, ignoreParseErrors(options)));
  }

  /** Whether every physical column is non-nested (and the arity fits the native presence bitmask). */
  private static boolean flatScalarColumns(StreamPhysicalTableSourceScan scan) {
    org.apache.flink.table.types.logical.RowType rowType = FilesystemTables.physicalRowType(scan);
    if (rowType == null || rowType.getFieldCount() > 128) {
      return false;
    }
    return rowType.getChildren().stream()
        .noneMatch(
            type -> {
              switch (type.getTypeRoot()) {
                case ROW:
                case ARRAY:
                case MAP:
                case MULTISET:
                  return true;
                default:
                  return false;
              }
            });
  }

  /**
   * Why a watermarked, otherwise-decodable insert-only Kafka table stayed on Flink, or null when the
   * scan isn't such a table. Checked after the native-source branch (which regenerates supported
   * watermark shapes per split), so a reason is produced only when the shape is outside the
   * reproducible set or the native source couldn't take the table at all.
   */
  static String appendWatermarkFallback(RelNode node) {
    return watermarkFallback(node, false);
  }

  /** The CDC-format variant of {@link #appendWatermarkFallback} (checked above the insert-only
   * guard, where the CDC branch lives): CDC decode runs as an operator downstream of Flink's source,
   * which regenerates no watermarks. */
  static String cdcWatermarkFallback(RelNode node) {
    return watermarkFallback(node, true);
  }

  private static String watermarkFallback(RelNode node, boolean cdc) {
    if (!(node instanceof StreamPhysicalTableSourceScan)) {
      return null;
    }
    StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) node;
    ScanWatermarkSpec watermark = ScanWatermarkSpec.of(scan);
    if (watermark == null) {
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
      return "kafka CDC decode: the table's WATERMARK is pushed into the scan, and the CDC decode"
          + " runs downstream of the source, which cannot regenerate it — the table stays on Flink";
    }
    if (watermark == ScanWatermarkSpec.UNSUPPORTED) {
      return "kafka source: the pushed WATERMARK isn't a shape the native source reproduces (bounded"
          + " out-of-orderness over a physical rowtime or TO_TIMESTAMP_LTZ(col, 3), periodic emit,"
          + " no alignment) — the table stays on Flink";
    }
    return "kafka decode: only the native source regenerates the pushed WATERMARK, and this table"
        + " couldn't take it — the table stays on Flink";
  }

  /** Builds Flink's own {@link KafkaSource} producing each record's raw value as a {@code byte[]} (no
   * decode) — the native decode operator turns those bytes into Arrow. Flink owns consume/offsets/auth. */
  static KafkaSource<byte[]> buildBytesSource(Map<String, String> options) {
    Properties props = consumerProperties(options);
    // A keyed table's edge carries both byte arrays per record as one frame element.
    KafkaRecordDeserializationSchema<byte[]> deserializer =
        options.containsKey(NativeFormatOptions.KEYED_KEY_POSITION)
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

  /** The subscriber: an explicit topic list, or the pattern subscriber for {@code topic-pattern} —
   * discovery runs in the reused enumerator either way, the reader only ever sees concrete splits. */
  private static KafkaSubscriber subscriber(Map<String, String> options) {
    String topic = options.get("topic");
    return topic != null
        ? KafkaSubscriber.getTopicListSubscriber(Arrays.asList(topic.split(";")))
        : KafkaSubscriber.getTopicPatternSubscriber(
            Pattern.compile(options.get("topic-pattern")));
  }

  /** Whether {@code scan.bounded.mode} is one the native source handles (unbounded or latest-offset). */
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
    // Offsets are checkpointed, never auto-committed; the reader assigns+seeks to concrete offsets.
    props.setProperty("enable.auto.commit", "false");
    return props;
  }

  /** Applies the overrides {@link KafkaSourceBuilder} makes when it builds a source. */
  static Properties configuredSourceProperties(
      Map<String, String> options, OffsetsInitializer startingOffsets) {
    Properties props = consumerProperties(options);
    props.setProperty(
        "auto.offset.reset",
        startingOffsets
            .getAutoOffsetResetStrategy()
            .name()
            .toLowerCase(Locale.ROOT));
    if (!props.containsKey("group.id")) {
      props.setProperty("commit.offsets.on.checkpoint", "false");
    }
    props.putIfAbsent(
        "client.id.prefix",
        props.containsKey("group.id")
            ? props.getProperty("group.id")
            : "KafkaSource-" + new Random().nextLong());
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
    Map<String, String> options = FilesystemTables.options(scan);
    if (options.containsKey("key.format")) {
      // The gate resolved the keyed composition; the markers ride the options into the exec node,
      // the format-option lines, and the native decoder.
      options =
          KeyedDecodeSpec.resolve(options, FilesystemTables.physicalRowType(scan))
              .optionsWithMarkers();
    }
    return new StreamPhysicalNativeKafkaDecode(
        scan.getCluster(), scan.getTraitSet(), scan.getRowType(), options);
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

  /**
   * The native rdkafka source consumes and decodes in one place: the installed format provider's
   * decoder runs inside the poll, so the source emits typed batches — and, because it therefore
   * holds decoded rowtimes, it regenerates a pushed WATERMARK per split (Flink's own min
   * combination and idleness over batch-max timestamps).
   */
  static RelNode substituteSource(StreamPhysicalTableSourceScan scan, PlanContext ctx) {
    return new StreamPhysicalNativeKafkaSource(
        scan.getCluster(),
        scan.getTraitSet(),
        scan.getRowType(),
        FilesystemTables.options(scan),
        ScanWatermarkSpec.of(scan));
  }
}
