package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.kafka.ConfluentSchemaRegistry;
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
import org.apache.flink.util.TimeUtils;
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
  // MessageDecoder format codes the native source decodes in Rust (the codes decodeFormatCode emits).
  private static final int BARE_AVRO = 4;
  private static final int PROTOBUF = 5;

  /** Whether the native Kafka source can faithfully run this scan's table. */
  static boolean isNativeKafka(org.apache.calcite.rel.RelNode node) {
    if (!(node instanceof StreamPhysicalTableSourceScan)) {
      return false;
    }
    // The kafka cargo feature is a default, but an opt-out library must fall back to the decode
    // path rather than route to JNI symbols it doesn't carry.
    if (!io.github.jordepic.streamfusion.Native.kafkaFeatureBuilt()) {
      return false;
    }
    StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) node;
    // A watermarked table routes only when the pushed watermark is a shape the source regenerates
    // (per-split bounded out-of-orderness); the scan replaces the host source, so an unreproducible
    // watermark must leave the whole scan on Flink.
    if (ScanWatermarkSpec.of(scan) == ScanWatermarkSpec.UNSUPPORTED) {
      return false;
    }
    return supports(FilesystemTables.options(scan));
  }

  /** The shared gate, also used by {@code build} to assume a translatable, supported table. The native
   * source decodes in Rust the same insert-only value formats the shallow decode path does — JSON (0),
   * bare Avro (4), and protobuf (5) — over the consume/topic/offset prerequisites in {@link
   * #decodeCommon}, plus a librdkafka-translatable consumer config (the native source owns the consume,
   * so its config must render to librdkafka). A table with {@code ignore-parse-errors} set stays off
   * the native source (its decoder fails on a bad message where Flink would skip it); a JSON one falls
   * through to the decode path, which honors the skip mode. */
  private static boolean supports(Map<String, String> options) {
    if (!decodeCommon(options)) {
      return false;
    }
    if (ignoreParseErrors(options)) {
      return false;
    }
    if (encodeFormatOptions(options) == null) {
      return false; // an unreproducible format option (e.g. fail-on-missing-field)
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

  /** Whether the table's value format sets Flink's {@code ignore-parse-errors} (skip malformed
   * messages instead of failing). */
  static boolean ignoreParseErrors(Map<String, String> options) {
    return "true".equalsIgnoreCase(formatOption(options, "ignore-parse-errors"));
  }

  /** A value-format option, resolved with Flink's own prefixing ({@code FactoryUtil.getFormatPrefix}):
   * {@code csv.field-delimiter} when the table uses {@code format = 'csv'}, but
   * {@code value.csv.field-delimiter} when it uses {@code value.format = 'csv'}. */
  private static String formatOption(Map<String, String> options, String suffix) {
    String valueFormat = options.get("value.format");
    return valueFormat != null
        ? options.get("value." + valueFormat + "." + suffix)
        : options.get(options.get("format") + "." + suffix);
  }

  /**
   * The decode-relevant format options rendered for the native decoder as {@code key=value} lines,
   * or null when an option value the native decode can't reproduce is present (the fallback gate).
   * CSV carries the Jackson {@code CsvSchema} knobs; the JSON family (plain {@code json} and the
   * CDC envelopes) carries {@code timestamp-format.standard} and gates
   * {@code fail-on-missing-field}. The CSV delimiter is Java-unescaped and truncated to its first
   * character exactly as {@code CsvFormatFactory} does; quote is a literal single character (the
   * factory validates the length). Each must be ASCII — csv-core splits on bytes — and a null
   * literal must fit the line encoding.
   */
  static String encodeFormatOptions(Map<String, String> options) {
    StringBuilder encoded = new StringBuilder();
    int code = decodeFormatCode(options);
    if (code == 0 || (code >= 6 && code <= 9)) {
      // A missing field is null natively (Flink's default); the fail mode isn't modeled.
      if ("true".equalsIgnoreCase(formatOption(options, "fail-on-missing-field"))) {
        return null;
      }
      String timestampFormat = formatOption(options, "timestamp-format.standard");
      if (timestampFormat == null || "SQL".equals(timestampFormat)) {
        return encoded.toString();
      }
      if ("ISO-8601".equals(timestampFormat)) {
        return "timestamp-format=ISO-8601\n";
      }
      return null; // the factory validates the value, so anything else is defensive
    }
    if (code != 2) {
      return encoded.toString();
    }
    String delimiter = formatOption(options, "field-delimiter");
    if (delimiter != null) {
      Character c = unescapedDelimiter(delimiter);
      if (c == null || !appendChar(encoded, "csv.field-delimiter", c)) {
        return null;
      }
    }
    String quote = formatOption(options, "quote-character");
    if (quote != null && !appendChar(encoded, "csv.quote-character", quote.charAt(0))) {
      return null;
    }
    if ("true".equalsIgnoreCase(formatOption(options, "disable-quote-character"))) {
      encoded.append("csv.disable-quote-character=true\n");
    }
    if (formatOption(options, "escape-character") != null) {
      // Jackson's escape applies in unquoted fields too (parity-pinned: "esc\;aped" unescapes);
      // csv-core's escape is quoted-context only, so the option can't be reproduced — fall back.
      return null;
    }
    if ("true".equalsIgnoreCase(formatOption(options, "allow-comments"))) {
      encoded.append("csv.allow-comments=true\n");
    }
    String nullLiteral = formatOption(options, "null-literal");
    if (nullLiteral != null) {
      if (nullLiteral.contains("\n") || nullLiteral.contains("\r")) {
        return null;
      }
      encoded.append("csv.null-literal=").append(nullLiteral).append('\n');
    }
    return encoded.toString();
  }

  private static boolean appendChar(StringBuilder encoded, String key, char c) {
    if (c > 127 || c == '\n' || c == '\r') {
      return false;
    }
    encoded.append(key).append('=').append(c).append('\n');
    return true;
  }

  /**
   * {@code field-delimiter} the way {@code CsvFormatFactory} reads it — Java-unescaped, first char
   * ({@code '\t'} arrives as the two characters backslash-t). Handles the escape forms that render
   * a single character; null (fall back) for anything else rather than risking a mis-read
   * delimiter.
   */
  private static Character unescapedDelimiter(String raw) {
    if (raw.length() == 1) {
      return raw.charAt(0);
    }
    if (raw.length() == 2 && raw.charAt(0) == '\\') {
      switch (raw.charAt(1)) {
        case 't':
          return '\t';
        case 'b':
          return '\b';
        case 'f':
          return '\f';
        case '\\':
          return '\\';
        case '\'':
          return '\'';
        case '"':
          return '"';
        default:
          return null;
      }
    }
    if (raw.length() == 6 && raw.startsWith("\\u")) {
      try {
        return (char) Integer.parseInt(raw.substring(2), 16);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  /** Whether every physical column is a type the native CSV decode converts with Flink's exact
   * semantics — the scalar family. ARRAY/ROW columns (Jackson's array-element-delimiter layer) and
   * the types outside the boundary set fall back. */
  private static boolean csvColumnsSupported(StreamPhysicalTableSourceScan scan) {
    org.apache.flink.table.types.logical.RowType rowType = FilesystemTables.physicalRowType(scan);
    if (rowType == null) {
      return false;
    }
    return rowType.getChildren().stream()
        .allMatch(
            type -> {
              switch (type.getTypeRoot()) {
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case CHAR:
                case VARCHAR:
                case DATE:
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case DECIMAL:
                  return true;
                default:
                  return false;
              }
            });
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
        subscriber(options),
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
        rowtimeIndex,
        encodeFormatOptions(options));
  }

  // --- Shallow decode path (Phase 2/3): Flink's own KafkaSource consumes raw value bytes, a native
  // operator decodes them to Arrow. Insert-only formats (JSON/CSV/raw/bare-Avro/Confluent-Avro/protobuf)
  // route via isNativeKafkaDecode; CDC changelog formats (Debezium/OGG) route via isCdcDecode, gated to
  // the cases reproduced identically to Flink.

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
    int code = decodeFormatCode(options);
    return code == 0 || code == 1 || code == 4 || code == 5; // JSON, both Avros, protobuf
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
      case "avro-confluent":
        return 1; // writer schemas fetched from the registry by frame id; reader from the RowType
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
        return -1;
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
    // The decode path replaces the scan but regenerates no watermarks, so a watermarked table (the
    // WATERMARK clause is pushed into the Kafka scan — no assigner node remains) must stay on the
    // host; only the native source reproduces the per-split source watermarks.
    if (ScanWatermarkSpec.of(scan) != null) {
      return false;
    }
    Map<String, String> options = FilesystemTables.options(scan);
    if (!decodeCommon(options)) {
      return false;
    }
    int code = decodeFormatCode(options);
    // Flink's ignore-parse-errors drops malformed data; the JSON decode honors the per-message skip
    // and the CSV decode reproduces Flink's per-field granularity natively. A protobuf table with
    // it set would fail where Flink skips — fall back.
    if (ignoreParseErrors(options) && code != 0 && code != 2) {
      return false;
    }
    if (code == 2) {
      // CSV routes only when every column is in the natively-converted scalar family and every
      // Jackson option the table sets is reproduced exactly (see encodeFormatOptions).
      return csvColumnsSupported(scan) && encodeFormatOptions(options) != null;
    }
    if (code == 0 && encodeFormatOptions(options) == null) {
      return false; // an unreproducible JSON option (e.g. fail-on-missing-field)
    }
    if (code == 5) {
      // Protobuf routes for any message whose fields (recursively — nested messages, repeated, maps) are
      // types the decode reproduces identically to Flink; enums, unsigned/fixed ints, bytes, and
      // well-known types still fall back (see ProtobufDescriptors).
      String messageClass = options.get("protobuf.message-class-name");
      return messageClass != null && ProtobufDescriptors.isSupportedMessage(messageClass);
    }
    if (code == 1) {
      // Confluent Avro routes when the registry options translate to the native fetch (a plain URL);
      // an explicit reader schema, registry auth/SSL, or pass-through client properties fall back.
      return ConfluentSchemaRegistry.fromOptions(options) != null;
    }
    return code == 0 || code == 2 || code == 3 || code == 4;
  }

  /** Whether this scan is a CDC changelog format the native decode reproduces <em>identically</em> to
   * Flink. Debezium/OGG JSON (full pre/post images) route for any converter-supported schema;
   * Maxwell/Canal (post-image + partial {@code old}) route for flat scalar schemas — their
   * UPDATE_BEFORE follows Flink's findValue key-presence rule, reproduced by a native per-message
   * key scan of the raw {@code old}. {@code ignore-parse-errors} is supported both ways — the
   * native decoder skips an undecodable message per Flink's catch-everything-per-message semantics.
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
    int code = decodeFormatCode(options);
    if (code < 6 || code > 9) {
      return false;
    }
    if (code == 8 || code == 9) {
      // Maxwell/Canal: the partial-`old` pre-image follows Flink's findValue KEY-presence rule,
      // reproduced natively by a per-message key scan — but findValue searches the `old` subtree
      // recursively, so a nested column's name could false-match inside another field's object.
      // Route only flat scalar schemas (capped at the presence bitmask's 128 columns); Canal's
      // database/table include filters are Java regexes the native decode doesn't run.
      if (!flatScalarColumns(scan)) {
        return false;
      }
      if (code == 9
          && (formatOption(options, "database.include") != null
              || formatOption(options, "table.include") != null)) {
        return false;
      }
    }
    if ("true".equalsIgnoreCase(formatOption(options, "schema-include"))) {
      return false; // the {schema, payload} envelope wrapper isn't handled
    }
    if (encodeFormatOptions(options) == null) {
      return false; // an unreproducible format option
    }
    return FilesystemTables.allPhysicalColumns(scan); // metadata/computed columns aren't decoded natively
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
    if (ScanWatermarkSpec.of(scan) == null) {
      return false;
    }
    Map<String, String> options = FilesystemTables.options(scan);
    if (!decodeCommon(options)) {
      return false;
    }
    int code = decodeFormatCode(options);
    return cdc ? code >= 6 && code <= 9 : code >= 0 && code <= 5;
  }

  /** Builds Flink's own {@link KafkaSource} producing each record's raw value as a {@code byte[]} (no
   * decode) — the native decode operator turns those bytes into Arrow. Flink owns consume/offsets/auth. */
  static KafkaSource<byte[]> buildBytesSource(Map<String, String> options) {
    Properties props = consumerProperties(options);
    KafkaSourceBuilder<byte[]> builder =
        KafkaSource.<byte[]>builder()
            .setProperties(props)
            .setStartingOffsets(mapStartupMode(options))
            .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class));
    if (options.get("topic") != null) {
      builder.setTopics(Arrays.asList(options.get("topic").split(";")));
    } else {
      builder.setTopicPattern(java.util.regex.Pattern.compile(options.get("topic-pattern")));
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
            java.util.regex.Pattern.compile(options.get("topic-pattern")));
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
    Map<TopicPartition, Long> byPartition = new java.util.HashMap<>();
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
}
