package tech.streamfusion.planner;

import tech.streamfusion.format.EncodeFormat;
import tech.streamfusion.format.FormatCodes;
import tech.streamfusion.kafka.JdkFloatSpelling;
import tech.streamfusion.kafka.NativeKafka;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;

/** Conservative match boundary for native JSON serialization into Flink's Kafka sink. */
final class KafkaSinkMatcher {

  private KafkaSinkMatcher() {}

  static final class Planned {
    final RowType rowType;
    final KafkaSinkTranslator.Planned sink;
    final EncodeFormat valueFormat;
    final EncodeFormat keyFormat;
    final int[] keyFields;
    final int[] valueFields;
    final boolean upsert;
    final String fallbackReason;

    private Planned(
        RowType rowType,
        KafkaSinkTranslator.Planned sink,
        EncodeFormat valueFormat,
        EncodeFormat keyFormat,
        int[] keyFields,
        int[] valueFields,
        boolean upsert,
        String fallbackReason) {
      this.rowType = rowType;
      this.sink = sink;
      this.valueFormat = valueFormat;
      this.keyFormat = keyFormat;
      this.keyFields = keyFields;
      this.valueFields = valueFields;
      this.upsert = upsert;
      this.fallbackReason = fallbackReason;
    }

    private static Planned fallback(String reason) {
      return new Planned(null, null, null, null, null, null, false, reason);
    }
  }

  /** The precise fallback reason when the JVM's float spelling probe fails (JDK 19+ host). */
  private static final String FLOAT_SPELLING_MISMATCH =
      "jdk float spelling mismatch (JDK 19+): this JVM's Double.toString is not the legacy"
          + " spelling the native text encoders produce";

  /** The value formats the native serializer implements, by Flink format identifier. */
  private static final Set<String> NATIVE_VALUE_FORMATS =
      Set.of("json", "csv", "avro", "avro-confluent", "debezium-avro-confluent", "protobuf", "raw");

  static boolean appliesTo(StreamPhysicalSink sink) {
    Map<String, String> options = options(sink);
    if (options == null) {
      return false;
    }
    String format = options.getOrDefault("value.format", options.get("format"));
    if ("kafka".equals(options.get("connector"))) {
      // Plain JSON, CSV, or a CDC JSON envelope; Flink forbids the CDC formats on upsert-kafka.
      return FormatCodes.isJsonFamily(format) || NATIVE_VALUE_FORMATS.contains(format);
    }
    return "upsert-kafka".equals(options.get("connector"))
        && NATIVE_VALUE_FORMATS.contains(format);
  }

  static Planned plan(StreamPhysicalSink sink) {
    // Flink materializes an out-of-order upsert changelog with a SinkUpsertMaterializer (a stateful
    // operator baked into its sink translation); substituting the sink would silently drop it.
    if (sink.upsertMaterialize()) {
      return Planned.fallback(
          "an upsert-materialized sink (SinkUpsertMaterializer) is not natively reproduced");
    }
    if (sink.abilitySpecs().length != 0) {
      SinkAbilitySpec spec = sink.abilitySpecs()[0];
      return Planned.fallback("sink ability " + spec.getClass().getSimpleName());
    }
    KafkaSinkTranslator.Result translated = KafkaSinkTranslator.translate(options(sink));
    if (translated.fallbackReason != null) {
      return Planned.fallback(translated.fallbackReason);
    }
    ContextResolvedTable context = sink.contextResolvedTable();
    ResolvedCatalogTable table = (ResolvedCatalogTable) context.getResolvedTable();
    RowType rowType =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    String valueFormatId = translated.planned().valueFormat;
    for (LogicalType type : rowType.getChildren()) {
      if (!supportsType(valueFormatId, type)) {
        return Planned.fallback(valueFormatId + " type " + type.asSummaryString());
      }
    }
    if (FormatCodes.isJsonFamily(valueFormatId) && !jsonFieldNamesEscapeFreely(rowType)) {
      return Planned.fallback("a field name needs a JSON control-character escape");
    }
    if (!floatSpellingVerified(valueFormatId, rowType.getChildren())) {
      return Planned.fallback(FLOAT_SPELLING_MISMATCH);
    }
    EncodeFormat valueFormat =
        EncodeFormat.of(valueFormatId, translated.planned().valueFormatOptions, rowType);
    if (valueFormat == null || !encodedByNativeLibrary(valueFormat)) {
      return Planned.fallback(
          "value format " + valueFormatId + " is not natively encoded"
              + " with these options and row type");
    }
    EncodeFormat keyFormat = valueFormat;
    int[] valueFields = IntStream.range(0, rowType.getFieldCount()).toArray();
    int[] keyFields = new int[0];
    if (translated.planned().upsert) {
      String keyFormatId = translated.planned().keyFormat;
      List<String> primaryKey =
          table.getResolvedSchema().getPrimaryKey().orElseThrow().getColumns();
      keyFields =
          primaryKey.stream().mapToInt(rowType.getFieldNames()::indexOf).toArray();
      List<LogicalType> keyTypes = new ArrayList<>();
      for (int keyField : keyFields) {
        LogicalType type = rowType.getTypeAt(keyField);
        if (!supportsType(keyFormatId, type)) {
          return Planned.fallback("key " + keyFormatId + " type " + type.asSummaryString());
        }
        keyTypes.add(type);
      }
      if (!floatSpellingVerified(keyFormatId, keyTypes)) {
        return Planned.fallback(FLOAT_SPELLING_MISMATCH);
      }
      // The key format serializes its own row: the PK projection, exactly the row type Flink's
      // upsert-kafka factory hands the key format's encoder.
      RowType keyRowType =
          new RowType(
              false,
              IntStream.of(keyFields)
                  .mapToObj(rowType.getFields()::get)
                  .collect(Collectors.toList()));
      keyFormat =
          EncodeFormat.of(keyFormatId, translated.planned().keyFormatOptions, keyRowType);
      if (keyFormat == null || !encodedByNativeLibrary(keyFormat)) {
        return Planned.fallback(
            "key format " + keyFormatId + " is not natively encoded"
                + " with these options and key type");
      }
    }
    return new Planned(
        rowType,
        translated.planned(),
        valueFormat,
        keyFormat,
        keyFields,
        valueFields,
        translated.planned().upsert,
        null);
  }

  /** Whether the loaded connector library was built with the format's encode arm. */
  private static boolean encodedByNativeLibrary(EncodeFormat format) {
    try {
      return NativeKafka.encodeFormatSupported(format.format);
    } catch (LinkageError missing) {
      return false;
    }
  }

  private static boolean supportsType(String formatIdentifier, LogicalType type) {
    if (FormatCodes.isJsonFamily(formatIdentifier)) {
      // The CDC dialects nest the physical row through the same JSON row serializer.
      return supportsJsonType(type);
    }
    switch (formatIdentifier) {
      case "csv":
        return supportsCsvType(type, false);
      case "avro":
      case "avro-confluent":
      case "debezium-avro-confluent":
        // The avro providers gate their own row type by rerunning Flink's schema derivation
        // inside EncodeFormat.of; nothing to pre-screen per column here.
        return true;
      case "protobuf":
        // The protobuf provider gates the row↔descriptor mapping inside EncodeFormat.of.
        return true;
      case "raw":
        // The raw provider gates the single NOT NULL column inside EncodeFormat.of.
        return true;
      default:
        return false;
    }
  }

  /**
   * Flink's CSV serializer covers scalars at the top level plus depth-one ARRAY and ROW (its
   * schema converter rejects deeper nesting, and its runtime converter has no MAP/MULTISET/
   * interval arm). FLOAT/DOUBLE additionally require the spelling probe (see {@code plan}).
   */
  private static boolean supportsCsvType(LogicalType type, boolean nested) {
    switch (type.getTypeRoot()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case DECIMAL:
      case DATE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return true;
      case TIME_WITHOUT_TIME_ZONE:
        // Flink's converter prints whatever milliseconds the value carries even under TIME(0),
        // but the Arrow boundary stores a TIME(0) column at second granularity — out-of-contract
        // millisecond data would silently truncate, so only millisecond-preserving precisions run.
        return ((TimeType) type).getPrecision() >= 1;
      case ROW:
      case ARRAY:
        return !nested
            && type.getChildren().stream().allMatch(child -> supportsCsvType(child, true));
      default:
        return false;
    }
  }

  /**
   * Field names are written by arrow-json's own object encoder (serde_json escaping, lowercase
   * hex), not the Jackson-parity string encoder, so a name that needs a control-character escape
   * would spell differently than Flink's uppercase form. Defensive — SQL identifiers don't carry
   * control characters in practice — declining keeps the byte-parity contract airtight.
   */
  private static boolean jsonFieldNamesEscapeFreely(LogicalType type) {
    if (type instanceof RowType row) {
      for (RowType.RowField field : row.getFields()) {
        if (field.getName().chars().anyMatch(c -> c < 0x20)
            || !jsonFieldNamesEscapeFreely(field.getType())) {
          return false;
        }
      }
      return true;
    }
    return type.getChildren().stream().allMatch(KafkaSinkMatcher::jsonFieldNamesEscapeFreely);
  }

  private static boolean supportsJsonType(LogicalType type) {
    switch (type.getTypeRoot()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case DECIMAL:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return true;
      case ROW:
      case ARRAY:
        return type.getChildren().stream().allMatch(KafkaSinkMatcher::supportsJsonType);
      case MAP:
      case MULTISET:
        // Flink's own converter rejects a non-string map key (a MULTISET's element is its key)
        // when the job starts; declining keeps that failure on Flink instead of accepting a
        // schema Flink itself cannot serialize.
        return type.getChildren().get(0).is(LogicalTypeFamily.CHARACTER_STRING)
            && type.getChildren().stream().allMatch(KafkaSinkMatcher::supportsJsonType);
      default:
        return false;
    }
  }

  /**
   * FLOAT/DOUBLE on a text format serialize through the native port of the legacy (JDK ≤ 18)
   * {@code Double.toString}; they are admitted only while the runtime probe confirms this JVM
   * still spells them identically. On a JDK 19+ host (shortest-representation digits) the probe
   * fails and the column keeps host serialization. Binary formats carry IEEE bytes and are exempt.
   */
  private static boolean floatSpellingVerified(String formatIdentifier, List<LogicalType> types) {
    boolean textual = FormatCodes.isJsonFamily(formatIdentifier) || "csv".equals(formatIdentifier);
    if (!textual || types.stream().noneMatch(KafkaSinkMatcher::containsFloat)) {
      return true;
    }
    return JdkFloatSpelling.nativeMatchesJvm();
  }

  private static boolean containsFloat(LogicalType type) {
    return type.is(LogicalTypeRoot.FLOAT)
        || type.is(LogicalTypeRoot.DOUBLE)
        || type.getChildren().stream().anyMatch(KafkaSinkMatcher::containsFloat);
  }

  private static Map<String, String> options(StreamPhysicalSink sink) {
    try {
      ContextResolvedTable context = sink.contextResolvedTable();
      if (context == null) {
        return null;
      }
      ResolvedCatalogBaseTable<?> resolved = context.getResolvedTable();
      return resolved instanceof ResolvedCatalogTable
          ? ((ResolvedCatalogTable) resolved).getOptions()
          : null;
    } catch (RuntimeException ignored) {
      return null;
    }
  }

  static RelNode substitute(StreamPhysicalSink sink, PlanContext ctx) {
    KafkaSinkMatcher.Planned planned = KafkaSinkMatcher.plan(sink);
    if (planned.fallbackReason != null) {
      ctx.decline("kafka sink: " + planned.fallbackReason);
      return null;
    }
    // A CDC envelope format is Flink's way of writing a changelog to an ordinary kafka table (its
    // sink requests the full changelog, UPDATE_BEFORE included), so changelog input is exactly the
    // admitted case there; every other non-upsert format requires an insert-only stream.
    boolean insertOnly =
        ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInputs().get(0));
    if (!planned.upsert && !FormatCodes.isCdc(planned.valueFormat.format) && !insertOnly) {
      ctx.decline("kafka sink: the input is a changelog, not an insert-only stream");
      return null;
    }
    // When sink.parallelism differs from the input's on a changelog edge, stock Flink keys the
    // edge by primary key (or rejects the plan without one) so same-key changes stay ordered; the
    // native sink only sets the parallelism, which would rebalance whole batches and let a key's
    // updates interleave across subtasks. Keep those plans on Flink's own translation.
    if (planned.sink.parallelism != null && !insertOnly) {
      ctx.decline(
          "kafka sink: sink.parallelism on a changelog input (Flink repartitions by primary key)");
      return null;
    }
    return new StreamPhysicalNativeKafkaSink(
        sink.getCluster(),
        sink.getTraitSet(),
        sink.getInputs().get(0),
        sink.getRowType(),
        planned);
  }
}
