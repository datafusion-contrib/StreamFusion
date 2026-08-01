package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.format.EncodeFormat;
import io.github.jordepic.streamfusion.format.FormatCodes;
import io.github.jordepic.streamfusion.kafka.NativeKafka;
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

  /** The value formats the native serializer implements, by Flink format identifier. */
  private static final Set<String> NATIVE_VALUE_FORMATS =
      Set.of("json", "csv", "avro", "avro-confluent", "protobuf", "raw");

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
      for (int keyField : keyFields) {
        LogicalType type = rowType.getTypeAt(keyField);
        if (!supportsType(keyFormatId, type)) {
          return Planned.fallback("key " + keyFormatId + " type " + type.asSummaryString());
        }
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
   * interval arm). FLOAT/DOUBLE are additionally declined natively: Flink spells them with the
   * JVM's JDK-version-dependent {@code Double.toString}, which has no byte-exact native
   * counterpart — the same reason the float-to-string CAST stays on the host.
   */
  private static boolean supportsCsvType(LogicalType type, boolean nested) {
    switch (type.getTypeRoot()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
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
    if (!planned.upsert
        && !FormatCodes.isCdc(planned.valueFormat.format)
        && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInputs().get(0))) {
      ctx.decline("kafka sink: the input is a changelog, not an insert-only stream");
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
