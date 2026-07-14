package io.github.jordepic.streamfusion.planner;

import java.util.Map;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/** Conservative match boundary for native JSON serialization into Flink's Kafka sink. */
final class KafkaSinkMatcher {

  private KafkaSinkMatcher() {}

  static final class Planned {
    final RowType rowType;
    final KafkaSinkTranslator.Planned sink;
    final String timestampFormat;
    final boolean ignoreNullFields;
    final String fallbackReason;

    private Planned(
        RowType rowType,
        KafkaSinkTranslator.Planned sink,
        String timestampFormat,
        boolean ignoreNullFields,
        String fallbackReason) {
      this.rowType = rowType;
      this.sink = sink;
      this.timestampFormat = timestampFormat;
      this.ignoreNullFields = ignoreNullFields;
      this.fallbackReason = fallbackReason;
    }

    private static Planned fallback(String reason) {
      return new Planned(null, null, null, false, reason);
    }
  }

  static boolean appliesTo(StreamPhysicalSink sink) {
    Map<String, String> options = options(sink);
    if (options == null || !"kafka".equals(options.get("connector"))) {
      return false;
    }
    return "json".equals(options.getOrDefault("value.format", options.get("format")));
  }

  static Planned plan(StreamPhysicalSink sink) {
    if (sink.abilitySpecs().length != 0) {
      SinkAbilitySpec spec = sink.abilitySpecs()[0];
      return Planned.fallback("sink ability " + spec.getClass().getSimpleName());
    }
    KafkaSinkTranslator.Result translated = KafkaSinkTranslator.translate(options(sink));
    if (!translated.isTranslated()) {
      return Planned.fallback(translated.fallbackReason().orElseThrow());
    }
    RowType rowType = FlinkTypeFactory$.MODULE$.toLogicalRowType(sink.getRowType());
    for (LogicalType type : rowType.getChildren()) {
      if (!supportsJsonType(type)) {
        return Planned.fallback("JSON type " + type.asSummaryString());
      }
    }
    Map<String, String> json = translated.planned().jsonOptions;
    String timestampFormat = json.getOrDefault("timestamp-format.standard", "SQL");
    boolean ignoreNullFields =
        Boolean.parseBoolean(json.getOrDefault("encode.ignore-null-fields", "false"));
    return new Planned(rowType, translated.planned(), timestampFormat, ignoreNullFields, null);
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
}
