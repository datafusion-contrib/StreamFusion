package tech.streamfusion.planner;

import io.delta.flink.sink.DeltaSinkConf;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.planner.plan.abilities.sink.OverwriteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/** Whitelist-first admission for the path-based Delta data-file writer. */
final class DeltaSinkMatcher {
  private DeltaSinkMatcher() {}

  static final class Planned {
    final String path;
    final String catalogName;
    final String tableName;
    final RowType rowType;
    final List<String> partitionKeys;
    final Map<String, String> options;
    final List<Integer> primaryKeyOrdinals;
    final String fallbackReason;

    private Planned(
        String path,
        String catalogName,
        String tableName,
        RowType rowType,
        List<String> partitionKeys,
        Map<String, String> options,
        List<Integer> primaryKeyOrdinals,
        String fallbackReason) {
      this.path = path;
      this.catalogName = catalogName;
      this.tableName = tableName;
      this.rowType = rowType;
      this.partitionKeys = partitionKeys;
      this.options = options;
      this.primaryKeyOrdinals = primaryKeyOrdinals;
      this.fallbackReason = fallbackReason;
    }

    static Planned fallback(String reason) {
      return new Planned(null, null, null, null, null, null, null, reason);
    }
  }

  static boolean appliesTo(StreamPhysicalSink sink) {
    Map<String, String> options = options(sink);
    return options != null && "delta".equals(options.get("connector"));
  }

  static Planned plan(StreamPhysicalSink sink) {
    ResolvedCatalogTable table = table(sink);
    Map<String, String> options = new LinkedHashMap<>(table.getOptions());
    String path = options.get("table_path");
    ObjectIdentifier identifier = sink.contextResolvedTable().getIdentifier();
    if (path == null) {
      return Planned.fallback(
          "native Delta writes currently require a path-based table on the published connector API");
    }
    for (SinkAbilitySpec ability : sink.abilitySpecs()) {
      if (ability instanceof OverwriteSpec) {
        return Planned.fallback("INSERT OVERWRITE is not supported");
      }
    }
    RowType rowType =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    for (LogicalType type : rowType.getChildren()) {
      if (!supported(type)) {
        return Planned.fallback("Delta column type " + type + " is not verified by the native writer");
      }
    }
    if (!"no".equalsIgnoreCase(options.getOrDefault("schema_evolution.mode", "no"))) {
      return Planned.fallback("schema evolution is enabled");
    }
    List<String> partitions =
        Arrays.stream(options.getOrDefault("partitions", "").split(","))
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .collect(Collectors.toList());
    List<Integer> primaryKeys = new ArrayList<>();
    if ("upsert".equalsIgnoreCase(options.getOrDefault("write.mode", "append"))) {
      Optional<UniqueConstraint> key = table.getResolvedSchema().getPrimaryKey();
      if (key.isEmpty()) {
        return Planned.fallback("Delta upsert requires a primary key");
      }
      List<String> names = rowType.getFieldNames();
      for (String column : key.get().getColumns()) {
        primaryKeys.add(names.indexOf(column));
      }
      options.put(
          DeltaSinkConf.PRIMARY_KEY.key(),
          primaryKeys.stream().map(String::valueOf).collect(Collectors.joining(",")));
    }
    return new Planned(
        path,
        identifier.getCatalogName(),
        identifier.asSummaryString(),
        rowType,
        partitions,
        options,
        primaryKeys,
        null);
  }

  private static boolean supported(LogicalType type) {
    boolean supported = switch (type.getTypeRoot()) {
      case BOOLEAN,
          TINYINT,
          SMALLINT,
          INTEGER,
          BIGINT,
          FLOAT,
          DOUBLE,
          DECIMAL,
          CHAR,
          VARCHAR,
          BINARY,
          VARBINARY,
          DATE,
          TIMESTAMP_WITHOUT_TIME_ZONE,
          TIMESTAMP_WITH_LOCAL_TIME_ZONE -> true;
      case ARRAY, MAP, ROW -> type.getChildren().stream().allMatch(DeltaSinkMatcher::supported);
      default -> false;
    };
    return supported;
  }

  static RelNode substitute(StreamPhysicalSink sink, PlanContext context) {
    if (!NativeConfig.operatorEnabled("deltaSink")) {
      context.decline(Substitution.disabledReason("deltaSink"));
      return null;
    }
    Planned planned = plan(sink);
    if (planned.fallbackReason != null) {
      context.decline("delta sink: " + planned.fallbackReason);
      return null;
    }
    return new StreamPhysicalNativeDeltaSink(
        sink.getCluster(), sink.getTraitSet(), sink.getInput(), sink.getRowType(), planned);
  }

  private static ResolvedCatalogTable table(StreamPhysicalSink sink) {
    return (ResolvedCatalogTable) sink.contextResolvedTable().getResolvedTable();
  }

  private static Map<String, String> options(StreamPhysicalSink sink) {
    try {
      ResolvedCatalogBaseTable<?> table = sink.contextResolvedTable().getResolvedTable();
      return table instanceof ResolvedCatalogTable
          ? ((ResolvedCatalogTable) table).getOptions()
          : null;
    } catch (RuntimeException failure) {
      return null;
    }
  }
}
