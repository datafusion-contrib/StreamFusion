package tech.streamfusion.planner;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.rel.RelNode;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.WriteMode;
import org.apache.doris.flink.source.DorisSource;
import org.apache.doris.flink.table.DorisDynamicTableSink;
import org.apache.doris.flink.table.DorisDynamicTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.doris.DorisArrowSerializer;
import tech.streamfusion.doris.DorisArrowSource;
import tech.streamfusion.doris.DorisSchemas;

/** Arrow batch boundaries around the released connector's source and sink machinery. */
final class DorisTables {
  static void addSubstitutions(List<Substitution<?>> entries) {
    entries.add(
        Substitution.of(StreamPhysicalTableSourceScan.class, "dorisSource", DorisTables::source)
            .matching(
                scan ->
                    enabled()
                        && scan.getTable().unwrap(TableSourceTable.class) != null
                        && scan.getTable().unwrap(TableSourceTable.class).tableSource().getClass()
                            == DorisDynamicTableSource.class)
            .changelogSafe());
    entries.add(
        Substitution.of(StreamPhysicalSink.class, "dorisSink", DorisTables::sink)
            .matching(
                sink -> enabled() && sink.tableSink().getClass() == DorisDynamicTableSink.class)
            .changelogSafe());
  }

  private static boolean enabled() {
    return Boolean.parseBoolean(System.getProperty("streamfusion.doris.enabled", "true"));
  }

  static boolean supported(RowType type) {
    return type.getChildren().stream().allMatch(DorisTables::supported);
  }

  private static boolean supported(LogicalType type) {
    if (type instanceof org.apache.flink.table.types.logical.MultisetType
        || !type.getChildren().stream().allMatch(DorisTables::supported)) return false;
    try {
      tech.streamfusion.arrow.ArrowConversion.toArrowSchema(
          RowType.of(new LogicalType[] {type}, new String[] {"column"}));
      return true;
    } catch (UnsupportedOperationException | IllegalArgumentException unsupported) {
      return false;
    }
  }

  private static RelNode source(StreamPhysicalTableSourceScan scan, PlanContext context) {
    try {
      var original =
          (DorisDynamicTableSource) scan.getTable().unwrap(TableSourceTable.class).tableSource();
      var source = (DorisDynamicTableSource) original.copy();
      var reads = (DorisReadOptions) field(source, "readOptions");
      RowType type = FlinkTypeFactory$.MODULE$.toLogicalRowType(scan.getRowType());
      String reason =
          !verifiedVersion()
              ? "only connector 26.3.0 on Flink 2.2 is verified"
              : !supported(type)
                  ? "unsupported Arrow types"
                  : ScanWatermarkSpec.of(scan) != null ? "source watermarks" : null;
      if (reason != null) {
        context.decline("doris source: " + reason);
        return null;
      }
      var options = (DorisOptions) field(source, "options");
      Integer port =
          !reads.getUseOldApi() && reads.getUseFlightSql()
              ? org.apache.doris.flink.rest.RestService.tryGetArrowFlightSqlPort(
                  options, reads, org.slf4j.LoggerFactory.getLogger(DorisPlannerExtension.class))
              : null;
      if ((port == null || port <= 0) && reads.getScanMode().hasIncrementalPhase()) {
        context.decline(
            "doris source: Flight port discovery failed; keep the stock transport selection");
        return null;
      }
      reads.setFlightSqlPort(port);
      var provider = source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
      DorisSource<?> delegate =
          provider instanceof SourceProvider
              ? (DorisSource<?>) ((SourceProvider) provider).createSource()
              : null;
      var configuredReads =
          delegate == null ? reads : (DorisReadOptions) field(delegate, "readOptions");
      String layoutFailure = DorisSchemas.incompatibility(options, configuredReads, type);
      if (layoutFailure != null) {
        context.decline("doris source: " + layoutFailure);
        return null;
      }
      return new StreamPhysicalNativeDorisSource(
          scan.getCluster(),
          scan.getTraitSet(),
          scan.getRowType(),
          delegate == null
              ? new tech.streamfusion.doris.DorisArrowInputFormat(options, configuredReads, type)
              : new DorisArrowSource(delegate, options, configuredReads, type));
    } catch (ReflectiveOperationException | LinkageError incompatible) {
      context.decline("doris source: incompatible connector API");
      return null;
    }
  }

  private static RelNode sink(StreamPhysicalSink sink, PlanContext context) {
    try {
      var table = (DorisDynamicTableSink) sink.tableSink().copy();
      var execution = (DorisExecutionOptions) field(table, "executionOptions");
      var reads = (DorisReadOptions) field(table, "readOptions");
      var options = (DorisOptions) field(table, "options");
      RowType type =
          (RowType)
              sink.contextResolvedTable()
                  .getResolvedTable()
                  .getResolvedSchema()
                  .toPhysicalRowDataType()
                  .getLogicalType();
      String reason =
          !verifiedVersion()
              ? "only connector 26.3.0 on Flink 2.2 is verified"
              : !"arrow".equals(execution.getStreamLoadProp().getProperty("format"))
                  ? "requires format=arrow"
                  : execution.getWriteMode() != WriteMode.STREAM_LOAD
                          && execution.getWriteMode() != WriteMode.STREAM_LOAD_BATCH
                      ? "requires Arrow Stream Load writer"
                      : sink.upsertMaterialize()
                          ? "upsert materialization"
                          : !tech.streamfusion.compat.FlinkCompat.sinkAbilities(sink).isEmpty()
                              ? "sink abilities"
                              : (boolean) field(table, "overwrite")
                                  ? "overwrite"
                                  : !supported(type)
                                      ? "unsupported Arrow types"
                                      : SinkConstraintGate.fallbackReason(sink, true);
      if (reason != null) {
        context.decline("doris sink: " + reason);
        return null;
      }
      boolean deletable =
          execution.getDeletable()
              && org.apache.doris.flink.rest.RestService.isUniqueKeyType(
                  options, reads, org.slf4j.LoggerFactory.getLogger(DorisPlannerExtension.class));
      var properties = execution.getStreamLoadProp();
      if (!properties.containsKey("columns")) {
        properties.setProperty(
            "columns", quotedColumns(type, ",") + (deletable ? ",__DORIS_DELETE_SIGN__" : ""));
      }
      var delegate =
          new DorisSink<>(options, reads, execution, new DorisArrowSerializer(type, deletable));
      int[] keys =
          sink.contextResolvedTable()
              .getResolvedSchema()
              .getPrimaryKey()
              .map(
                  key ->
                      key.getColumns().stream().mapToInt(type.getFieldNames()::indexOf).toArray())
              .orElse(new int[0]);
      var notNull = new String[type.getFieldCount()];
      for (int i = 0; i < type.getFieldCount(); i++)
        if (!type.getTypeAt(i).isNullable()
            && sink.getInput().getRowType().getFieldList().get(i).getType().isNullable())
          notNull[i] = type.getFieldNames().get(i);
      var runtime =
          new DorisSinkRuntime(
              delegate,
              type,
              (Integer) field(table, "sinkParallelism"),
              keys,
              FlinkKeyGroupUtils.timestampPrecisions(sink.getInput().getRowType(), keys),
              notNull);
      return new StreamPhysicalNativeDorisSink(
          sink.getCluster(), sink.getTraitSet(), sink.getInput(), sink.getRowType(), runtime);
    } catch (ReflectiveOperationException | LinkageError incompatible) {
      context.decline("doris sink: incompatible connector API");
      return null;
    }
  }

  private static Object field(Object owner, String name) throws ReflectiveOperationException {
    Field field = owner.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(owner);
  }

  private static String quotedColumns(RowType type, String separator) {
    return type.getFieldNames().stream()
        .map(name -> "`" + name.trim().replace("`", "") + "`")
        .collect(java.util.stream.Collectors.joining(separator));
  }

  private static boolean verifiedVersion() {
    try (var input =
        DorisSource.class.getResourceAsStream(
            "/META-INF/maven/org.apache.doris/flink-doris-connector-2.2/pom.properties")) {
      if (input == null) return false;
      var properties = new Properties();
      properties.load(input);
      return "26.3.0".equals(properties.getProperty("version"));
    } catch (java.io.IOException unavailable) {
      return false;
    }
  }
}
