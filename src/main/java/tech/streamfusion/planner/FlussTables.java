package tech.streamfusion.planner;

import java.lang.reflect.Field;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.*;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.flink.FlinkConnectorOptions.ScanStartupMode;
import org.apache.fluss.flink.sink.FlinkTableSink;
import org.apache.fluss.flink.source.FlinkSource;
import org.apache.fluss.flink.source.FlinkTableSource;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.flink.source.reader.LeaseContext;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TablePath;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Whitelist around the released Fluss 1.0 Java connector's internal batch-level contracts. */
final class FlussTables {
  private FlussTables() {}

  static boolean isSource(StreamPhysicalTableSourceScan scan) {
    TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
    return table != null && table.tableSource().getClass() == FlinkTableSource.class;
  }

  static boolean isSink(StreamPhysicalSink sink) {
    return sink.tableSink() instanceof FlinkTableSink;
  }

  static RelNode source(StreamPhysicalTableSourceScan scan, PlanContext context) {
    TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
    FlinkTableSource source = (FlinkTableSource) table.tableSource();
    try {
      String fallback = sourceFallback(source);
      if (fallback != null) {
        context.decline("fluss source: " + fallback);
        return null;
      }
      RowType type = FlinkTypeFactory$.MODULE$.toLogicalRowType(scan.getRowType());
      RowType physical = (RowType) field(source, "tableOutputType");
      if (!supportedArrowLayout(type)
          || (type.getFieldCount() == 0
              && physical.getFieldNames().contains(RowDataArrowConverter.ROW_KIND_COLUMN))) {
        context.decline("fluss source: unsupported physical types");
        return null;
      }
      ScanWatermarkSpec watermark = ScanWatermarkSpec.of(scan);
      if (watermark == ScanWatermarkSpec.UNSUPPORTED) {
        context.decline("fluss source: unsupported source watermark");
        return null;
      }
      var startup = (FlinkConnectorOptionsUtils.StartupOptions) field(source, "startupOptions");
      var bounded = (FlinkConnectorOptionsUtils.BoundedOptions) field(source, "boundedOptions");
      OffsetsInitializer start;
      switch (startup.startupMode) {
        case EARLIEST:
        case FULL:
          start = OffsetsInitializer.earliest();
          break;
        case LATEST:
          start = OffsetsInitializer.latest();
          break;
        case TIMESTAMP:
          start = OffsetsInitializer.timestamp(startup.startupTimestampMs);
          break;
        default:
          throw new IllegalArgumentException("snapshot startup is outside Arrow admission");
      }
      Configuration config =
          clientConfiguration(
              (Configuration) field(source, "flussConfig"),
              (java.util.Map<?, ?>) field(source, "tableOptions"));
      TablePath path = (TablePath) field(source, "tablePath");
      FlinkSource<RowData> delegate =
          new FlinkSource<>(
              config,
              path,
              ((int[]) field(source, "primaryKeyIndexes")).length > 0,
              ((int[]) field(source, "partitionKeyIndexes")).length > 0,
              FlinkConversions.toFlussRowType(physical),
              null,
              (org.apache.fluss.predicate.Predicate) field(source, "logRecordBatchFilter"),
              start,
              FlinkConnectorOptionsUtils.toStoppingOffsetsInitializer(bounded),
              FlinkConnectorOptionsUtils.toBoundedness(true, bounded),
              (long) field(source, "scanPartitionDiscoveryIntervalMs"),
              (int) field(source, "splitPerAssignmentBatchSize"),
              new RowDataDeserializationSchema(),
              null,
              true,
              (org.apache.fluss.predicate.Predicate) field(source, "partitionFilters"),
              null,
              (LeaseContext) field(source, "leaseContext"));
      String sourceKey = sourceSharingKey(scan);
      boolean shared =
          context.repeatedSource(FlussPlannerExtension.class.getName() + "|" + sourceKey);
      if (shared && !supportedArrowLayout(physical)) {
        context.decline("fluss source: shared physical schema has unsupported Arrow layouts");
        return null;
      }
      org.apache.calcite.rel.type.RelDataType readType =
          shared
              ? ((org.apache.flink.table.planner.calcite.FlinkTypeFactory)
                      scan.getCluster().getTypeFactory())
                  .buildRelNodeRowType(physical)
              : scan.getRowType();
      ScanWatermarkSpec readWatermark =
          watermark == null
              ? null
              : watermark.withRowtimeIndex(
                  readType.getFieldNames().indexOf(watermark.rowtimeFieldName));
      RelNode result =
          new StreamPhysicalNativeFlussSource(
              scan.getCluster(),
              scan.getTraitSet(),
              readType,
              delegate,
              config,
              path,
              (org.apache.fluss.predicate.Predicate) field(source, "logRecordBatchFilter"),
              readWatermark,
              sourceKey,
              shared,
              0);
      if (!readType.equals(scan.getRowType())) {
        List<org.apache.calcite.rex.RexNode> projections = new java.util.ArrayList<>();
        for (var output : scan.getRowType().getFieldList()) {
          var input = readType.getField(output.getName(), true, false);
          if (input == null) {
            context.decline("fluss source: projection is not a physical column");
            return null;
          }
          projections.add(
              new org.apache.calcite.rex.RexInputRef(input.getIndex(), input.getType()));
        }
        result =
            new StreamPhysicalNativeCalc(
                scan.getCluster(),
                scan.getTraitSet(),
                result,
                scan.getRowType(),
                RexExpression.encodeProjections(projections, scan.getRowType().getFieldNames()));
      }
      return result;
    } catch (ReflectiveOperationException | LinkageError incompatible) {
      context.decline("fluss source: installed Java connector is outside the verified 1.0 API");
      return null;
    }
  }

  static String sourceSharingKey(StreamPhysicalTableSourceScan scan) {
    try {
      FlinkTableSource source =
          (FlinkTableSource) scan.getTable().unwrap(TableSourceTable.class).tableSource();
      RowType physical = (RowType) field(source, "tableOutputType");
      ScanWatermarkSpec watermark = ScanWatermarkSpec.of(scan);
      if (watermark == ScanWatermarkSpec.UNSUPPORTED) return null;
      if (watermark != null)
        watermark =
            watermark.withRowtimeIndex(
                physical.getFieldNames().indexOf(watermark.rowtimeFieldName));
      return field(source, "tablePath")
          + "|"
          + new java.util.TreeMap<>(FilesystemTables.options(scan))
          + "|"
          + predicateKey((org.apache.fluss.predicate.Predicate) field(source, "partitionFilters"))
          + "|"
          + predicateKey(
              (org.apache.fluss.predicate.Predicate) field(source, "logRecordBatchFilter"))
          + "|"
          + physical.asSerializableString()
          + "|"
          + (watermark == null
              ? "none"
              : watermark.rowtimeIndex
                  + ":"
                  + watermark.rowtimeFieldName
                  + ":"
                  + watermark.expression.digest()
                  + ":"
                  + watermark.idleTimeoutMillis);
    } catch (ReflectiveOperationException | java.io.IOException | RuntimeException incompatible) {
      return null;
    }
  }

  static String predicateKey(org.apache.fluss.predicate.Predicate predicate)
      throws java.io.IOException {
    if (predicate == null) return "none";
    return org.apache.flink.util.StringUtils.byteToHexString(
        org.apache.flink.util.InstantiationUtil.serializeObject(predicate));
  }

  static String sourceFallback(FlinkTableSource source) throws ReflectiveOperationException {
    if (!"1.0.0".equals(source.getClass().getPackage().getImplementationVersion()))
      return "only the released 1.0.0 Java connector is verified";
    if (!tech.streamfusion.fluss.FlussArrowClient.supportedTransport())
      return "released Java transport hook is unavailable";
    String clientFallback = clientOptionsFallback((Configuration) field(source, "flussConfig"));
    if (clientFallback == null)
      clientFallback = tableTransportFallback((java.util.Map<?, ?>) field(source, "tableOptions"));
    if (clientFallback != null) return clientFallback;
    if (!(boolean) field(source, "streaming")) return "batch/snapshot reads use Flink";
    if ((long) field(source, "limit") >= 0
        || (boolean) field(source, "selectRowCount")
        || field(source, "singleRowFilter") != null)
      return "point lookup, limit or aggregate pushdown uses Flink";
    TableConfig config = (TableConfig) field(source, "tableConfig");
    if (config.getLogFormat() != LogFormat.ARROW) return "only ARROW logs are admitted";
    if ((boolean) field(source, "isDataLakeEnabled") || field(source, "lakeSource") != null)
      return "data-lake hybrid reads use Flink";
    if (field(source, "mergeEngineType") != null)
      return "merge-engine changelog semantics use Flink";
    var startup = (FlinkConnectorOptionsUtils.StartupOptions) field(source, "startupOptions");
    if (startup.startupMode == ScanStartupMode.FULL
        && ((int[]) field(source, "primaryKeyIndexes")).length > 0)
      return "initial snapshots use Flink";
    return null;
  }

  static RelNode sink(StreamPhysicalSink sink, PlanContext context) {
    FlinkTableSink tableSink = (FlinkTableSink) sink.tableSink();
    try {
      String fallback = sinkFallback(tableSink);
      if (fallback == null)
        fallback = sinkOptionsFallback(sink.contextResolvedTable().getResolvedTable().getOptions());
      if (fallback == null) fallback = SinkConstraintGate.fallbackReason(sink);
      if (fallback == null
          && (sink.upsertMaterialize()
              || !tech.streamfusion.compat.FlinkCompat.sinkAbilities(sink).isEmpty()))
        fallback = "sink materialization or pushed abilities use Flink";
      if (fallback == null && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInput()))
        fallback = "append production requires insert-only input";
      RowType type = (RowType) field(tableSink, "tableRowType");
      if (fallback == null && !supportedArrowLayout(type)) fallback = "unsupported physical types";
      if (fallback == null
          && !"ARROW"
              .equalsIgnoreCase(
                  sink.contextResolvedTable()
                      .getResolvedTable()
                      .getOptions()
                      .get("table.log.format"))) {
        fallback = "append production requires an explicitly resolved ARROW table";
      }
      if (fallback == null
          && (!((List<?>) field(tableSink, "bucketKeys")).isEmpty()
              || new TableConfig(
                      Configuration.fromMap(
                          sink.contextResolvedTable().getResolvedTable().getOptions()))
                  .isStatisticsEnabled())) {
        try {
          tech.streamfusion.fluss.NativeFluss.requireLoaded();
        } catch (LinkageError unavailable) {
          fallback = "native bucket/statistics extension is unavailable";
        }
      }
      if (fallback != null) {
        context.decline("fluss sink: " + fallback);
        return null;
      }
      return new StreamPhysicalNativeFlussSink(
          sink.getCluster(),
          sink.getTraitSet(),
          sink.getInput(),
          sink.getRowType(),
          type,
          clientConfiguration(
              (Configuration) field(tableSink, "flussConfig"),
              sink.contextResolvedTable().getResolvedTable().getOptions()),
          (TablePath) field(tableSink, "tablePath"),
          (List<String>) field(tableSink, "partitionKeys"));
    } catch (ReflectiveOperationException | LinkageError incompatible) {
      context.decline("fluss sink: installed Java connector is outside the verified 1.0 API");
      return null;
    }
  }

  static String sinkFallback(FlinkTableSink sink) throws ReflectiveOperationException {
    if (!"1.0.0".equals(sink.getClass().getPackage().getImplementationVersion()))
      return "only the released 1.0.0 Java connector is verified";
    if (!tech.streamfusion.fluss.FlussArrowClient.supportedTransport())
      return "released Java transport hook is unavailable";
    String clientFallback = clientOptionsFallback((Configuration) field(sink, "flussConfig"));
    if (clientFallback != null) return clientFallback;
    if (((int[]) field(sink, "primaryKeyIndexes")).length != 0)
      return "primary-key production is explicitly not accelerated";

    RowType rowType = (RowType) field(sink, "tableRowType");
    for (Object key : (List<?>) field(sink, "bucketKeys")) {
      var type = rowType.getTypeAt(rowType.getFieldNames().indexOf(key.toString()));
      {
        boolean switchResult0;
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
          case BINARY:
          case VARBINARY:
          case DECIMAL:
          case DATE:
          case TIME_WITHOUT_TIME_ZONE:
          case TIMESTAMP_WITHOUT_TIME_ZONE:
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            switchResult0 = true;
            break;
          default:
            switchResult0 = false;
            break;
        }
        if (!switchResult0) return "complex bucket-key encoding uses Flink";
      }
    }
    if (!(boolean) field(sink, "streaming")) return "batch sinks use Flink";
    if (field(sink, "producerId") != null)
      return "explicit undo-recovery producer identity uses Flink";
    var distribution = field(sink, "distributionMode");
    if (distribution != org.apache.fluss.flink.sink.shuffle.DistributionMode.AUTO
        && distribution != org.apache.fluss.flink.sink.shuffle.DistributionMode.NONE)
      return "explicit bucket or dynamic partition shuffle uses Flink";
    if (field(sink, "mergeEngineType") != null
        || field(sink, "lakeFormat") != null
        || (boolean) field(sink, "sinkIgnoreDelete")
        || (boolean) field(sink, "appliedUpdates")
        || field(sink, "deleteRow") != null)
      return "merge engines, lake writes and row modifications use Flink";
    return null;
  }

  private static boolean supportedArrowLayout(RowType type) {
    if (!RowDataArrowConverter.supports(type)
        || type.getFieldNames().contains(RowDataArrowConverter.ROW_KIND_COLUMN)) return false;
    try {
      var wire =
          tech.streamfusion.fluss.FlussArrowSchema.wireSchema(
              FlinkConversions.toFlussRowType(type));
      var nativeSchema = tech.streamfusion.arrow.ArrowConversion.toArrowSchema(type);
      for (int i = 0; i < wire.getFields().size(); i++) {
        if (!tech.streamfusion.fluss.FlussArrowSchema.compatible(
            wire.getFields().get(i), nativeSchema.getFields().get(i))) return false;
      }
      return true;
    } catch (java.io.IOException | IllegalArgumentException unsupported) {
      return false;
    }
  }

  static String sinkOptionsFallback(java.util.Map<String, String> options) {
    String transportFallback = tableTransportFallback(options);
    if (transportFallback != null) return transportFallback;

    return null;
  }

  private static String tableTransportFallback(java.util.Map<?, ?> options) {
    return options.keySet().stream()
        .map(Object::toString)
        .filter(
            key ->
                (key.startsWith("client.") || key.startsWith("netty."))
                    && !tech.streamfusion.fluss.FlussClientOptions.supported(key))
        .sorted()
        .findFirst()
        .map(key -> "client setting " + key + " is outside the verified defaults")
        .orElse(null);
  }

  private static String clientOptionsFallback(Configuration config) {
    for (String key : new java.util.TreeSet<>(config.keySet())) {
      if (!tech.streamfusion.fluss.FlussClientOptions.supported(key))
        return "client setting " + key + " is outside the verified defaults";
    }
    return null;
  }

  private static Configuration clientConfiguration(
      Configuration base, java.util.Map<?, ?> options) {
    Configuration result = new Configuration(base);
    options.forEach(
        (key, value) -> {
          if (tech.streamfusion.fluss.FlussClientOptions.supported(key.toString()))
            result.setString(key.toString(), value.toString());
        });
    return result;
  }

  private static Object field(Object object, String name) throws ReflectiveOperationException {
    Field field = object.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(object);
  }
}
