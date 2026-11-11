package tech.streamfusion.planner;

import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ProjectPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ReadingMetadataSpec;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.parquet.NativeParquet;
import tech.streamfusion.parquet.NativeParquetSource;
import tech.streamfusion.planner.common.StreamRule;

/** Native Parquet sources and sink rewrites contributed only by streamfusion-parquet. */
public final class ParquetPlannerExtension implements NativePlannerExtension {
  @Override
  public Transformation<ArrowBatch> lowerBatchFileSource(
      BatchExecTableSourceScan scan,
      Transformation<RowData> original,
      RowType output,
      int batchRows,
      ReadableConfig config) {
    if (!(original instanceof SourceTransformation) || output.getFieldCount() == 0) return null;
    var stock = (SourceTransformation<?, ?, ?>) original;
    if (stock.getSource().getClass() != FileSource.class
        || stock.getBoundedness() != Boundedness.BOUNDED) return null;
    var table = scan.getTableSourceSpec().getContextResolvedTable().getResolvedTable();
    if (!(table instanceof ResolvedCatalogTable)) return null;
    var resolved = (ResolvedCatalogTable) table;
    var options = resolved.getOptions();
    if (!"filesystem".equals(options.get("connector"))
        || !"parquet".equals(options.get("format"))
        || !options.containsKey("path")
        || !resolved.getPartitionKeys().isEmpty()
        || !resolved.getResolvedSchema().getColumns().stream().allMatch(Column::isPhysical)
        || !Set.of("connector", "format", "path").containsAll(options.keySet())) return null;
    // Deliberately narrow first admission: no timezone, nested layout or schema-evolution claims.
    if (!output.getChildren().stream()
        .allMatch(
            type -> {
              switch (type.getTypeRoot()) {
                case INTEGER:
                case BIGINT:
                case DATE:
                case VARCHAR:
                  return true;
                case DECIMAL:
                  return ((DecimalType) type).getPrecision() <= 18;
                default:
                  return false;
              }
            })) return null;
    var physical = (RowType) resolved.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    for (var field : output.getFields()) {
      int index = physical.getFieldNames().indexOf(field.getName());
      if (index < 0 || !physical.getTypeAt(index).equals(field.getType())) return null;
    }
    var abilities = scan.getTableSourceSpec().getSourceAbilities();
    if (abilities != null)
      for (var ability : abilities) {
        if (ability instanceof ProjectPushDownSpec) {
          for (int[] projection : ((ProjectPushDownSpec) ability).getProjectedFields())
            if (projection.length != 1) return null;
        } else if (ability instanceof ReadingMetadataSpec
            && ((ReadingMetadataSpec) ability).getMetadataKeys().isEmpty()) {
          // The filesystem connector records an empty metadata projection even for physical-only
          // scans.
        } else if (!(ability instanceof FilterPushDownSpec)
            || !((FilterPushDownSpec) ability).isAllPredicatesRetained()) return null;
      }
    try {
      NativeParquet.isLoaded();
    } catch (LinkageError unavailable) {
      return null;
    }
    var paths = new Path[] {new Path(options.get("path"))};
    var source =
        new NativeParquetSource(
            paths,
            ArrowConversion.toArrowInputSchema(output, false).serializeAsMessage(),
            output.getFieldNames().toArray(new String[0]),
            batchRows);
    var converted =
        new SourceTransformation<>(
            "NativeParquetSource",
            source,
            WatermarkStrategy.noWatermarks(),
            ArrowBatchTypeInformation.INSTANCE,
            original.getParallelism());
    if (original.getMaxParallelism() > 0) converted.setMaxParallelism(original.getMaxParallelism());
    converted.setResources(original.getMinResources(), original.getPreferredResources());
    original.getSlotSharingGroup().ifPresent(converted::setSlotSharingGroup);
    return converted;
  }

  private static final FileSinkMatcher.Format FORMAT =
      new FileSinkMatcher.Format() {
        public boolean changelog(java.util.Map<String, String> options) {
          return "changelog-parquet".equals(options.get("connector"));
        }

        public boolean matches(java.util.Map<String, String> options) {
          return FileSinkMatcher.Format.super.matches(options) || changelog(options);
        }

        public java.util.Map<String, String> writerOptions(java.util.Map<String, String> options) {
          if (!changelog(options)) return options;
          var result = new java.util.LinkedHashMap<>(options);
          result.put("connector", "filesystem");
          result.put("format", "parquet");
          result.putIfAbsent("parquet.write.int64.timestamp", "true");
          result.putIfAbsent("parquet.utc-timezone", "true");
          return result;
        }

        public String name() {
          return "parquet";
        }

        public FileSinkMatcher.Settings settings(
            java.util.Map<String, String> options, RowType type, List<String> partitions) {
          ParquetSinkTranslator.Result result =
              ParquetSinkTranslator.translate(options, type, partitions);
          return result.fallbackReason == null
              ? new FileSinkMatcher.Settings(result.encoderKeys(), result.encoderValues(), null)
              : new FileSinkMatcher.Settings(null, null, result.fallbackReason);
        }

        public tech.streamfusion.format.ColumnarFileCodec codec(
            RowType type, List<String> partitions) {
          return new tech.streamfusion.parquet.ParquetCodec();
        }
      };

  @Override
  public void addSubstitutions(List<StreamRule<?>> entries) {
    entries.add(
        StreamRule.of(
                StreamPhysicalSink.class,
                (sink, ctx) -> FileSinkMatcher.substitute(sink, ctx, FORMAT))
            .matching(sink -> FileSinkMatcher.appliesTo(sink, FORMAT))
            .changelogSafe());
  }
}
