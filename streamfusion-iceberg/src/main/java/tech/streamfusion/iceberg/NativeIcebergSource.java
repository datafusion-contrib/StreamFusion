package tech.streamfusion.iceberg;

import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformationWrapper;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkReadConf;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;

/** Java plans snapshots and splits; native readers decode admitted Parquet tasks. */
public final class NativeIcebergSource {
  private NativeIcebergSource() {}

  public static DataStreamSource<ArrowBatch> read(
      StreamExecutionEnvironment env,
      Table table,
      TableLoader loader,
      ReadableConfig configuration,
      Map<String, String> options,
      Schema projection,
      int batchRows) {
    return read(env, table, loader, configuration, options, projection, List.of(), batchRows);
  }

  public static DataStreamSource<ArrowBatch> read(
      StreamExecutionEnvironment env,
      Table table,
      TableLoader loader,
      ReadableConfig configuration,
      Map<String, String> options,
      Schema projection,
      List<Expression> filters,
      int batchRows) {
    IcebergSourcePlan.validate(table, configuration, options, projection, filters, false);
    var stream =
        env.fromSource(
            arrowSource(
                table, loader, configuration, options, projection, filters, batchRows, null),
            WatermarkStrategy.noWatermarks(),
            "IcebergSource-" + table.name(),
            ArrowBatchTypeInformation.INSTANCE);
    String parallelism = options.get("source.parallelism");
    if (parallelism != null) stream.setParallelism(Integer.parseInt(parallelism));
    return stream;
  }

  public static Transformation<ArrowBatch> readBatch(
      Transformation<RowData> input, IcebergSourcePlan plan, int batchRows) {
    var sourceInput =
        input instanceof SourceTransformationWrapper<?> wrapper ? wrapper.getInput() : input;
    if (!(sourceInput instanceof SourceTransformation<?, ?, ?> original)
        || !(original.getSource() instanceof IcebergSource<?> host)
        || original.getBoundedness() != Boundedness.BOUNDED) return null;
    var source =
        arrowSource(
            plan.table,
            plan.loader,
            plan.configuration,
            plan.options,
            plan.projection,
            plan.filters,
            batchRows,
            host);
    // The stock source has already inferred parallelism and may have cached its planned splits.
    var result =
        new SourceTransformation<>(
            "NativeIcebergBatchSource-" + plan.table.name(),
            source,
            WatermarkStrategy.noWatermarks(),
            ArrowBatchTypeInformation.INSTANCE,
            original.getParallelism(),
            original.isParallelismConfigured());
    if (original.getMaxParallelism() > 0) result.setMaxParallelism(original.getMaxParallelism());
    result.setUid(original.getUid());
    if (original.getUserProvidedNodeHash() != null)
      result.setUidHash(original.getUserProvidedNodeHash());
    result.setDescription(original.getDescription());
    result.setResources(original.getMinResources(), original.getPreferredResources());
    result.setChainingStrategy(original.getChainingStrategy());
    result.setLineageVertex(original.getLineageVertex());
    original.getSlotSharingGroup().ifPresent(result::setSlotSharingGroup);
    return sourceInput == input ? result : new SourceTransformationWrapper<>(result);
  }

  @SuppressWarnings("deprecation")
  private static IcebergArrowSource arrowSource(
      Table table,
      TableLoader loader,
      ReadableConfig configuration,
      Map<String, String> options,
      Schema projection,
      List<Expression> filters,
      int batchRows,
      IcebergSource<?> host) {
    if (batchRows <= 0) throw new IllegalArgumentException("Arrow batch size must be positive");
    var conf = new FlinkReadConf(table, options, configuration);
    var function =
        new IcebergArrowReaderFunction(
            table.schema(),
            projection,
            table.io(),
            batchRows,
            filters,
            conf.caseSensitive(),
            conf.nameMapping(),
            table.encryption(),
            configuration);
    var source =
        IcebergSource.<ArrowBatch>builder()
            .table(table)
            .tableLoader(loader)
            .flinkConfig(configuration)
            .setAll(options)
            .assignerFactory(
                configuration.get(FlinkConfigOptions.TABLE_EXEC_SPLIT_ASSIGNER_TYPE).factory())
            .project(projection)
            .filters(filters)
            .readerFunction(function)
            .build();
    return new IcebergArrowSource(source, host == null ? source : host, function);
  }
}
