package tech.streamfusion.iceberg;

import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkReadConf;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;

/** Java discovers streaming snapshots and splits; native readers decode admitted Parquet tasks. */
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

  @SuppressWarnings("deprecation")
  public static DataStreamSource<ArrowBatch> read(
      StreamExecutionEnvironment env,
      Table table,
      TableLoader loader,
      ReadableConfig configuration,
      Map<String, String> options,
      Schema projection,
      List<Expression> filters,
      int batchRows) {
    if (batchRows <= 0) throw new IllegalArgumentException("Arrow batch size must be positive");
    IcebergSourcePlan.validate(table, configuration, options, projection, filters);
    var conf = new FlinkReadConf(table, options, configuration);
    var function =
        new IcebergArrowReaderFunction(
            table.schema(),
            projection.columns().stream().map(f -> f.fieldId()).toList(),
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
    var stream =
        env.fromSource(
            new IcebergArrowSource(source, function),
            WatermarkStrategy.noWatermarks(),
            "IcebergSource-" + table.name(),
            ArrowBatchTypeInformation.INSTANCE);
    String parallelism = options.get("source.parallelism");
    if (parallelism != null) stream.setParallelism(Integer.parseInt(parallelism));
    return stream;
  }
}
