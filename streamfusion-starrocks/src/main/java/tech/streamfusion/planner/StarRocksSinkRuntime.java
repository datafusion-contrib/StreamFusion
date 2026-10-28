package tech.streamfusion.planner;

import java.util.Arrays;
import java.util.Objects;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeConstraintEnforcer;
import tech.streamfusion.operator.NativeSinkMaterializer;
import tech.streamfusion.starrocks.NativeStarRocksSink;
import tech.streamfusion.starrocks.NativeStarRocksSinkFunction;
import tech.streamfusion.starrocks.StarRocksEncoding;

/**
 * Carries the stock SQL sink's NOT NULL check, primary-key routing, upsert materialization and
 * parallelism to the connector writer the table selected, now fed with Arrow batches.
 */
final class StarRocksSinkRuntime {
  final RowType type;
  final String table;
  final boolean insertOnly;
  final Integer parallelism;
  final boolean inheritsDefaultParallelism;
  final int[] keys;
  final int[] timestampPrecisions;
  final String[] notNull;
  final boolean materialize;
  final int[] inputUpsertKeys;
  final StarRocksEncoding encoding;
  final NativeStarRocksSink unifiedSink;
  final NativeStarRocksSinkFunction sinkFunction;

  StarRocksSinkRuntime(
      RowType type,
      String table,
      boolean insertOnly,
      Integer parallelism,
      boolean inheritsDefaultParallelism,
      int[] keys,
      int[] timestampPrecisions,
      String[] notNull,
      boolean materialize,
      int[] inputUpsertKeys,
      StarRocksEncoding encoding,
      NativeStarRocksSink unifiedSink,
      NativeStarRocksSinkFunction sinkFunction) {
    this.type = type;
    this.table = table;
    this.insertOnly = insertOnly;
    this.parallelism = parallelism;
    this.inheritsDefaultParallelism = inheritsDefaultParallelism;
    this.keys = keys;
    this.timestampPrecisions = timestampPrecisions;
    this.notNull = notNull;
    this.materialize = materialize;
    this.inputUpsertKeys = inputUpsertKeys;
    this.encoding = encoding;
    this.unifiedSink = unifiedSink;
    this.sinkFunction = sinkFunction;
  }

  @SuppressWarnings("unchecked")
  Transformation<Object> translate(
      Transformation<ArrowBatch> input,
      StreamExecutionEnvironment environment,
      ExecNodeConfig config,
      String description) {
    int flinkInput = ColumnarSinkRouting.flinkInputParallelism(input, inheritsDefaultParallelism);
    ColumnarSinkRouting.requirePrimaryKeyForRedistributedChangelog(
        table, insertOnly, keys, parallelism == null ? flinkInput : parallelism, flinkInput);
    int target = parallelism == null ? input.getParallelism() : parallelism;
    var stream = new DataStream<>(environment, prepare(input, environment, config));
    DataStreamSink<ArrowBatch> sink =
        unifiedSink != null
            ? stream.sinkTo(unifiedSink)
            : stream.addSink((SinkFunction<ArrowBatch>) (SinkFunction<?>) sinkFunction);
    sink.name(NativePlanDetails.name("NativeSink", description)).setParallelism(target);
    sink.getTransformation().setDescription(description);
    return (Transformation<Object>) (Transformation<?>) sink.getTransformation();
  }

  Transformation<ArrowBatch> prepare(
      Transformation<ArrowBatch> input,
      StreamExecutionEnvironment environment,
      ExecNodeConfig config) {
    if (Arrays.stream(notNull).anyMatch(Objects::nonNull))
      input =
          new OneInputTransformation<>(
              input,
              "starrocks-not-null-enforcer",
              SimpleOperatorFactory.of(new NativeConstraintEnforcer(notNull)),
              ArrowBatchTypeInformation.INSTANCE,
              input.getParallelism(),
              false);
    int flinkInput = ColumnarSinkRouting.flinkInputParallelism(input, inheritsDefaultParallelism);
    if (!ColumnarSinkRouting.shuffles(
        config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE),
        keys,
        parallelism == null ? flinkInput : parallelism,
        flinkInput,
        materialize)) return input;
    int target = parallelism == null ? input.getParallelism() : parallelism;
    var routed =
        ColumnarSinkRouting.route(
            input, environment, keys, timestampPrecisions, target, "starrocks");
    if (!materialize) return routed;
    int groups = FlinkKeyGroupUtils.maxParallelism(environment, target);
    // Flink 1.18's materializer keeps rows for the job's idle-state retention.
    var materialized =
        new OneInputTransformation<>(
            routed,
            "native-sink-upsert-materialize",
            SimpleOperatorFactory.of(
                new NativeSinkMaterializer(
                    keys,
                    timestampPrecisions,
                    inputUpsertKeys,
                    config.getStateRetentionTime(),
                    groups)),
            ArrowBatchTypeInformation.INSTANCE,
            target,
            false);
    FlinkKeyGroupUtils.applyColumnarKeying(materialized, groups);
    return materialized;
  }
}
