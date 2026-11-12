package tech.streamfusion.planner;

import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeConstraintEnforcer;
import tech.streamfusion.operator.NativeSinkConstraintEnforcer;
import tech.streamfusion.operator.NativeSinkMaterializer;

/**
 * Carries the stock SQL sink's constraints, parallelism and primary-key routing to its Arrow edge.
 */
final class DorisSinkRuntime {
  final DorisSink<ArrowBatch> sink;
  final RowType type;
  final Integer parallelism;
  final int[] keys;
  final int[] timestampPrecisions;
  final String[] notNull;
  final boolean materialize;
  final int[] inputUpsertKeys;

  DorisSinkRuntime(
      DorisSink<ArrowBatch> sink,
      RowType type,
      Integer parallelism,
      int[] keys,
      int[] timestampPrecisions,
      String[] notNull) {
    this(sink, type, parallelism, keys, timestampPrecisions, notNull, false, null);
  }

  DorisSinkRuntime(
      DorisSink<ArrowBatch> sink,
      RowType type,
      Integer parallelism,
      int[] keys,
      int[] timestampPrecisions,
      String[] notNull,
      boolean materialize,
      int[] inputUpsertKeys) {
    this.sink = sink;
    this.type = type;
    this.parallelism = parallelism;
    this.keys = keys;
    this.timestampPrecisions = timestampPrecisions;
    this.notNull = notNull;
    this.materialize = materialize;
    this.inputUpsertKeys = inputUpsertKeys;
  }

  Transformation<ArrowBatch> prepare(
      Transformation<ArrowBatch> input,
      StreamExecutionEnvironment environment,
      ExecNodeConfig config) {
    var notNullPolicy = config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER);
    var lengthPolicy = config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER);
    boolean[] lengths = new boolean[type.getFieldCount()];
    for (int i = 0; i < lengths.length; i++)
      lengths[i] =
          lengthPolicy != ExecutionConfigOptions.TypeLengthEnforcer.IGNORE
              && (type.getTypeAt(i) instanceof org.apache.flink.table.types.logical.CharType
                  || type.getTypeAt(i) instanceof org.apache.flink.table.types.logical.BinaryType
                  || type.getTypeAt(i) instanceof org.apache.flink.table.types.logical.VarCharType
                      && ((org.apache.flink.table.types.logical.VarCharType) type.getTypeAt(i))
                              .getLength()
                          < org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH
                  || type.getTypeAt(i) instanceof org.apache.flink.table.types.logical.VarBinaryType
                      && ((org.apache.flink.table.types.logical.VarBinaryType) type.getTypeAt(i))
                              .getLength()
                          < org.apache.flink.table.types.logical.VarBinaryType.MAX_LENGTH);
    boolean hasLengths = false;
    for (boolean length : lengths) hasLengths |= length;
    boolean hasNotNull = java.util.Arrays.stream(notNull).anyMatch(name -> name != null);
    if (hasLengths || hasNotNull)
      input =
          new OneInputTransformation<>(
              input,
              !hasLengths && notNullPolicy == ExecutionConfigOptions.NotNullEnforcer.ERROR
                  ? "doris-not-null-enforcer"
                  : "doris-sink-constraint-enforcer",
              SimpleOperatorFactory.of(
                  !hasLengths && notNullPolicy == ExecutionConfigOptions.NotNullEnforcer.ERROR
                      ? new NativeConstraintEnforcer(notNull)
                      : new NativeSinkConstraintEnforcer(
                          type,
                          org.apache.flink.table.runtime.operators.sink.constraint
                                  .ConstraintEnforcerExecutor.create(
                                      type,
                                      notNullPolicy,
                                      lengthPolicy,
                                      ExecutionConfigOptions.NestedEnforcer.IGNORE)
                                  .orElseThrow()
                              ::enforce,
                          lengths)),
              ArrowBatchTypeInformation.INSTANCE,
              input.getParallelism(),
              false);
    int target = parallelism == null ? input.getParallelism() : parallelism;
    if (!ColumnarSinkRouting.shuffles(
        config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE),
        keys,
        target,
        input.getParallelism(),
        materialize)) return input;
    var routed =
        ColumnarSinkRouting.route(input, environment, keys, timestampPrecisions, target, "doris");
    if (!materialize) return routed;
    int groups = FlinkKeyGroupUtils.maxParallelism(environment, target);
    // Flink 2.2 explicitly disables TTL for its non-legacy materializer strategies.
    var strategy = config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_STRATEGY);
    long retention = config.getStateRetentionTime();
    long ttl =
        strategy == ExecutionConfigOptions.SinkUpsertMaterializeStrategy.LEGACY ? retention : 0;
    NativeSinkMaterializer operator =
        SinkMaterializerFactory.create(
            type, keys, timestampPrecisions, inputUpsertKeys, ttl, groups, strategy, config);
    var materialized =
        new OneInputTransformation<>(
            routed,
            "native-sink-upsert-materialize",
            SimpleOperatorFactory.of(operator),
            ArrowBatchTypeInformation.INSTANCE,
            target,
            false);
    FlinkKeyGroupUtils.applyColumnarKeying(materialized, groups);
    return materialized;
  }
}
