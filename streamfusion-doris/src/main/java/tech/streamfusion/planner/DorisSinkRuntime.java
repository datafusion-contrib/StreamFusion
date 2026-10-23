package tech.streamfusion.planner;

import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeConstraintEnforcer;
import tech.streamfusion.operator.NativeSinkConstraintEnforcer;
import tech.streamfusion.operator.NativeSinkMaterializer;
import tech.streamfusion.operator.OrderedKeyGroupReassembler;
import tech.streamfusion.operator.SplitByKeyGroupOperator;

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
    var policy = config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE);
    boolean shuffle =
        materialize
            || keys.length != 0
                && target != 1
                && (policy == ExecutionConfigOptions.SinkKeyedShuffle.FORCE
                    || policy == ExecutionConfigOptions.SinkKeyedShuffle.AUTO
                        && target != input.getParallelism());
    if (!shuffle) return input;
    int groups = FlinkKeyGroupUtils.maxParallelism(environment, target);
    boolean recoverable = environment.getCheckpointConfig().isUnalignedCheckpointsEnabled();
    var split =
        new OneInputTransformation<>(
            input,
            "doris-primary-key-split",
            SimpleOperatorFactory.of(
                new SplitByKeyGroupOperator(keys, timestampPrecisions, groups, recoverable)),
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    var partition =
        new PartitionTransformation<>(split, new ColumnarKeyGroupPartitioner(groups, recoverable));
    partition.setParallelism(target);
    partition.setMaxParallelism(groups);
    Transformation<ArrowBatch> routed = partition;
    if (recoverable) {
      var reassembled =
          new OneInputTransformation<>(
              partition,
              "doris-primary-key-reassemble",
              SimpleOperatorFactory.of(new OrderedKeyGroupReassembler(groups)),
              ArrowBatchTypeInformation.INSTANCE,
              target,
              false);
      reassembled.setMaxParallelism(groups);
      routed = reassembled;
    }
    if (!materialize) return routed;
    // Flink 2.2 explicitly disables TTL for its non-legacy materializer strategies.
    var strategy = config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_STRATEGY);
    long retention = config.getStateRetentionTime();
    long ttl =
        strategy == ExecutionConfigOptions.SinkUpsertMaterializeStrategy.LEGACY ? retention : 0;
    boolean adaptive = strategy == ExecutionConfigOptions.SinkUpsertMaterializeStrategy.ADAPTIVE;
    Long high =
        adaptive
            ? config.get(
                ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_HIGH)
            : null;
    Long low =
        adaptive
            ? config.get(
                ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_LOW)
            : null;
    NativeSinkMaterializer operator =
        strategy == ExecutionConfigOptions.SinkUpsertMaterializeStrategy.LEGACY
            ? new NativeSinkMaterializer(keys, timestampPrecisions, inputUpsertKeys, ttl, groups)
            : new StrategyMaterializer(
                type, strategy, keys, timestampPrecisions, inputUpsertKeys, groups, high, low);
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

  /** Retains released Flink 2.2's state strategy and backend-dependent threshold validation. */
  private static final class StrategyMaterializer extends NativeSinkMaterializer {
    private final ExecutionConfigOptions.SinkUpsertMaterializeStrategy strategy;
    private final Long high;
    private final Long low;
    private final int[] matchPrecisions;
    private boolean serializedKeys;
    private long effectiveHigh;
    private long effectiveLow;

    StrategyMaterializer(
        RowType type,
        ExecutionConfigOptions.SinkUpsertMaterializeStrategy strategy,
        int[] keys,
        int[] precisions,
        int[] upsertKeys,
        int groups,
        Long high,
        Long low) {
      super(keys, precisions, upsertKeys, 0, groups);
      this.strategy = strategy;
      int[] match =
          upsertKeys == null || upsertKeys.length == 0
              ? java.util.stream.IntStream.range(0, type.getFieldCount()).toArray()
              : upsertKeys;
      matchPrecisions = FlinkKeyGroupUtils.timestampPrecisions(type, match);
      this.high = high;
      this.low = low;
    }

    @Override
    protected void beforeHandleCreation() {
      String backend = getKeyedStateStore().getBackendTypeIdentifier().trim();
      boolean heap = backend.equalsIgnoreCase("heap") || backend.equalsIgnoreCase("hashmap");
      serializedKeys = !heap;
      if (strategy == ExecutionConfigOptions.SinkUpsertMaterializeStrategy.ADAPTIVE) {
        effectiveHigh = high == null ? (heap ? 400 : 50) : high;
        effectiveLow = low == null ? (heap ? 300 : 40) : low;
        org.apache.flink.util.Preconditions.checkArgument(effectiveHigh > effectiveLow);
      }
    }

    @Override
    protected int strategyMode() {
      switch (strategy) {
        case VALUE:
          return 1;
        case MAP:
          return serializedKeys ? 4 : 2;
        case ADAPTIVE:
          return serializedKeys ? 5 : 3;
        default:
          return 0;
      }
    }

    @Override
    protected int[] matchTimestampPrecisions() {
      return matchPrecisions;
    }

    @Override
    protected long adaptiveHighThreshold() {
      return effectiveHigh;
    }

    @Override
    protected long adaptiveLowThreshold() {
      return effectiveLow;
    }
  }
}
