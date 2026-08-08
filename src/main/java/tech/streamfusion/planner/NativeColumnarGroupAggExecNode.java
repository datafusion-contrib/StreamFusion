package tech.streamfusion.planner;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;

/** Wraps the columnar non-windowed GROUP BY operator into the plan; Arrow batches in and out. */
public class NativeColumnarGroupAggExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String TRANSFORMATION = "native-columnar-group-aggregate";

  private final int[] aggregateKinds;
  private final int[] valueTypes;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] filterColumns;
  private final int[] countColumns;
  private final int[] distinctViewColumns;
  private final int recordCountColumn;
  private final boolean generateUpdateBefore;
  // Per-operator TTL from a STATE_TTL hint on the aggregate (-1 = no hint); resolved against the
  // job-wide table.exec.state.ttl at translate time, hint winning — Flink's StateMetadata rule.
  private final long stateTtlHintMillis;
  private final int[] keyTimestampPrecisions;

  public NativeColumnarGroupAggExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      int[] filterColumns,
      int[] countColumns,
      int[] distinctViewColumns,
      int recordCountColumn,
      boolean generateUpdateBefore,
      long stateTtlHintMillis,
      int[] keyTimestampPrecisions) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-columnar-group-aggregate_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.aggregateKinds = aggregateKinds;
    this.valueTypes = valueTypes;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.filterColumns = filterColumns;
    this.countColumns = countColumns;
    this.distinctViewColumns = distinctViewColumns;
    this.recordCountColumn = recordCountColumn;
    this.generateUpdateBefore = generateUpdateBefore;
    this.stateTtlHintMillis = stateTtlHintMillis;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    int maxParallelism =
        FlinkKeyGroupUtils.maxParallelism(planner.getExecEnv(), input.getParallelism());
    boolean miniBatch = config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
    long miniBatchSize = config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE);
    long stateTtlMillis =
        stateTtlHintMillis >= 0 ? stateTtlHintMillis : config.getStateRetentionTime();
    OneInputTransformation<ArrowBatch, ArrowBatch> transformation =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta(TRANSFORMATION, config),
            new NativeColumnarGroupAggregateOperator(
                aggregateKinds, valueTypes, valueColumns, keyColumns, filterColumns, countColumns,
                distinctViewColumns,
                recordCountColumn,
                generateUpdateBefore,
                miniBatch,
                miniBatchSize,
                stateTtlMillis,
                keyTimestampPrecisions,
                maxParallelism),
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    FlinkKeyGroupUtils.applyColumnarKeying(transformation, maxParallelism);
    return transformation;
  }
}
