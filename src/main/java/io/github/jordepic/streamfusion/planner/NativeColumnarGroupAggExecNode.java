package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import java.util.Collections;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
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
    this.keyTimestampPrecisions = keyTimestampPrecisions;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    int maxParallelism = FlinkKeyGroupUtils.defaultMaxParallelism(input.getParallelism());
    int[] stateKeys = FlinkKeyGroupUtils.stateKeysForSubtasks(maxParallelism, input.getParallelism());
    // Raw keyed state uses the exchange's BinaryRow key groups. This selector only establishes a
    // valid Flink keyed context for an entire columnar batch; no managed keyed state reads it.
    KeySelector<ArrowBatch, Integer> stateKeySelector =
        batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0];
    boolean miniBatch =
        config.get(
            org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
    long miniBatchSize =
        config.get(org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE);
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
                keyTimestampPrecisions,
                maxParallelism),
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    transformation.setMaxParallelism(maxParallelism);
    transformation.setStateKeySelector(stateKeySelector);
    transformation.setStateKeyType(Types.INT);
    NativeManagedMemory.declareOperatorWeight(transformation);
    return transformation;
  }
}
