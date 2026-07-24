package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeColumnarDeduplicateOperator;
import io.github.jordepic.streamfusion.operator.NativeColumnarKeepLastDeduplicateOperator;
import java.util.Collections;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;

/**
 * Wraps the native columnar keep-first deduplicator into the plan; it consumes and produces Arrow
 * batches, buffering each and emitting each key's first (minimum-rowtime) row as a watermark
 * completes it.
 */
public class NativeDeduplicateExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-deduplicate";

  private final int[] partitionColumns;
  private final int rowtimeColumn;
  private final boolean keepLast;
  private final boolean generateUpdateBefore;
  private final boolean proctime;
  private final int[] keyTimestampPrecisions;

  public NativeDeduplicateExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int[] partitionColumns,
      int rowtimeColumn,
      boolean keepLast,
      boolean generateUpdateBefore,
      boolean proctime,
      int[] keyTimestampPrecisions) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-deduplicate_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.partitionColumns = partitionColumns;
    this.rowtimeColumn = rowtimeColumn;
    this.keepLast = keepLast;
    this.generateUpdateBefore = generateUpdateBefore;
    this.proctime = proctime;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    // The eager push→emit operator serves proctime (either keep mode) and rowtime keep-last; only
    // rowtime keep-first is watermark-buffered (it emits a key's min-rowtime row once a watermark
    // completes it).
    boolean eager = proctime || keepLast;
    int maxParallelism = FlinkKeyGroupUtils.defaultMaxParallelism(input.getParallelism());
    int[] stateKeys = FlinkKeyGroupUtils.stateKeysForSubtasks(maxParallelism, input.getParallelism());
    KeySelector<ArrowBatch, Integer> stateKeySelector =
        batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0];
    boolean miniBatch =
        keepLast
            && config.get(
                org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
    long miniBatchSize =
        config.get(org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE);
    OneInputStreamOperator<ArrowBatch, ArrowBatch> operator =
        eager
            ? new NativeColumnarKeepLastDeduplicateOperator(
                partitionColumns,
                keyTimestampPrecisions,
                rowtimeColumn,
                (RowType) getOutputType(),
                generateUpdateBefore,
                !proctime,
                !keepLast,
                miniBatch,
                miniBatchSize,
                maxParallelism)
            : new NativeColumnarDeduplicateOperator(
                partitionColumns, keyTimestampPrecisions, rowtimeColumn, maxParallelism);
    OneInputTransformation<ArrowBatch, ArrowBatch> transformation =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta(TRANSFORMATION, config),
            operator,
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
