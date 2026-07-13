package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeColumnarChangelogNormalizeOperator;
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
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;

/**
 * Wraps the native changelog normalizer into the plan; it consumes and produces Arrow batches,
 * keeping the last row per unique key and emitting the normalized changelog.
 */
public class NativeChangelogNormalizeExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-changelog-normalize";

  private final int[] keyColumns;
  private final boolean generateUpdateBefore;
  private final int[] keyTimestampPrecisions;

  public NativeChangelogNormalizeExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int[] keyColumns,
      boolean generateUpdateBefore,
      int[] keyTimestampPrecisions) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-changelog-normalize_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.keyColumns = keyColumns;
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
            new NativeColumnarChangelogNormalizeOperator(
                keyColumns,
                keyTimestampPrecisions,
                generateUpdateBefore,
                miniBatch,
                miniBatchSize,
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
