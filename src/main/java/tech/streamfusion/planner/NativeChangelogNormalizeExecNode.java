package tech.streamfusion.planner;

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
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeColumnarChangelogNormalizeOperator;

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

  private Integer stateMaxParallelism;

  public NativeChangelogNormalizeExecNode withStateMaxParallelism(int maximum) {
    if (maximum < 1)
      throw new IllegalArgumentException("State maximum parallelism must be positive");
    stateMaxParallelism = maximum;
    return this;
  }

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
    int maxParallelism =
        stateMaxParallelism != null
            ? stateMaxParallelism
            : FlinkKeyGroupUtils.maxParallelism(planner.getExecEnv(), input.getParallelism());
    boolean miniBatch = config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
    long miniBatchSize =
        miniBatch ? tech.streamfusion.compat.FlinkCompat.miniBatchSize(config) : -1;
    long stateTtlMillis = config.getStateRetentionTime();
    OneInputTransformation<ArrowBatch, ArrowBatch> transformation =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta(TRANSFORMATION, config),
            new NativeColumnarChangelogNormalizeOperator(
                keyColumns,
                keyTimestampPrecisions,
                (RowType) getOutputType(),
                generateUpdateBefore,
                miniBatch,
                miniBatchSize,
                stateTtlMillis,
                maxParallelism),
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    FlinkKeyGroupUtils.applyColumnarKeying(transformation, maxParallelism);
    return transformation;
  }
}
