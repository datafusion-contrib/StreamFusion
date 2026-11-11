package tech.streamfusion.planner.stream;

import java.util.Arrays;
import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
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
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeColumnarFirstNOperator;
import tech.streamfusion.planner.FlinkKeyGroupUtils;

public class NativeFirstNExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {
  private final int[] partitions;
  private final int[] precisions;
  private final int limit;
  private final boolean outputRank;
  private Integer stateMaxParallelism;

  public NativeFirstNExecNode withStateMaxParallelism(int maximum) {
    if (maximum < 1)
      throw new IllegalArgumentException("State maximum parallelism must be positive");
    stateMaxParallelism = maximum;
    return this;
  }

  boolean sameStateDefinition(NativeFirstNExecNode other) {
    return limit == other.limit
        && outputRank == other.outputRank
        && Arrays.equals(partitions, other.partitions)
        && Arrays.equals(precisions, other.precisions);
  }

  public NativeFirstNExecNode(
      ReadableConfig config,
      InputProperty input,
      RowType output,
      String description,
      int[] partitions,
      int[] precisions,
      int limit,
      boolean outputRank) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-first-n_1"),
        config,
        Collections.singletonList(input),
        output,
        description);
    this.partitions = partitions;
    this.precisions = precisions;
    this.limit = limit;
    this.outputRank = outputRank;
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
    OneInputTransformation<ArrowBatch, ArrowBatch> transformation =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta("native-first-n", config),
            new NativeColumnarFirstNOperator(
                partitions,
                precisions,
                limit,
                outputRank,
                config.getStateRetentionTime(),
                maxParallelism),
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    FlinkKeyGroupUtils.applyColumnarKeying(transformation, maxParallelism);
    return transformation;
  }
}
