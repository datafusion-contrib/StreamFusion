package tech.streamfusion.planner;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeTemporalJoinOperator;
import java.util.Arrays;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;

/** Wraps the native temporal-join operator into the plan; Arrow batches on both inputs and out. */
public class NativeTemporalJoinExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String TRANSFORMATION = "native-temporal-join";

  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int leftTime;
  private final int rightTime;
  private final int joinType;
  private final RowType leftType;
  private final RowType rightType;
  private final RexExpression predicate;
  private final int[] keyTimestampPrecisions;

  public NativeTemporalJoinExecNode(
      ReadableConfig tableConfig,
      InputProperty leftInputProperty,
      InputProperty rightInputProperty,
      RowType outputType,
      String description,
      int[] leftKeys,
      int[] rightKeys,
      int leftTime,
      int rightTime,
      int joinType,
      RowType leftType,
      RowType rightType,
      RexExpression predicate,
      int[] keyTimestampPrecisions) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-temporal-join_1"),
        tableConfig,
        Arrays.asList(leftInputProperty, rightInputProperty),
        outputType,
        description);
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.leftTime = leftTime;
    this.rightTime = rightTime;
    this.joinType = joinType;
    this.leftType = leftType;
    this.rightType = rightType;
    this.predicate = predicate;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> left =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    Transformation<ArrowBatch> right =
        (Transformation<ArrowBatch>) getInputEdges().get(1).translateToPlan(planner);
    int maxParallelism = FlinkKeyGroupUtils.defaultMaxParallelism(left.getParallelism());
    // The job-wide retention only: Flink's StreamExecTemporalJoin reads the config directly, with
    // no STATE_TTL hint support. The 1.5x max deadline horizon is derived natively.
    long stateTtlMillis = config.getStateRetentionTime();
    TwoInputTransformation<ArrowBatch, ArrowBatch, ArrowBatch> transformation =
        ExecNodeUtil.createTwoInputTransformation(
            left,
            right,
            createTransformationMeta(TRANSFORMATION, config),
            new NativeTemporalJoinOperator(
                leftKeys,
                rightKeys,
                leftTime,
                rightTime,
                joinType,
                leftType,
                rightType,
                RexExpression.toEncodedPredicate(predicate),
                keyTimestampPrecisions,
                stateTtlMillis,
                maxParallelism),
            ArrowBatchTypeInformation.INSTANCE,
            left.getParallelism(),
            false);
    FlinkKeyGroupUtils.applyColumnarKeying(transformation, maxParallelism);
    return transformation;
  }
}
