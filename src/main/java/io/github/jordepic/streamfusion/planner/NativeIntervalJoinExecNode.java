package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeIntervalJoinOperator;
import java.util.Arrays;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;

/** Wraps the native columnar interval-join operator into the plan; Arrow batches on both inputs and out. */
public class NativeIntervalJoinExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String TRANSFORMATION = "native-interval-join";

  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int leftTime;
  private final int rightTime;
  private final long lowerMillis;
  private final long upperMillis;
  private final int joinType;
  private final RowType leftType;
  private final RowType rightType;
  private final RexExpression predicate;
  private final boolean proctime;

  public NativeIntervalJoinExecNode(
      ReadableConfig tableConfig,
      InputProperty leftInputProperty,
      InputProperty rightInputProperty,
      RowType outputType,
      String description,
      int[] leftKeys,
      int[] rightKeys,
      int leftTime,
      int rightTime,
      long lowerMillis,
      long upperMillis,
      int joinType,
      RowType leftType,
      RowType rightType,
      RexExpression predicate,
      boolean proctime) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-interval-join_1"),
        tableConfig,
        Arrays.asList(leftInputProperty, rightInputProperty),
        outputType,
        description);
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.leftTime = leftTime;
    this.rightTime = rightTime;
    this.lowerMillis = lowerMillis;
    this.upperMillis = upperMillis;
    this.joinType = joinType;
    this.leftType = leftType;
    this.rightType = rightType;
    this.predicate = predicate;
    this.proctime = proctime;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> left =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    Transformation<ArrowBatch> right =
        (Transformation<ArrowBatch>) getInputEdges().get(1).translateToPlan(planner);
    return ExecNodeUtil.createTwoInputTransformation(
        left,
        right,
        createTransformationMeta(TRANSFORMATION, config),
        new NativeIntervalJoinOperator(
            leftKeys,
            rightKeys,
            leftTime,
            rightTime,
            lowerMillis,
            upperMillis,
            joinType,
            leftType,
            rightType,
            RexExpression.toEncodedPredicate(predicate),
            proctime),
        ArrowBatchTypeInformation.INSTANCE,
        left.getParallelism(),
        false);
  }
}
