package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeWindowJoinOperator;
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

/** Wraps the native columnar window-join operator into the plan; Arrow batches on both inputs and out. */
public class NativeWindowJoinExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String TRANSFORMATION = "native-window-join";

  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int leftWindowStart;
  private final int leftWindowEnd;
  private final int rightWindowStart;
  private final int rightWindowEnd;
  private final int joinType;
  private final RowType leftType;
  private final RowType rightType;
  private final RexExpression predicate;
  private final boolean proctime;
  private final long windowMillis;
  private final long slideMillis;
  private final boolean cumulative;

  public NativeWindowJoinExecNode(
      ReadableConfig tableConfig,
      InputProperty leftInputProperty,
      InputProperty rightInputProperty,
      RowType outputType,
      String description,
      int[] leftKeys,
      int[] rightKeys,
      int leftWindowStart,
      int leftWindowEnd,
      int rightWindowStart,
      int rightWindowEnd,
      int joinType,
      RowType leftType,
      RowType rightType,
      RexExpression predicate,
      boolean proctime,
      long windowMillis,
      long slideMillis,
      boolean cumulative) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-window-join_1"),
        tableConfig,
        Arrays.asList(leftInputProperty, rightInputProperty),
        outputType,
        description);
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.leftWindowStart = leftWindowStart;
    this.leftWindowEnd = leftWindowEnd;
    this.rightWindowStart = rightWindowStart;
    this.rightWindowEnd = rightWindowEnd;
    this.joinType = joinType;
    this.leftType = leftType;
    this.rightType = rightType;
    this.predicate = predicate;
    this.proctime = proctime;
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.cumulative = cumulative;
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
        new NativeWindowJoinOperator(
            leftKeys,
            rightKeys,
            leftWindowStart,
            leftWindowEnd,
            rightWindowStart,
            rightWindowEnd,
            joinType,
            leftType,
            rightType,
            RexExpression.toEncodedPredicate(predicate),
            proctime,
            windowMillis,
            slideMillis,
            cumulative),
        ArrowBatchTypeInformation.INSTANCE,
        left.getParallelism(),
        false);
  }
}
