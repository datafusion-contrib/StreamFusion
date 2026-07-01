package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeColumnarUpdatingJoinOperator;
import io.github.jordepic.streamfusion.operator.NativeUdf;
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

/**
 * Wraps the columnar regular (updating) join operator into the plan; Arrow batches in and out.
 * INNER/LEFT/RIGHT/FULL/SEMI/ANTI, with an optional residual non-equi predicate.
 */
public class NativeColumnarUpdatingJoinExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String TRANSFORMATION = "native-updating-join";
  private static final int[] EMPTY_INT = new int[0];
  private static final long[] EMPTY_LONG = new long[0];
  private static final double[] EMPTY_DOUBLE = new double[0];
  private static final String[] EMPTY_STRING = new String[0];

  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int joinType;
  private final RowType leftType;
  private final RowType rightType;
  private final RexExpression predicate;

  public NativeColumnarUpdatingJoinExecNode(
      ReadableConfig tableConfig,
      InputProperty leftInputProperty,
      InputProperty rightInputProperty,
      RowType outputType,
      String description,
      int[] leftKeys,
      int[] rightKeys,
      int joinType,
      RowType leftType,
      RowType rightType,
      RexExpression predicate) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-updating-join_1"),
        tableConfig,
        Arrays.asList(leftInputProperty, rightInputProperty),
        outputType,
        description);
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.joinType = joinType;
    this.leftType = leftType;
    this.rightType = rightType;
    this.predicate = predicate;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> left =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    Transformation<ArrowBatch> right =
        (Transformation<ArrowBatch>) getInputEdges().get(1).translateToPlan(planner);
    Transformation<ArrowBatch> transformation =
        ExecNodeUtil.createTwoInputTransformation(
            left,
            right,
            createTransformationMeta(TRANSFORMATION, config),
            new NativeColumnarUpdatingJoinOperator(
                leftKeys,
                rightKeys,
                joinType,
                leftType,
                rightType,
                predicate == null ? EMPTY_INT : predicate.kinds(),
                predicate == null ? EMPTY_INT : predicate.payload(),
                predicate == null ? EMPTY_INT : predicate.childCounts(),
                predicate == null ? EMPTY_LONG : predicate.longs(),
                predicate == null ? EMPTY_DOUBLE : predicate.doubles(),
                predicate == null ? EMPTY_STRING : predicate.strings(),
                predicate == null ? NativeUdf.Binding.EMPTY : predicate.udfBinding()),
            ArrowBatchTypeInformation.INSTANCE,
            left.getParallelism(),
            false);
    NativeManagedMemory.declareOperatorWeight(transformation);
    return transformation;
  }
}
