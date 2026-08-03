package tech.streamfusion.planner;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeColumnarExpandOperator;
import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
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
 * Wraps the stateless native GROUPING SETS / CUBE / ROLLUP expansion into the plan; it consumes and
 * produces Arrow batches, fanning each row out to one row per grouping set.
 */
public class NativeExpandExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-expand";

  private final int numExpandRows;
  private final int numOutputColumns;
  private final int expandIdIndex;
  private final boolean expandIdIsLong;
  private final int[] copyIndices;
  private final long[] expandIdValues;

  public NativeExpandExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int numExpandRows,
      int numOutputColumns,
      int expandIdIndex,
      boolean expandIdIsLong,
      int[] copyIndices,
      long[] expandIdValues) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-expand_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.numExpandRows = numExpandRows;
    this.numOutputColumns = numOutputColumns;
    this.expandIdIndex = expandIdIndex;
    this.expandIdIsLong = expandIdIsLong;
    this.copyIndices = copyIndices;
    this.expandIdValues = expandIdValues;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta(TRANSFORMATION, config),
        new NativeColumnarExpandOperator(
            numExpandRows,
            numOutputColumns,
            expandIdIndex,
            expandIdIsLong,
            copyIndices,
            expandIdValues),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
