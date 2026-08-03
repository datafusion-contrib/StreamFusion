package tech.streamfusion.planner;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeColumnarMiniBatchAssignerOperator;
import tech.streamfusion.operator.NativeColumnarRowTimeMiniBatchAssignerOperator;
import java.util.Collections;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
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

/** Wraps the columnar mini-batch assigner into the plan; Arrow batches in/out, plus marker watermarks. */
public class NativeMiniBatchAssignerExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String TRANSFORMATION = "native-columnar-mini-batch-assigner";

  private final long intervalMs;
  private final boolean rowTime;

  public NativeMiniBatchAssignerExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      long intervalMs,
      boolean rowTime) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-columnar-mini-batch-assigner_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.intervalMs = intervalMs;
    this.rowTime = rowTime;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    OneInputStreamOperator<ArrowBatch, ArrowBatch> operator =
        rowTime
            ? new NativeColumnarRowTimeMiniBatchAssignerOperator(intervalMs)
            : new NativeColumnarMiniBatchAssignerOperator(intervalMs);
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta(TRANSFORMATION, config),
        operator,
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
