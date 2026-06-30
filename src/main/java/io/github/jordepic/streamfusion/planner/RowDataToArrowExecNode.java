package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.RowDataToArrowOperator;
import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;

/** Wraps the row→Arrow transpose operator into the plan, producing the columnar batch stream. */
public class RowDataToArrowExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final int BATCH_SIZE = 1024;
  private static final String TRANSFORMATION = "row-to-arrow";

  private final RowType rowType;
  private final boolean carryRowKind;
  private final RowType sourceType;

  public RowDataToArrowExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType rowType,
      String description,
      boolean carryRowKind,
      RowType sourceType) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-row-to-arrow_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        rowType,
        description);
    this.rowType = rowType;
    this.carryRowKind = carryRowKind;
    this.sourceType = sourceType;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<RowData> input =
        (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta(TRANSFORMATION, config),
        new RowDataToArrowOperator(rowType, BATCH_SIZE, carryRowKind, sourceType),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
