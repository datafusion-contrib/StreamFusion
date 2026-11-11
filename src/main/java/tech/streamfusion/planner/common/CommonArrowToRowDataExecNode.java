package tech.streamfusion.planner.common;

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
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowToRowDataOperator;

/** Wraps the Arrow→row transpose operator into the plan, returning the stream to rows. */
public class CommonArrowToRowDataExecNode extends ExecNodeBase<RowData>
    implements SingleTransformationTranslator<RowData> {

  private static final String TRANSFORMATION = "arrow-to-row";

  private final RowType rowType;
  private final boolean cachedBinaryRows;

  public CommonArrowToRowDataExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType rowType,
      String description) {
    this(tableConfig, inputProperty, rowType, description, false);
  }

  public CommonArrowToRowDataExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType rowType,
      String description,
      boolean cachedBinaryRows) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-arrow-to-row_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        rowType,
        description);
    this.rowType = rowType;
    this.cachedBinaryRows = cachedBinaryRows;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta(TRANSFORMATION, config),
        new ArrowToRowDataOperator(rowType, cachedBinaryRows),
        InternalTypeInfo.of(rowType),
        input.getParallelism(),
        false);
  }
}
