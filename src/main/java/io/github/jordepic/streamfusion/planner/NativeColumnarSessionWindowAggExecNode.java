package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeColumnarSessionWindowAggregateOperator;
import io.github.jordepic.streamfusion.operator.NativeWindowOperatorCore;
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
 * Execution node for the columnar session-window aggregate: it consumes Arrow batches from a columnar
 * exchange and emits the window-result batches (Arrow), wrapping {@link
 * NativeColumnarSessionWindowAggregateOperator}. The row-fed twin is {@link
 * NativeSessionWindowAggExecNode}.
 */
public class NativeColumnarSessionWindowAggExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-columnar-session-window-aggregate";

  private final long gapMillis;
  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;
  private final boolean proctime;
  private final boolean timestampLtz;

  public NativeColumnarSessionWindowAggExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      long gapMillis,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] valueTypes,
      int[] aggregateKinds,
      boolean proctime,
      boolean timestampLtz) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-columnar-session-window-aggregate_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.gapMillis = gapMillis;
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.valueTypes = valueTypes;
    this.aggregateKinds = aggregateKinds;
    this.proctime = proctime;
    this.timestampLtz = timestampLtz;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    String timeZoneId =
        timestampLtz ? planner.getTableConfig().getLocalTimeZone().getId() : "UTC";
    RowType outputType = (RowType) getOutputType();
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta(TRANSFORMATION, config),
        new NativeColumnarSessionWindowAggregateOperator(
            gapMillis,
            timeColumn,
            valueColumns,
            keyColumns,
            NativeWindowOperatorCore.keyTypes(outputType, keyColumns.length),
            valueTypes,
            aggregateKinds,
            timeZoneId,
            outputType,
            proctime),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
