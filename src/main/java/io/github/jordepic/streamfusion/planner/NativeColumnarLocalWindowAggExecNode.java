package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeColumnarLocalWindowAggregateOperator;
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

/** Wraps the columnar local (pre-aggregate) window operator into the plan; Arrow batches in and out. */
public class NativeColumnarLocalWindowAggExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-columnar-local-window-aggregate";

  private final long sliceMillis;
  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;

  public NativeColumnarLocalWindowAggExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      long sliceMillis,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] valueTypes,
      int[] aggregateKinds) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-columnar-local-window-aggregate_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.sliceMillis = sliceMillis;
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.valueTypes = valueTypes;
    this.aggregateKinds = aggregateKinds;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    String timeZoneId = planner.getTableConfig().getLocalTimeZone().getId();
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta(TRANSFORMATION, config),
        new NativeColumnarLocalWindowAggregateOperator(
            sliceMillis,
            sliceMillis,
            timeColumn,
            valueColumns,
            keyColumns,
            NativeWindowOperatorCore.keyTypes((RowType) getOutputType(), keyColumns.length),
            valueTypes,
            aggregateKinds,
            timeZoneId),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
