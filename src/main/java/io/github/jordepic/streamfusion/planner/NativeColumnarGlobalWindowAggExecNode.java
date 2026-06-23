package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.NativeColumnarGlobalWindowAggregateOperator;
import io.github.jordepic.streamfusion.operator.NativeWindowOperatorCore;
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
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/**
 * Wraps the columnar global (merge) window operator into the plan: partial-state Arrow batches in,
 * final result rows out.
 */
public class NativeColumnarGlobalWindowAggExecNode extends ExecNodeBase<RowData>
    implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

  private static final String TRANSFORMATION = "native-columnar-global-window-aggregate";

  private final long windowMillis;
  private final long slideMillis;
  private final boolean cumulative;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;

  public NativeColumnarGlobalWindowAggExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      long windowMillis,
      long slideMillis,
      boolean cumulative,
      int[] keyColumns,
      int[] valueTypes,
      int[] aggregateKinds) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-columnar-global-window-aggregate_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.cumulative = cumulative;
    this.keyColumns = keyColumns;
    this.valueTypes = valueTypes;
    this.aggregateKinds = aggregateKinds;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    String timeZoneId = planner.getTableConfig().getLocalTimeZone().getId();
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta(TRANSFORMATION, config),
        new NativeColumnarGlobalWindowAggregateOperator(
            windowMillis,
            slideMillis,
            cumulative,
            NativeWindowOperatorCore.keyTypes((RowType) getOutputType(), keyColumns.length),
            valueTypes,
            aggregateKinds,
            timeZoneId),
        InternalTypeInfo.of(getOutputType()),
        input.getParallelism(),
        false);
  }
}
