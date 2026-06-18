package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.NativeWindowAggregateOperator;
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
 * Execution node that wraps the native tumbling-window aggregate operator into the plan, resolving
 * its input transformation and carrying the window size and the event-time and value column
 * positions the operator needs.
 */
public class NativeWindowAggExecNode extends ExecNodeBase<RowData>
    implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

  private static final int BATCH_SIZE = 1024;
  private static final String TRANSFORMATION = "native-window-aggregate";

  private final boolean cumulative;
  private final long windowMillis;
  private final long slideMillis;
  private final int timeColumn;
  private final int valueColumn;
  private final int[] keyColumns;
  private final int valueType;
  private final int[] aggregateKinds;

  public NativeWindowAggExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      boolean cumulative,
      long windowMillis,
      long slideMillis,
      int timeColumn,
      int valueColumn,
      int[] keyColumns,
      int valueType,
      int[] aggregateKinds) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-window-aggregate_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.cumulative = cumulative;
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.timeColumn = timeColumn;
    this.valueColumn = valueColumn;
    this.keyColumns = keyColumns;
    this.valueType = valueType;
    this.aggregateKinds = aggregateKinds;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<RowData> input =
        (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
    String timeZoneId = planner.getTableConfig().getLocalTimeZone().getId();
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta(TRANSFORMATION, config),
        new NativeWindowAggregateOperator(
            cumulative,
            windowMillis,
            slideMillis,
            timeColumn,
            valueColumn,
            keyColumns,
            valueType,
            aggregateKinds,
            timeZoneId,
            BATCH_SIZE),
        InternalTypeInfo.of(getOutputType()),
        input.getParallelism(),
        false);
  }
}
