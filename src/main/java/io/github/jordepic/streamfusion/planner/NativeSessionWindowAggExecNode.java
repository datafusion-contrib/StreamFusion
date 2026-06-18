package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.NativeSessionWindowAggregateOperator;
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
 * Execution node wrapping the native session-window aggregate operator into the plan, carrying the
 * inactivity gap and the event-time, value, and key column positions the operator needs.
 */
public class NativeSessionWindowAggExecNode extends ExecNodeBase<RowData>
    implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

  private static final int BATCH_SIZE = 1024;
  private static final String TRANSFORMATION = "native-session-window-aggregate";

  private final long gapMillis;
  private final int timeColumn;
  private final int valueColumn;
  private final int[] keyColumns;
  private final int valueType;
  private final int[] aggregateKinds;

  public NativeSessionWindowAggExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      long gapMillis,
      int timeColumn,
      int valueColumn,
      int[] keyColumns,
      int valueType,
      int[] aggregateKinds) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-session-window-aggregate_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.gapMillis = gapMillis;
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
        new NativeSessionWindowAggregateOperator(
            gapMillis,
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
