package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.NativeGlobalWindowAggregateOperator;
import io.github.jordepic.streamfusion.operator.NativeWindowOperatorBase;
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

/** Wraps the native global (merge) window operator into the plan. */
public class NativeGlobalWindowAggExecNode extends ExecNodeBase<RowData>
    implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

  private static final int BATCH_SIZE = 1024;
  private static final String TRANSFORMATION = "native-global-window-aggregate";

  private final long windowMillis;
  private final int[] keyColumns;
  private final long slideMillis;
  private final int[] partialColumns;
  private final int sliceEndColumn;
  private final int[] aggregateKinds;

  public NativeGlobalWindowAggExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      long windowMillis,
      long slideMillis,
      int[] keyColumns,
      int[] partialColumns,
      int sliceEndColumn,
      int[] aggregateKinds) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-global-window-aggregate_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.keyColumns = keyColumns;
    this.partialColumns = partialColumns;
    this.sliceEndColumn = sliceEndColumn;
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
        new NativeGlobalWindowAggregateOperator(
            windowMillis,
            slideMillis,
            keyColumns,
            NativeWindowOperatorBase.keyTypes((RowType) getOutputType(), keyColumns.length),
            partialColumns,
            sliceEndColumn,
            aggregateKinds,
            timeZoneId,
            BATCH_SIZE),
        InternalTypeInfo.of(getOutputType()),
        input.getParallelism(),
        false);
  }
}
