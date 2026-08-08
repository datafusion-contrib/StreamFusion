package tech.streamfusion.planner;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeColumnarGlobalWindowAggregateOperator;
import tech.streamfusion.operator.NativeWindowOperatorCore;
import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
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
 * Wraps the columnar global (merge) window operator into the plan: partial-state Arrow batches in,
 * final result batches (Arrow) out.
 */
public class NativeColumnarGlobalWindowAggExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-columnar-global-window-aggregate";

  private final long windowMillis;
  private final long slideMillis;
  private final boolean cumulative;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;
  private final boolean timestampLtz;
  private final int[] keyTimestampPrecisions;

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
      int[] aggregateKinds,
      boolean timestampLtz,
      int[] keyTimestampPrecisions) {
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
    this.timestampLtz = timestampLtz;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
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
    int maxParallelism =
        FlinkKeyGroupUtils.maxParallelism(planner.getExecEnv(), input.getParallelism());
    OneInputTransformation<ArrowBatch, ArrowBatch> transformation =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta(TRANSFORMATION, config),
            new NativeColumnarGlobalWindowAggregateOperator(
                windowMillis,
                slideMillis,
                cumulative,
                NativeWindowOperatorCore.keyTypes(outputType, keyColumns.length),
                valueTypes,
                aggregateKinds,
                timeZoneId,
                timestampLtz,
                planner.getTableConfig().getLocalTimeZone().getId(),
                outputType,
                keyTimestampPrecisions,
                maxParallelism),
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    FlinkKeyGroupUtils.applyColumnarKeying(transformation, maxParallelism);
    return transformation;
  }
}
