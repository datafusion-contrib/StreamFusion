package tech.streamfusion.planner;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeColumnarLocalWindowAggregateOperator;
import tech.streamfusion.operator.NativeWindowOperatorCore;
import java.util.Collections;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
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

/** Wraps the columnar local (pre-aggregate) window operator into the plan; Arrow batches in and out. */
public class NativeColumnarLocalWindowAggExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-columnar-local-window-aggregate";

  private final long sliceMillis;
  private final int timeColumn;
  private final int windowStartColumn;
  private final int windowEndColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;
  private final boolean timestampLtz;
  private final int[] keyTimestampPrecisions;

  public NativeColumnarLocalWindowAggExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      long sliceMillis,
      int timeColumn,
      int windowStartColumn,
      int windowEndColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] valueTypes,
      int[] aggregateKinds,
      boolean timestampLtz,
      int[] keyTimestampPrecisions) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-columnar-local-window-aggregate_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.sliceMillis = sliceMillis;
    this.timeColumn = timeColumn;
    this.windowStartColumn = windowStartColumn;
    this.windowEndColumn = windowEndColumn;
    this.valueColumns = valueColumns;
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
    // Flink's shift-zone rule: plain-TIMESTAMP rowtime windows compute on epoch millis with UTC
    // digits; only a TIMESTAMP_LTZ rowtime shifts boundaries into the session zone. The zone is
    // read by the window-attached ingest, which must invert exactly the shift the upstream window
    // aggregate's boundary rendering applied.
    String timeZoneId =
        timestampLtz ? planner.getTableConfig().getLocalTimeZone().getId() : "UTC";
    int maxParallelism = FlinkKeyGroupUtils.defaultMaxParallelism(input.getParallelism());
    // The local half sits upstream of the exchange, so its batches carry no destination tag; the
    // operator sets its own subtask-local key context in open(). This selector exists only so
    // Flink creates the keyed backend for the raw-state plumbing and is never consulted.
    KeySelector<ArrowBatch, Integer> stateKeySelector = batch -> 0;
    OneInputTransformation<ArrowBatch, ArrowBatch> transformation =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta(TRANSFORMATION, config),
            new NativeColumnarLocalWindowAggregateOperator(
                sliceMillis,
                sliceMillis,
                timeColumn,
                windowStartColumn,
                windowEndColumn,
                valueColumns,
                keyColumns,
                NativeWindowOperatorCore.keyTypes((RowType) getOutputType(), keyColumns.length),
                valueTypes,
                aggregateKinds,
                timeZoneId,
                keyTimestampPrecisions,
                maxParallelism),
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    transformation.setMaxParallelism(maxParallelism);
    transformation.setStateKeySelector(stateKeySelector);
    transformation.setStateKeyType(Types.INT);
    NativeManagedMemory.declareOperatorWeight(transformation);
    return transformation;
  }
}
