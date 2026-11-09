package tech.streamfusion.planner;

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
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeOverAggregateOperator;

/** Wraps the native columnar OVER aggregate operator into the plan; Arrow batches in and out. */
public class NativeOverAggExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-over-aggregate";

  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;
  private final int frameKind;
  private final long frameOffset;
  private final boolean proctime;
  private final int[] keyTimestampPrecisions;
  private int stateMaxParallelism = -1;
  private RowType constantType;
  private byte[] constantValues;
  private boolean eventCleanupTimers;

  public NativeOverAggExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] valueTypes,
      int[] aggregateKinds,
      int frameKind,
      long frameOffset,
      boolean proctime,
      int[] keyTimestampPrecisions) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-over-aggregate_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.valueTypes = valueTypes;
    this.aggregateKinds = aggregateKinds;
    this.frameKind = frameKind;
    this.frameOffset = frameOffset;
    this.proctime = proctime;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
  }

  public boolean hasStockArithmeticState() {
    boolean bounded = frameKind == 1 || frameKind == 2;
    if (!bounded
            && (frameKind != 0 && frameKind != 3
                || !proctime
                    && getPersistedConfig()
                            .get(
                                org.apache.flink.configuration.ConfigOptions.key(
                                        "table.exec.unbounded-over.version")
                                    .intType()
                                    .defaultValue(
                                        tech.streamfusion.compat.FlinkPlanCompat
                                            .unboundedOverVersion()))
                        != tech.streamfusion.compat.FlinkPlanCompat.unboundedOverVersion())
        || bounded && proctime && frameKind == 2
        || keyColumns.length > 1
        || java.util.Arrays.stream(aggregateKinds)
            .anyMatch(kind -> kind != 0 && kind != 3 && kind != 4 && kind != 22)
        || !proctime
            && frameKind == 1
            && getPersistedConfig()
                    .get(
                        org.apache.flink.table.api.config.ExecutionConfigOptions
                            .IDLE_STATE_RETENTION)
                    .toMillis()
                > 1) return false;
    var input = (RowType) getInputEdges().get(0).getOutputType();
    if (keyColumns.length == 1
        && !input
            .getTypeAt(keyColumns[0])
            .is(org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT)) return false;
    for (int column : valueColumns) {
      if (column < 0 || column >= input.getFieldCount()) return false;
      var type = input.getTypeAt(column);
      if (!stockArithmeticValueType(type)
          || !bounded && type.is(org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL))
        return false;
    }
    return true;
  }

  static boolean stockArithmeticValueType(org.apache.flink.table.types.logical.LogicalType type) {
    return type.is(org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT)
        || type.is(org.apache.flink.table.types.logical.LogicalTypeRoot.FLOAT)
        || type.is(org.apache.flink.table.types.logical.LogicalTypeRoot.DOUBLE)
        || type instanceof org.apache.flink.table.types.logical.DecimalType
            && ((org.apache.flink.table.types.logical.DecimalType) type).getPrecision() == 38
            && ((org.apache.flink.table.types.logical.DecimalType) type).getScale() == 2;
  }

  public NativeOverAggExecNode withStateMaxParallelism(int maximum) {
    if (maximum <= 0) throw new IllegalArgumentException("Invalid OVER maximum parallelism");
    stateMaxParallelism = maximum;
    return this;
  }

  public NativeOverAggExecNode withEventTimeCleanupTimers() {
    eventCleanupTimers = true;
    return this;
  }

  public NativeOverAggExecNode withConstantArguments(RowType type, byte[] values) {
    constantType = type;
    constantValues = values.clone();
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    int maxParallelism =
        stateMaxParallelism > 0
            ? stateMaxParallelism
            : FlinkKeyGroupUtils.maxParallelism(planner.getExecEnv(), input.getParallelism());
    // The job-wide retention only: Flink's StreamExecOverAggregate reads the config directly,
    // with no STATE_TTL hint support. The 1.5x max deadline horizon is derived natively.
    long stateTtlMillis = config.getStateRetentionTime();
    var operator =
        new NativeOverAggregateOperator(
            timeColumn,
            valueColumns,
            keyColumns,
            valueTypes,
            aggregateKinds,
            frameKind,
            frameOffset,
            proctime,
            keyTimestampPrecisions,
            stateTtlMillis,
            (RowType) getInputEdges().get(0).getOutputType(),
            maxParallelism);
    if (constantType != null) operator.withConstantArguments(constantType, constantValues);
    if (eventCleanupTimers) operator.withEventTimeCleanupTimers();
    OneInputTransformation<ArrowBatch, ArrowBatch> transformation =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta(TRANSFORMATION, config),
            operator,
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    FlinkKeyGroupUtils.applyColumnarKeying(transformation, maxParallelism);
    return transformation;
  }
}
