package tech.streamfusion.planner;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeColumnarTopNOperator;
import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;

/** Wraps the columnar append-only Top-N operator into the plan; Arrow batches in and out. */
public class NativeColumnarTopNExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String TRANSFORMATION = "native-columnar-top-n";

  private final int[] partitionColumns;
  private final int[] sortIndices;
  private final int[] sortAscending;
  private final int[] sortNullsFirst;
  private final long offset;
  private final long limit;
  private final boolean outputRankNumber;
  private final boolean retracting;
  // Update-fast mode: the unique-key columns identifying the row a record replaces (null otherwise).
  private final int[] rowKeyColumns;
  private final int[] rowKeyTimestampPrecisions;
  private final int[] keyTimestampPrecisions;

  public NativeColumnarTopNExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int[] partitionColumns,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long offset,
      long limit,
      boolean outputRankNumber,
      boolean retracting,
      int[] rowKeyColumns,
      int[] rowKeyTimestampPrecisions,
      int[] keyTimestampPrecisions) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-columnar-top-n_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.partitionColumns = partitionColumns;
    this.sortIndices = sortIndices;
    this.sortAscending = sortAscending;
    this.sortNullsFirst = sortNullsFirst;
    this.offset = offset;
    this.limit = limit;
    this.outputRankNumber = outputRankNumber;
    this.retracting = retracting;
    this.rowKeyColumns = rowKeyColumns;
    this.rowKeyTimestampPrecisions = rowKeyTimestampPrecisions;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    // Under mini-batch, both rankers emit the net logical-bundle rank diff instead of exposing
    // per-record intermediate rank windows. The final materialized Top-N is identical; with
    // mini-batch off, the per-input-row changelog remains byte-identical to the host path.
    boolean netDiff = config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
    long miniBatchSize = config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE);
    // The job-wide idle-state retention; Flink defines STATE_TTL hints only for joins and
    // aggregates, so ranks have no per-operator override to resolve.
    long stateTtlMillis = config.getStateRetentionTime();
    int maxParallelism = FlinkKeyGroupUtils.defaultMaxParallelism(input.getParallelism());
    OneInputTransformation<ArrowBatch, ArrowBatch> transformation =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta(TRANSFORMATION, config),
            new NativeColumnarTopNOperator(
                partitionColumns,
                keyTimestampPrecisions,
                // The buffered state row is the INPUT row; the output may append a rank column.
                (RowType) getInputEdges().get(0).getOutputType(),
                sortIndices,
                sortAscending,
                sortNullsFirst,
                offset,
                limit,
                outputRankNumber,
                retracting,
                rowKeyColumns,
                rowKeyTimestampPrecisions,
                netDiff,
                miniBatchSize,
                stateTtlMillis,
                maxParallelism),
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    FlinkKeyGroupUtils.applyColumnarKeying(transformation, maxParallelism);
    return transformation;
  }
}
