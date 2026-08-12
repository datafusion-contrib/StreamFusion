package tech.streamfusion.planner;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.OrderedKeyGroupReassembler;
import tech.streamfusion.operator.SplitByKeyGroupOperator;
import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
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
 * Builds the columnar keyed exchange transformation: a {@link SplitByKeyGroupOperator} that splits
 * each Arrow batch into one sub-batch per non-empty key group, followed by a {@link
 * PartitionTransformation} using {@link ColumnarKeyGroupPartitioner} to route each sub-batch to its
 * current owner. The result is an {@code ArrowBatch} stream the downstream native operator consumes
 * without a row transpose; watermarks ride through the partition transformation as usual.
 */
public class NativeColumnarExchangeExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-columnar-exchange-split";

  private final int[] keyColumns;
  private final int[] timestampPrecisions;

  public NativeColumnarExchangeExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int[] keyColumns,
      int[] timestampPrecisions) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-columnar-exchange_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.keyColumns = keyColumns;
    this.timestampPrecisions = timestampPrecisions;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    // An exchange with no key columns is Flink's SINGLETON distribution (global aggregate/rank):
    // it must collapse all upstream subtasks onto one downstream state instance.  Inheriting the
    // producer parallelism here creates N independent "global" native accumulators.
    // Flink's HASH exchange deliberately uses PARALLELISM_DEFAULT rather than inheriting the
    // producer's parallelism.  The target therefore runs at the execution environment's default
    // parallelism (for example, a parallelism-one collection source can still feed a four-way
    // keyed aggregate).  The splitter must know that concrete target count up front, so resolve the
    // default here instead of using the producer count.  Inheriting the producer count can also put
    // a parallelism-one aggregate in front of a parallel sink, whose rescale edge separates an
    // UPDATE_BEFORE from its earlier INSERT/UPDATE_AFTER.
    int numChannels =
        keyColumns.length == 0 ? 1 : Math.max(1, planner.getExecEnv().getParallelism());
    int maxParallelism =
        keyColumns.length == 0
            ? 1
            : FlinkKeyGroupUtils.maxParallelism(planner.getExecEnv(), numChannels);
    boolean recoverable = planner.getExecEnv().getCheckpointConfig().isUnalignedCheckpointsEnabled();
    // Aligned jobs keep destination batching. Unaligned-enabled jobs use independently recoverable
    // key-group fragments because any checkpoint can capture the already-buffered network records.
    Transformation<ArrowBatch> split =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta(TRANSFORMATION, config),
            new SplitByKeyGroupOperator(
                keyColumns, timestampPrecisions, maxParallelism, numChannels, recoverable),
            NativeConfig.zeroCopyExchange(planner.getExecEnv())
                ? ArrowBatchTypeInformation.ZERO_COPY
                : ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    // ...then route each whole sub-batch to its current owner. Pipelined so watermarks flow.
    PartitionTransformation<ArrowBatch> partition =
        new PartitionTransformation<>(
            split,
            new ColumnarKeyGroupPartitioner(maxParallelism, recoverable),
            StreamExchangeMode.PIPELINED);
    partition.setParallelism(numChannels);
    partition.setMaxParallelism(maxParallelism);
    if (!recoverable) {
      return partition;
    }
    Transformation<ArrowBatch> reassembled =
        ExecNodeUtil.createOneInputTransformation(
            partition,
            createTransformationMeta("native-columnar-exchange-reassemble", config),
            new OrderedKeyGroupReassembler(maxParallelism),
            ArrowBatchTypeInformation.INSTANCE,
            numChannels,
            false);
    reassembled.setMaxParallelism(maxParallelism);
    return reassembled;
  }
}
