package tech.streamfusion.planner;

import static org.apache.flink.util.Preconditions.checkArgument;

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
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.OrderedKeyGroupReassembler;
import tech.streamfusion.operator.SplitByKeyGroupOperator;

/**
 * Builds the columnar keyed exchange transformation: a {@link SplitByKeyGroupOperator} that splits
 * each Arrow batch into one sub-batch per destination (or recoverable key group), followed by a
 * {@link PartitionTransformation} using {@link ColumnarKeyGroupPartitioner} to route each sub-batch
 * to its current owner. The result is an {@code ArrowBatch} stream the downstream native operator
 * consumes without a row transpose; watermarks ride through the partition transformation as usual.
 */
public class NativeColumnarExchangeExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-columnar-exchange-split";

  private final int[] keyColumns;
  private final int[] timestampPrecisions;
  private final int[] compactTimestampColumns;
  private final boolean singleKeyGroup;
  private Integer stateMaxParallelism;

  public NativeColumnarExchangeExecNode withStateMaxParallelism(int maximum) {
    if (maximum < 1)
      throw new IllegalArgumentException("State maximum parallelism must be positive");
    stateMaxParallelism = maximum;
    return this;
  }

  public NativeColumnarExchangeExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int[] keyColumns,
      int[] timestampPrecisions,
      boolean singleKeyGroup) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-columnar-exchange_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    checkArgument(
        !singleKeyGroup || keyColumns.length == 0,
        "A single-key-group exchange requires a SINGLETON distribution");
    this.keyColumns = keyColumns;
    this.timestampPrecisions = timestampPrecisions;
    this.compactTimestampColumns =
        java.util.stream.IntStream.range(0, outputType.getFieldCount())
            .filter(
                index -> {
                  var type = outputType.getTypeAt(index);
                  var root = type.getTypeRoot();
                  return (root
                              == org.apache.flink.table.types.logical.LogicalTypeRoot
                                  .TIMESTAMP_WITHOUT_TIME_ZONE
                          || root
                              == org.apache.flink.table.types.logical.LogicalTypeRoot
                                  .TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                      && org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision(
                              type)
                          <= 3;
                })
            .toArray();
    this.singleKeyGroup = singleKeyGroup;
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
    // keyed aggregate). Resolve the initial transformation default here; the splitter binds to
    // its actual network output at runtime after resource planning. Inheriting the producer count
    // can also put a parallelism-one aggregate in front of a parallel sink, whose rescale edge
    // separates an UPDATE_BEFORE from its earlier INSERT/UPDATE_AFTER.
    boolean singleton = keyColumns.length == 0;
    int numChannels = singleton ? 1 : Math.max(1, planner.getExecEnv().getParallelism());
    // Match the consumer's state namespace. Fixed-namespace singleton consumers use one group;
    // other consumers honor the configured count, independently of the destination count.
    int maxParallelism =
        stateMaxParallelism != null
            ? stateMaxParallelism
            : singleKeyGroup
                ? 1
                : FlinkKeyGroupUtils.maxParallelism(planner.getExecEnv(), numChannels);
    boolean recoverable =
        planner.getExecEnv().getCheckpointConfig().isUnalignedCheckpointsEnabled();
    // Aligned jobs keep destination batching. Unaligned-enabled jobs use independently recoverable
    // key-group fragments because any checkpoint can capture the already-buffered network records.
    Transformation<ArrowBatch> split =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta(TRANSFORMATION, config),
            new SplitByKeyGroupOperator(
                keyColumns,
                timestampPrecisions,
                maxParallelism,
                recoverable,
                compactTimestampColumns),
            NativeConfig.zeroCopyExchange(planner.getExecEnv())
                ? ArrowBatchTypeInformation.ZERO_COPY
                : ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    // ...then route each whole sub-batch to its current owner. Pipelined so watermarks flow.
    PartitionTransformation<ArrowBatch> partition =
        new PartitionTransformation<>(
            split,
            singleton
                ? ColumnarKeyGroupPartitioner.singleton(maxParallelism, recoverable)
                : new ColumnarKeyGroupPartitioner(maxParallelism, recoverable),
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
