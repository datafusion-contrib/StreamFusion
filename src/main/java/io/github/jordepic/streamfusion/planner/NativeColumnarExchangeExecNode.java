package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.SplitByKeyGroupOperator;
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
 * each Arrow batch into one sub-batch per destination channel, followed by a {@link
 * PartitionTransformation} using {@link ColumnarKeyGroupPartitioner} to route each sub-batch to its
 * channel. The result is an {@code ArrowBatch} stream the downstream native operator consumes
 * without a row transpose; watermarks ride through the partition transformation as usual.
 */
public class NativeColumnarExchangeExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-columnar-exchange-split";

  private final int[] keyColumns;

  public NativeColumnarExchangeExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int[] keyColumns) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-columnar-exchange_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.keyColumns = keyColumns;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    int numChannels = Math.max(1, input.getParallelism());
    // Split each batch into per-channel sub-batches (homogeneous in destination)...
    Transformation<ArrowBatch> split =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta(TRANSFORMATION, config),
            new SplitByKeyGroupOperator(keyColumns, numChannels),
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    // ...then route each whole sub-batch to its channel. Pipelined so watermarks flow downstream.
    return new PartitionTransformation<>(
        split, new ColumnarKeyGroupPartitioner(), StreamExchangeMode.PIPELINED);
  }
}
