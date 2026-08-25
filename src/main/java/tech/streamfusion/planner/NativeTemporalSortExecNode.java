package tech.streamfusion.planner;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.ConstantArrowBatchKeySelector;
import tech.streamfusion.operator.NativeColumnarTemporalSortOperator;
import java.util.Collections;
import org.apache.flink.api.common.typeinfo.Types;
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
 * Wraps the native columnar event-time sorter into the plan; it consumes and produces Arrow batches,
 * buffering each and emitting the rows a watermark completes in ascending rowtime order.
 */
public class NativeTemporalSortExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-temporal-sort";

  private final int rowtimeColumn;

  public NativeTemporalSortExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int rowtimeColumn) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-temporal-sort_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.rowtimeColumn = rowtimeColumn;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    ConstantArrowBatchKeySelector emptyKeySelector = new ConstantArrowBatchKeySelector(0);
    OneInputTransformation<ArrowBatch, ArrowBatch> transformation =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta(TRANSFORMATION, config),
            new NativeColumnarTemporalSortOperator(rowtimeColumn, (RowType) getOutputType()),
            ArrowBatchTypeInformation.INSTANCE,
            1,
            false);
    // Flink's RowTimeSortOperator is keyed by EmptyRowDataKeySelector and runs after a singleton
    // exchange. Keep the same one logical key: its raw state can move on restore but cannot shard.
    transformation.setParallelism(1);
    transformation.setMaxParallelism(1);
    transformation.setStateKeySelector(emptyKeySelector);
    transformation.setStateKeyType(Types.INT);
    return transformation;
  }
}
