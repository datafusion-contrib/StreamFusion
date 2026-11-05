package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.*;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchGroupAggFunction;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.*;

/** Retains the released handler and managed state behind the existing Arrow UDF boundary. */
final class CapturedJvmGroupExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {
  private final RowType inputType;
  private final GroupAggFunction function;
  private final MiniBatchGroupAggFunction bundleFunction;
  private final BundleTrigger<RowData> bundleTrigger;
  private final RowDataKeySelector selector;
  private final int[] grouping;
  private final int maximum;
  private final long retention;

  CapturedJvmGroupExecNode(
      ReadableConfig config,
      RowType inputType,
      RowType outputType,
      GroupAggFunction function,
      MiniBatchGroupAggFunction bundleFunction,
      BundleTrigger<RowData> bundleTrigger,
      RowDataKeySelector selector,
      int[] grouping,
      int maximum,
      long retention) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-jvm-group_1"),
        config,
        List.of(InputProperty.DEFAULT),
        outputType,
        "NativeColumnarJvmGroupAggregate");
    this.inputType = inputType;
    this.function = function;
    this.bundleFunction = bundleFunction;
    this.bundleTrigger = bundleTrigger;
    this.selector = selector;
    this.grouping = grouping.clone();
    this.maximum = maximum;
    this.retention = retention;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    var input = (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    LogicalTypeRoot key =
        grouping.length == 1 ? inputType.getTypeAt(grouping[0]).getTypeRoot() : null;
    if (key != LogicalTypeRoot.INTEGER
        && key != LogicalTypeRoot.BIGINT
        && key != LogicalTypeRoot.VARCHAR) key = null;
    var operator =
        new NativeColumnarJvmGroupOperator(
            inputType,
            (RowType) getOutputType(),
            function,
            bundleFunction,
            bundleTrigger,
            selector,
            NativeConfig.transposeBatchRows(config),
            Integer.getInteger("streamfusion.experimental.jvmAccumulatorCacheEntries", 0),
            key,
            grouping.length == 1 ? grouping[0] : -1,
            retention);
    var result =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta(
                "native-jvm-group", getDescription(), "NativeColumnarJvmGroupAggregate", config),
            operator,
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    result.setMaxParallelism(maximum);
    result.setStateKeySelector(new ArrowBatchRowKeySelector(inputType, selector));
    result.setStateKeyType(selector.getProducedType());
    return result;
  }
}
