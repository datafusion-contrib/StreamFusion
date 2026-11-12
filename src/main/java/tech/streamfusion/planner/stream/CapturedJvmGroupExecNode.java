package tech.streamfusion.planner.stream;

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
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.*;
import tech.streamfusion.planner.NativeConfig;

/** Retains the released handler and managed state behind the existing Arrow UDF boundary. */
final class CapturedJvmGroupExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {
  private final RowType inputType;
  private final GroupAggFunction function;
  private final MapBundleFunction<RowData, ?, RowData, RowData> bundleFunction;
  private final JvmGroupPartialTransport inputPartials, outputPartials;
  private final BundleTrigger<RowData> bundleTrigger;
  private final RowDataKeySelector selector;
  private final int[] grouping;
  private final int maximum;
  private final long retention;
  private Integer parallelism;

  CapturedJvmGroupExecNode(
      ReadableConfig config,
      RowType inputType,
      RowType outputType,
      GroupAggFunction function,
      MapBundleFunction<RowData, ?, RowData, RowData> bundleFunction,
      BundleTrigger<RowData> bundleTrigger,
      RowDataKeySelector selector,
      int[] grouping,
      int maximum,
      long retention,
      JvmGroupPartialTransport inputPartials,
      JvmGroupPartialTransport outputPartials) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-jvm-group_1"),
        config,
        List.of(InputProperty.DEFAULT),
        outputType,
        outputPartials == null
            ? "NativeColumnarJvmGroupAggregate"
            : "NativeColumnarLocalJvmGroupAggregate");
    this.inputType = inputType;
    this.function = function;
    this.bundleFunction = bundleFunction;
    this.inputPartials = inputPartials;
    this.outputPartials = outputPartials;
    this.bundleTrigger = bundleTrigger;
    this.selector = selector;
    this.grouping = grouping.clone();
    this.maximum = maximum;
    this.retention = retention;
  }

  CapturedJvmGroupExecNode withParallelism(int parallelism) {
    if (parallelism > 0) this.parallelism = parallelism;
    return this;
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
        outputPartials != null
            ? new NativeColumnarLocalJvmGroupOperator(
                inputType,
                (RowType) getOutputType(),
                (org.apache.flink.table.runtime.operators.aggregate.MiniBatchLocalGroupAggFunction)
                    bundleFunction,
                bundleTrigger,
                selector,
                NativeConfig.transposeBatchRows(config),
                outputPartials)
            : new NativeColumnarJvmGroupOperator(
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
                retention,
                inputPartials,
                null);
    var result =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta(
                outputPartials == null ? "native-jvm-group" : "native-local-jvm-group",
                getDescription(),
                outputPartials == null
                    ? "NativeColumnarJvmGroupAggregate"
                    : "NativeColumnarLocalJvmGroupAggregate",
                config),
            operator,
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    if (parallelism != null) result.setParallelism(parallelism);
    result.setMaxParallelism(maximum);
    if (outputPartials == null) {
      result.setStateKeySelector(new ArrowBatchRowKeySelector(inputType, selector));
      result.setStateKeyType(selector.getProducedType());
    }
    return result;
  }
}
