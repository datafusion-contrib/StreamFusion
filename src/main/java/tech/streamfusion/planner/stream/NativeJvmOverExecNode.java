package tech.streamfusion.planner.stream;

import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchRowKeySelector;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeColumnarJvmOverOperator;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.planner.NativeConfig;

final class NativeJvmOverExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {
  private final RowType inputType;
  private final ExecNode<?> released;

  NativeJvmOverExecNode(
      ReadableConfig config,
      RowType inputType,
      RowType outputType,
      ExecNode<?> released,
      String description) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-jvm-over_1"),
        config,
        Collections.singletonList(InputProperty.DEFAULT),
        outputType,
        description);
    this.inputType = inputType;
    this.released = released;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    var input = (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    // Only the unopened handler and selector are retained; the temporary row transformation
    // never becomes a runtime edge. Released Flink owns version-specific frame generation.
    released.setInputEdges(getInputEdges());
    var host = (OneInputTransformation<RowData, RowData>) released.translateToPlan(planner);
    var process = (KeyedProcessOperator<RowData, RowData, RowData>) host.getOperator();
    var selector = (RowDataKeySelector) host.getStateKeySelector();
    var operator =
        new NativeColumnarJvmOverOperator(
            inputType,
            (RowType) getOutputType(),
            process.getUserFunction(),
            selector,
            NativeConfig.transposeBatchRows(config));
    var transform =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta("native-jvm-over", config),
            operator,
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    transform.setMaxParallelism(
        FlinkKeyGroupUtils.maxParallelism(planner.getExecEnv(), input.getParallelism()));
    transform.setStateKeySelector(new ArrowBatchRowKeySelector(inputType, selector));
    transform.setStateKeyType(selector.getProducedType());
    return transform;
  }
}
