package tech.streamfusion.planner;

import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.hudi.HudiSinkPlan;
import tech.streamfusion.operator.ArrowBatch;

final class NativeHudiSinkExecNode extends ExecNodeBase<Object>
    implements StreamExecNode<Object>, SingleTransformationTranslator<Object> {
  private final HudiSinkPlan plan;

  NativeHudiSinkExecNode(
      ReadableConfig config,
      InputProperty input,
      RowType output,
      String description,
      HudiSinkPlan plan) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-hudi-sink_1"),
        config,
        Collections.singletonList(input),
        output,
        description);
    this.plan = plan;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<Object> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    var input = (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    var sink =
        plan.append(
            new DataStream<>(planner.getExecEnv(), input),
            new org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext(false));
    return (Transformation<Object>) (Transformation<?>) sink.getTransformation();
  }
}
