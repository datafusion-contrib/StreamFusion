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
import tech.streamfusion.iceberg.IcebergSinkPlan;
import tech.streamfusion.iceberg.NativeIcebergSink;
import tech.streamfusion.operator.ArrowBatch;

public final class NativeIcebergSinkExecNode extends ExecNodeBase<Object>
    implements StreamExecNode<Object>, SingleTransformationTranslator<Object> {
  private final IcebergSinkPlan plan;

  NativeIcebergSinkExecNode(
      ReadableConfig configuration,
      InputProperty input,
      RowType output,
      String description,
      IcebergSinkPlan plan) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-iceberg-sink_1"),
        configuration,
        Collections.singletonList(input),
        output,
        description);
    this.plan = plan;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<Object> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    var sink =
        NativeIcebergSink.append(
            new DataStream<>(planner.getExecEnv(), input),
            plan.table,
            plan.loader,
            plan.configuration,
            plan.options);
    return (Transformation<Object>) (Transformation<?>) sink.getTransformation();
  }
}
