package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import tech.streamfusion.operator.ArrowBatch;

/** Translates the native StarRocks sink onto the released connector writer. */
public final class NativeStarRocksSinkExecNode extends ExecNodeBase<Object>
    implements StreamExecNode<Object> {
  private final StarRocksSinkRuntime runtime;

  NativeStarRocksSinkExecNode(
      ReadableConfig config, String description, StarRocksSinkRuntime runtime) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-starrocks-sink_1"),
        config,
        List.of(InputProperty.DEFAULT),
        runtime.type,
        description);
    this.runtime = runtime;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<Object> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    var input = (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    return runtime.translate(input, planner.getExecEnv(), config, getDescription());
  }
}
