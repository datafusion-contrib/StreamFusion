package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.*;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import tech.streamfusion.operator.ArrowBatch;

public final class NativeDorisSinkExecNode extends ExecNodeBase<Object>
    implements StreamExecNode<Object> {
  private final DorisSinkRuntime runtime;

  NativeDorisSinkExecNode(ReadableConfig config, String description, DorisSinkRuntime runtime) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-doris-sink_1"),
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
    int parallelism = runtime.parallelism == null ? input.getParallelism() : runtime.parallelism;
    input = runtime.prepare(input, planner.getExecEnv(), config);
    var result =
        new DataStream<>(planner.getExecEnv(), input)
            .sinkTo(runtime.sink)
            .name("native-doris-sink")
            .setParallelism(parallelism);
    return (Transformation<Object>) (Transformation<?>) result.getTransformation();
  }
}
