package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.*;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import tech.streamfusion.fluss.FlussArrowSink;
import tech.streamfusion.operator.ArrowBatch;

public final class NativeFlussSinkExecNode extends ExecNodeBase<Object>
    implements StreamExecNode<Object> {
  private final FlussArrowSink sink;

  NativeFlussSinkExecNode(
      ReadableConfig tableConfig,
      RowType type,
      String description,
      Configuration config,
      TablePath path) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-fluss-sink_1"),
        tableConfig,
        List.of(InputProperty.DEFAULT),
        type,
        description);
    sink = new FlussArrowSink(config, path, type);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<Object> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    return (Transformation<Object>)
        (Transformation<?>)
            new DataStream<>(planner.getExecEnv(), input)
                .sinkTo(sink)
                .name("native-fluss-append-sink")
                .getTransformation();
  }
}
