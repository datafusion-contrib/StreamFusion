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
import tech.streamfusion.fluss.FlussPartitionSplitOperator;
import tech.streamfusion.fluss.FlussPartitionedArrowSink;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.PartitionedArrowBatchTypeInformation;

public final class NativeFlussSinkExecNode extends ExecNodeBase<Object>
    implements StreamExecNode<Object> {
  private final FlussArrowSink sink;
  private final Configuration clientConfig;
  private final TablePath path;
  private final RowType type;
  private final List<String> partitionKeys;

  NativeFlussSinkExecNode(
      ReadableConfig tableConfig,
      RowType type,
      String description,
      Configuration config,
      TablePath path,
      List<String> partitionKeys) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-fluss-sink_1"),
        tableConfig,
        List.of(InputProperty.DEFAULT),
        type,
        description);
    sink = new FlussArrowSink(config, path, type);
    clientConfig = config;
    this.path = path;
    this.type = type;
    this.partitionKeys = List.copyOf(partitionKeys);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<Object> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    if (!partitionKeys.isEmpty()) {
      return (Transformation<Object>)
          (Transformation<?>)
              new DataStream<>(planner.getExecEnv(), input)
                  .transform(
                      "native-fluss-partition-split",
                      PartitionedArrowBatchTypeInformation.INSTANCE,
                      new FlussPartitionSplitOperator(type, partitionKeys))
                  .sinkTo(new FlussPartitionedArrowSink(clientConfig, path, type))
                  .name("native-fluss-append-sink")
                  .getTransformation();
    }
    return (Transformation<Object>)
        (Transformation<?>)
            new DataStream<>(planner.getExecEnv(), input)
                .sinkTo(sink)
                .name("native-fluss-append-sink")
                .getTransformation();
  }
}
