package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.kafka.NativeKafkaSource;
import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import java.util.Collections;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/**
 * Zero-input exec node for the native Kafka source: it contributes an unbounded source transformation
 * that consumes the topic with a native rdkafka reader and emits Arrow batches into the columnar stream.
 * The reused {@code KafkaSourceEnumerator} owns partition discovery, offset resolution, and split
 * assignment; the native reader fetches+decodes the assigned partitions.
 */
public class NativeKafkaSourceExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String TRANSFORMATION = "native-kafka-source";

  private final RowType outputType;
  private final Map<String, String> options;

  public NativeKafkaSourceExecNode(
      ReadableConfig tableConfig, RowType outputType, String description, Map<String, String> options) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-kafka-source_1"),
        tableConfig,
        Collections.emptyList(),
        outputType,
        description);
    this.outputType = outputType;
    this.options = options;
  }

  @Override
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    StreamExecutionEnvironment env = planner.getExecEnv();
    NativeKafkaSource source = KafkaTables.build(options, outputType);
    DataStreamSource<ArrowBatch> stream =
        env.fromSource(
            source, WatermarkStrategy.noWatermarks(), TRANSFORMATION, ArrowBatchTypeInformation.INSTANCE);
    return stream.getTransformation();
  }
}
