package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.kafka.NativeKafkaSource;
import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.NativeFormatProviders;
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
 * that consumes the topic with a native rdkafka reader and emits typed Arrow batches. The selected
 * format artifact's decoder rides into the source and runs on the fetch thread — decoding as a
 * downstream operator serialized the format work (profiled at ~65% of a JSON job) behind the whole
 * island on the task thread, halving the rung. The reused {@code KafkaSourceEnumerator} owns partition
 * discovery, offset resolution, and split assignment.
 */
public class NativeKafkaSourceExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String TRANSFORMATION = "native-kafka-source";

  private final RowType writerType;
  private final RowType outputType;
  private final Map<String, String> options;
  private final ScanWatermarkSpec watermark;

  public NativeKafkaSourceExecNode(
      ReadableConfig tableConfig,
      RowType writerType,
      RowType outputType,
      String description,
      Map<String, String> options,
      ScanWatermarkSpec watermark) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-kafka-source_1"),
        tableConfig,
        Collections.emptyList(),
        outputType,
        description);
    this.writerType = writerType;
    this.outputType = outputType;
    this.options = options;
    this.watermark = watermark;
  }

  @Override
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    StreamExecutionEnvironment env = planner.getExecEnv();
    NativeFormatContext formatContext =
        new NativeFormatContext(outputType, writerType, options, KafkaTables.ignoreParseErrors(options));
    NativeFormatProvider formatProvider =
        NativeFormatProviders.find(formatContext)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No installed StreamFusion provider for format "
                            + NativeFormatProviders.formatIdentifier(options)));
    NativeKafkaSource source =
        KafkaTables.build(options, formatProvider.createDecoder(formatContext), outputType);
    DataStreamSource<ArrowBatch> decoded =
        env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            TRANSFORMATION,
            ArrowBatchTypeInformation.INSTANCE);
    return decoded.getTransformation();
  }
}
