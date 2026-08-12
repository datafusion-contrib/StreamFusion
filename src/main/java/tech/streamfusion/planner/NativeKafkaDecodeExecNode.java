package tech.streamfusion.planner;

import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatOptions;
import tech.streamfusion.format.NativeFormatProvider;
import tech.streamfusion.format.NativeFormatProviders;
import tech.streamfusion.kafka.NativeKafkaSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeSourceWatermarks;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/**
 * Zero-input exec node for the native-decode Kafka path. Flink's Kafka enumerator and split state
 * remain authoritative, while a split-aware reader batches each partition's bytes and decodes them
 * natively to Arrow before collection. The result starts columnar without materializing RowData.
 */
public class NativeKafkaDecodeExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String SOURCE_TRANSFORMATION = "native-kafka-source";
  private final RowType outputType;
  private final RowType writerType;
  private final Map<String, String> options;
  private final ScanWatermarkSpec watermark;

  public NativeKafkaDecodeExecNode(
      ReadableConfig tableConfig,
      RowType outputType,
      RowType writerType,
      String description,
      Map<String, String> options,
      ScanWatermarkSpec watermark) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-kafka-decode_1"),
        tableConfig,
        Collections.emptyList(),
        outputType,
        description);
    this.outputType = outputType;
    this.writerType = writerType;
    this.options = options;
    this.watermark = watermark;
  }

  @Override
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    StreamExecutionEnvironment env = planner.getExecEnv();
    // A keyed table's value format sees only the value positions — the provider's type gate and
    // decoder run against that projection, while the operator still exports the physical schema
    // (the native keyed composition owns the split and the scatter).
    boolean keyed = options.containsKey(NativeFormatOptions.KEYED_KEY_POSITION);
    RowType formatType = keyed ? valueRowType() : writerType;
    NativeFormatContext formatContext =
        new NativeFormatContext(
            keyed ? formatType : outputType,
            formatType,
            options,
            KafkaTables.ignoreParseErrors(options));
    NativeFormatProvider formatProvider =
        NativeFormatProviders.find(formatContext)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No installed StreamFusion provider for format "
                            + NativeFormatProviders.formatIdentifier(options)));
    Properties properties = KafkaTables.sourceConsumerProperties(options);
    KafkaSource<byte[]> kafkaSource = KafkaTables.buildBytesSource(options);
    NativeKafkaSource source =
        new NativeKafkaSource(
            kafkaSource,
            properties,
            outputType,
            formatProvider.createDecoder(formatContext),
            keyed,
            watermark == null ? -1 : watermark.rowtimeIndex);
    WatermarkStrategy<ArrowBatch> strategy =
        watermark == null
            ? WatermarkStrategy.noWatermarks()
            : NativeSourceWatermarks.strategy(
                watermark.delayMillis, watermark.idleTimeoutMillis);
    DataStreamSource<ArrowBatch> stream =
        env.fromSource(
            source, strategy, SOURCE_TRANSFORMATION, ArrowBatchTypeInformation.INSTANCE);
    return stream.getTransformation();
  }

  /** The physical schema projected to the keyed markers' value positions. */
  private RowType valueRowType() {
    java.util.List<RowType.RowField> fields = new java.util.ArrayList<>();
    for (String position :
        options.get(NativeFormatOptions.KEYED_VALUE_POSITIONS).split(",", -1)) {
      if (!position.isEmpty()) {
        fields.add(outputType.getFields().get(Integer.parseInt(position)));
      }
    }
    return new RowType(false, fields);
  }
}
