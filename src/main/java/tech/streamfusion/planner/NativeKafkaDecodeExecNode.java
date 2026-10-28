package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
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
import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatOptions;
import tech.streamfusion.format.NativeFormatProvider;
import tech.streamfusion.format.NativeFormatProviders;
import tech.streamfusion.kafka.NativeKafkaSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeSourceWatermarks;

/**
 * Zero-input exec node for the native-decode Kafka path. Flink's Kafka enumerator and split state
 * remain authoritative, while a split-aware reader batches each partition's bytes and decodes them
 * natively to Arrow before collection. The result starts columnar without materializing RowData.
 */
public class NativeKafkaDecodeExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private final RowType outputType;
  private final RowType writerType;
  private final Map<String, String> options;
  private final Map<String, String> metadata;
  private final ScanWatermarkSpec watermark;

  public NativeKafkaDecodeExecNode(
      ReadableConfig tableConfig,
      RowType outputType,
      RowType writerType,
      String description,
      Map<String, String> options,
      Map<String, String> metadata,
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
    this.metadata = metadata;
    this.watermark = watermark;
  }

  @Override
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    StreamExecutionEnvironment env = planner.getExecEnv();
    // A keyed table's value format sees only the value positions — the provider's type gate and
    // decoder run against that projection, while the operator still exports the physical schema
    // (the native keyed composition owns the split and the scatter).
    boolean keyed = options.containsKey(NativeFormatOptions.KEYED_KEY_FORMAT);
    RowType formatType = keyed ? valueRowType() : withoutMetadata(writerType);
    // A table reading connector metadata: the value format sees only the other columns, and the
    // native decode fills each metadata column's output position from the row's record.
    Map<String, String> formatOptions = options;
    List<String> metadataKeys = new ArrayList<>();
    StringJoiner metadataColumns = new StringJoiner(",");
    for (int position = 0; position < outputType.getFieldCount(); position++) {
      String key = metadata.get(outputType.getFieldNames().get(position));
      if (key != null) {
        metadataColumns.add(position + ":" + key);
        if (!metadataKeys.contains(key)) {
          metadataKeys.add(key);
        }
      }
    }
    if (!metadataKeys.isEmpty()) {
      formatOptions = new HashMap<>(options);
      formatOptions.put(NativeFormatOptions.METADATA_COLUMNS, metadataColumns.toString());
    }
    NativeFormatContext formatContext =
        new NativeFormatContext(
            keyed ? formatType : withoutMetadata(outputType),
            formatType,
            formatOptions,
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
            metadataKeys,
            watermark == null ? -1 : watermark.rowtimeIndex,
            watermark == null ? null : watermark.expression);
    WatermarkStrategy<ArrowBatch> strategy =
        watermark == null
            ? WatermarkStrategy.noWatermarks()
            : NativeSourceWatermarks.strategy(watermark.idleTimeoutMillis);
    DataStreamSource<ArrowBatch> stream =
        env.fromSource(
            source,
            strategy,
            NativePlanDetails.name("NativeSource", getDescription()),
            ArrowBatchTypeInformation.INSTANCE);
    stream.getTransformation().setDescription(getDescription());
    return stream.getTransformation();
  }

  /** {@code type} without its metadata columns: the columns the value format decodes. */
  private RowType withoutMetadata(RowType type) {
    if (metadata.isEmpty()) {
      return type;
    }
    List<RowType.RowField> fields = new ArrayList<>();
    for (RowType.RowField field : type.getFields()) {
      if (!metadata.containsKey(field.getName())) {
        fields.add(field);
      }
    }
    return new RowType(type.isNullable(), fields);
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
