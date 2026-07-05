package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.kafka.ConfluentSchemaRegistry;
import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeBytesDecodeOperator;
import java.util.Collections;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/**
 * Zero-input exec node for the shallow native-decode Kafka path. It chains two transformations: Flink's
 * {@code KafkaSource} producing raw value {@code byte[]}s, then a {@link NativeBytesDecodeOperator} that
 * batches the bytes and decodes them natively to Arrow. The result starts the pipeline columnar without
 * Flink ever materializing a {@code RowData} for the message.
 */
public class NativeKafkaDecodeExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String SOURCE_TRANSFORMATION = "kafka-bytes-source";
  private static final String DECODE_TRANSFORMATION = "native-decode";
  // Bytes accumulated into one Arrow body batch before a native decode (matches the columnar batch size
  // the rest of the pipeline uses).
  private static final int BATCH_SIZE = 8192;
  // Longest a buffered record waits before a partial batch flushes — the latency bound the batching
  // trades against per-batch decode efficiency (the native Kafka source's poll timeout is the same
  // order, so both ingest paths cap tail latency alike).
  private static final long FLUSH_INTERVAL_MILLIS = 100;
  // The MessageDecoder codes for the formats whose decode needs a derived Avro schema.
  private static final int CONFLUENT_AVRO = 1;
  private static final int BARE_AVRO = 4;
  // The operator's protobuf sentinel (decoder built from the message-class-name's descriptor).
  private static final int PROTOBUF = 5;

  private final RowType outputType;
  private final RowType writerType;
  private final Map<String, String> options;

  public NativeKafkaDecodeExecNode(
      ReadableConfig tableConfig,
      RowType outputType,
      RowType writerType,
      String description,
      Map<String, String> options) {
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
  }

  @Override
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    StreamExecutionEnvironment env = planner.getExecEnv();
    KafkaSource<byte[]> source = KafkaTables.buildBytesSource(options);
    DataStreamSource<byte[]> bytes =
        env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            SOURCE_TRANSFORMATION,
            PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
    int format = KafkaTables.decodeFormatCode(options);
    // Bare Avro decodes against the writer schema (its datums are schema-less): derive it from the full
    // writer RowType with the same converter Flink's own `avro` format uses, so the decode matches. The
    // row is forced non-null first (a record, never a `["null", record]` union — which both Flink's own
    // format and the Arrow reader treat as a record, the row itself never being null). When the output
    // is a pruned subset of the writer, also pass a reader schema (the narrowed output) so Avro
    // resolution materializes only the read fields.
    boolean pruned = !writerType.equals(outputType);
    String avroSchema =
        format == BARE_AVRO
            ? AvroSchemaConverter.convertToSchema(writerType.copy(false)).toString()
            : "";
    // Confluent Avro has no plan-time writer schema at all — each message names its writer by
    // registry id, fetched and registered by the operator as ids first appear. It therefore always
    // decodes through a reader schema (the possibly-pruned output), the same resolution Flink's
    // deserializer applies against the table-derived schema.
    String readerAvroSchema =
        format == CONFLUENT_AVRO || (format == BARE_AVRO && pruned)
            ? AvroSchemaConverter.convertToSchema(outputType.copy(false)).toString()
            : "";
    ConfluentSchemaRegistry registry =
        format == CONFLUENT_AVRO ? ConfluentSchemaRegistry.fromOptions(options) : null;
    // Flink's ignore-parse-errors: the native decode skips an undecodable message the way Flink's
    // catch-everything-per-message does (CSV applies Flink's finer per-field granularity itself).
    // Honored by JSON, the CDC envelopes, and CSV; the planner only routes other formats with it off.
    boolean skipParseErrors = KafkaTables.ignoreParseErrors(options);
    // Protobuf decodes against the descriptor of the generated message class the table names — extracted
    // by reflection so this carries no compile-time protobuf-java dependency (the class and its runtime
    // are supplied by the Flink distribution, like the protobuf format itself).
    String messageClass = options.get("protobuf.message-class-name");
    byte[] protoDescriptor =
        format == PROTOBUF ? ProtobufDescriptors.descriptorSet(messageClass) : null;
    String protoMessageName = format == PROTOBUF ? ProtobufDescriptors.messageName(messageClass) : null;
    DataStream<ArrowBatch> decoded =
        bytes.transform(
            DECODE_TRANSFORMATION,
            ArrowBatchTypeInformation.INSTANCE,
            new NativeBytesDecodeOperator(
                outputType,
                BATCH_SIZE,
                format,
                avroSchema,
                readerAvroSchema,
                0,
                protoDescriptor,
                protoMessageName,
                registry,
                skipParseErrors,
                FLUSH_INTERVAL_MILLIS,
                KafkaTables.encodeFormatOptions(options)));
    return decoded.getTransformation();
  }
}
