package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.kafka.NativeKafkaJsonSerializationOperator;
import io.github.jordepic.streamfusion.kafka.PreSerializedKafkaRecordSchema;
import io.github.jordepic.streamfusion.operator.ArrowBatch;
import java.util.Collections;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Builds native batch serialization followed by Flink's unmodified Kafka sink. */
public final class NativeKafkaSinkExecNode extends ExecNodeBase<Object>
    implements StreamExecNode<Object>, SingleTransformationTranslator<Object> {

  private final KafkaSinkMatcher.Planned planned;

  NativeKafkaSinkExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      KafkaSinkMatcher.Planned planned) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-kafka-sink_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.planned = planned;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<Object> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    boolean parallelismConfigured = planned.sink.parallelism != null;
    int parallelism =
        parallelismConfigured ? planned.sink.parallelism : input.getParallelism();
    OneInputTransformation<ArrowBatch, byte[]> serialization =
        new OneInputTransformation<>(
            input,
            "native-kafka-json-serialization",
            SimpleOperatorFactory.of(
                new NativeKafkaJsonSerializationOperator(
                    planned.ignoreNullFields, planned.timestampFormat)),
            PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
            parallelism,
            parallelismConfigured);

    KafkaSinkBuilder<byte[]> builder =
        KafkaSink.<byte[]>builder()
            .setKafkaProducerConfig(planned.sink.producerProperties)
            .setRecordSerializer(new PreSerializedKafkaRecordSchema(planned.sink.topic))
            .setDeliveryGuarantee(planned.sink.deliveryGuarantee)
            .setTransactionNamingStrategy(planned.sink.transactionNamingStrategy);
    if (planned.sink.transactionalIdPrefix != null) {
      builder.setTransactionalIdPrefix(planned.sink.transactionalIdPrefix);
    }
    DataStream<byte[]> stream = new DataStream<>(planner.getExecEnv(), serialization);
    DataStreamSink<byte[]> sink =
        stream.sinkTo(builder.build()).name("native-kafka-sink").setParallelism(parallelism);
    return (Transformation<Object>) (Transformation<?>) sink.getTransformation();
  }
}
