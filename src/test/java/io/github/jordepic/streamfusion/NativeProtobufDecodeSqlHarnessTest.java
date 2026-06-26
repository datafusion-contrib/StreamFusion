package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.proto.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end parity test for the protobuf native-decode path: a {@code CREATE TABLE ...
 * 'connector'='kafka', 'format'='protobuf'} routes through the planner to {@link
 * io.github.jordepic.streamfusion.planner.StreamPhysicalNativeKafkaDecode} — Flink's own KafkaSource
 * consumes the raw protobuf messages and a native operator decodes them straight to Arrow, against the
 * descriptor reflectively extracted from the table's {@code message-class-name} (a protoc-generated
 * class).
 *
 * <p>{@link NativeParity#assertParity} runs the query on stock Flink (its own {@code protobuf} decoder,
 * which parses the same generated class) and on the native decode and asserts the rows match — a real
 * host-parity comparison. The fields are all set to non-default values so proto3's missing-field
 * semantics don't enter this comparison; the native module pins protobuf-java 4.x to match Flink's
 * protobuf runtime, which is why the ORC host baseline (orc-core 1.5.6, protobuf 2.5) lives in its own
 * module.
 *
 * <p>Opt-in via {@code SF_BENCHMARK=true} (Docker for Testcontainers Kafka). A bounded ({@code
 * latest-offset}) scan so both runs terminate.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeProtobufDecodeSqlHarnessTest {

  private static final String TOPIC = "native-protobuf-decode-sql-it";
  private static final String MESSAGE_CLASS = "io.github.jordepic.streamfusion.proto.Row";
  private static final int MESSAGES = 2_000;

  @Test
  void protobufKafkaTableDecodesNativelyWithFlinkParity() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produceProtobuf(brokers, MESSAGES);

      Supplier<TableEnvironment> environment =
          () -> {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
            tEnv.executeSql(protobufTable("t", brokers));
            return tEnv;
          };

      NativeParity.assertParity(environment, "SELECT * FROM t");
    }
  }

  private static String protobufTable(String name, String brokers) {
    return "CREATE TABLE "
        + name
        + " (id BIGINT, name STRING, score DOUBLE) WITH ("
        + "'connector' = 'kafka', "
        + "'topic' = '"
        + TOPIC
        + "', 'properties.bootstrap.servers' = '"
        + brokers
        + "', 'properties.group.id' = 'native-protobuf-decode-sql-it', "
        + "'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset', "
        + "'format' = 'protobuf', 'protobuf.message-class-name' = '"
        + MESSAGE_CLASS
        + "')";
  }

  private static void produceProtobuf(String brokers, int messages) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      List<byte[]> values = new ArrayList<>(messages);
      for (int i = 0; i < messages; i++) {
        // All fields non-default (id ≥ 1, non-empty name, non-zero score) so every field is on the wire.
        Row row = Row.newBuilder().setId(i + 1L).setName("row-" + i).setScore(i + 0.5).build();
        values.add(row.toByteArray());
      }
      for (byte[] value : values) {
        producer.send(new ProducerRecord<>(TOPIC, 0, null, value));
      }
      producer.flush();
    }
  }
}
