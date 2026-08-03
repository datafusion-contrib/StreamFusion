package tech.streamfusion;

import tech.streamfusion.proto.Complex;
import tech.streamfusion.proto.Row;
import tech.streamfusion.proto.Scalars;
import tech.streamfusion.proto.WithNested;
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
 * End-to-end parity tests for the protobuf native-decode path: a {@code CREATE TABLE ...
 * 'connector'='kafka', 'format'='protobuf'} with a {@code protobuf.message-class-name} routes through
 * the planner to {@link tech.streamfusion.planner.StreamPhysicalNativeKafkaDecode} — Flink
 * consumes the raw messages and a native operator decodes them straight to Arrow against the descriptor
 * reflectively extracted from the generated class — or falls back when a field type isn't reproduced
 * identically (enum, unsigned/fixed int, bytes, well-known type).
 *
 * <p>Each case uses {@link NativeParity#assertParity} to compare the native decode against Flink's own
 * {@code protobuf} format. Covered: a flat scalar message, a nested message (ROW column), and a message
 * with repeated and map fields (ARRAY/MAP columns) — the complex shapes the row boundary now carries.
 * The complex columns are read through extracting projections ({@code nested.id}, {@code nums[1]},
 * {@code tags['a']}) so the compared values are scalars (Flink's {@code collect()} surfaces ARRAY as a
 * Java array, which compares by identity); the projection itself runs on the host over the natively
 * decoded column, exercising the column across the boundary.
 *
 * <p>All fields are set to non-default values, so proto3's missing-field semantics don't enter. Opt-in
 * via {@code SF_BENCHMARK=true} (Docker for Testcontainers Kafka).
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeProtobufDecodeSqlHarnessTest {

  @org.junit.jupiter.api.BeforeEach
  void pinDecodePath() {
    // These tests cover the shallow decode operator; with the native rdkafka source now on by
    // default it would otherwise take the JSON/Avro/protobuf tables, testing the wrong path.
    System.setProperty("streamfusion.operator.kafkaSource.enabled", "false");
  }

  @org.junit.jupiter.api.AfterEach
  void unpinDecodePath() {
    System.clearProperty("streamfusion.operator.kafkaSource.enabled");
  }

  private static final int MESSAGES = 2_000;
  private static final String PKG = "tech.streamfusion.proto";

  @Test
  void protobufMessagesDecodeNativelyWithFlinkParity() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();

      // Flat scalar message — every signed-int/float/double/bool/string field type.
      produce(brokers, "pb-scalars", scalarMessages());
      NativeParity.assertParity(
          environment(
              brokers,
              "pb-scalars",
              "i32 INT, i64 BIGINT, flag BOOLEAN, f32 FLOAT, f64 DOUBLE, text STRING, si32 INT, si64 BIGINT",
              PKG + ".Scalars"),
          "SELECT * FROM t");

      // Nested message → a ROW column; the whole row compares (Flink surfaces ROW as a Row, by value).
      produce(brokers, "pb-nested", nestedMessages());
      NativeParity.assertParity(
          environment(
              brokers,
              "pb-nested",
              "id BIGINT, nested ROW<id BIGINT, name STRING, score DOUBLE>",
              PKG + ".WithNested"),
          "SELECT * FROM t");

      // Repeated + map (+ nested) → ARRAY/MAP/ROW columns; read element-wise so the comparison is scalar.
      produce(brokers, "pb-complex", complexMessages());
      NativeParity.assertParity(
          environment(
              brokers,
              "pb-complex",
              "id BIGINT, nums ARRAY<BIGINT>, tags MAP<STRING, BIGINT>, nested ROW<id BIGINT, name STRING, score DOUBLE>",
              PKG + ".Complex"),
          "SELECT id, nums[1], nums[2], tags['a'], tags['b'], nested.id, nested.name FROM t");
    }
  }

  @Test
  void nestedProjectionPrunesDecodedColumns() throws Exception {
    // Read one nested field of a wider message: the planner prunes the protobuf descriptor to id +
    // nested.score, so ptars builds only those columns and skips nested.id/name on the wire. Must still
    // match Flink's full decode + calc.
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produce(brokers, "pb-prune", nestedMessages());
      NativeParity.assertParity(
          environment(
              brokers,
              "pb-prune",
              "id BIGINT, nested ROW<id BIGINT, name STRING, score DOUBLE>",
              PKG + ".WithNested"),
          "SELECT nested.score FROM t WHERE id > 5");
    }
  }

  private static List<byte[]> scalarMessages() {
    List<byte[]> values = new ArrayList<>(MESSAGES);
    for (int i = 0; i < MESSAGES; i++) {
      values.add(
          Scalars.newBuilder()
              .setI32(i + 1)
              .setI64(i + 1L)
              .setFlag(i % 2 == 0)
              .setF32(i + 0.5f)
              .setF64(i + 0.25)
              .setText("row-" + i)
              .setSi32(-i - 1)
              .setSi64(-i - 1L)
              .build()
              .toByteArray());
    }
    return values;
  }

  private static List<byte[]> nestedMessages() {
    List<byte[]> values = new ArrayList<>(MESSAGES);
    for (int i = 0; i < MESSAGES; i++) {
      values.add(
          WithNested.newBuilder()
              .setId(i + 1L)
              .setNested(Row.newBuilder().setId(i).setName("n-" + i).setScore(i + 0.5).build())
              .build()
              .toByteArray());
    }
    return values;
  }

  private static List<byte[]> complexMessages() {
    List<byte[]> values = new ArrayList<>(MESSAGES);
    for (int i = 0; i < MESSAGES; i++) {
      values.add(
          Complex.newBuilder()
              .setId(i + 1L)
              .addNums(i)
              .addNums(i + 100L)
              .putTags("a", i + 1L)
              .putTags("b", i + 2L)
              .setNested(Row.newBuilder().setId(i).setName("n-" + i).setScore(i + 0.5).build())
              .build()
              .toByteArray());
    }
    return values;
  }

  private static Supplier<TableEnvironment> environment(
      String brokers, String topic, String columns, String messageClass) {
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      tEnv.executeSql(
          "CREATE TABLE t ("
              + columns
              + ") WITH ('connector' = 'kafka', 'topic' = '"
              + topic
              + "', 'properties.bootstrap.servers' = '"
              + brokers
              + "', 'properties.group.id' = '"
              + topic
              + "', 'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset', "
              + "'format' = 'protobuf', 'protobuf.message-class-name' = '"
              + messageClass
              + "')");
      return tEnv;
    };
  }

  private static void produce(String brokers, String topic, List<byte[]> values) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (byte[] value : values) {
        producer.send(new ProducerRecord<>(topic, 0, null, value));
      }
      producer.flush();
    }
  }
}
