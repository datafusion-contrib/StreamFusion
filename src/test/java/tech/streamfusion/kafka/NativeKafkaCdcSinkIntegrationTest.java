package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Broker-level parity for a changelog written through a CDC envelope format: the same updating
 * aggregate runs once natively and once on stock Flink, and the two topics must hold the exact
 * same record sequence — proving the planner keeps UPDATE_BEFORE flowing to the sink and the
 * envelopes match Flink's byte for byte end to end.
 */
@Tag("streamfusion-kafka")
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeKafkaCdcSinkIntegrationTest {

  private static final int ROWS = 90;
  private static final int KEYS = 3;

  @Test
  void debeziumChangelogMatchesStockFlinkOnTheBroker() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      String inputTopic = "cdc-sink-input-" + UUID.randomUUID();
      produceJson(brokers, inputTopic, ROWS);

      String nativeTopic = "cdc-sink-native-" + UUID.randomUUID();
      String flinkTopic = "cdc-sink-flink-" + UUID.randomUUID();
      // Each row after a key's first emits an UPDATE_BEFORE and UPDATE_AFTER envelope.
      int expected = 2 * ROWS - KEYS;
      runAggregateInto(brokers, inputTopic, nativeTopic, true);
      runAggregateInto(brokers, inputTopic, flinkTopic, false);

      List<String> nativeRecords = consume(brokers, nativeTopic, expected);
      List<String> flinkRecords = consume(brokers, flinkTopic, expected);
      assertEquals(expected, flinkRecords.size());
      assertEquals(flinkRecords, nativeRecords);
    }
  }

  private static void runAggregateInto(
      String brokers, String inputTopic, String outputTopic, boolean nativePlanner)
      throws Exception {
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.setParallelism(1);
    environment.enableCheckpointing(200);
    StreamTableEnvironment table = StreamTableEnvironment.create(environment);
    table.executeSql(
        "CREATE TABLE input (id BIGINT, name STRING) WITH ("
            + "'connector' = 'kafka', 'topic' = '"
            + inputTopic
            + "', 'properties.bootstrap.servers' = '"
            + brokers
            + "', 'properties.group.id' = 'cdc-sink-"
            + UUID.randomUUID()
            + "', 'scan.startup.mode' = 'earliest-offset', "
            + "'scan.bounded.mode' = 'latest-offset', 'format' = 'json')");
    table.executeSql(
        "CREATE TABLE output (name STRING, total BIGINT) WITH ("
            + "'connector' = 'kafka', 'topic' = '"
            + outputTopic
            + "', 'properties.bootstrap.servers' = '"
            + brokers
            + "', 'format' = 'debezium-json')");
    PhysicalPlanScan scan = nativePlanner ? NativePlanner.install(table) : null;

    table
        .executeSql("INSERT INTO output SELECT name, COUNT(*) FROM input GROUP BY name")
        .await();

    if (scan != null) {
      assertTrue(scan.substitutions() >= 2, scan::explainSummary);
    }
  }

  private static void produceJson(String brokers, String topic, int rows) {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
      for (long id = 0; id < rows; id++) {
        byte[] value =
            ("{\"id\":" + id + ",\"name\":\"key-" + (id % KEYS) + "\"}")
                .getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(topic, value));
      }
      producer.flush();
    }
  }

  private static List<String> consume(String brokers, String topic, int expected) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    List<String> values = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
      consumer.subscribe(List.of(topic));
      int idlePolls = 0;
      while (values.size() < expected && idlePolls < 20) {
        int before = values.size();
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(250))) {
          values.add(new String(record.value(), StandardCharsets.UTF_8));
        }
        idlePolls = values.size() == before ? idlePolls + 1 : 0;
      }
    }
    return values;
  }
}
