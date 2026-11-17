package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import tech.streamfusion.planner.NativePlanner;

/**
 * End-to-end parity for Kafka connector metadata read by the native decode: the same broker
 * records, read by stock Flink and natively, must produce the same rows. A collected header map
 * compares as a map; the entry order the decode reproduces is pinned record by record against
 * Flink's own deserializer by {@code KafkaMetadataParityTest}. The native source's decode counter
 * must account for every record it read, so a run that substituted only downstream operators
 * cannot pass. Opt-in via {@code SF_BENCHMARK=true} (Docker for Testcontainers Kafka); bounded
 * scans so the runs terminate.
 */
@Tag("streamfusion-kafka")
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeKafkaMetadataSqlHarnessTest {

  private static final String METADATA_COLUMNS =
      "tp STRING METADATA FROM 'topic', part INT METADATA FROM 'partition',"
          + " hdrs MAP<STRING, BYTES> NOT NULL METADATA FROM 'headers',"
          + " epoch INT METADATA FROM 'leader-epoch', off BIGINT METADATA FROM 'offset',"
          + " ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',"
          + " tst STRING METADATA FROM 'timestamp-type'";

  private static final String METADATA_QUERY =
      "SELECT id, name, tp, part, hdrs, epoch, off, ts, tst FROM t";

  /**
   * Every connector metadata column of a JSON table over records with repeated and null-valued
   * headers, three partitions and a tombstone — on a topic whose records keep their producer
   * timestamps and on one whose broker assigns them. Canal's data lists cover a record fanning out
   * into several rows below.
   */
  @Test
  void jsonConnectorMetadataMatchesFlink() throws Exception {
    try (KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
        RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics()) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      createTopic(brokers, "metadata-create-time", Map.of());
      createTopic(
          brokers,
          "metadata-log-append-time",
          Map.of("message.timestamp.type", "LogAppendTime"));
      for (String topic : List.of("metadata-create-time", "metadata-log-append-time")) {
        produce(
            brokers,
            List.of(
                new ProducerRecord<>(
                    topic,
                    0,
                    1_000L,
                    null,
                    bytes("{\"id\":1,\"name\":\"a\"}"),
                    List.of(
                        header("trace", "t1"),
                        header("tenant", null),
                        header("trace", "t2"),
                        header("source", "s"))),
                new ProducerRecord<>(
                    topic,
                    1,
                    2_000L,
                    null,
                    bytes("{\"id\":2,\"name\":\"b\"}"),
                    List.of(header("z", "1"), header("a", "2"), header("m", "3"))),
                new ProducerRecord<>(topic, 2, 3_000L, null, null, List.<Header>of()),
                new ProducerRecord<>(
                    topic, 2, 4_000L, null, bytes("{\"id\":4,\"name\":null}"), List.<Header>of())));
        Supplier<TableEnvironment> environment =
            environment(
                metrics,
                table(brokers, topic, "id BIGINT, name STRING, " + METADATA_COLUMNS, "json"));

        String plan = NativePlanner.explain(environment.get(), METADATA_QUERY);
        assertTrue(plan.contains("NativeKafkaDecode"), plan);
        NativeParity.assertParity(environment, METADATA_QUERY);
      }
      assertEquals(8, metrics.counterValue("nativeKafkaDecodeRecords"));
    }
  }

  /** CSV values and Canal changes carry connector metadata too, an update's two rows sharing it. */
  @Test
  void csvAndCanalConnectorMetadataMatchFlink() throws Exception {
    try (KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
        RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics()) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      createTopic(brokers, "metadata-csv", Map.of());
      produce(
          brokers,
          List.of(
              new ProducerRecord<>(
                  "metadata-csv", 0, 1_000L, null, bytes("1,a"), List.of(header("k", "v"))),
              new ProducerRecord<>(
                  "metadata-csv", 1, 2_000L, null, bytes("2,b"), List.<Header>of())));
      Supplier<TableEnvironment> csv =
          environment(
              metrics,
              table(
                  brokers, "metadata-csv", "id BIGINT, name STRING, " + METADATA_COLUMNS, "csv"));
      assertTrue(NativePlanner.explain(csv.get(), METADATA_QUERY).contains("NativeKafkaDecode"));
      NativeParity.assertParity(csv, METADATA_QUERY);

      createTopic(brokers, "metadata-canal", Map.of());
      produce(
          brokers,
          List.of(
              new ProducerRecord<>(
                  "metadata-canal",
                  0,
                  1_000L,
                  null,
                  bytes(
                      "{\"data\":[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"}],"
                          + "\"type\":\"INSERT\"}"),
                  List.of(header("op", "i"))),
              new ProducerRecord<>(
                  "metadata-canal",
                  0,
                  2_000L,
                  null,
                  bytes(
                      "{\"data\":[{\"id\":1,\"name\":\"a2\"}],\"old\":[{\"name\":\"a\"}],"
                          + "\"type\":\"UPDATE\"}"),
                  List.of(header("op", "u")))));
      String changes = "SELECT id, name, part, hdrs, off, ts, tst FROM t";
      Supplier<TableEnvironment> canal =
          environment(
              metrics,
              table(
                  brokers,
                  "metadata-canal",
                  "id BIGINT, name STRING, " + METADATA_COLUMNS,
                  "canal-json"));
      assertTrue(NativePlanner.explain(canal.get(), changes).contains("NativeKafkaDecode"));
      NativeParity.assertKindedParity(canal, changes);
      assertEquals(4, metrics.counterValue("nativeKafkaDecodeRecords"));
    }
  }

  /** An event-time window over the record timestamp, with the watermark regenerated natively. */
  @Test
  void windowsOverTheRecordTimestampMatchFlink() throws Exception {
    try (KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
        RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics()) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      createTopic(brokers, "metadata-windows", Map.of());
      produce(
          brokers,
          List.of(
              new ProducerRecord<>(
                  "metadata-windows", 0, 1_000L, null, bytes("{\"id\":1}"), List.<Header>of()),
              new ProducerRecord<>(
                  "metadata-windows", 1, 4_000L, null, bytes("{\"id\":2}"), List.<Header>of()),
              new ProducerRecord<>(
                  "metadata-windows", 2, 6_000L, null, bytes("{\"id\":3}"), List.<Header>of()),
              new ProducerRecord<>(
                  "metadata-windows", 0, 12_000L, null, bytes("{\"id\":4}"), List.<Header>of())));
      Supplier<TableEnvironment> environment =
          environment(
              metrics,
              table(
                  brokers,
                  "metadata-windows",
                  "id BIGINT, ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',"
                      + " WATERMARK FOR ts AS ts - INTERVAL '1' SECOND",
                  "json"));
      String windows =
          "SELECT window_start, window_end, COUNT(*) AS records, SUM(id) AS ids"
              + " FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '5' SECOND))"
              + " GROUP BY window_start, window_end";

      String plan = NativePlanner.explain(environment.get(), windows);
      assertTrue(
          plan.contains("NativeKafkaDecode") && plan.contains("watermarkColumn=[ts]"), plan);
      NativeParity.assertParity(environment, windows);
      assertEquals(4, metrics.counterValue("nativeKafkaDecodeRecords"));
    }
  }

  private static Supplier<TableEnvironment> environment(
      RecoveryExecutionMetrics metrics, String ddl) {
    return () -> {
      StreamTableEnvironment tEnv =
          StreamTableEnvironment.create(metrics.environment(new Configuration(), 1, 0));
      tEnv.getConfig().set("table.local-time-zone", "UTC");
      tEnv.executeSql(ddl);
      return tEnv;
    };
  }

  private static String table(String brokers, String topic, String columns, String format) {
    return "CREATE TABLE t ("
        + columns
        + ") WITH ('connector' = 'kafka', 'topic' = '"
        + topic
        + "', 'properties.bootstrap.servers' = '"
        + brokers
        + "', 'properties.group.id' = '"
        + topic
        + "', 'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset',"
        + " 'format' = '"
        + format
        + "')";
  }

  private static void createTopic(String brokers, String topic, Map<String, String> config)
      throws Exception {
    Properties admin = new Properties();
    admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    try (AdminClient client = AdminClient.create(admin)) {
      client.createTopics(List.of(new NewTopic(topic, 3, (short) 1).configs(config))).all().get();
    }
  }

  private static void produce(String brokers, List<ProducerRecord<byte[], byte[]>> records) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      records.forEach(producer::send);
      producer.flush();
    }
  }

  private static Header header(String key, String value) {
    return new RecordHeader(key, value == null ? null : bytes(value));
  }

  private static byte[] bytes(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }
}
