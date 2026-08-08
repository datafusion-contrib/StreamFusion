package tech.streamfusion;

import java.nio.charset.StandardCharsets;
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
 * End-to-end parity tests for the JSON native-decode path downstream of Flink's Kafka source.
 * by default, a {@code 'format'='json'} Kafka table routes through the shallow decode operator (Flink
 * consumes the raw bytes, a native operator decodes them to Arrow), like the other value formats.
 * {@link NativeParity#assertParity} compares the native decode against Flink's own {@code json} format.
 *
 * <p>Covers a flat record and a record with a nested object, an array, and a map — confirming arrow-json
 * decodes the complex shapes identically to Flink across the boundary. Complex columns are read
 * element-wise ({@code nested.a}, {@code nums[1]}, {@code tags['a']}) so the comparison stays scalar.
 * Opt-in via {@code SF_BENCHMARK=true} (Docker for Testcontainers Kafka).
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeJsonDecodeSqlHarnessTest {

  private static final int MESSAGES = 2_000;

  @Test
  void jsonMessagesDecodeNativelyWithFlinkParity() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();

      List<String> flat = new ArrayList<>(MESSAGES);
      for (int i = 0; i < MESSAGES; i++) {
        flat.add(String.format("{\"id\":%d,\"name\":\"row-%d\",\"score\":%d.5}", i, i, i));
      }
      produce(brokers, "json-flat", flat);
      NativeParity.assertParity(
          environment(brokers, "json-flat", "id BIGINT, name STRING, score DOUBLE"),
          "SELECT * FROM t");

      List<String> complex = new ArrayList<>(MESSAGES);
      for (int i = 0; i < MESSAGES; i++) {
        complex.add(
            String.format(
                "{\"id\":%d,\"nested\":{\"a\":%d,\"b\":\"b-%d\"},\"nums\":[%d,%d],"
                    + "\"tags\":{\"a\":%d,\"b\":%d}}",
                i, i + 1, i, i, i + 100, i + 1, i + 2));
      }
      produce(brokers, "json-complex", complex);
      NativeParity.assertParity(
          environment(
              brokers,
              "json-complex",
              "id BIGINT, nested ROW<a BIGINT, b STRING>, nums ARRAY<BIGINT>, tags MAP<STRING, BIGINT>"),
          "SELECT id, nested.a, nested.b, nums[1], nums[2], tags['a'], tags['b'] FROM t");
    }
  }

  @Test
  void metadataColumnFallsBackAndMatchesFlink() throws Exception {
    // `ts` is Kafka's record timestamp, filled by the connector — a native value decode would emit
    // it as a missing body field (silently NULL). The scan stays on Flink (pinned plan-side by
    // KafkaDecodeRoutingTest; a native Calc above it still substitutes, which is fine) and the run
    // must produce Flink's values end to end — both runs read the same records, so the connector
    // timestamps compare exactly, and a silently-NULL ts would diverge here.
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      List<String> flat = new ArrayList<>(MESSAGES);
      for (int i = 0; i < MESSAGES; i++) {
        flat.add(String.format("{\"id\":%d,\"name\":\"row-%d\"}", i, i));
      }
      produce(brokers, "json-metadata", flat);
      NativeParity.assertParity(
          environment(
              brokers,
              "json-metadata",
              "id BIGINT, name STRING, ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'"),
          "SELECT * FROM t");
    }
  }

  @Test
  void nestedProjectionPrunesDecodedColumns() throws Exception {
    // A wide record read through a strict subset: the planner prunes the decode to event_type + the two
    // bid sub-fields, so arrow-json never materializes person or bid.channel/url. Must still match Flink.
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      List<String> wide = new ArrayList<>(MESSAGES);
      for (int i = 0; i < MESSAGES; i++) {
        wide.add(
            String.format(
                "{\"event_type\":%d,\"person\":{\"id\":%d,\"name\":\"p-%d\",\"email\":\"e-%d\","
                    + "\"city\":\"c-%d\"},\"bid\":{\"auction\":%d,\"bidder\":%d,\"price\":%d,"
                    + "\"channel\":\"ch-%d\",\"url\":\"u-%d\"}}",
                i % 3, i, i, i, i, i, i + 1, (i % 1000) + 1, i % 8, i));
      }
      produce(brokers, "json-wide", wide);
      NativeParity.assertParity(
          environment(
              brokers,
              "json-wide",
              "event_type INT, person ROW<id BIGINT, name STRING, email STRING, city STRING>,"
                  + " bid ROW<auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING>"),
          "SELECT bid.auction, bid.price FROM t WHERE event_type = 2");
    }
  }

  private static Supplier<TableEnvironment> environment(String brokers, String topic, String columns) {
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
              + "'format' = 'json')");
      return tEnv;
    };
  }

  private static void produce(String brokers, String topic, List<String> messages) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (String message : messages) {
        producer.send(new ProducerRecord<>(topic, 0, null, message.getBytes(StandardCharsets.UTF_8)));
      }
      producer.flush();
    }
  }
}
