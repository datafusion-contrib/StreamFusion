package tech.streamfusion;

import java.nio.charset.StandardCharsets;
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
 * End-to-end referee for the keyed decode increment: a {@code key.format = 'raw'} table runs the
 * same bounded read under the native planner and under stock Flink (whose factory assembles the
 * real {@code DynamicKafkaDeserializationSchema} key/value merge), and every row must match — the
 * key column filled from the record key, NULL Kafka keys kept as NULL key columns, and both
 * {@code value.fields-include} modes. Opt-in via {@code SF_BENCHMARK=true} (Docker).
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeKeyedDecodeSqlHarnessTest {

  @org.junit.jupiter.api.BeforeEach
  void pinDecodePath() {
    // The fused source declines keyed tables; pin the decode path so the parity run exercises it.
    System.setProperty("streamfusion.operator.kafkaSource.enabled", "false");
  }

  @org.junit.jupiter.api.AfterEach
  void unpinDecodePath() {
    System.clearProperty("streamfusion.operator.kafkaSource.enabled");
  }

  @Test
  void rawKeyedTableMatchesFlinkRowForRow() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produceKeyed(brokers, "keyed-except");
      NativeParity.assertParity(
          environment(brokers, "keyed-except", "EXCEPT_KEY"), "SELECT k, id, name FROM t");

      produceKeyed(brokers, "keyed-all");
      // ALL: every physical field comes from the value body; the key column is decoded from the
      // record's value JSON like any other field, exactly Flink's overlap behavior.
      NativeParity.assertParity(
          environment(brokers, "keyed-all", "ALL"), "SELECT k, id, name FROM t");
    }
  }

  private static Supplier<TableEnvironment> environment(
      String brokers, String topic, String include) {
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      tEnv.executeSql(
          "CREATE TABLE t (k BIGINT, id BIGINT, name STRING) WITH ("
              + "'connector' = 'kafka', 'topic' = '"
              + topic
              + "', 'properties.bootstrap.servers' = '"
              + brokers
              + "', 'properties.group.id' = '"
              + topic
              + "', 'scan.startup.mode' = 'earliest-offset',"
              + " 'scan.bounded.mode' = 'latest-offset',"
              + " 'value.format' = 'json', 'key.format' = 'raw', 'key.fields' = 'k',"
              + " 'value.fields-include' = '"
              + include
              + "')");
      return tEnv;
    };
  }

  private static void produceKeyed(String brokers, String topic) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < 500; i++) {
        // Every record's value carries a k field too, so the ALL projection has one to decode;
        // every seventh record has a NULL Kafka key (kept, with a NULL key column, by raw).
        byte[] key = i % 7 == 0 ? null : longBytes(1000L + i);
        String value =
            String.format("{\"k\": %d, \"id\": %d, \"name\": \"row-%d\"}", 1000L + i, i, i);
        producer.send(
            new ProducerRecord<>(topic, 0, key, value.getBytes(StandardCharsets.UTF_8)));
      }
      producer.flush();
    }
  }

  private static byte[] longBytes(long value) {
    byte[] bytes = new byte[8];
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) (value >>> (56 - 8 * i));
    }
    return bytes;
  }
}
