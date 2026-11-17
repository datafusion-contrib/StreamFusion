package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end referee for keyed Kafka tables: a {@code key.format = 'raw'} table runs the same
 * bounded read under the native planner and under stock Flink (whose factory assembles the real
 * {@code DynamicKafkaDeserializationSchema} key/value merge), every row must match — the key
 * column filled from the record key, NULL Kafka keys kept as NULL key columns, and both
 * {@code value.fields-include} modes — and the native source's own decode counter must account for
 * every record it read; a plan that merely substituted some operator would not. Opt-in via
 * {@code SF_BENCHMARK=true} (Docker).
 */
@Tag("streamfusion-kafka")
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeKeyedDecodeSqlHarnessTest {

  @Test
  void rawKeyedTableMatchesFlinkRowForRow() throws Exception {
    try (KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
        RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics()) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
      for (String topic : List.of("keyed-except", "keyed-all")) {
        for (int i = 0; i < 500; i++) {
          // Every record's value carries a k field too, so the ALL projection has one to decode;
          // every seventh record has a NULL Kafka key (kept, with a NULL key column, by raw).
          byte[] key = i % 7 == 0 ? null : longBytes(1000L + i);
          String value =
              String.format("{\"k\": %d, \"id\": %d, \"name\": \"row-%d\"}", 1000L + i, i, i);
          records.add(new ProducerRecord<>(topic, 0, key, bytes(value)));
        }
      }
      produce(brokers, records);
      Supplier<TableEnvironment> except =
          environment(
              metrics,
              "CREATE TABLE t (k BIGINT, id BIGINT, name STRING) WITH ("
                  + source(brokers, "keyed-except")
                  + " 'value.format' = 'json', 'key.format' = 'raw', 'key.fields' = 'k',"
                  + " 'value.fields-include' = 'EXCEPT_KEY')");
      // ALL: every physical field comes from the value body; the key column is decoded from the
      // record's value JSON like any other field, exactly Flink's overlap behavior.
      Supplier<TableEnvironment> all =
          environment(
              metrics,
              "CREATE TABLE t (k BIGINT, id BIGINT, name STRING) WITH ("
                  + source(brokers, "keyed-all")
                  + " 'value.format' = 'json', 'key.format' = 'raw', 'key.fields' = 'k',"
                  + " 'value.fields-include' = 'ALL')");

      NativeParity.assertParity(except, "SELECT k, id, name FROM t");
      NativeParity.assertParity(all, "SELECT k, id, name FROM t");

      assertEquals(1000, metrics.counterValue("nativeKafkaDecodeRecords"));
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

  private static String source(String brokers, String topic) {
    return "'connector' = 'kafka', 'topic' = '"
        + topic
        + "', 'properties.bootstrap.servers' = '"
        + brokers
        + "', 'properties.group.id' = '"
        + topic
        + "', 'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset',";
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

  private static byte[] bytes(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] longBytes(long value) {
    byte[] bytes = new byte[8];
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) (value >>> (56 - 8 * i));
    }
    return bytes;
  }
}
