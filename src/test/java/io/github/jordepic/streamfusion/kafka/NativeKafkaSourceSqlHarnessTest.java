package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end SQL test: a {@code CREATE TABLE ... WITH ('connector'='kafka', 'format'='json')} queried
 * through the {@code TableEnvironment} routes to the native source via the planner hook (like the
 * Parquet/ORC source harness), reads the topic with the native rdkafka reader, and produces the
 * expected rows. Confirms the whole path — matcher → physical rel → exec node → FLIP-27 source → native
 * consume+decode — and that the source actually substituted (not silently fell back to Flink's).
 *
 * <p>Opt-in via {@code SF_BENCHMARK=true} (Docker for Testcontainers Kafka + a native build with the
 * {@code kafka} cargo feature, which statically links a bundled librdkafka). The default build
 * excludes rdkafka and skips this test.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeKafkaSourceSqlHarnessTest {

  private static final String TOPIC = "native-source-sql-it";
  private static final int MESSAGES = 2_000;

  @Test
  void nativeKafkaSourceReadsTopicThroughSql() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produce(brokers, MESSAGES);

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      tEnv.executeSql(kafkaTable("k", brokers));
      PhysicalPlanScan scan = NativePlanner.install(tEnv);

      Set<Long> ids = new HashSet<>();
      try (CloseableIterator<Row> iterator = tEnv.executeSql("SELECT * FROM k").collect()) {
        while (ids.size() < MESSAGES && iterator.hasNext()) {
          ids.add((Long) iterator.next().getField("id"));
        }
      }

      assertTrue(scan.substitutions() >= 1, "Kafka source did not route to native");
      assertEquals(MESSAGES, ids.size(), "expected every produced id exactly once");
      for (long i = 0; i < MESSAGES; i++) {
        assertTrue(ids.contains(i), "missing id " + i);
      }
    }
  }

  private static String kafkaTable(String name, String brokers) {
    return "CREATE TABLE "
        + name
        + " (id BIGINT, name STRING, score DOUBLE) WITH ("
        + "'connector' = 'kafka', "
        + "'topic' = '"
        + TOPIC
        + "', 'properties.bootstrap.servers' = '"
        + brokers
        + "', 'properties.group.id' = 'native-source-sql-it', "
        + "'scan.startup.mode' = 'earliest-offset', 'format' = 'json')";
  }

  private static void produce(String brokers, int messages) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < messages; i++) {
        byte[] value =
            String.format("{\"id\": %d, \"name\": \"row-%d\", \"score\": %d.5}", i, i, i % 100)
                .getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(TOPIC, 0, null, value));
      }
      producer.flush();
    }
  }
}
