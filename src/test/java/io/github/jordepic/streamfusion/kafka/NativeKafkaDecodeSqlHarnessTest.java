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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end SQL test for the shallow native-decode path (Phase 2): a {@code CREATE TABLE ...
 * 'connector'='kafka', 'format'='csv'} routes through the planner to {@link
 * io.github.jordepic.streamfusion.planner.StreamPhysicalNativeKafkaDecode} — Flink's own KafkaSource
 * consumes raw bytes, a native operator decodes them straight to Arrow (no Flink {@code RowData} decode)
 * — and produces the expected rows. A bounded ({@code latest-offset}) scan so the query terminates and
 * the decode operator's end-of-input flush fires.
 *
 * <p>Opt-in via {@code SF_BENCHMARK=true} (Docker for Testcontainers Kafka). Uses the default native
 * build's decoders — no {@code kafka} cargo feature needed (Flink owns the consume).
 */
@Tag("streamfusion-kafka")
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeKafkaDecodeSqlHarnessTest {

  private static final String TOPIC = "native-decode-sql-it";
  private static final int MESSAGES = 2_000;

  @Test
  void csvKafkaTableDecodesNativelyThroughSql() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produceCsv(brokers, MESSAGES);

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      tEnv.executeSql(csvTable("k", brokers));
      PhysicalPlanScan scan = NativePlanner.install(tEnv);

      Set<Long> ids = new HashSet<>();
      try (CloseableIterator<Row> iterator = tEnv.executeSql("SELECT * FROM k").collect()) {
        while (iterator.hasNext()) {
          ids.add((Long) iterator.next().getField("id"));
        }
      }

      assertTrue(scan.substitutions() >= 1, "CSV Kafka table did not route to the native decode path");
      assertEquals(MESSAGES, ids.size(), "expected every produced id exactly once");
      for (long i = 0; i < MESSAGES; i++) {
        assertTrue(ids.contains(i), "missing id " + i);
      }
    }
  }

  private static String csvTable(String name, String brokers) {
    return "CREATE TABLE "
        + name
        + " (id BIGINT, name STRING, score DOUBLE) WITH ("
        + "'connector' = 'kafka', "
        + "'topic' = '"
        + TOPIC
        + "', 'properties.bootstrap.servers' = '"
        + brokers
        + "', 'properties.group.id' = 'native-decode-sql-it', "
        + "'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset', "
        + "'format' = 'csv')";
  }

  private static void produceCsv(String brokers, int messages) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < messages; i++) {
        byte[] value =
            String.format("%d,row-%d,%d.5", i, i, i % 100).getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(TOPIC, 0, null, value));
      }
      producer.flush();
    }
  }
}
