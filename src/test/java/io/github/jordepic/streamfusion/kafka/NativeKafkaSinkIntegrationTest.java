package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Broker-level proof that native values remain governed by Flink's exactly-once Kafka writer. */
@Tag("streamfusion-kafka")
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeKafkaSinkIntegrationTest {

  private static final int ROWS = 100;

  @Test
  void publishesEveryNativeJsonValueInCommittedTransactions() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withEnv("KAFKA_TRANSACTION_MAX_TIMEOUT_MS", "7200000")) {
      kafka.start();
      String topic = "native-sink-" + UUID.randomUUID();

      StreamExecutionEnvironment environment =
          StreamExecutionEnvironment.getExecutionEnvironment();
      environment.setParallelism(1);
      Configuration configuration = new Configuration();
      configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
      environment.configure(configuration);
      environment.enableCheckpointing(50);
      StreamTableEnvironment table = StreamTableEnvironment.create(environment);
      List<Row> input = new ArrayList<>(ROWS);
      for (long id = 0; id < ROWS; id++) {
        input.add(Row.of(id, "row-" + id));
      }
      DataStream<Row> source =
          environment.fromData(
              Types.ROW_NAMED(new String[] {"id", "name"}, Types.LONG, Types.STRING),
              input.toArray(Row[]::new));
      table.createTemporaryView(
          "src",
          source,
          Schema.newBuilder()
              .column("id", DataTypes.BIGINT())
              .column("name", DataTypes.STRING())
              .build());
      table.executeSql(
          "CREATE TABLE output (id BIGINT, name STRING) WITH ("
              + "'connector' = 'kafka', 'topic' = '"
              + topic
              + "', 'properties.bootstrap.servers' = '"
              + kafka.getBootstrapServers()
              + "', 'format' = 'json', "
              + "'sink.delivery-guarantee' = 'exactly-once', "
              + "'sink.transactional-id-prefix' = 'native-sink-test')");
      PhysicalPlanScan scan = NativePlanner.install(table);

      table.executeSql("INSERT INTO output SELECT * FROM src").await();

      assertTrue(scan.substitutions() > 0, scan::explainSummary);
      Set<String> values = consumeCommitted(kafka.getBootstrapServers(), topic);
      assertEquals(ROWS, values.size());
      for (long id = 0; id < ROWS; id++) {
        assertTrue(values.contains("{\"id\":" + id + ",\"name\":\"row-" + id + "\"}"));
      }
    }
  }

  private static Set<String> consumeCommitted(String brokers, String topic) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    Set<String> values = new HashSet<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
      consumer.subscribe(List.of(topic));
      int idlePolls = 0;
      while (values.size() < ROWS && idlePolls < 20) {
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
