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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
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
  private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();

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
      Set<String> values = new HashSet<>(consumeCommitted(kafka.getBootstrapServers(), topic, ROWS));
      assertEquals(ROWS, values.size());
      for (long id = 0; id < ROWS; id++) {
        assertTrue(values.contains("{\"id\":" + id + ",\"name\":\"row-" + id + "\"}"));
      }
    }
  }

  @Test
  void abortsReplayedTransactionsAcrossFailover() throws Exception {
    FAILED_ONCE.set(false);
    int rows = 300;
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withEnv("KAFKA_TRANSACTION_MAX_TIMEOUT_MS", "7200000")) {
      kafka.start();
      String topic = "native-sink-failover-" + UUID.randomUUID();
      StreamExecutionEnvironment environment =
          StreamExecutionEnvironment.getExecutionEnvironment();
      environment.setParallelism(1);
      environment.enableCheckpointing(50);
      Configuration configuration = new Configuration();
      configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
      configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
      configuration.set(
          RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(10));
      environment.configure(configuration);
      StreamTableEnvironment table = StreamTableEnvironment.create(environment);
      List<Row> input = new ArrayList<>(rows);
      for (long id = 0; id < rows; id++) {
        input.add(Row.of(id, "row-" + id));
      }
      DataStream<Row> source =
          environment
              .fromData(
                  Types.ROW_NAMED(
                      new String[] {"id", "name"}, Types.LONG, Types.STRING),
                  input.toArray(Row[]::new))
              .map(new CheckpointFailingMap())
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"id", "name"}, Types.LONG, Types.STRING));
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
              + "'sink.transactional-id-prefix' = 'native-sink-failover')");
      PhysicalPlanScan scan = NativePlanner.install(table);

      table.executeSql("INSERT INTO output SELECT * FROM src").await();

      assertTrue(FAILED_ONCE.get(), "source never exercised recovery");
      assertTrue(scan.substitutions() > 0, scan::explainSummary);
      List<String> committed = consumeCommitted(kafka.getBootstrapServers(), topic, rows);
      assertEquals(rows, committed.size(), "replayed records must remain invisible");
      assertEquals(rows, new HashSet<>(committed).size(), "committed records must be unique");
    }
  }

  private static List<String> consumeCommitted(String brokers, String topic, int expected) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
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

  private static final class CheckpointFailingMap extends RichMapFunction<Row, Row>
      implements CheckpointedFunction, CheckpointListener {

    private volatile boolean checkpointCompleted;
    private transient ListState<Long> state;
    private long seen;

    @Override
    public Row map(Row value) throws Exception {
      seen++;
      if (checkpointCompleted && seen >= 100 && FAILED_ONCE.compareAndSet(false, true)) {
        throw new RuntimeException("intentional post-checkpoint failure");
      }
      Thread.sleep(2);
      return value;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      state.clear();
      state.add(seen);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      state =
          context
              .getOperatorStateStore()
              .getListState(new ListStateDescriptor<>("next", Long.class));
      if (context.isRestored()) {
        for (long restored : state.get()) {
          seen = restored;
        }
      }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      checkpointCompleted = true;
    }
  }
}
