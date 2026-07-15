package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Broker-level proof that native values remain governed by Flink's exactly-once Kafka writer. */
@Tag("streamfusion-kafka")
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeKafkaSinkIntegrationTest {

  private static final int ROWS = 100;
  private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();

  @Test
  void keepsNativeKafkaInputColumnarThroughExactlyOnceOutput() throws Exception {
    System.setProperty("streamfusion.operator.kafkaSource.enabled", "true");
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withEnv("KAFKA_TRANSACTION_MAX_TIMEOUT_MS", "7200000")) {
      kafka.start();
      int rows = 100;
      String inputTopic = "native-source-sink-input-" + UUID.randomUUID();
      String outputTopic = "native-source-sink-output-" + UUID.randomUUID();
      produceJson(kafka.getBootstrapServers(), inputTopic, rows);

      StreamExecutionEnvironment environment =
          StreamExecutionEnvironment.getExecutionEnvironment();
      environment.setParallelism(1);
      environment.enableCheckpointing(50);
      StreamTableEnvironment table = StreamTableEnvironment.create(environment);
      table.executeSql(
          "CREATE TABLE input (id BIGINT, name STRING) WITH ("
              + "'connector' = 'kafka', 'topic' = '"
              + inputTopic
              + "', 'properties.bootstrap.servers' = '"
              + kafka.getBootstrapServers()
              + "', 'properties.group.id' = 'native-source-sink', "
              + "'scan.startup.mode' = 'earliest-offset', "
              + "'scan.bounded.mode' = 'latest-offset', 'format' = 'json')");
      table.executeSql(
          "CREATE TABLE output (id BIGINT, name STRING) WITH ("
              + "'connector' = 'kafka', 'topic' = '"
              + outputTopic
              + "', 'properties.bootstrap.servers' = '"
              + kafka.getBootstrapServers()
              + "', 'format' = 'json', "
              + "'sink.delivery-guarantee' = 'exactly-once', "
              + "'sink.transactional-id-prefix' = 'native-source-sink')");
      PhysicalPlanScan scan = NativePlanner.install(table);
      String statement = "INSERT INTO output SELECT * FROM input";
      String plan = table.explainSql(statement);

      table.executeSql(statement).await();

      assertTrue(scan.substitutions() >= 2, scan::explainSummary);
      assertFalse(plan.contains("RowDataToArrow"), plan);
      assertFalse(plan.contains("ArrowToRowData"), plan);
      List<String> committed =
          consumeCommitted(kafka.getBootstrapServers(), outputTopic, rows);
      assertEquals(rows, committed.size());
      assertEquals(rows, new HashSet<>(committed).size());
    } finally {
      System.clearProperty("streamfusion.operator.kafkaSource.enabled");
    }
  }

  @Test
  void restoresNativeKafkaInputAcrossExactlyOnceFailover() throws Exception {
    System.setProperty("streamfusion.operator.kafkaSource.enabled", "true");
    FAILED_ONCE.set(false);
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withEnv("KAFKA_TRANSACTION_MAX_TIMEOUT_MS", "7200000")) {
      kafka.start();
      int rows = 10_000;
      String inputTopic = "native-source-failover-input-" + UUID.randomUUID();
      String outputTopic = "native-source-failover-output-" + UUID.randomUUID();
      produceJson(kafka.getBootstrapServers(), inputTopic, rows);

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
      table.executeSql(
          "CREATE TABLE input (id BIGINT, name STRING) WITH ("
              + "'connector' = 'kafka', 'topic' = '"
              + inputTopic
              + "', 'properties.bootstrap.servers' = '"
              + kafka.getBootstrapServers()
              + "', 'properties.group.id' = 'native-source-failover', "
              + "'scan.startup.mode' = 'earliest-offset', "
              + "'scan.bounded.mode' = 'latest-offset', 'format' = 'json')");
      PhysicalPlanScan scan = NativePlanner.install(table);
      DataStream<Row> recovered =
          table
              .toDataStream(table.from("input"))
              .map(new CheckpointFailingMap(250_000))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"id", "name"}, Types.LONG, Types.STRING));
      table.createTemporaryView(
          "recovered",
          recovered,
          Schema.newBuilder()
              .column("id", DataTypes.BIGINT())
              .column("name", DataTypes.STRING())
              .build());
      table.executeSql(
          "CREATE TABLE output (id BIGINT, name STRING) WITH ("
              + "'connector' = 'kafka', 'topic' = '"
              + outputTopic
              + "', 'properties.bootstrap.servers' = '"
              + kafka.getBootstrapServers()
              + "', 'format' = 'json', "
              + "'sink.delivery-guarantee' = 'exactly-once', "
              + "'sink.transactional-id-prefix' = 'native-source-failover')");

      table.executeSql("INSERT INTO output SELECT * FROM recovered").await();

      assertTrue(FAILED_ONCE.get(), "native source never exercised recovery");
      assertTrue(scan.substitutions() >= 2, scan::explainSummary);
      List<String> committed =
          consumeCommitted(kafka.getBootstrapServers(), outputTopic, rows);
      assertEquals(rows, committed.size(), "replayed source records must remain invisible");
      assertEquals(rows, new HashSet<>(committed).size(), "restored source records must be unique");
    } finally {
      System.clearProperty("streamfusion.operator.kafkaSource.enabled");
    }
  }

  @Test
  void restoresNativeKafkaInputIntoCheckpointedFilesystemSink(@TempDir Path outputDirectory)
      throws Exception {
    System.setProperty("streamfusion.operator.kafkaSource.enabled", "true");
    FAILED_ONCE.set(false);
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      int rows = 10_000;
      String inputTopic = "native-source-filesystem-input-" + UUID.randomUUID();
      produceJson(kafka.getBootstrapServers(), inputTopic, rows);

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
      table.executeSql(
          "CREATE TABLE input (id BIGINT, name STRING) WITH ("
              + "'connector' = 'kafka', 'topic' = '"
              + inputTopic
              + "', 'properties.bootstrap.servers' = '"
              + kafka.getBootstrapServers()
              + "', 'properties.group.id' = 'native-source-filesystem-failover', "
              + "'scan.startup.mode' = 'earliest-offset', "
              + "'scan.bounded.mode' = 'latest-offset', 'format' = 'json')");
      PhysicalPlanScan scan = NativePlanner.install(table);
      DataStream<Row> recovered =
          table
              .toDataStream(table.from("input"))
              .map(new CheckpointFailingMap(250_000))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"id", "name"}, Types.LONG, Types.STRING));
      table.createTemporaryView(
          "recovered",
          recovered,
          Schema.newBuilder()
              .column("id", DataTypes.BIGINT())
              .column("name", DataTypes.STRING())
              .build());
      table.executeSql(
          "CREATE TABLE output (id BIGINT, name STRING) WITH ("
              + "'connector' = 'filesystem', 'path' = '"
              + outputDirectory.toUri()
              + "', 'format' = 'json')");

      table.executeSql("INSERT INTO output SELECT * FROM recovered").await();

      assertTrue(FAILED_ONCE.get(), "native source never exercised recovery");
      assertTrue(scan.substitutions() > 0, scan::explainSummary);
      List<String> output = new ArrayList<>();
      try (java.util.stream.Stream<Path> paths = Files.walk(outputDirectory)) {
        for (Path path :
            paths
                .filter(Files::isRegularFile)
                .filter(file -> !file.getFileName().toString().startsWith("."))
                .toList()) {
          output.addAll(Files.readAllLines(path));
        }
      }
      assertEquals(rows, output.size(), "checkpointed sink must not retain replayed records");
      assertEquals(rows, new HashSet<>(output).size(), "restored source records must be unique");
    } finally {
      System.clearProperty("streamfusion.operator.kafkaSource.enabled");
    }
  }

  @Test
  void publishesWithNonTransactionalDeliveryGuarantees() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      for (String guarantee : List.of("none", "at-least-once")) {
        int rows = 50;
        String topic = "native-sink-" + guarantee + "-" + UUID.randomUUID();
        StreamExecutionEnvironment environment =
            StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment table = StreamTableEnvironment.create(environment);
        List<Row> input = new ArrayList<>(rows);
        for (long id = 0; id < rows; id++) {
          input.add(Row.of(id, "row-" + id));
        }
        table.createTemporaryView(
            "src",
            environment.fromData(
                Types.ROW_NAMED(new String[] {"id", "name"}, Types.LONG, Types.STRING),
                input.toArray(Row[]::new)),
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
                + "', 'format' = 'json', 'sink.delivery-guarantee' = '"
                + guarantee
                + "')");
        PhysicalPlanScan scan = NativePlanner.install(table);

        table.executeSql("INSERT INTO output SELECT * FROM src").await();

        assertTrue(scan.substitutions() > 0, scan::explainSummary);
        assertEquals(
            rows,
            consume(kafka.getBootstrapServers(), topic, rows, "read_uncommitted").size());
      }
    }
  }

  @Test
  void publishesUpdatingResultsAsExactlyOnceUpserts() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withEnv("KAFKA_TRANSACTION_MAX_TIMEOUT_MS", "7200000")) {
      kafka.start();
      String topic = "native-upsert-sink-" + UUID.randomUUID();
      StreamExecutionEnvironment environment =
          StreamExecutionEnvironment.getExecutionEnvironment();
      environment.setParallelism(1);
      environment.enableCheckpointing(50);
      StreamTableEnvironment table = StreamTableEnvironment.create(environment);
      Row[] input = new Row[100];
      for (int index = 0; index < input.length; index++) {
        input[index] = Row.of((long) (index % 10));
      }
      table.createTemporaryView(
          "src",
          environment.fromData(
              Types.ROW_NAMED(new String[] {"id"}, Types.LONG), input),
          Schema.newBuilder().column("id", DataTypes.BIGINT()).build());
      table.executeSql(
          "CREATE TABLE output (id BIGINT, total BIGINT, PRIMARY KEY (id) NOT ENFORCED) WITH ("
              + "'connector' = 'upsert-kafka', 'topic' = '"
              + topic
              + "', 'properties.bootstrap.servers' = '"
              + kafka.getBootstrapServers()
              + "', 'key.format' = 'json', 'value.format' = 'json', "
              + "'sink.delivery-guarantee' = 'exactly-once', "
              + "'sink.transactional-id-prefix' = 'native-upsert-sink')");
      PhysicalPlanScan scan = NativePlanner.install(table);

      table.executeSql("INSERT INTO output SELECT id, COUNT(*) FROM src GROUP BY id").await();

      assertTrue(scan.substitutions() > 0, scan::explainSummary);
      Map<String, String> state = new HashMap<>();
      for (ConsumerRecord<byte[], byte[]> record :
          consumeAllCommitted(kafka.getBootstrapServers(), topic)) {
        String key = new String(record.key(), StandardCharsets.UTF_8);
        if (record.value() == null) {
          state.remove(key);
        } else {
          state.put(key, new String(record.value(), StandardCharsets.UTF_8));
        }
      }
      assertEquals(10, state.size());
      for (long id = 0; id < 10; id++) {
        assertEquals("{\"id\":" + id + ",\"total\":10}", state.get("{\"id\":" + id + "}"));
      }
    }
  }

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
    return consume(brokers, topic, expected, "read_committed");
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
            ("{\"id\":" + id + ",\"name\":\"row-" + id + "\"}")
                .getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(topic, value));
      }
      producer.flush();
    }
  }

  private static List<String> consume(
      String brokers, String topic, int expected, String isolationLevel) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
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

  private static List<ConsumerRecord<byte[], byte[]>> consumeAllCommitted(
      String brokers, String topic) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
      consumer.subscribe(List.of(topic));
      int idlePolls = 0;
      while (idlePolls < 20) {
        int count = 0;
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(250))) {
          records.add(record);
          count++;
        }
        idlePolls = count == 0 ? idlePolls + 1 : 0;
      }
    }
    return records;
  }

  private static final class CheckpointFailingMap extends RichMapFunction<Row, Row>
      implements CheckpointedFunction, CheckpointListener {

    private final long delayNanos;
    private volatile boolean checkpointCompleted;
    private transient ListState<Long> state;
    private long seen;

    private CheckpointFailingMap() {
      this(2_000_000);
    }

    private CheckpointFailingMap(long delayNanos) {
      this.delayNanos = delayNanos;
    }

    @Override
    public Row map(Row value) throws Exception {
      seen++;
      if (checkpointCompleted && seen >= 100 && FAILED_ONCE.compareAndSet(false, true)) {
        throw new RuntimeException("intentional post-checkpoint failure");
      }
      java.util.concurrent.locks.LockSupport.parkNanos(delayNanos);
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
