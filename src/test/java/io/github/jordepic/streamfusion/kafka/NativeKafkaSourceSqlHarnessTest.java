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
  private static final String PROTO_TOPIC = "native-source-proto-it";
  private static final String WATERMARK_TOPIC = "native-source-watermark-it";
  private static final String IDLE_TOPIC = "native-source-idle-it";
  private static final int MESSAGES = 2_000;

  @Test
  void nativeKafkaSourceReadsTopicThroughSql() throws Exception {
    // The native rdkafka source is opt-in (off by default); this test builds and exercises it, so it
    // enables the routing. (A JSON table otherwise takes the shallow decode path.)
    System.setProperty("streamfusion.operator.kafkaSource.enabled", "true");
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
    } finally {
      System.clearProperty("streamfusion.operator.kafkaSource.enabled");
    }
  }

  @Test
  void nativeKafkaProtobufSourceReadsTopicThroughSql() throws Exception {
    // Protobuf is the path only reachable from the native source after this change (the in-Rust decoder
    // is built from the descriptor on the decode thread): produce bare protobuf messages and confirm the
    // native source decodes every one to the right value, not just that it routed.
    System.setProperty("streamfusion.operator.kafkaSource.enabled", "true");
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produceProtobuf(brokers, MESSAGES);

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      // The source decodes the full descriptor (no projection pushed into it yet), so the table must
      // declare every Scalars field in descriptor order; the query then projects i64/text after decode.
      tEnv.executeSql(
          "CREATE TABLE p (i32 INT, i64 BIGINT, flag BOOLEAN, f32 FLOAT, f64 DOUBLE, text STRING,"
              + " si32 INT, si64 BIGINT) WITH ('connector' = 'kafka', 'topic' = '"
              + PROTO_TOPIC
              + "', 'properties.bootstrap.servers' = '"
              + brokers
              + "', 'properties.group.id' = 'native-source-proto-it',"
              + " 'scan.startup.mode' = 'earliest-offset', 'format' = 'protobuf',"
              + " 'protobuf.message-class-name' = 'io.github.jordepic.streamfusion.proto.Scalars')");
      PhysicalPlanScan scan = NativePlanner.install(tEnv);

      Set<Long> ids = new HashSet<>();
      try (CloseableIterator<Row> iterator = tEnv.executeSql("SELECT i64, text FROM p").collect()) {
        while (ids.size() < MESSAGES && iterator.hasNext()) {
          Row row = iterator.next();
          long id = (Long) row.getField("i64");
          assertEquals("row-" + (id - 1), row.getField("text"), "wrong text for i64 " + id);
          ids.add(id);
        }
      }

      assertTrue(scan.substitutions() >= 1, "protobuf Kafka source did not route to native");
      assertEquals(MESSAGES, ids.size(), "expected every produced message exactly once");
      for (long i = 1; i <= MESSAGES; i++) {
        assertTrue(ids.contains(i), "missing i64 " + i);
      }
    } finally {
      System.clearProperty("streamfusion.operator.kafkaSource.enabled");
    }
  }

  @Test
  void watermarkedSourceHoldsWindowUntilEveryPartitionAdvances() throws Exception {
    // Per-partition (per-split) watermark semantics, the property the source must share with Flink's
    // own Kafka source: the combined watermark is the MIN over partitions, so a window can only fire
    // once every partition has advanced past it. A global max-based watermark would fire early and
    // drop the lagging partition's rows. This must run UNBOUNDED — a bounded run's final
    // MAX_WATERMARK closes every window regardless, masking a broken watermark path entirely.
    System.setProperty("streamfusion.operator.kafkaSource.enabled", "true");
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      createTopic(brokers, WATERMARK_TOPIC, 2);
      // Partition 0 advances far past the first window; partition 1 stays silent.
      produceTimed(brokers, WATERMARK_TOPIC, 0, new long[][] {{1, 10, 10_000}, {2, 100, 90_000}});

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      tEnv.executeSql(watermarkedTable("w", brokers, WATERMARK_TOPIC, ""));
      PhysicalPlanScan scan = NativePlanner.install(tEnv);

      java.util.concurrent.BlockingQueue<Row> results =
          new java.util.concurrent.LinkedBlockingQueue<>();
      try (CloseableIterator<Row> iterator = tEnv.executeSql(windowQuery("w")).collect()) {
        Thread drainer =
            new Thread(
                () -> {
                  try {
                    while (iterator.hasNext()) {
                      results.add(iterator.next());
                    }
                  } catch (Exception ignored) {
                    // the iterator is closed underneath us when the test ends
                  }
                });
        drainer.setDaemon(true);
        drainer.start();

        // The [0s, 60s) window must NOT fire: partition 1's watermark is still at MIN, holding the
        // combined watermark back no matter how far partition 0 ran ahead.
        org.junit.jupiter.api.Assertions.assertNull(
            results.poll(8, java.util.concurrent.TimeUnit.SECONDS),
            "window fired despite a silent partition — watermark is not per-partition");

        // Advance partition 1 past window end + delay: min(86s, 86s) >= 60s fires [0s, 60s) with
        // both partitions' rows — including partition 1's 20s row, which a global max-based
        // watermark (already at 86s) would have dropped as late.
        produceTimed(brokers, WATERMARK_TOPIC, 1, new long[][] {{3, 7, 20_000}, {4, 200, 90_000}});
        Row first = results.poll(30, java.util.concurrent.TimeUnit.SECONDS);
        org.junit.jupiter.api.Assertions.assertNotNull(
            first, "window did not fire after every partition advanced");
        assertEquals(epochWindowStart(), first.getField(0));
        assertEquals(17L, first.getField(1), "expected both partitions' in-window prices (10 + 7)");
        assertEquals(2L, first.getField(2));
      }
      assertTrue(scan.substitutions() >= 1, "watermarked Kafka source did not route to native");
    } finally {
      System.clearProperty("streamfusion.operator.kafkaSource.enabled");
    }
  }

  @Test
  void idleTimeoutReleasesSilentPartition() throws Exception {
    // With scan.watermark.idle-timeout, a silent partition is marked idle and stops holding the
    // combined watermark back — the window fires with just the active partition's rows.
    System.setProperty("streamfusion.operator.kafkaSource.enabled", "true");
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      createTopic(brokers, IDLE_TOPIC, 2);
      produceTimed(brokers, IDLE_TOPIC, 0, new long[][] {{1, 10, 10_000}, {2, 100, 90_000}});

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      tEnv.executeSql(
          watermarkedTable("idle", brokers, IDLE_TOPIC, ", 'scan.watermark.idle-timeout' = '2 s'"));
      PhysicalPlanScan scan = NativePlanner.install(tEnv);

      java.util.concurrent.BlockingQueue<Row> results =
          new java.util.concurrent.LinkedBlockingQueue<>();
      try (CloseableIterator<Row> iterator = tEnv.executeSql(windowQuery("idle")).collect()) {
        Thread drainer =
            new Thread(
                () -> {
                  try {
                    while (iterator.hasNext()) {
                      results.add(iterator.next());
                    }
                  } catch (Exception ignored) {
                    // the iterator is closed underneath us when the test ends
                  }
                });
        drainer.setDaemon(true);
        drainer.start();
        Row first = results.poll(30, java.util.concurrent.TimeUnit.SECONDS);
        org.junit.jupiter.api.Assertions.assertNotNull(
            first, "idle timeout did not release the silent partition");
        assertEquals(epochWindowStart(), first.getField(0));
        assertEquals(10L, first.getField(1), "expected only the active partition's in-window price");
        assertEquals(1L, first.getField(2));
      }
      assertTrue(scan.substitutions() >= 1, "watermarked Kafka source did not route to native");
    } finally {
      System.clearProperty("streamfusion.operator.kafkaSource.enabled");
    }
  }

  /** An UNBOUNDED watermarked table in the common Kafka idiom: epoch-millis bigint + computed rowtime. */
  private static String watermarkedTable(String name, String brokers, String topic, String extra) {
    return "CREATE TABLE "
        + name
        + " (id BIGINT, price BIGINT, `dateTime` BIGINT,"
        + " rowtime AS TO_TIMESTAMP_LTZ(`dateTime`, 3),"
        + " WATERMARK FOR rowtime AS rowtime - INTERVAL '4' SECOND"
        + ") WITH ('connector' = 'kafka', 'topic' = '"
        + topic
        + "', 'properties.bootstrap.servers' = '"
        + brokers
        + "', 'properties.group.id' = '"
        + name
        + "-it', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json'"
        + extra
        + ")";
  }

  /** The first window's start: epoch, rendered as the session (system) zone's wall clock — the LTZ
   * rowtime makes TUMBLE's window_start a session-zone TIMESTAMP. */
  private static java.time.LocalDateTime epochWindowStart() {
    return java.time.LocalDateTime.ofInstant(
        java.time.Instant.EPOCH, java.time.ZoneId.systemDefault());
  }

  private static String windowQuery(String table) {
    // Grouping on (window_start, window_end) plans a real window aggregate — emission gated on the
    // watermark passing window end, which is the property under test. (Grouping on window_start
    // alone re-plans as an incremental GroupAggregate that emits changelog updates immediately.)
    return "SELECT window_start, SUM(price), COUNT(*) FROM TABLE(TUMBLE(TABLE "
        + table
        + ", DESCRIPTOR(rowtime), INTERVAL '1' MINUTE)) GROUP BY window_start, window_end";
  }

  private static void createTopic(String brokers, String topic, int partitions) throws Exception {
    Properties props = new Properties();
    props.put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    try (org.apache.kafka.clients.admin.AdminClient admin =
        org.apache.kafka.clients.admin.AdminClient.create(props)) {
      admin
          .createTopics(
              java.util.List.of(
                  new org.apache.kafka.clients.admin.NewTopic(topic, partitions, (short) 1)))
          .all()
          .get();
    }
  }

  /** Produces {@code {id, price, dateTime}} JSON rows into one specific partition. */
  private static void produceTimed(String brokers, String topic, int partition, long[][] rows) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (long[] row : rows) {
        byte[] value =
            String.format("{\"id\": %d, \"price\": %d, \"dateTime\": %d}", row[0], row[1], row[2])
                .getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(topic, partition, null, value));
      }
      producer.flush();
    }
  }

  private static void produceProtobuf(String brokers, int messages) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < messages; i++) {
        byte[] value =
            io.github.jordepic.streamfusion.proto.Scalars.newBuilder()
                .setI64(i + 1L)
                .setText("row-" + i)
                .build()
                .toByteArray();
        producer.send(new ProducerRecord<>(PROTO_TOPIC, 0, null, value));
      }
      producer.flush();
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
