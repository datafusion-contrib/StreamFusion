package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import tech.streamfusion.compat.CheckpointFileSink;
import tech.streamfusion.compat.CheckpointTestConfig;
import tech.streamfusion.planner.NativePlanner;

/**
 * End-to-end referee for keyed Kafka tables: each table runs the same bounded read under the native
 * planner and under stock Flink (whose factory assembles the real key/value merge), every row must
 * match, and the native source's own decode counter must account for every record it read — a
 * plan that merely substituted some operator would not. Covers raw, JSON and CSV keys, both
 * {@code value.fields-include} modes, multi-field keys with a prefix, records whose key yields no
 * row, and a keyed table reading connector metadata across partitions, which must also recover
 * natively: it fails over mid-stream and restores a savepoint at another parallelism, and every
 * record must reach an exactly-once sink once, with its own partition and offset. The
 * record-level contract is pinned against Flink's own deserializer by {@code
 * KeyedDecodeParityTest}. Opt-in via {@code SF_BENCHMARK=true} (Docker).
 */
@Tag("streamfusion-kafka")
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeKeyedDecodeSqlHarnessTest {

  private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();

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

  @Test
  void jsonAndCsvKeysMatchFlinkWithConnectorMetadata() throws Exception {
    try (KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
        RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics()) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      Properties admin = new Properties();
      admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
      try (AdminClient client = AdminClient.create(admin)) {
        client
            .createTopics(
                List.of(new NewTopic("keyed-json", 2, (short) 1), new NewTopic("keyed-csv", 2, (short) 1)))
            .all()
            .get();
      }
      List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
      for (int i = 0; i < 400; i++) {
        // A null key yields no JSON key row, so its record emits nothing; a key or value without
        // a field leaves it NULL.
        String key =
            i % 11 == 0
                ? null
                : i % 13 == 0
                    ? String.format("{\"id\": %d}", i)
                    : String.format("{\"region\": \"r%d\", \"id\": %d}", i % 5, i);
        String value =
            i % 17 == 0 ? "{\"name\": null}" : String.format("{\"name\": \"row-%d\"}", i);
        records.add(
            new ProducerRecord<>(
                "keyed-json", i % 2, (long) i * 10, key == null ? null : bytes(key), bytes(value)));
        String csvKey = i % 11 == 0 ? null : String.format("%d|r%d", i, i % 5);
        records.add(
            new ProducerRecord<>(
                "keyed-csv",
                i % 2,
                csvKey == null ? null : bytes(csvKey),
                bytes(String.format("row-%d,%d", i, i * 3))));
      }
      produce(brokers, records);
      Supplier<TableEnvironment> json =
          environment(
              metrics,
              "CREATE TABLE t (k_region STRING, name STRING, k_id BIGINT,"
                  + " part INT METADATA FROM 'partition', off BIGINT METADATA FROM 'offset',"
                  + " ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp') WITH ("
                  + source(brokers, "keyed-json")
                  + " 'key.format' = 'json', 'key.fields' = 'k_id;k_region',"
                  + " 'key.fields-prefix' = 'k_',"
                  + " 'value.format' = 'json', 'value.fields-include' = 'EXCEPT_KEY')");
      Supplier<TableEnvironment> csv =
          environment(
              metrics,
              "CREATE TABLE t (id BIGINT, region STRING, name STRING, amount BIGINT) WITH ("
                  + source(brokers, "keyed-csv")
                  + " 'key.format' = 'csv', 'key.fields' = 'id;region',"
                  + " 'key.csv.field-delimiter' = '|',"
                  + " 'value.format' = 'csv', 'value.fields-include' = 'EXCEPT_KEY')");
      String jsonQuery = "SELECT k_region, name, k_id, part, off, ts FROM t";

      String plan = NativePlanner.explain(json.get(), jsonQuery);
      NativeParity.assertParity(json, jsonQuery);
      NativeParity.assertParity(csv, "SELECT id, region, name, amount FROM t");

      assertTrue(
          plan.contains("NativeKafkaDecode") && plan.contains("keyFormat=[json]"), plan);
      assertEquals(800, metrics.counterValue("nativeKafkaDecodeRecords"));
    }
  }

  @Test
  void keyedMetadataTableRecoversExactlyOnceAcrossFailoverAndRescale(@TempDir Path directory)
      throws Exception {
    FAILED_ONCE.set(false);
    try (KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
        RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics()) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      Properties admin = new Properties();
      admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
      try (AdminClient client = AdminClient.create(admin)) {
        client.createTopics(List.of(new NewTopic("keyed-recovery", 4, (short) 1))).all().get();
      }
      // Record i lands in partition i % 4 at offset i / 4; every ninth has a null key, which the
      // JSON key format turns into no row. The first 4,000 precede the first job, the rest arrive
      // between its savepoint and the restored job.
      List<ProducerRecord<byte[], byte[]>> beforeSavepoint = new ArrayList<>();
      List<ProducerRecord<byte[], byte[]>> afterSavepoint = new ArrayList<>();
      Map<String, Long> expected = new TreeMap<>();
      int expectedBeforeSavepoint = 0;
      for (int i = 0; i < 6_000; i++) {
        byte[] key =
            i % 9 == 0 ? null : bytes(String.format("{\"region\": \"r%d\", \"id\": %d}", i % 5, i));
        (i < 4_000 ? beforeSavepoint : afterSavepoint)
            .add(
                new ProducerRecord<>(
                    "keyed-recovery", i % 4, key, bytes(String.format("{\"name\": \"row-%d\"}", i))));
        if (key != null) {
          expected.put(i + "|r" + i % 5 + "|row-" + i + "|" + i % 4 + "|" + i / 4, 1L);
          expectedBeforeSavepoint += i < 4_000 ? 1 : 0;
        }
      }
      produce(brokers, beforeSavepoint);
      String ddl =
          "CREATE TABLE t (k_id BIGINT, k_region STRING, name STRING,"
              + " part INT METADATA FROM 'partition', off BIGINT METADATA FROM 'offset') WITH ("
              + "'connector' = 'kafka', 'topic' = 'keyed-recovery', 'properties.bootstrap.servers' = '"
              + brokers
              + "', 'properties.group.id' = 'keyed-recovery', 'scan.startup.mode' = 'earliest-offset',"
              + " 'key.format' = 'json', 'key.fields' = 'k_id;k_region',"
              + " 'key.fields-prefix' = 'k_', 'value.format' = 'json',"
              + " 'value.fields-include' = 'EXCEPT_KEY')";
      String query = "SELECT k_id, k_region, name, part, off FROM t";
      Path output = directory.resolve("output");

      // Parallelism 2, failing once mid-stream and restarting from its last checkpoint.
      Configuration first = new Configuration();
      first.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
      first.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
      first.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(10));
      first.set(
          CheckpointingOptions.CHECKPOINTS_DIRECTORY,
          directory.resolve("checkpoints").toUri().toString());
      StreamExecutionEnvironment firstEnvironment = metrics.environment(first, 2, 0);
      firstEnvironment.enableCheckpointing(50);
      StreamTableEnvironment firstTables = StreamTableEnvironment.create(firstEnvironment);
      firstTables.executeSql(ddl);
      NativePlanner.install(firstTables);
      String plan = firstTables.sqlQuery(query).explain();
      DataStream<Row> firstRows = firstTables.toDataStream(firstTables.sqlQuery(query));
      int index = 0;
      for (var transformation : firstRows.getTransformation().getTransitivePredecessors()) {
        transformation.setUid("keyed-recovery-sql-" + index++);
      }
      DataStream<String> firstEncoded =
          firstRows
              .map(new FailOnceAfterCheckpoint())
              .returns(firstRows.getType())
              .uid("keyed-recovery-fail")
              .map(
                  row ->
                      row.getField(0) + "|" + row.getField(1) + "|" + row.getField(2) + "|"
                          + row.getField(3) + "|" + row.getField(4))
              .returns(Types.STRING)
              .uid("keyed-recovery-encode");
      CheckpointFileSink.attach(firstEncoded, output, "keyed-recovery-output");
      JobClient firstJob = firstEnvironment.executeAsync();
      long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
      while (committedLines(output).size() < expectedBeforeSavepoint
          && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
      String savepoint =
          firstJob
              .stopWithSavepoint(
                  false,
                  directory.resolve("savepoints").toUri().toString(),
                  SavepointFormatType.CANONICAL)
              .get(2, TimeUnit.MINUTES);
      produce(brokers, afterSavepoint);

      // The savepoint restored at parallelism 3; a stalled restore fails the run.
      Configuration second = new Configuration();
      second.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
      second.set(
          CheckpointingOptions.CHECKPOINTS_DIRECTORY,
          directory.resolve("checkpoints").toUri().toString());
      CheckpointTestConfig.restore(second, savepoint);
      StreamExecutionEnvironment secondEnvironment = metrics.environment(second, 3, 0);
      secondEnvironment.enableCheckpointing(50);
      StreamTableEnvironment secondTables = StreamTableEnvironment.create(secondEnvironment);
      secondTables.executeSql(ddl);
      NativePlanner.install(secondTables);
      DataStream<Row> secondRows = secondTables.toDataStream(secondTables.sqlQuery(query));
      index = 0;
      for (var transformation : secondRows.getTransformation().getTransitivePredecessors()) {
        transformation.setUid("keyed-recovery-sql-" + index++);
      }
      DataStream<String> secondEncoded =
          secondRows
              .map(new FailOnceAfterCheckpoint())
              .returns(secondRows.getType())
              .uid("keyed-recovery-fail")
              .map(
                  row ->
                      row.getField(0) + "|" + row.getField(1) + "|" + row.getField(2) + "|"
                          + row.getField(3) + "|" + row.getField(4))
              .returns(Types.STRING)
              .uid("keyed-recovery-encode");
      CheckpointFileSink.attach(secondEncoded, output, "keyed-recovery-output");
      JobClient secondJob = secondEnvironment.executeAsync();
      String secondJobId = secondJob.getJobID().toString();
      deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
      while (committedLines(output).size() < expected.size() && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
      // Two more completed checkpoints commit anything a replay could still add.
      long settled = metrics.completedCheckpoints(secondJobId) + 2;
      while (metrics.completedCheckpoints(secondJobId) < settled && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
      secondJob.cancel().get(1, TimeUnit.MINUTES);
      Map<String, Long> committed = new TreeMap<>();
      for (String line : committedLines(output)) {
        committed.merge(line, 1L, Long::sum);
      }

      assertTrue(
          plan.contains("NativeKafkaDecode") && plan.contains("keyFormat=[json]"), plan);
      assertTrue(FAILED_ONCE.get(), "the first job never failed over");
      assertEquals(expected, committed, "a record was lost, duplicated or misplaced in recovery");
      assertTrue(
          metrics.counterValue("nativeKafkaDecodeRecords") >= 6_000,
          "the native decode read " + metrics.counterValue("nativeKafkaDecodeRecords"));
      long restoredDecoded = metrics.counterValue("nativeKafkaDecodeRecords", secondJobId);
      assertTrue(
          restoredDecoded >= afterSavepoint.size(),
          "the restored job decoded " + restoredDecoded + " records natively");
    }
  }

  /**
   * Slows the stream so a checkpoint completes mid-read, then fails once, 100 rows later, so the
   * restart must replay rows the sink had written but not committed.
   */
  private static final class FailOnceAfterCheckpoint extends RichMapFunction<Row, Row>
      implements CheckpointListener {
    private boolean checkpointed;
    private long seenSinceCheckpoint;

    @Override
    public Row map(Row row) {
      if (checkpointed
          && ++seenSinceCheckpoint >= 100
          && FAILED_ONCE.compareAndSet(false, true)) {
        throw new IllegalStateException("intentional failure after a completed checkpoint");
      }
      LockSupport.parkNanos(500_000);
      return row;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      checkpointed = true;
    }
  }

  private static List<String> committedLines(Path output) throws IOException {
    List<String> lines = new ArrayList<>();
    if (Files.exists(output)) {
      Files.walkFileTree(
          output,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
                throws IOException {
              // Hidden files are the sink's in-progress and pending parts, which a running job
              // renames away; committed parts are renamed into place once and never change.
              if (!file.getFileName().toString().startsWith(".")) {
                lines.addAll(Files.readAllLines(file));
              }
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException failure) {
              return FileVisitResult.CONTINUE;
            }
          });
    }
    return lines;
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
