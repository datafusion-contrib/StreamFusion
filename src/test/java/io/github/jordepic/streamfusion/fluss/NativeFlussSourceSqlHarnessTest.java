package io.github.jordepic.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * End-to-end SQL test for the native Fluss log-table source. It writes rows through Flink's stock
 * Fluss connector, reads them back through the native planner path, and asserts the optimizer
 * substituted the native Fluss source so fallback cannot masquerade as coverage.
 *
 * <p>Run with a native library built using {@code -Dnative.cargo.args="build --features fluss"}.
 */
@EnabledIf("nativeFlussFeatureBuilt")
class NativeFlussSourceSqlHarnessTest {

  private static final String CATALOG = "fluss_it_catalog";
  private static final String DATABASE = "fluss";
  private static volatile CountDownLatch firstRowCollected;
  private static volatile CountDownLatch rowsCollected;
  private static volatile List<List<Object>> collectedRows;

  @Test
  void nativeFlussSourceReadsLogTableThroughSql() throws Exception {
    FlussClusterExtension cluster = FlussClusterExtension.builder().setNumOfTabletServers(1).build();
    String bootstrapServers = null;
    String tablePath = null;
    try {
      cluster.start();
      bootstrapServers = cluster.getBootstrapServers();
      tablePath = CATALOG + "." + DATABASE + ".native_fluss_source_it_" + System.nanoTime();
      writeRows(bootstrapServers, tablePath);

      List<List<Object>> rows =
          readRows(bootstrapServers, "SELECT id, name, score FROM " + tablePath, 3);

      assertEquals(
          List.of(List.of(1L, "alice", 10), List.of(2L, "bob", 20), List.of(3L, "carol", 30)),
          rows);
    } finally {
      if (bootstrapServers != null && tablePath != null) {
        dropTable(bootstrapServers, tablePath);
      }
      cluster.close();
    }
  }

  @Test
  void nativeFlussSourceReadsProjectedColumnsThroughSql() throws Exception {
    FlussClusterExtension cluster = FlussClusterExtension.builder().setNumOfTabletServers(1).build();
    String bootstrapServers = null;
    String tablePath = null;
    try {
      cluster.start();
      bootstrapServers = cluster.getBootstrapServers();
      tablePath = CATALOG + "." + DATABASE + ".native_fluss_projection_it_" + System.nanoTime();
      writeRows(bootstrapServers, tablePath);

      List<List<Object>> rows = readRows(bootstrapServers, "SELECT name FROM " + tablePath, 3);

      assertEquals(List.of(List.of("alice"), List.of("bob"), List.of("carol")), rows);
    } finally {
      if (bootstrapServers != null && tablePath != null) {
        dropTable(bootstrapServers, tablePath);
      }
      cluster.close();
    }
  }

  @Test
  void nativeFlussSourceReadsStaticPartitionedLogTableThroughSql() throws Exception {
    FlussClusterExtension cluster = FlussClusterExtension.builder().setNumOfTabletServers(1).build();
    String bootstrapServers = null;
    String tablePath = null;
    try {
      cluster.start();
      bootstrapServers = cluster.getBootstrapServers();
      tablePath = CATALOG + "." + DATABASE + ".native_fluss_partitioned_it_" + System.nanoTime();
      writePartitionedRows(bootstrapServers, tablePath);

      List<List<Object>> rows =
          readRows(bootstrapServers, "SELECT id, region, score FROM " + tablePath, 3);

      assertEquals(
          List.of(List.of(1L, "US", 10), List.of(2L, "EU", 20), List.of(3L, "US", 30)),
          rows);
    } finally {
      if (bootstrapServers != null && tablePath != null) {
        dropTable(bootstrapServers, tablePath);
      }
      cluster.close();
    }
  }

  @Test
  void nativeFlussSourceDiscoversDynamicPartitionedLogTableThroughSql() throws Exception {
    FlussClusterExtension cluster = FlussClusterExtension.builder().setNumOfTabletServers(1).build();
    String bootstrapServers = null;
    String tablePath = null;
    try {
      cluster.start();
      bootstrapServers = cluster.getBootstrapServers();
      tablePath =
          CATALOG + "." + DATABASE + ".native_fluss_dynamic_partitioned_it_" + System.nanoTime();
      createPartitionedTable(bootstrapServers, tablePath, "100 ms");
      addPartitionedRow(bootstrapServers, tablePath, "US", 1, 10);

      String finalBootstrapServers = bootstrapServers;
      String finalTablePath = tablePath;
      List<List<Object>> rows =
          readRows(
              bootstrapServers,
              "SELECT id, region, score FROM " + tablePath,
              2,
              () -> addPartitionedRow(finalBootstrapServers, finalTablePath, "EU", 2, 20));

      assertEquals(List.of(List.of(1L, "US", 10), List.of(2L, "EU", 20)), rows);
    } finally {
      if (bootstrapServers != null && tablePath != null) {
        dropTable(bootstrapServers, tablePath);
      }
      cluster.close();
    }
  }

  @Test
  void nativeFlussSourceAcknowledgesDroppedDynamicPartitionThroughSql() throws Exception {
    FlussClusterExtension cluster = FlussClusterExtension.builder().setNumOfTabletServers(1).build();
    String bootstrapServers = null;
    String tablePath = null;
    try {
      cluster.start();
      bootstrapServers = cluster.getBootstrapServers();
      tablePath =
          CATALOG + "." + DATABASE + ".native_fluss_dropped_partition_it_" + System.nanoTime();
      createPartitionedTable(bootstrapServers, tablePath, "100 ms");
      addPartitionedRow(bootstrapServers, tablePath, "US", 1, 10);

      String finalBootstrapServers = bootstrapServers;
      String finalTablePath = tablePath;
      List<List<Object>> rows =
          readRowsAfterFirstRow(
              bootstrapServers,
              "SELECT id, region, score FROM " + tablePath,
              2,
              () -> {
                dropPartition(finalBootstrapServers, finalTablePath, "US");
                addPartitionedRow(finalBootstrapServers, finalTablePath, "EU", 2, 20);
              });

      assertEquals(List.of(List.of(1L, "US", 10), List.of(2L, "EU", 20)), rows);
    } finally {
      if (bootstrapServers != null && tablePath != null) {
        dropTable(bootstrapServers, tablePath);
      }
      cluster.close();
    }
  }

  @Test
  void nativeFlussSourceReadsMultiBucketLogTableThroughSql() throws Exception {
    FlussClusterExtension cluster = FlussClusterExtension.builder().setNumOfTabletServers(1).build();
    String bootstrapServers = null;
    String tablePath = null;
    try {
      cluster.start();
      bootstrapServers = cluster.getBootstrapServers();
      tablePath = CATALOG + "." + DATABASE + ".native_fluss_multi_bucket_it_" + System.nanoTime();
      writeMultiBucketRows(bootstrapServers, tablePath);

      List<List<Object>> rows =
          readRows(bootstrapServers, "SELECT id, name, score FROM " + tablePath, 9);

      assertEquals(
          List.of(
              List.of(1L, "alice", 10),
              List.of(2L, "bob", 20),
              List.of(3L, "carol", 30),
              List.of(4L, "dave", 40),
              List.of(5L, "erin", 50),
              List.of(6L, "frank", 60),
              List.of(7L, "grace", 70),
              List.of(8L, "heidi", 80),
              List.of(9L, "ivan", 90)),
          rows);
    } finally {
      if (bootstrapServers != null && tablePath != null) {
        dropTable(bootstrapServers, tablePath);
      }
      cluster.close();
    }
  }

  @Test
  void nativeFlussSplitReaderReportsRemovedSplitsAsFinished() throws Exception {
    FlussClusterExtension cluster = FlussClusterExtension.builder().setNumOfTabletServers(1).build();
    String bootstrapServers = null;
    String tablePath = null;
    try {
      cluster.start();
      bootstrapServers = cluster.getBootstrapServers();
      String tableName = "native_fluss_split_removal_it_" + System.nanoTime();
      tablePath = CATALOG + "." + DATABASE + "." + tableName;
      createPartitionedTable(bootstrapServers, tablePath, "100 ms");
      addPartitionedRow(bootstrapServers, tablePath, "US", 1, 10);
      addPartitionedRow(bootstrapServers, tablePath, "EU", 2, 20);

      Map<String, SourceSplitBase> splitsByPartition =
          logSplitsByPartition(cluster.getClientConfig(), tableName);
      SourceSplitBase usSplit = splitsByPartition.get("US");
      SourceSplitBase euSplit = splitsByPartition.get("EU");

      NativeFlussSplitReader reader =
          new NativeFlussSplitReader(
              new String[] {"bootstrap_servers"},
              new String[] {bootstrapServers},
              DATABASE,
              tableName,
              new int[0],
              100L);
      try {
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(usSplit, euSplit)));
        Set<String> finishedSplitIds = new HashSet<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        int rows = 0;
        while (rows < 2) {
          if (System.nanoTime() > deadline) {
            throw new TimeoutException("timed out waiting for rows from both partition splits");
          }
          rows += drainBatches(reader.fetch(), finishedSplitIds);
        }

        dropPartition(bootstrapServers, tablePath, "US");
        reader.handleSplitsChanges(new SplitsRemoval<>(List.of(usSplit)));
        while (!finishedSplitIds.contains(usSplit.splitId()) && System.nanoTime() < deadline) {
          drainBatches(reader.fetch(), finishedSplitIds);
        }

        assertTrue(
            finishedSplitIds.contains(usSplit.splitId()),
            "removed split was never reported finished; finished=" + finishedSplitIds);
        assertFalse(finishedSplitIds.contains(euSplit.splitId()));
      } finally {
        reader.close();
      }
    } finally {
      if (bootstrapServers != null && tablePath != null) {
        dropTable(bootstrapServers, tablePath);
      }
      cluster.close();
    }
  }

  static boolean nativeFlussFeatureBuilt() {
    return Native.flussFeatureBuilt();
  }

  private static Map<String, SourceSplitBase> logSplitsByPartition(
      org.apache.fluss.config.Configuration clientConfig, String tableName) throws Exception {
    TablePath flussTablePath = TablePath.of(DATABASE, tableName);
    try (Connection connection = ConnectionFactory.createConnection(clientConfig)) {
      Admin admin = connection.getAdmin();
      long tableId = admin.getTableInfo(flussTablePath).get().getTableId();
      Map<String, SourceSplitBase> splits = new HashMap<>();
      for (PartitionInfo partition : admin.listPartitionInfos(flussTablePath).get()) {
        TableBucket bucket = new TableBucket(tableId, partition.getPartitionId(), 0);
        splits.put(
            partition.getPartitionName(), new LogSplit(bucket, partition.getPartitionName(), 0L));
      }
      return splits;
    }
  }

  private static int drainBatches(
      RecordsWithSplitIds<NativeFlussRecord> records, Set<String> finishedSplitIds) {
    int rows = 0;
    String splitId = records.nextSplit();
    while (splitId != null) {
      NativeFlussRecord record = records.nextRecordFromSplit();
      while (record != null) {
        try (VectorSchemaRoot root = record.batch().root()) {
          rows += root.getRowCount();
        }
        record = records.nextRecordFromSplit();
      }
      splitId = records.nextSplit();
    }
    finishedSplitIds.addAll(records.finishedSplits());
    return rows;
  }

  private static void writeRows(String bootstrapServers, String tablePath) throws Exception {
    StreamTableEnvironment tEnv = environment(bootstrapServers);
    tEnv.executeSql("DROP TABLE IF EXISTS " + tablePath);
    tEnv.executeSql(
        "CREATE TABLE "
            + tablePath
            + " (id BIGINT, name STRING, score INT) WITH ('bucket.num' = '1')");
    tEnv.executeSql(
            "INSERT INTO "
                + tablePath
                + " VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'carol', 30)")
        .await();
  }

  private static void writeMultiBucketRows(String bootstrapServers, String tablePath)
      throws Exception {
    StreamTableEnvironment tEnv = environment(bootstrapServers);
    tEnv.executeSql("DROP TABLE IF EXISTS " + tablePath);
    tEnv.executeSql(
        "CREATE TABLE "
            + tablePath
            + " (id BIGINT, name STRING, score INT)"
            + " WITH ('bucket.num' = '3', 'bucket.key' = 'name')");
    tEnv.executeSql(
            "INSERT INTO "
                + tablePath
                + " VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'carol', 30),"
                + " (4, 'dave', 40), (5, 'erin', 50), (6, 'frank', 60),"
                + " (7, 'grace', 70), (8, 'heidi', 80), (9, 'ivan', 90)")
        .await();
  }

  private static void writePartitionedRows(String bootstrapServers, String tablePath)
      throws Exception {
    createPartitionedTable(bootstrapServers, tablePath, "0 ms");
    StreamTableEnvironment tEnv = environment(bootstrapServers);
    tEnv.executeSql("ALTER TABLE " + tablePath + " ADD PARTITION (region = 'US')");
    tEnv.executeSql("ALTER TABLE " + tablePath + " ADD PARTITION (region = 'EU')");
    tEnv.executeSql(
            "INSERT INTO "
                + tablePath
                + " VALUES (1, 'US', 10), (2, 'EU', 20), (3, 'US', 30)")
        .await();
  }

  private static void createPartitionedTable(
      String bootstrapServers, String tablePath, String discoveryInterval) {
    StreamTableEnvironment tEnv = environment(bootstrapServers);
    tEnv.executeSql("DROP TABLE IF EXISTS " + tablePath);
    tEnv.executeSql(
        "CREATE TABLE "
            + tablePath
            + " (id BIGINT, region STRING, score INT)"
            + " PARTITIONED BY (region)"
            + " WITH ('bucket.num' = '1', 'scan.partition.discovery.interval' = '"
            + discoveryInterval
            + "')");
  }

  private static void addPartitionedRow(
      String bootstrapServers, String tablePath, String region, int id, int score) throws Exception {
    StreamTableEnvironment tEnv = environment(bootstrapServers);
    tEnv.executeSql("ALTER TABLE " + tablePath + " ADD PARTITION (region = '" + region + "')");
    tEnv.executeSql(
            "INSERT INTO "
                + tablePath
                + " VALUES ("
                + id
                + ", '"
                + region
                + "', "
                + score
                + ")")
        .await();
  }

  private static void dropPartition(String bootstrapServers, String tablePath, String region) {
    environment(bootstrapServers)
        .executeSql("ALTER TABLE " + tablePath + " DROP PARTITION (region = '" + region + "')");
  }

  private static List<List<Object>> readRows(String bootstrapServers, String sql, int targetRows)
      throws Exception {
    return readRows(bootstrapServers, sql, targetRows, () -> {});
  }

  private static List<List<Object>> readRows(
      String bootstrapServers, String sql, int targetRows, ThrowingRunnable afterStart)
      throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tEnv = environment(env, bootstrapServers);
    PhysicalPlanScan scan = NativePlanner.install(tEnv);

    rowsCollected = new CountDownLatch(1);
    collectedRows = Collections.synchronizedList(new ArrayList<>());
    Table table = tEnv.sqlQuery(sql);
    tEnv.toDataStream(table).addSink(new CollectingSink(targetRows)).name("collect-native-fluss-it");
    JobClient job = env.executeAsync("native-fluss-source-sql-harness");
    try {
      assertTrue(
          scan.substitutions() > 0,
          "Fluss source did not route to native; reasons=" + scan.fallbackReasons());
      afterStart.run();
      if (!rowsCollected.await(30, TimeUnit.SECONDS)) {
        throw new TimeoutException("timed out waiting for native Fluss rows: " + collectedRows);
      }
      List<List<Object>> rows;
      synchronized (collectedRows) {
        rows = new ArrayList<>(collectedRows);
      }
      rows.sort(Comparator.comparing(Object::toString));
      return rows;
    } finally {
      job.cancel().get();
      rowsCollected = null;
      collectedRows = null;
    }
  }

  private static List<List<Object>> readRowsAfterFirstRow(
      String bootstrapServers, String sql, int targetRows, ThrowingRunnable afterFirstRow)
      throws Exception {
    firstRowCollected = new CountDownLatch(1);
    try {
      return readRows(
          bootstrapServers,
          sql,
          targetRows,
          () -> {
            if (!firstRowCollected.await(30, TimeUnit.SECONDS)) {
              throw new TimeoutException(
                  "timed out waiting for first native Fluss row: " + collectedRows);
            }
            afterFirstRow.run();
          });
    } finally {
      firstRowCollected = null;
    }
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private static StreamTableEnvironment environment(String bootstrapServers) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    return environment(env, bootstrapServers);
  }

  private static StreamTableEnvironment environment(
      StreamExecutionEnvironment env, String bootstrapServers) {
    env.setParallelism(1);
    env.getConfig().enableObjectReuse();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().getConfiguration().setString("execution.runtime-mode", "streaming");
    tEnv.executeSql(
        "CREATE CATALOG "
            + CATALOG
            + " WITH ('type' = 'fluss', 'bootstrap.servers' = '"
            + bootstrapServers
            + "')");
    return tEnv;
  }

  private static void dropTable(String bootstrapServers, String tablePath) {
    try {
      environment(bootstrapServers).executeSql("DROP TABLE IF EXISTS " + tablePath);
    } catch (Exception ignored) {
      // Best-effort cleanup; the embedded cluster is closed immediately afterwards.
    }
  }

  private static final class CollectingSink extends RichSinkFunction<Row> {
    private final int targetRows;

    private CollectingSink(int targetRows) {
      this.targetRows = targetRows;
    }

    @Override
    public void invoke(Row value, Context context) {
      List<List<Object>> rows = collectedRows;
      CountDownLatch latch = rowsCollected;
      CountDownLatch firstLatch = firstRowCollected;
      if (rows == null || latch == null) {
        return;
      }
      synchronized (rows) {
        if (rows.size() < targetRows) {
          List<Object> fields = new ArrayList<>(value.getArity());
          for (int i = 0; i < value.getArity(); i++) {
            fields.add(value.getField(i));
          }
          rows.add(fields);
          if (rows.size() == 1 && firstLatch != null) {
            firstLatch.countDown();
          }
        }
        if (rows.size() >= targetRows) {
          latch.countDown();
        }
      }
    }
  }
}
