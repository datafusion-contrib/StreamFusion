package io.github.jordepic.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
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

  static boolean nativeFlussFeatureBuilt() {
    return Native.flussFeatureBuilt();
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

  private static void writePartitionedRows(String bootstrapServers, String tablePath)
      throws Exception {
    StreamTableEnvironment tEnv = environment(bootstrapServers);
    tEnv.executeSql("DROP TABLE IF EXISTS " + tablePath);
    tEnv.executeSql(
        "CREATE TABLE "
            + tablePath
            + " (id BIGINT, region STRING, score INT)"
            + " PARTITIONED BY (region)"
            + " WITH ('bucket.num' = '1', 'scan.partition.discovery.interval' = '0 ms')");
    tEnv.executeSql("ALTER TABLE " + tablePath + " ADD PARTITION (region = 'US')");
    tEnv.executeSql("ALTER TABLE " + tablePath + " ADD PARTITION (region = 'EU')");
    tEnv.executeSql(
            "INSERT INTO "
                + tablePath
                + " VALUES (1, 'US', 10), (2, 'EU', 20), (3, 'US', 30)")
        .await();
  }

  private static List<List<Object>> readRows(String bootstrapServers, String sql, int targetRows)
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
        }
        if (rows.size() >= targetRows) {
          latch.countDown();
        }
      }
    }
  }
}
