package io.github.jordepic.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * End-to-end SQL test for the native Fluss log-table source. It writes rows through Flink's stock
 * Fluss connector, reads them back through the native planner path, and asserts the plan contains the
 * native Fluss source so fallback cannot masquerade as coverage.
 *
 * <p>Run with a native library built using {@code -Dnative.cargo.args="build --features fluss"}.
 */
@EnabledIf("nativeFlussFeatureBuilt")
class NativeFlussSourceSqlHarnessTest {

  private static final String CATALOG = "fluss_it_catalog";
  private static final String DATABASE = "fluss";

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

      List<List<Object>> rows = readRows(bootstrapServers, tablePath);

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

  private static List<List<Object>> readRows(String bootstrapServers, String tablePath)
      throws Exception {
    StreamTableEnvironment tEnv = environment(bootstrapServers);
    String sql = "SELECT id, name, score FROM " + tablePath;
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    String plan = tEnv.explainSql(sql);
    assertTrue(
        plan.contains("StreamPhysicalNativeFlussSource") || plan.contains("native-fluss-source"),
        "expected native Fluss source in plan:\n" + plan);

    List<List<Object>> rows = collect(tEnv, sql);
    assertTrue(
        scan.substitutions() > 0,
        "Fluss source did not route to native; reasons=" + scan.fallbackReasons());
    rows.sort(Comparator.comparing(Object::toString));
    return rows;
  }

  private static StreamTableEnvironment environment(String bootstrapServers) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    env.getConfig().enableObjectReuse();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().getConfiguration().setString("execution.runtime-mode", "batch");
    tEnv.executeSql(
        "CREATE CATALOG "
            + CATALOG
            + " WITH ('type' = 'fluss', 'bootstrap.servers' = '"
            + bootstrapServers
            + "')");
    return tEnv;
  }

  private static List<List<Object>> collect(StreamTableEnvironment tEnv, String sql)
      throws Exception {
    List<List<Object>> rows = new ArrayList<>();
    try (CloseableIterator<Row> iterator = tEnv.executeSql(sql).collect()) {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        List<Object> fields = new ArrayList<>(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
          fields.add(row.getField(i));
        }
        rows.add(fields);
      }
    }
    return rows;
  }

  private static void dropTable(String bootstrapServers, String tablePath) {
    try {
      environment(bootstrapServers).executeSql("DROP TABLE IF EXISTS " + tablePath);
    } catch (Exception ignored) {
      // Best-effort cleanup; the embedded cluster is closed immediately afterwards.
    }
  }
}
