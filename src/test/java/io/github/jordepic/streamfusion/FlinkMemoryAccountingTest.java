package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.Test;

/**
 * End-to-end managed-memory accounting: the native window aggregate's transformation declares an
 * operator-scope managed-memory weight, the operator reserves the resulting budget from the slot's
 * memory manager, and the native side bounds its window state by it. With the task manager's managed
 * memory squeezed far below the state a high-cardinality window accumulates, the job must fail with
 * the budget exception naming the remedy — the accounted alternative to the container OOM the same
 * state growth would otherwise cause. The release half (reservation back to zero when windows close
 * and at operator close) is pinned natively in the Rust tests; every columnar window parity test
 * exercises it end to end, since accounting is on by default.
 */
class FlinkMemoryAccountingTest {

  private static final int ROWS = 200_000;

  @Test
  void windowStateBeyondManagedMemoryBudgetFailsWithBudgetError() throws Exception {
    Path input = Files.createTempDirectory("mem-budget-in");
    writeHighCardinalityInput(input);

    // A private local environment, NOT getExecutionEnvironment(): the auto-registered
    // SharedFlinkCluster redirects the latter to the shared mini-cluster, whose deployment-sized
    // managed memory would swallow this test's squeezed budget. Constructed directly (with an
    // explicit local target) because LocalStreamEnvironment refuses to exist inside the
    // extension's TestEnvironment context.
    Configuration conf = new Configuration();
    conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("1m"));
    conf.set(DeploymentOptions.TARGET, "local");
    conf.set(DeploymentOptions.ATTACHED, true);
    StreamExecutionEnvironment env = new StreamExecutionEnvironment(conf);
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    // Every row is a distinct group key, and the far-off watermark delay keeps the window open, so
    // the native state must grow well past the 1 MB managed budget before anything could close it.
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3), "
            + "WATERMARK FOR rt AS rt - INTERVAL '1' HOUR) WITH ('connector' = 'filesystem', "
            + "'path' = '"
            + input.toUri()
            + "', 'format' = 'parquet')");
    PhysicalPlanScan scan = NativePlanner.install(tEnv);

    Exception failure =
        assertThrows(
            Exception.class,
            () ->
                drain(
                    tEnv,
                    "SELECT k, window_start, window_end, SUM(v) AS total "
                        + "FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
                        + "GROUP BY k, window_start, window_end"));
    assertTrue(scan.substitutions() > 0, "query did not route to native");
    assertTrue(
        ExceptionUtils.findThrowable(failure, NativeMemoryLimitException.class).isPresent(),
        () -> "expected the managed-memory budget failure, got: " + failure);
  }

  /** One row per distinct key, all inside the same one-second window. */
  private static void writeHighCardinalityInput(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE gen (k BIGINT) WITH ('connector' = 'datagen', "
            + "'number-of-rows' = '"
            + ROWS
            + "', 'fields.k.kind' = 'sequence', 'fields.k.start' = '0', 'fields.k.end' = '"
            + (ROWS - 1)
            + "')");
    tEnv.executeSql(
        "CREATE TABLE in_write (k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3)) WITH ('connector' = "
            + "'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql("INSERT INTO in_write SELECT k, 1, TO_TIMESTAMP_LTZ(0, 3) FROM gen").await();
  }

  private static void drain(StreamTableEnvironment tEnv, String sql) throws Exception {
    try (CloseableIterator<Row> iterator = tEnv.executeSql(sql).collect()) {
      while (iterator.hasNext()) {
        iterator.next();
      }
    }
  }
}
