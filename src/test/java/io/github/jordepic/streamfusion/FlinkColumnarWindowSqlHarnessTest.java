package io.github.jordepic.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

/**
 * The fully-columnar windowed pipeline: a native Parquet source feeds a native watermark assigner,
 * a native columnar exchange (keeping the keyed shuffle in Arrow), and a native columnar window —
 * no row transpose anywhere. Results must match the host. The rowtime is {@code TIMESTAMP_LTZ}
 * (what the window matcher admits), and the watermark delay keeps every window open until
 * end-of-input MAX so per-batch watermark assignment (divergences/09) does not affect the result.
 */
class FlinkColumnarWindowSqlHarnessTest {

  @Test
  void globalWindowOverColumnarSourceMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("cwin-global-in");
    writeInput(input);
    // No grouping key: the exchange is SINGLETON (one channel).
    NativeParity.assertParity(
        () -> readEnvironment(input),
        "SELECT window_start, window_end, SUM(v) AS total "
            + "FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void keyedWindowOverColumnarSourceMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("cwin-keyed-in");
    writeInput(input);
    // GROUP BY k: the exchange is a hash shuffle on k, split by key into channels columnar.
    NativeParity.assertParity(
        () -> readEnvironment(input),
        "SELECT k, window_start, window_end, SUM(v) AS total "
            + "FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  private static void writeInput(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE in_write (k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3)) WITH ('connector' = "
            + "'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql(
            "INSERT INTO in_write VALUES "
                + "(1, 10, TO_TIMESTAMP_LTZ(0, 3)), "
                + "(1, 20, TO_TIMESTAMP_LTZ(500, 3)), "
                + "(2, 100, TO_TIMESTAMP_LTZ(500, 3)), "
                + "(1, 30, TO_TIMESTAMP_LTZ(1000, 3)), "
                + "(2, 200, TO_TIMESTAMP_LTZ(1500, 3)), "
                + "(1, 40, TO_TIMESTAMP_LTZ(2500, 3))")
        .await();
  }

  private static TableEnvironment readEnvironment(Path directory) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3), "
            + "WATERMARK FOR rt AS rt - INTERVAL '2' SECOND) WITH ('connector' = 'filesystem', "
            + "'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    return tEnv;
  }
}
