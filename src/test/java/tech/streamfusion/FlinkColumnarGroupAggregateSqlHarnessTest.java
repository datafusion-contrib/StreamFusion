package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

/**
 * A non-windowed {@code GROUP BY} fed by a native Parquet source keeps the keyed shuffle columnar (a
 * native exchange splits the Arrow batch by the grouping key) and runs the columnar aggregate, so the
 * input never transposes to rows — the changelog flows Arrow until the host edge. Results must match
 * the host. Single-input, so the emitted changelog is deterministic (unlike a two-input join).
 */
class FlinkColumnarGroupAggregateSqlHarnessTest {

  @Test
  void keyedGroupByOverColumnarSourceMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("cgrp-keyed-in");
    writeInput(input);
    NativeParity.assertParity(
        () -> readEnvironment(input), "SELECT k, SUM(v) AS total FROM t GROUP BY k");
  }

  @Test
  void countAndExtremaOverColumnarSourceMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("cgrp-cme-in");
    writeInput(input);
    NativeParity.assertParity(
        () -> readEnvironment(input),
        "SELECT k, COUNT(*) AS c, MIN(v) AS mn, MAX(v) AS mx, SUM(v) AS s FROM t GROUP BY k");
  }

  @Test
  void globalAggregateOverColumnarSourceMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("cgrp-global-in");
    writeInput(input);
    NativeParity.assertParity(
        () -> readEnvironment(input), "SELECT SUM(v) AS s, COUNT(*) AS c FROM t");
  }

  private static void writeInput(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE in_write (k BIGINT, v BIGINT) WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql(
            "INSERT INTO in_write VALUES (1, 10), (1, 20), (2, 5), (1, 30), (2, 15)")
        .await();
  }

  private static TableEnvironment readEnvironment(Path directory) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, v BIGINT) WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    return tEnv;
  }
}
