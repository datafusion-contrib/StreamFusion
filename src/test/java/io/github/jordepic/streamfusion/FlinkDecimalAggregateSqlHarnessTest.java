package io.github.jordepic.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

/**
 * Non-windowed {@code GROUP BY} {@code SUM(DECIMAL(p, s))}: the native running aggregate accumulates
 * the unscaled value as an i128 at scale {@code s} and emits {@code DECIMAL(38, s)} — Flink's
 * {@code findSumAggType} — so the result matches the host exactly (a plain decimal add, no rounding).
 * Decimal {@code MIN}/{@code MAX}/{@code AVG} are not modelled natively yet and fall back.
 */
class FlinkDecimalAggregateSqlHarnessTest {

  @Test
  void keyedDecimalSumMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("dec-keyed-in");
    writeInput(input);
    NativeParity.assertParity(
        () -> readEnvironment(input), "SELECT k, SUM(d) AS s FROM t GROUP BY k");
  }

  @Test
  void globalDecimalSumMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("dec-global-in");
    writeInput(input);
    // A global (no GROUP BY) running SUM emits a retract changelog whose intermediate values depend on
    // input arrival order; the native and host source paths need not read the parquet in the same order,
    // so the raw changelog is legitimately incomparable. The net materialized state is the contract —
    // compare the collapsed changelog.
    NativeParity.assertChangelogParity(() -> readEnvironment(input), "SELECT SUM(d) AS s FROM t");
  }

  @Test
  void decimalMinMaxCountMatchesHost() throws Exception {
    // MIN/MAX keep the extreme as an i128 and report DECIMAL(p, s) (the input type, preserved);
    // COUNT over a decimal counts non-null rows. SUM is DECIMAL(38, s).
    Path input = Files.createTempDirectory("dec-minmax-in");
    writeInput(input);
    NativeParity.assertParity(
        () -> readEnvironment(input),
        "SELECT k, MIN(d) AS mn, MAX(d) AS mx, COUNT(d) AS c, SUM(d) AS s FROM t GROUP BY k");
  }

  private static void writeInput(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE in_write (k BIGINT, d DECIMAL(10, 2)) WITH ('connector' = 'filesystem',"
            + " 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql(
            "INSERT INTO in_write VALUES (1, 10.50), (1, 20.25), (2, 5.00), (1, 30.75), (2, 15.10)")
        .await();
  }

  private static TableEnvironment readEnvironment(Path directory) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, d DECIMAL(10, 2)) WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    return tEnv;
  }
}
