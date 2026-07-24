package io.github.jordepic.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

/**
 * The Paimon state backend behind Flink's normal backend toggle: with {@code state.backend.type}
 * set to the StreamFusion factory, a native group aggregate keeps its state in a local Paimon
 * table (read-through probes, barrier commits) and must produce exactly the host's results; host
 * (fallback) operators in the same job run unchanged on the wrapped hashmap backend. MIN/MAX keep
 * multiset state, which the Paimon row codec does not carry — that query exercises the
 * per-operator fallback to memory state under the same backend.
 */
class FlinkPaimonStateBackendSqlHarnessTest {

  // Collapsed-changelog parity: the bounded filesystem source may split the input across part
  // files whose read order differs run to run, so the raw -U/+U interleaving is not stable here.
  // Per-row changelog parity on the Paimon backend is covered deterministically by the operator
  // harness test; this verifies the materialized end state through the whole SQL stack.

  @Test
  void groupBySumOnPaimonBackendMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("paimon-sum-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        () -> paimonEnvironment(input), "SELECT k, SUM(v) AS total, COUNT(*) AS c FROM t GROUP BY k");
  }

  @Test
  void unsupportedAggregatesFallBackToMemoryStateUnderPaimonBackend() throws Exception {
    Path input = Files.createTempDirectory("paimon-minmax-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        () -> paimonEnvironment(input),
        "SELECT k, MIN(v) AS mn, MAX(v) AS mx, SUM(v) AS s FROM t GROUP BY k");
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
            "INSERT INTO in_write VALUES (1, 10), (1, 20), (2, 5), (1, 30), (2, 15), (3, 7)")
        .await();
  }

  private static TableEnvironment paimonEnvironment(Path directory) {
    Configuration configuration = new Configuration();
    configuration.setString(
        "state.backend.type", "io.github.jordepic.streamfusion.state.PaimonStateBackendFactory");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(1);
    env.enableCheckpointing(50);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, v BIGINT) WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    return tEnv;
  }
}
