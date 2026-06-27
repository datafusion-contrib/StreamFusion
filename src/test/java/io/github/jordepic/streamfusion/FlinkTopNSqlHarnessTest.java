package io.github.jordepic.streamfusion;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

/**
 * Append-only streaming Top-N ({@code ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) <= N}, rank
 * number not projected) matches the host: the native ranker keeps the top-N per partition and emits
 * the same insert/delete changelog as the bounded set changes. The parity harness compares the full
 * set of emitted change rows, so a differing changelog fails.
 */
class FlinkTopNSqlHarnessTest {

  @Test
  void topNAscendingMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkTopNSqlHarnessTest::environment,
        "SELECT k, v FROM (SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn "
            + "FROM src) WHERE rn <= 2");
  }

  @Test
  void topNDescendingMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkTopNSqlHarnessTest::environment,
        "SELECT k, v FROM (SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v DESC) AS rn "
            + "FROM src) WHERE rn <= 1");
  }

  @Test
  void topNWithRankNumberMatchesHost() throws Exception {
    // Projecting the rank number: a row entering the top-N shifts those below it, so the operator
    // emits the UPDATE_BEFORE/UPDATE_AFTER cascade (with the rank column) Flink does.
    NativeParity.assertParity(
        FlinkTopNSqlHarnessTest::environment,
        "SELECT k, v, rn FROM (SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn "
            + "FROM src) WHERE rn <= 2");
  }

  @Test
  void topNWithRankNumberDescendingMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkTopNSqlHarnessTest::environment,
        "SELECT k, v, rn FROM (SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v DESC) AS rn "
            + "FROM src) WHERE rn <= 2");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
            Row.of(1L, 5L),
            Row.of(1L, 3L),
            Row.of(1L, 8L),
            Row.of(1L, 1L),
            Row.of(2L, 9L),
            Row.of(2L, 4L));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.BIGINT()).build());
    return tEnv;
  }
}
