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
 * Retracting streaming Top-N: a {@code ROW_NUMBER() OVER (PARTITION BY g ORDER BY total DESC) <= 2}
 * over a {@code GROUP BY} result — a changelog input. As a key's running total updates, the ranker
 * retracts the stale (key, total) and promotes the row that moves into the top-N, emitting a
 * changelog the collapsed result of which must match the host's {@code RetractableTopNFunction}.
 */
class FlinkRetractingTopNSqlHarnessTest {

  private static final String TOP_N =
      "SELECT g, k, total FROM ("
          + "  SELECT g, k, total, ROW_NUMBER() OVER (PARTITION BY g ORDER BY total DESC, k DESC) AS rn"
          + "  FROM (SELECT g, k, SUM(v) AS total FROM src GROUP BY g, k)"
          + ") WHERE rn <= 2";

  private static final String TOP_N_WITH_RANK =
      "SELECT g, k, total, rn FROM ("
          + "  SELECT g, k, total, ROW_NUMBER() OVER (PARTITION BY g ORDER BY total DESC, k DESC) AS rn"
          + "  FROM (SELECT g, k, SUM(v) AS total FROM src GROUP BY g, k)"
          + ") WHERE rn <= 2";

  @Test
  void retractingTopNMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(FlinkRetractingTopNSqlHarnessTest::environment, TOP_N);
  }

  @Test
  void retractingTopNWithRankNumberMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkRetractingTopNSqlHarnessTest::environment, TOP_N_WITH_RANK);
  }

  @Test
  void retractingTopNUnderMiniBatchCollapsesToHost() throws Exception {
    NativeParity.assertChangelogParity(
        () -> {
          TableEnvironment tEnv = environment();
          tEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
          tEnv.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
          tEnv.getConfig().set("table.exec.mini-batch.size", "4");
          return tEnv;
        },
        TOP_N_WITH_RANK);
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    // Two groups; within each, several keys whose running totals cross as v accumulates, so the
    // per-group top-2 by total changes over the stream (exercising retraction + promotion).
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"g", "k", "v"}, Types.LONG, Types.LONG, Types.LONG),
            Row.of(1L, 10L, 5L),
            Row.of(1L, 20L, 3L),
            Row.of(1L, 30L, 1L),
            Row.of(1L, 20L, 9L), // k=20 total 12 → now tops the group
            Row.of(1L, 30L, 8L), // k=30 total 9 → enters top-2, k=10 (5) drops out
            Row.of(2L, 40L, 7L),
            Row.of(2L, 50L, 7L),
            Row.of(2L, 60L, 2L),
            Row.of(2L, 60L, 10L)); // k=60 total 12 → promotes into top-2
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("g", DataTypes.BIGINT())
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .build());
    return tEnv;
  }
}
