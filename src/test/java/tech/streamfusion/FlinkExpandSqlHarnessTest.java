package tech.streamfusion;

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
 * GROUPING SETS / CUBE / ROLLUP matches the host. The host plans these as an {@code Expand} (fan each
 * row out to one row per grouping set, stamping an expand id) feeding a {@code GROUP BY} over the
 * keys plus the expand id. The native expansion reproduces Flink's {@code ExpandFunction} row for
 * row, so the downstream native aggregate's collapsed changelog matches.
 */
class FlinkExpandSqlHarnessTest {

  @Test
  void groupingSetsMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkExpandSqlHarnessTest::environment,
        "SELECT k, s, COUNT(*) FROM src GROUP BY GROUPING SETS ((k), (s))");
  }

  @Test
  void groupingSetsWithGrandTotalMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkExpandSqlHarnessTest::environment,
        "SELECT k, s, COUNT(*) FROM src GROUP BY GROUPING SETS ((k), (s), ())");
  }

  @Test
  void cubeMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkExpandSqlHarnessTest::environment,
        "SELECT k, s, COUNT(*) FROM src GROUP BY CUBE (k, s)");
  }

  @Test
  void rollupMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkExpandSqlHarnessTest::environment,
        "SELECT k, s, COUNT(*) FROM src GROUP BY ROLLUP (k, s)");
  }

  @Test
  void cubeWithSumMatchesHost() throws Exception {
    // A value aggregate (SUM(v)) over the cube — the measure column is copied in every grouping set.
    NativeParity.assertChangelogParity(
        FlinkExpandSqlHarnessTest::environment,
        "SELECT k, s, SUM(v) FROM src GROUP BY CUBE (k, s)");
  }

  @Test
  void perOperatorFlagKeepsExpandOnHost() throws Exception {
    System.setProperty("streamfusion.operator.expand.enabled", "false");
    try {
      NativeParity.assertFallbackReasonContains(
          FlinkExpandSqlHarnessTest::environment,
          "SELECT k, s, COUNT(*) FROM src GROUP BY GROUPING SETS ((k), (s))",
          "expand: disabled by config");
    } finally {
      System.clearProperty("streamfusion.operator.expand.enabled");
    }
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v", "s"}, Types.LONG, Types.LONG, Types.STRING),
            Row.of(1L, 5L, "a"),
            Row.of(2L, 3L, "b"),
            Row.of(1L, 8L, "a"),
            Row.of(2L, 1L, "b"),
            Row.of(1L, 9L, "b"));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("s", DataTypes.STRING())
            .build());
    return tEnv;
  }
}
