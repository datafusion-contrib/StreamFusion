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
 * INNER {@code UNNEST} of an array matches the host. The native operator fans each input row out to
 * one row per array element — Flink's {@code Correlate} over {@code $UNNEST_ROWS$} — so an empty or
 * null array yields no rows and a null element yields a null row. The array column rides the Arrow
 * boundary (now that nested types are carried) and is unnested natively.
 */
class FlinkUnnestSqlHarnessTest {

  @Test
  void unnestArrayMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkUnnestSqlHarnessTest::environment, "SELECT k, e FROM t CROSS JOIN UNNEST(vs) AS u(e)");
  }

  @Test
  void unnestArrayCommaSyntaxMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkUnnestSqlHarnessTest::environment, "SELECT k, e FROM t, UNNEST(vs) AS u(e)");
  }

  @Test
  void unnestWithPushedFilterFallsBack() throws Exception {
    // A filter on the unnested element is pushed into the Correlate as a `condition` (not a separate
    // Calc); the native operator doesn't apply that condition, so it falls back cleanly. Filtering on
    // an input column (not the element) stays a separate Calc and accelerates.
    NativeParity.assertFallback(
        FlinkUnnestSqlHarnessTest::environment,
        "SELECT k, e FROM t CROSS JOIN UNNEST(vs) AS u(e) WHERE e > 25");
  }

  @Test
  void unnestStringArrayMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkUnnestSqlHarnessTest::stringArrayEnvironment,
        "SELECT k, s FROM t CROSS JOIN UNNEST(ss) AS u(s)");
  }

  @Test
  void perOperatorFlagKeepsUnnestOnHost() throws Exception {
    System.setProperty("streamfusion.operator.unnest.enabled", "false");
    try {
      NativeParity.assertFallbackReasonContains(
          FlinkUnnestSqlHarnessTest::environment,
          "SELECT k, e FROM t CROSS JOIN UNNEST(vs) AS u(e)",
          "unnest: disabled by config");
    } finally {
      System.clearProperty("streamfusion.operator.unnest.enabled");
    }
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "vs"}, Types.LONG, Types.OBJECT_ARRAY(Types.LONG)),
            Row.of(1L, new Long[] {10L, 20L}),
            Row.of(2L, new Long[] {30L}),
            Row.of(3L, new Long[] {}), // empty array → no rows (INNER)
            Row.of(4L, null), // null array → no rows (INNER)
            Row.of(5L, new Long[] {40L, null, 50L})); // null element → null row
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("vs", DataTypes.ARRAY(DataTypes.BIGINT()))
            .build());
    return tEnv;
  }

  private static TableEnvironment stringArrayEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"k", "ss"}, Types.LONG, Types.OBJECT_ARRAY(Types.STRING)),
            Row.of(1L, new String[] {"a", "b"}),
            Row.of(2L, new String[] {"c"}));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("ss", DataTypes.ARRAY(DataTypes.STRING()))
            .build());
    return tEnv;
  }
}
