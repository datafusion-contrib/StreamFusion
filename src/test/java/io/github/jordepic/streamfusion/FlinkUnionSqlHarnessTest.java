package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * UNION ALL matches the host. A union is a pure stream merge — the native node carries no operator
 * (it lowers to a UnionTransformation over the inputs' Arrow streams), so these check that merging
 * two (or more) columnar islands stays inside one island and reproduces the host row for row,
 * including over changelog inputs (the merge is changelog-transparent).
 */
class FlinkUnionSqlHarnessTest {

  @Test
  void twoWayUnionAllMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkUnionSqlHarnessTest::environment,
        "SELECT k, v FROM f UNION ALL SELECT k, v FROM g");
  }

  @Test
  void unionAllWithPerBranchFilterMatchesHost() throws Exception {
    // A Calc on each branch then the union — the whole thing is one columnar island.
    NativeParity.assertParity(
        FlinkUnionSqlHarnessTest::environment,
        "SELECT k, v FROM f WHERE v > 15 UNION ALL SELECT k, v FROM g WHERE v < 35");
  }

  @Test
  void unionAllWithComputedProjectionMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkUnionSqlHarnessTest::environment,
        "SELECT k, v + 1 FROM f UNION ALL SELECT k, v * 2 FROM g");
  }

  @Test
  void threeWayUnionAllMatchesHost() throws Exception {
    // Flink left-associates into nested binary unions, but the native node accepts N inputs; this
    // exercises the multi-input merge.
    NativeParity.assertParity(
        FlinkUnionSqlHarnessTest::environment,
        "SELECT k, v FROM f UNION ALL SELECT k, v FROM g UNION ALL SELECT k, v FROM f");
  }

  @Test
  void unionAllOfChangelogsMatchesHost() throws Exception {
    // Union of two GROUP BY results: each branch emits a retracting changelog, and the merge forwards
    // every change row untouched ($row_kind$ rides through), so the collapsed result matches the host.
    NativeParity.assertChangelogParity(
        FlinkUnionSqlHarnessTest::environment,
        "SELECT k, COUNT(*) FROM f GROUP BY k UNION ALL SELECT k, COUNT(*) FROM g GROUP BY k");
  }

  @Test
  void perOperatorFlagKeepsUnionOnHost() throws Exception {
    // Disabling the union operator keeps the whole query on the host (the union is interior, so the
    // all-or-nothing gate falls the query back), with a config fallback reason.
    System.setProperty("streamfusion.operator.union.enabled", "false");
    try {
      NativeParity.assertFallbackReasonContains(
          FlinkUnionSqlHarnessTest::environment,
          "SELECT k, v FROM f UNION ALL SELECT k, v FROM g",
          "union: disabled by config");
    } finally {
      System.clearProperty("streamfusion.operator.union.enabled");
    }
  }

  @Test
  void explainReportsNativeUnion() {
    PhysicalPlanScan scan = NativePlanner.install(environment());
    String explain =
        NativePlanner.explain(environment(), "SELECT k, v FROM f UNION ALL SELECT k, v FROM g");
    assertEquals(true, explain.contains("ran natively"), explain);
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    register(tEnv, env, "f");
    register(tEnv, env, "g");
    return tEnv;
  }

  private static void register(
      StreamTableEnvironment tEnv, StreamExecutionEnvironment env, String name) {
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v", "s"}, Types.LONG, Types.INT, Types.STRING),
            Row.of(1L, 10, "a"),
            Row.of(2L, 30, "b"),
            Row.of(3L, 20, "c"),
            Row.of(4L, 40, "d"));
    tEnv.createTemporaryView(
        name,
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .build());
  }
}
