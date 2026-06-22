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

/** General Calc (computed/constant projections, with and without a filter) matches the host. */
class FlinkCalcSqlHarnessTest {

  @Test
  void computedProjectionMatchesHost() throws Exception {
    // A computed column with no filter — a projection-only Calc.
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT v + k FROM f");
  }

  @Test
  void multiplyProjectionMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT v * 2 FROM f");
  }

  @Test
  void mixedComputedAndColumnProjectionMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT v + k, s, k FROM f");
  }

  @Test
  void constantProjectionMatchesHost() throws Exception {
    NativeParity.assertParity(FlinkCalcSqlHarnessTest::environment, "SELECT k, 100 FROM f");
  }

  @Test
  void computedProjectionWithFilterMatchesHost() throws Exception {
    // Condition plus a computed projection: filtered first, then projected.
    NativeParity.assertParity(
        FlinkCalcSqlHarnessTest::environment, "SELECT v + k, s FROM f WHERE v > 15");
  }

  @Test
  void caseProjectionMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkCalcSqlHarnessTest::environment,
        "SELECT CASE WHEN v > 20 THEN 1 ELSE 0 END FROM f");
  }

  @Test
  void caseWithComputedBranchesMatchesHost() throws Exception {
    // Same-width branches (a column and a computed column); mixed widths need CAST, not yet admitted.
    NativeParity.assertParity(
        FlinkCalcSqlHarnessTest::environment,
        "SELECT CASE WHEN s <> 'a' THEN v ELSE v + 1 END FROM f");
  }

  @Test
  void caseInFilterMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkCalcSqlHarnessTest::environment,
        "SELECT k FROM f WHERE (CASE WHEN v > 20 THEN 1 ELSE 0 END) = 1");
  }

  @Test
  void unsupportedProjectionFunctionFallsBack() throws Exception {
    // A function the expression encoder does not admit makes the whole Calc fall back to the host.
    NativeParity.assertFallback(FlinkCalcSqlHarnessTest::environment, "SELECT ABS(v) FROM f");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v", "s"}, Types.LONG, Types.INT, Types.STRING),
            Row.of(1L, 10, "a"),
            Row.of(2L, 30, "b"),
            Row.of(3L, 20, "c"),
            Row.of(4L, 40, "d"));
    tEnv.createTemporaryView(
        "f",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .build());
    return tEnv;
  }
}
