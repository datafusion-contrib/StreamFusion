package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkRunningRowsSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"ROWS UNBOUNDED PRECEDING", "RANGE UNBOUNDED PRECEDING",
      "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW", "RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW"})
  void countStarAndNullableAggregatesMatchEventTimeFrames(String frame) throws Exception {
    String sql = "SELECT id, k, COUNT(*) OVER w, COUNT(v) OVER w, SUM(v) OVER w, "
        + "MIN(v) OVER w, MAX(v) OVER w FROM src WINDOW w AS (PARTITION BY k ORDER BY rt " + frame + ")";
    assertNative(sql);
    NativeParity.assertParity(() -> environment(5003, false), sql);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ROWS UNBOUNDED PRECEDING", "RANGE UNBOUNDED PRECEDING",
      "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"})
  void proctimeCountsUseArrivalOrder(String frame) throws Exception {
    String sql = "SELECT id, k, COUNT(*) OVER w, COUNT(v) OVER w, SUM(v) OVER w "
        + "FROM src WINDOW w AS (PARTITION BY k ORDER BY pt " + frame + ")";
    assertNative(sql);
    NativeParity.assertParity(() -> environment(5003, false), sql);
  }

  @Test
  void unpartitionedAllNullAndEmptyInputsRetainCountContract() throws Exception {
    String sql = "SELECT id, COUNT(*) OVER w, COUNT(v) OVER w, SUM(v) OVER w FROM src "
        + "WINDOW w AS (ORDER BY rt ROWS UNBOUNDED PRECEDING)";
    NativeParity.assertParity(() -> environment(100, true), sql);
    NativeParity.assertParity(() -> environment(0, true), sql);
  }

  @Test
  void constantArgumentsAndDistinctStateComposeWithCountStar() throws Exception {
    NativeParity.assertParity(() -> environment(100, false),
        "SELECT id, COUNT(*) OVER w, COUNT(1) OVER w, COUNT(CAST(NULL AS BIGINT)) OVER w, "
            + "SUM(DISTINCT v) OVER w FROM src WINDOW w AS (PARTITION BY k ORDER BY rt ROWS UNBOUNDED PRECEDING)");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "ROWS UNBOUNDED PRECEDING",
        "RANGE UNBOUNDED PRECEDING",
        "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
        "RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW"
      })
  void typedNullStringConstantsRetainTheirOverResultType(String frame) throws Exception {
    String sql =
        "SELECT id, COUNT(*) OVER w, MAX(CAST(NULL AS VARCHAR)) OVER w, "
            + "MIN(CAST(NULL AS VARCHAR)) OVER w, COUNT(CAST(NULL AS VARCHAR)) OVER w "
            + "FROM src WINDOW w AS (PARTITION BY k ORDER BY rt "
            + frame
            + ")";
    assertNative(sql);
    NativeParity.assertParity(() -> environment(100, false), sql);
  }

  private static void assertNative(String sql) {
    String plan = NativePlanner.explain(environment(100, false), sql);
    assertTrue(plan.contains("NativeOverAggregate"), plan);
  }

  private static TableEnvironment environment(int count, boolean allNull) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    var source = env.fromSequence(0, Math.max(0, count - 1)).filter(i -> i < count)
        .map(i -> Row.of(i, i % 3 == 0 ? null : (int) (i % 3),
            allNull || i % 5 == 0 ? null : i % 13, 1000L + i / 8))
        .returns(Types.ROW_NAMED(new String[] {"id", "k", "v", "ts"}, Types.LONG, Types.INT, Types.LONG, Types.LONG))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
            .withTimestampAssigner((row, previous) -> (Long) row.getField(3)));
    table.createTemporaryView("src", source, Schema.newBuilder()
        .column("id", DataTypes.BIGINT()).column("k", DataTypes.INT()).column("v", DataTypes.BIGINT())
        .column("ts", DataTypes.BIGINT()).columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
        .watermark("rt", "SOURCE_WATERMARK()").columnByExpression("pt", "PROCTIME()").build());
    return table;
  }
}
