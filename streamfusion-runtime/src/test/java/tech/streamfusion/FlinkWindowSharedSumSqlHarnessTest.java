package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkWindowSharedSumSqlHarnessTest {
  @Test
  void sharedSessionAggregatesPreserveTypedZeroAndNullCorrection() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSessionTableFunction();
    String sql = "SELECT k, window_start, window_end, COUNT(v), AVG(v), SUM(v), "
        + "COUNT(d), AVG(d), SUM(d) FROM TABLE(SESSION(TABLE src PARTITION BY k, "
        + "DESCRIPTOR(rt), INTERVAL '2' SECOND)) GROUP BY k, window_start, window_end";
    var runs = NativeFailureParity.run(() -> environment("ONE_PHASE", false), sql);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertTrue(runs.nativeRun().plan().contains("NativeColumnarSessionWindowAggregate"),
        runs.nativeRun().plan());
  }

  @ParameterizedTest
  @ValueSource(strings = {"TUMBLE", "HOP", "SESSION"})
  void sharedAggregatesKeepLegacyWindowLayout(String shape) {
    String interval = shape.equals("HOP") ? "INTERVAL '2' SECOND, INTERVAL '10' SECOND"
        : "INTERVAL '10' SECOND";
    String sql = "SELECT k, COUNT(v), AVG(v), SUM(v), COUNT(d), AVG(d), SUM(d), "
        + shape + "_START(rt, " + interval + "), " + shape + "_END(rt, " + interval
        + ") FROM src GROUP BY k, " + shape + "(rt, " + interval + ")";
    var runs = NativeFailureParity.run(() -> environment("ONE_PHASE", false), sql);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void sharedAttachedAggregatesPreserveValuesInBothPhases(String phase) {
    String sql = "SELECT window_start, window_end, COUNT(total), AVG(total), SUM(total) FROM ("
        + "SELECT k, window_start, window_end, SUM(d) AS total FROM TABLE(" + window("TUMBLE")
        + ") GROUP BY k, window_start, window_end) GROUP BY window_start, window_end";
    var runs = NativeFailureParity.run(() -> environment(phase, false), sql);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE", "TWO_PHASE,TUMBLE", "ONE_PHASE,HOP", "TWO_PHASE,HOP",
    "ONE_PHASE,CUMULATE", "TWO_PHASE,CUMULATE"
  })
  void sharedExactAggregatesPreserveNullsWidthsAndScale(String phase, String shape) {
    check(phase, shape, false, false);
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE", "TWO_PHASE,TUMBLE", "ONE_PHASE,HOP", "TWO_PHASE,HOP",
    "ONE_PHASE,CUMULATE", "TWO_PHASE,CUMULATE"
  })
  void filteredReductionsPreserveAllRejectedGroups(String phase, String shape) {
    check(phase, shape, false, true);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void reducedSumsPreserveIntegerWrappingAndStickyDecimalOverflow(String phase) {
    check(phase, "HOP", true, false);
  }

  @ParameterizedTest
  @CsvSource({"ONE_PHASE,TINYINT", "TWO_PHASE,TINYINT", "ONE_PHASE,SMALLINT", "TWO_PHASE,SMALLINT",
      "ONE_PHASE,FLOAT", "TWO_PHASE,FLOAT", "ONE_PHASE,DOUBLE", "TWO_PHASE,DOUBLE"})
  void narrowReducedAggregatesRetainTheirDeclaredWidth(String phase, String type) {
    String value = "CAST(i AS " + type + ")";
    String sql = "SELECT k, window_start, window_end, COUNT(" + value + "), AVG(" + value
        + "), SUM(" + value + ") FROM TABLE(" + window("HOP")
        + ") GROUP BY k, window_start, window_end";
    var runs = NativeFailureParity.run(() -> environment(phase, false), sql);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void scale38RetainsTheReleasedFlinkPlanningFailure(String phase) {
    String value = "CAST(d AS DECIMAL(38,38))";
    String sql = "SELECT k, COUNT(" + value + "), AVG(" + value + "), SUM(" + value
        + ") FROM TABLE(" + window("TUMBLE") + ") GROUP BY k, window_start, window_end";
    NativeFailureParity.run(() -> environment(phase, false), sql).assertFailure(
        IllegalArgumentException.class, "Cannot convert 0 to DECIMAL(38, 38)",
        NativeFailureParity.Phase.PLANNING, NativeFailureParity.Route.FALLBACK);
  }

  private static void check(String phase, String shape, boolean boundaries, boolean filtered) {
    String filter = filtered ? " FILTER (WHERE k < 3 AND MOD(ts, 2) = 0)" : "";
    String aggregates = java.util.Arrays.stream(new String[] {"i", "v", "d", "wide"})
        .map(value -> "COUNT(" + value + ")" + filter + ", AVG(" + value + ")" + filter
            + ", SUM(" + value + ")" + filter)
        .collect(java.util.stream.Collectors.joining(", "));
    String sql = "SELECT k, window_start, window_end, " + aggregates + " FROM TABLE("
        + window(shape) + ") GROUP BY k, window_start, window_end";
    var runs = NativeFailureParity.run(() -> environment(phase, boundaries), sql);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    String plan = runs.nativeRun().plan();
    assertTrue(plan.contains(phase.equals("TWO_PHASE")
        ? "NativeColumnarGlobalWindowAggregate" : "NativeColumnarWindowAggregate"), plan);
    if (phase.equals("TWO_PHASE")) {
      assertTrue(plan.contains("NativeColumnarLocalWindowAggregate"), plan);
    }
  }

  static String window(String shape) {
    switch (shape) {
      case "TUMBLE":
        return "TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '10' SECOND)";
      case "HOP":
        return "HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)";
      default:
        return "CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)";
    }
  }

  private static TableEnvironment environment(String phase, boolean boundaries) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.configure(org.apache.flink.configuration.Configuration.fromMap(
        java.util.Map.of("restart-strategy.type", "none")));
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    var input = env.fromSequence(0, 1002).map(id -> {
      boolean empty = id % 5 == 4 || id % 7 == 0;
      return Row.of(id % 5 == 0 ? null : (int) (id % 5),
          empty ? null : boundaries ? Integer.MAX_VALUE : (int) (id % 23) - 11,
          empty ? null : boundaries ? Long.MAX_VALUE : id % 101 - 50,
          empty ? null : BigDecimal.valueOf(id % 211 - 100, 2),
          empty ? null : boundaries
              ? new BigDecimal("999999999999999999999999999999999999.99")
              : BigDecimal.valueOf(id % 211 - 100, 9),
          1000L + id % 11000);
    }).returns(Types.ROW_NAMED(new String[] {"k", "i", "v", "d", "wide", "ts"},
        Types.INT, Types.INT, Types.LONG, Types.BIG_DEC, Types.BIG_DEC, Types.LONG))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
            .withTimestampAssigner((row, previous) -> (Long) row.getField(5)));
    table.createTemporaryView("src", input, Schema.newBuilder()
        .column("k", DataTypes.INT()).column("i", DataTypes.INT())
        .column("v", DataTypes.BIGINT()).column("d", DataTypes.DECIMAL(12, 2))
        .column("wide", boundaries ? DataTypes.DECIMAL(38, 2) : DataTypes.DECIMAL(38, 9))
        .column("ts", DataTypes.BIGINT())
        .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
        .watermark("rt", "SOURCE_WATERMARK()").build());
    return table;
  }
}
