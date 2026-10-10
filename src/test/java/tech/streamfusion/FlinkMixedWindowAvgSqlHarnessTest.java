package tech.streamfusion;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkMixedWindowAvgSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"ONE_PHASE,TUMBLE", "TWO_PHASE,TUMBLE", "ONE_PHASE,HOP", "TWO_PHASE,HOP",
      "ONE_PHASE,CUMULATE", "TWO_PHASE,CUMULATE"})
  void mixedAggregatePositionsAndTypesMatchHost(String phase, String shape) throws Exception {
    String sql = "SELECT k, window_start, window_end, AVG(i), SUM(v), COUNT(i), MIN(v), "
        + "AVG(d), MAX(i), COUNT(*), AVG(v) FROM TABLE(" + window(shape) + ") "
        + "GROUP BY k, window_start, window_end";
    check(phase, false, sql);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void syntheticCountFollowsEveryAvgStateField(String phase) throws Exception {
    check(phase, false, "SELECT k, window_start, window_end, SUM(i), AVG(i), AVG(d), MAX(v) "
        + "FROM TABLE(" + window("HOP") + ") GROUP BY k, window_start, window_end");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void integerWrappingAndDecimalOverflowMatchHost(String phase) throws Exception {
    check(phase, true, "SELECT k, window_start, window_end, AVG(v), AVG(wide), COUNT(*), MIN(v) "
        + "FROM TABLE(" + window("HOP") + ") GROUP BY k, window_start, window_end");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void attachedWindowsMergeMixedAvg(String phase) throws Exception {
    String sql = "SELECT window_start, window_end, AVG(total), COUNT(*), MIN(total) FROM ("
        + "SELECT k, window_start, window_end, SUM(v) AS total FROM TABLE(" + window("TUMBLE")
        + ") GROUP BY k, window_start, window_end) GROUP BY window_start, window_end";
    if (phase.equals("ONE_PHASE")) {
      NativeParity.assertFallbackReasonContains(() -> environment(phase, false), sql,
          "attached-window aggregation requires two-phase execution");
    } else {
      check(phase, false, sql);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"SMALLINT", "TINYINT", "FLOAT", "DOUBLE"})
  void widenedAvgPartialsKeepTheDeclaredResultType(String type) throws Exception {
    String value = "CAST(i AS " + type + ")";
    check("TWO_PHASE", false, "SELECT k, window_start, window_end, MIN(i), AVG(" + value
        + "), COUNT(*) FROM TABLE(" + window("TUMBLE") + ") GROUP BY k, window_start, window_end");
  }

  private static void check(String phase, boolean boundaries, String sql) throws Exception {
    String plan = NativePlanner.explain(environment(phase, boundaries), sql);
    assertTrue(plan.contains(phase.equals("TWO_PHASE")
        ? "NativeColumnarGlobalWindowAggregate" : "NativeColumnarWindowAggregate"), plan);
    if (phase.equals("TWO_PHASE"))
      assertTrue(plan.contains("NativeColumnarLocalWindowAggregate"), plan);
    NativeParity.assertParity(() -> environment(phase, boundaries), sql);
  }

  private static String window(String shape) {
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
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    var input = env.fromSequence(0, 5002).map(id -> {
      boolean empty = id % 5 == 4 || id % 7 == 0;
      return Row.of(id % 5 == 0 ? null : (int) (id % 5),
          empty ? null : (int) (id % 23) - 11,
          empty ? null : boundaries ? Long.MAX_VALUE : id % 101 - 50,
          empty ? null : BigDecimal.valueOf(id % 211 - 100, 2),
          empty ? null : new BigDecimal("99999999999999999999999999999999999999"),
          1000L + id % 11000);
    }).returns(Types.ROW_NAMED(new String[] {"k", "i", "v", "d", "wide", "ts"},
        Types.INT, Types.INT, Types.LONG, Types.BIG_DEC, Types.BIG_DEC, Types.LONG))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
            .withTimestampAssigner((row, previous) -> (Long) row.getField(5)));
    table.createTemporaryView(
        "src",
        input,
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("i", DataTypes.INT())
            .column("v", DataTypes.BIGINT())
            .column("d", DataTypes.DECIMAL(20, 2))
            .column("wide", DataTypes.DECIMAL(38, 0))
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return table;
  }
}
