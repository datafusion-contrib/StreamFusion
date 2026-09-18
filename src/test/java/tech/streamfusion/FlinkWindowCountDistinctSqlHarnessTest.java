package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
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
import tech.streamfusion.planner.NativePlanner;

class FlinkWindowCountDistinctSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"TUMBLE", "HOP", "CUMULATE"})
  void splitDistinctRetainsOnlyTheUnimplementedWindowFallback(String shape) throws Exception {
    String sql = "SELECT k, window_start, window_end, COUNT(DISTINCT i), COUNT(DISTINCT s) "
        + "FROM TABLE(" + window(shape) + ") GROUP BY k, window_start, window_end";
    java.util.function.Supplier<TableEnvironment> factory = () -> {
      var table = environment("TWO_PHASE");
      table.getConfig().set("table.optimizer.distinct-agg.split.enabled", "true");
      return table;
    };
    var table = factory.get();
    var scan = NativePlanner.install(table);
    table.explainSql(sql);
    assertTrue(scan.fallbackReasons().stream().noneMatch(reason -> reason.contains("HASH_CODE")),
        scan.fallbackReasons().toString());
    NativeParity.assertFallbackReasonContains(factory, sql,
        "attached-window aggregation requires two-phase execution");
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE",
    "TWO_PHASE,TUMBLE",
    "ONE_PHASE,HOP",
    "TWO_PHASE,HOP",
    "ONE_PHASE,CUMULATE",
    "TWO_PHASE,CUMULATE"
  })
  void valuesAreDeduplicatedWithinEachWindowAcrossLocalPartials(String phase, String shape)
      throws Exception {
    check(
        phase,
        "SELECT k, window_start, window_end, COUNT(DISTINCT i), COUNT(DISTINCT s), "
            + "COUNT(DISTINCT d), COUNT(DISTINCT t), SUM(v), COUNT(*) FROM TABLE("
            + window(shape)
            + ") GROUP BY k, window_start, window_end");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void unkeyedWindowsAndSyntheticCountPreserveNullOnlyGroups(String phase) throws Exception {
    check(
        phase,
        "SELECT window_start, window_end, COUNT(DISTINCT i), AVG(v), COUNT(DISTINCT d) "
            + "FROM TABLE("
            + window("HOP")
            + ") GROUP BY window_start, window_end");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void windowTimeAndOuterGroupRetainDistinctResults(String phase) throws Exception {
    check(
        phase,
        "SELECT window_time, SUM(n) FROM (SELECT k, window_time, COUNT(DISTINCT i) AS n "
            + "FROM TABLE("
            + window("TUMBLE")
            + ") GROUP BY k, window_start, window_end, window_time) "
            + "GROUP BY window_time",
        true);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void exactAndTemporalTypesRetainTheirEquality(String phase) throws Exception {
    check(
        phase,
        "SELECT k, window_start, window_end, COUNT(DISTINCT CAST(i AS TINYINT)), "
            + "COUNT(DISTINCT CAST(i AS SMALLINT)), COUNT(DISTINCT v), "
            + "COUNT(DISTINCT CAST(t AS DATE)), COUNT(DISTINCT rt), "
            + "COUNT(DISTINCT CAST(t AS TIMESTAMP(3))), COUNT(DISTINCT CAST(t AS TIMESTAMP(6))), "
            + "COUNT(DISTINCT CAST(s AS CHAR(12))) "
            + "FROM TABLE("
            + window("TUMBLE")
            + ") GROUP BY k, window_start, window_end");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void attachedWindowsDeduplicateInnerCounts(String phase) throws Exception {
    String sql =
        "SELECT window_start, window_end, COUNT(DISTINCT n), AVG(n), COUNT(*) FROM ("
            + "SELECT k, window_start, window_end, COUNT(DISTINCT i) AS n FROM TABLE("
            + window("TUMBLE")
            + ") GROUP BY k, window_start, window_end) "
            + "GROUP BY window_start, window_end";
    if (phase.equals("ONE_PHASE")) {
      NativeParity.assertFallbackReasonContains(
          () -> environment(phase),
          sql,
          "attached-window aggregation requires two-phase execution");
    } else {
      check(phase, sql);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"TUMBLE", "HOP", "SESSION"})
  void legacyWindowsRetainDistinctSets(String shape) throws Exception {
    String args =
        shape.equals("HOP") ? "INTERVAL '2' SECOND, INTERVAL '10' SECOND" : "INTERVAL '10' SECOND";
    String sql =
        "SELECT k, "
            + shape
            + "_START(rt, "
            + args
            + "), COUNT(DISTINCT s), "
            + "COUNT(DISTINCT d), COUNT(DISTINCT t) FROM src GROUP BY k, "
            + shape
            + "(rt, "
            + args
            + ")";
    assertTrue(
        NativePlanner.explain(environment("ONE_PHASE"), sql)
            .contains(
                shape.equals("SESSION")
                    ? "NativeColumnarSessionWindowAggregate"
                    : "NativeColumnarWindowAggregate"));
    NativeParity.assertParity(() -> environment("ONE_PHASE"), sql);
  }

  @Test
  void sessionWindowsMergeDistinctValues() throws Exception {
    String sql =
        "SELECT k, window_start, window_end, COUNT(DISTINCT s), COUNT(DISTINCT t) "
            + "FROM TABLE(SESSION(TABLE src PARTITION BY k, DESCRIPTOR(rt), INTERVAL '2' SECOND)) "
            + "GROUP BY k, window_start, window_end";
    assertTrue(
        NativePlanner.explain(environment("ONE_PHASE"), sql)
            .contains("NativeColumnarSessionWindowAggregate"));
    NativeParity.assertParity(() -> environment("ONE_PHASE"), sql);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SUM(DISTINCT i)",
        "AVG(DISTINCT i)",
        "COUNT(DISTINCT CAST(i AS DOUBLE))",
        "COUNT(DISTINCT i) FILTER (WHERE i > 0)"
      })
  void unverifiedDistinctFormsFallBack(String expression) throws Exception {
    String sql =
        "SELECT k, window_start, window_end, "
            + expression
            + " FROM TABLE("
            + window("TUMBLE")
            + ") GROUP BY k, window_start, window_end";
    NativeParity.assertFallbackReasonContains(() -> environment("TWO_PHASE"), sql, "window");
  }

  private static void check(String phase, String sql) throws Exception {
    check(phase, sql, false);
  }

  private static void check(String phase, String sql, boolean changelog) throws Exception {
    String plan = NativePlanner.explain(environment(phase), sql);
    assertTrue(
        plan.contains(
            phase.equals("TWO_PHASE")
                ? "NativeColumnarGlobalWindowAggregate"
                : "NativeColumnarWindowAggregate"),
        plan);
    if (phase.equals("TWO_PHASE"))
      assertTrue(plan.contains("NativeColumnarLocalWindowAggregate"), plan);
    if (changelog) NativeParity.assertChangelogParity(() -> environment(phase), sql);
    else NativeParity.assertParity(() -> environment(phase), sql);
  }

  private static String window(String shape) {
    return switch (shape) {
      case "TUMBLE" -> "TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '10' SECOND)";
      case "HOP" -> "HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)";
      default -> "CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)";
    };
  }

  private static TableEnvironment environment(String phase) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.configure(
        org.apache.flink.configuration.Configuration.fromMap(
            java.util.Map.of("restart-strategy.type", "none")));
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    var input =
        env.fromSequence(0, 5002)
            .map(
                id -> {
                  boolean empty = id % 5 == 4 || id % 7 == 0;
                  return Row.of(
                      id % 5 == 0 ? null : (int) (id % 5),
                      empty ? null : (int) (id % 23) - 11,
                      empty ? null : id % 101 - 50,
                      empty ? null : BigDecimal.valueOf(id % 31 - 15, 2),
                      empty ? null : id % 3 == 0 ? "" : "\u00e9\ud83d\ude00" + id % 11,
                      empty ? null : LocalDateTime.of(1969, 12, 31, 23, 59, 59, (int) (id % 31)),
                      1000L + id % 11000);
                })
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "i", "v", "d", "s", "t", "ts"},
                    Types.INT,
                    Types.INT,
                    Types.LONG,
                    Types.BIG_DEC,
                    Types.STRING,
                    Types.LOCAL_DATE_TIME,
                    Types.LONG))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner((row, previous) -> (Long) row.getField(6)));
    table.createTemporaryView(
        "src",
        input,
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("i", DataTypes.INT())
            .column("v", DataTypes.BIGINT())
            .column("d", DataTypes.DECIMAL(20, 2))
            .column("s", DataTypes.STRING())
            .column("t", DataTypes.TIMESTAMP(9))
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return table;
  }
}
