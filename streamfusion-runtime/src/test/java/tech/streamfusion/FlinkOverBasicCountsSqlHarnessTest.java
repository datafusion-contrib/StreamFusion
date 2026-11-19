package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
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
import tech.streamfusion.planner.NativePlanner;

class FlinkOverBasicCountsSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "rt,ROWS UNBOUNDED PRECEDING",
    "rt,RANGE UNBOUNDED PRECEDING",
    "rt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
    "rt,RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW",
    "pt,ROWS UNBOUNDED PRECEDING",
    "pt,RANGE UNBOUNDED PRECEDING",
    "pt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
  })
  void basicCountPlansSelectNativeOver(String order, String frame) {
    var table = environment(5003);
    var scan = NativePlanner.install(table);
    String plan = table.explainSql(sql(order, frame));
    assertTrue(plan.contains("NativeOverAggregate"), plan);
    assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
  }

  @ParameterizedTest
  @CsvSource({
    "rt,ROWS UNBOUNDED PRECEDING",
    "rt,RANGE UNBOUNDED PRECEDING",
    "rt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
    "rt,RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW",
    "pt,ROWS UNBOUNDED PRECEDING",
    "pt,RANGE UNBOUNDED PRECEDING",
    "pt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
  })
  void countsKeepRuntimeValidityTypesPeersAndAllNullPartitions(String order, String frame)
      throws Exception {
    var outcomes = NativeFailureParity.run(() -> environment(5003), sql(order, frame));
    outcomes.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(outcomes.host().resultTypes(), outcomes.nativeRun().resultTypes());
  }

  private static String sql(String order, String frame) {
    return "SELECT id, k, COUNT(*) OVER w, COUNT(d) OVER w, COUNT(s) OVER w, "
        + "COUNT(t) OVER w, COUNT(instant_value) OVER w, COUNT(day_value) OVER w, "
        + "COUNT(time_value) OVER w, COUNT(flag) OVER w, MIN(n) OVER w, "
        + "COUNT(CAST(NULL AS VARCHAR)) OVER w FROM src "
        + "WINDOW w AS (PARTITION BY k ORDER BY "
        + order
        + " "
        + frame
        + ")";
  }

  static TableEnvironment environment(int count) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.ofHoursMinutes(5, 45));
    var source =
        env.fromSequence(0, count - 1)
            .map(
                id -> {
                  boolean missing = id % 5 == 0 || id % 4 == 0;
                  var time =
                      LocalDateTime.of(
                          id % 3 == 0 ? 1 : id % 3 == 1 ? 2024 : 9999, 1, 1, 0, 0, 0, 123456789);
                  return Row.of(
                      id,
                      id % 3 == 0 ? null : (int) (id % 4),
                      missing ? null : new BigDecimal("0.99999999999999999999999999999999999999"),
                      missing ? null : new String[] {"", "😀", "\ue000", "é"}[(int) (id % 4)],
                      missing ? null : time,
                      missing ? null : time.toInstant(ZoneOffset.UTC),
                      missing ? null : id,
                      1000L + id / 8,
                      missing ? null : LocalDate.of(2024, 1, 1).plusDays(id % 31),
                      missing ? null : LocalTime.of(12, 34, 56, 123000000),
                      missing ? null : id % 2 == 0);
                })
            .returns(
                Types.ROW_NAMED(
                    new String[] {
                      "id", "k", "d", "s", "t", "instant_value", "n", "ts",
                      "day_value", "time_value", "flag"
                    },
                    Types.LONG,
                    Types.INT,
                    Types.BIG_DEC,
                    Types.STRING,
                    Types.LOCAL_DATE_TIME,
                    Types.INSTANT,
                    Types.LONG,
                    Types.LONG,
                    Types.LOCAL_DATE,
                    Types.LOCAL_TIME,
                    Types.BOOLEAN))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner((row, previous) -> (Long) row.getField(7)));
    table.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("k", DataTypes.INT())
            .column("d", DataTypes.DECIMAL(38, 38))
            .column("s", DataTypes.STRING())
            .column("t", DataTypes.TIMESTAMP(9))
            .column("instant_value", DataTypes.TIMESTAMP_LTZ(9))
            .column("n", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .column("day_value", DataTypes.DATE())
            .column("time_value", DataTypes.TIME(3))
            .column("flag", DataTypes.BOOLEAN())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .columnByExpression("pt", "PROCTIME()")
            .build());
    return table;
  }
}
