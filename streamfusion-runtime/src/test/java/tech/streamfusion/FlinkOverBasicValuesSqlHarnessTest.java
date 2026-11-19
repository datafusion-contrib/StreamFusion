package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkOverBasicValuesSqlHarnessTest {
  static Stream<Arguments> fixtures() {
    return Stream.of("decimal", "string", "timestamp", "ltz")
        .flatMap(
            type ->
                Stream.of(
                        "rt,ROWS UNBOUNDED PRECEDING",
                        "rt,RANGE UNBOUNDED PRECEDING",
                        "rt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
                        "rt,RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW",
                        "pt,ROWS UNBOUNDED PRECEDING",
                        "pt,RANGE UNBOUNDED PRECEDING",
                        "pt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW")
                    .map(
                        frame -> {
                          String[] parts = frame.split(",", 2);
                          return Arguments.of(type, parts[0], parts[1]);
                        }));
  }

  @ParameterizedTest
  @MethodSource("fixtures")
  void runtimeValuesUseNativeOverWithStockTypesAndNullSemantics(
      String type, String order, String frame) throws Exception {
    boolean temporal = type.equals("timestamp") || type.equals("ltz");
    String column =
        type.equals("decimal")
            ? "d"
            : type.equals("string") ? "s" : type.equals("timestamp") ? "t" : "lv";
    String reductions =
        "COUNT(" + column + ") OVER w, MIN(" + column + ") OVER w, MAX(" + column + ") OVER w";
    // Released 1.18 rejects temporal FIRST/LAST in its aggregate factory.
    if (!temporal
        || !org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18")) {
      reductions += ", FIRST_VALUE(" + column + ") OVER w, LAST_VALUE(" + column + ") OVER w";
    }
    if (type.equals("decimal")) {
      reductions += ", SUM(d) OVER w, AVG(d) OVER w, SUM(x) OVER w, AVG(x) OVER w";
    }
    String query =
        "SELECT id, k, "
            + reductions
            + " FROM src WINDOW w AS (PARTITION BY k ORDER BY "
            + order
            + " "
            + frame
            + ")";
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs = NativeFailureParity.run(() -> environment(metrics), query);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertTrue(runs.nativeRun().plan().contains("NativeOverAggregate"), runs.nativeRun().plan());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  @ParameterizedTest
  @CsvSource({"t,FIRST_VALUE", "t,LAST_VALUE", "lv,FIRST_VALUE", "lv,LAST_VALUE"})
  void releasedTemporalFirstLastPlanningFailuresRemainFailures(String column, String function)
      throws Exception {
    org.junit.jupiter.api.Assumptions.assumeFalse(
        tech.streamfusion.compat.RuntimeCompat.supportsTemporalOverFirstLast());
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(metrics),
              "SELECT id, "
                  + function
                  + "("
                  + column
                  + ") OVER (PARTITION BY k ORDER BY rt "
                  + "ROWS UNBOUNDED PRECEDING) FROM src");
      assertNotNull(runs.host().failure(), runs.toString());
      assertNotNull(runs.nativeRun().failure(), runs.toString());
      assertEquals(runs.host().rootCause().getClass(), runs.nativeRun().rootCause().getClass());
      assertEquals(runs.host().phase(), runs.nativeRun().phase());
      assertFalse(runs.nativeRun().plan().contains("NativeOverAggregate"), runs.nativeRun().plan());
    }
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
  void unpartitionedStringViewsPreserveSourceAndComputedRepresentation(String order, String frame)
      throws Exception {
    for (String value : new String[] {"s", "CASE WHEN MOD(id, 2) = 0 THEN '😀' ELSE s END"}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        String query =
            "SELECT id, MIN(v) OVER w, MAX(v) OVER w, FIRST_VALUE(v) OVER w, "
                + "LAST_VALUE(v) OVER w FROM (SELECT *, "
                + value
                + " AS v FROM src) "
                + "WINDOW w AS (ORDER BY "
                + order
                + " "
                + frame
                + ")";
        var runs = NativeFailureParity.run(() -> environment(metrics), query);
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
      }
    }
  }

  @org.junit.jupiter.api.Test
  void chainedOverComputedStringsKeepTheHostComparisonRepresentation() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      String query =
          "SELECT id, MIN(v) OVER w, MAX(v) OVER w FROM ("
              + "SELECT *, CASE WHEN MOD(n, 2) = 0 THEN '😀' ELSE '\ue000' END AS v FROM ("
              + "SELECT *, COUNT(*) OVER (ORDER BY rt ROWS UNBOUNDED PRECEDING) AS n FROM src)) "
              + "WINDOW w AS (ORDER BY rt ROWS UNBOUNDED PRECEDING)";
      var runs = NativeFailureParity.run(() -> environment(metrics), query);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  @org.junit.jupiter.api.Test
  void windowToOverComputedStringsKeepTheHostComparisonRepresentation() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      String query =
          "SELECT k, MIN(v) OVER w, MAX(v) OVER w FROM ("
              + "SELECT *, CASE WHEN MOD(n, 2) = 0 THEN '😀' ELSE '\ue000' END AS v FROM ("
              + "SELECT k, window_time, COUNT(*) AS n FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), "
              + "INTERVAL '2' SECOND)) GROUP BY k, window_start, window_end, window_time)) "
              + "WINDOW w AS (PARTITION BY k ORDER BY window_time ROWS UNBOUNDED PRECEDING)";
      var runs = NativeFailureParity.run(() -> environment(metrics), query);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  @ParameterizedTest
  @CsvSource({"0,false", "3,false", "6,false", "9,false", "0,true", "3,true", "6,true", "9,true"})
  void compactRuntimeFractionsAndWideTemporalValuesMatchTheReleasedSerializer(
      int precision, boolean ltz) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 1, 0);
                var table = StreamTableEnvironment.create(env);
                table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
                var type =
                    DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD(
                            "v",
                            ltz
                                ? DataTypes.TIMESTAMP_LTZ(precision)
                                : DataTypes.TIMESTAMP(precision)),
                        DataTypes.FIELD("ts", DataTypes.BIGINT()));
                var times =
                    new LocalDateTime[] {
                      LocalDateTime.parse("1969-12-31T23:59:59.999999999"),
                      LocalDateTime.parse("1969-12-31T23:59:59.999999998"),
                      LocalDateTime.parse("0001-01-01T00:00:00.123456789"),
                      LocalDateTime.parse("9999-12-31T23:59:59.123456789")
                    };
                var source =
                    env.fromSequence(0, 4)
                        .map(
                            id -> {
                              var value = id == 4 ? null : times[id.intValue()];
                              return Row.of(
                                  id.intValue(),
                                  value == null
                                      ? null
                                      : ltz ? value.toInstant(ZoneOffset.UTC) : value,
                                  1000L + id);
                            })
                        .returns(ExternalTypeInfo.<Row>of(type))
                        .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                                .withTimestampAssigner((row, previous) -> (Long) row.getField(2)));
                table.createTemporaryView(
                    "fractional_input",
                    source,
                    Schema.newBuilder()
                        .fromRowDataType(type)
                        .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                        .watermark("rt", "SOURCE_WATERMARK()")
                        .build());
                return table;
              },
              "SELECT id, CAST(MIN(v) OVER w AS "
                  + (ltz ? "TIMESTAMP_LTZ(9)" : "TIMESTAMP(9)")
                  + "), CAST(MAX(v) OVER w AS "
                  + (ltz ? "TIMESTAMP_LTZ(9)" : "TIMESTAMP(9)")
                  + ") FROM fractional_input WINDOW w AS (ORDER BY rt ROWS UNBOUNDED PRECEDING)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  @ParameterizedTest
  @CsvSource({"0,false", "1,false", "2,false", "3,false", "0,true", "1,true", "2,true", "3,true"})
  void mixedTypedOverRecoversAfterRepeatedCompletedCheckpoints(int frame, boolean rocks)
      throws Exception {
    var rows =
        java.util.stream.IntStream.range(0, 12)
            .mapToObj(
                i ->
                    Row.of(
                        i,
                        i % 3,
                        i % 3 == 2 ? null : new BigDecimal(i % 2 == 0 ? "3.25" : "-1.75"),
                        i % 3 == 2 ? null : new String[] {"😀", "\ue000", "", "tail"}[i / 3 % 4],
                        i % 3 == 2 ? null : LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123456789 + i),
                        i % 3 == 2
                            ? null
                            : java.time.Instant.ofEpochSecond(1704067200, 123456789 + i),
                        LocalDateTime.ofInstant(
                            java.time.Instant.ofEpochMilli(1000L + i / 3 * 400L), ZoneOffset.UTC)))
            .collect(java.util.stream.Collectors.toList());
    var schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("k", DataTypes.INT())
            .column("d", DataTypes.DECIMAL(12, 2))
            .column("s", DataTypes.STRING())
            .column("t", DataTypes.TIMESTAMP(9))
            .column("lv", DataTypes.TIMESTAMP_LTZ(9))
            .column("rt", DataTypes.TIMESTAMP(3))
            .watermark("rt", "SOURCE_WATERMARK()")
            .build();
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    var info =
        org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(
            new String[] {"id", "k", "d", "s", "t", "lv", "rt"},
            org.apache.flink.api.common.typeinfo.Types.INT,
            org.apache.flink.api.common.typeinfo.Types.INT,
            org.apache.flink.api.common.typeinfo.Types.BIG_DEC,
            org.apache.flink.api.common.typeinfo.Types.STRING,
            org.apache.flink.api.common.typeinfo.Types.LOCAL_DATE_TIME,
            org.apache.flink.api.common.typeinfo.Types.INSTANT,
            org.apache.flink.api.common.typeinfo.Types.LOCAL_DATE_TIME);
    String[] bounds = {
      "RANGE UNBOUNDED PRECEDING",
      "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
      "RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW",
      "ROWS UNBOUNDED PRECEDING"
    };
    try (var recovery =
        new PortableSqlRecovery(backend, rows, info, schema, false, 4, 8)
            .withExecutionMetrics()
            .withWatermarks(java.util.Map.of(3, 1000L, 6, 1400L, 9, 1800L, 12, 5000L))) {
      String query =
          "SELECT id, k, SUM(d) OVER w, AVG(d) OVER w, MIN(s) OVER w, MAX(s) OVER w, FIRST_VALUE(s)"
              + " OVER w, LAST_VALUE(s) OVER w, MIN(t) OVER w, MAX(lv) OVER w, COUNT(d) OVER w FROM"
              + " recovery_input WINDOW w AS (PARTITION BY k ORDER BY rt "
              + bounds[frame]
              + ")";
      java.util.function.UnaryOperator<TableEnvironment> configure =
          table -> {
            table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
            table.getConfig().set("streamfusion.transpose.batchRows", "3");
            return table;
          };
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure.apply(recovery.uninterrupted()),
              () -> configure.apply(recovery.get()),
              query);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeOverAggregate");
    }
  }

  static TableEnvironment environment(RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.ofHoursMinutes(5, 45));
    var type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("k", DataTypes.INT()),
            DataTypes.FIELD("d", DataTypes.DECIMAL(12, 2)),
            DataTypes.FIELD("x", DataTypes.DECIMAL(38, 0)),
            DataTypes.FIELD("s", DataTypes.STRING()),
            DataTypes.FIELD("t", DataTypes.TIMESTAMP(9)),
            DataTypes.FIELD("lv", DataTypes.TIMESTAMP_LTZ(9)),
            DataTypes.FIELD("ts", DataTypes.BIGINT()));
    var source =
        env.fromSequence(0, 48)
            .map(
                id -> {
                  int key = (int) (id % 3);
                  boolean missing = id % 5 == 0 || key == 2;
                  var timestamp =
                      LocalDateTime.of(
                          new int[] {1, 1969, 2024, 9999}[(int) (id / 3 % 4)],
                          1,
                          1,
                          0,
                          0,
                          0,
                          123456789 + id.intValue());
                  return Row.of(
                      id,
                      id % 11 == 0 ? null : key,
                      missing ? null : new BigDecimal(id % 2 == 0 ? "3.25" : "-1.75"),
                      missing
                          ? null
                          : new BigDecimal(
                              id % 7 == 0
                                  ? "99999999999999999999999999999999999999"
                                  : id % 2 == 0 ? "2" : "-1"),
                      missing
                          ? null
                          : new String[] {"😀", "\ue000", "", "é", "z"}[(int) (id / 3 % 5)],
                      missing ? null : timestamp,
                      missing ? null : timestamp.toInstant(ZoneOffset.UTC),
                      1000L + id / 3 * 400L);
                })
            .returns(ExternalTypeInfo.<Row>of(type))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner((row, previous) -> (Long) row.getField(7)));
    table.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .fromRowDataType(type)
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .columnByExpression("pt", "PROCTIME()")
            .build());
    return table;
  }
}
