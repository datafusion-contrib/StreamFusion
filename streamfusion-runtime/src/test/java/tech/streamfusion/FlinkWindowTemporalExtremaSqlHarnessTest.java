package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.NativePlanner;

class FlinkWindowTemporalExtremaSqlHarnessTest {
  private static final LocalDateTime[] VALUES = {
    LocalDateTime.parse("0001-01-01T00:00:00.000000001"),
    LocalDateTime.parse("1582-10-15T00:00:00.999999999"),
    LocalDateTime.parse("1969-12-31T23:59:59.999999999"),
    LocalDateTime.parse("1970-01-01T00:00:00.000000001"),
    LocalDateTime.parse("2024-11-03T05:30:00.001000001"),
    LocalDateTime.parse("2024-11-03T05:30:00.001000002"),
    LocalDateTime.parse("2024-11-03T06:30:00.001000001"),
    LocalDateTime.parse("9999-12-31T23:59:59.999999999")
  };

  static Stream<Arguments> fixtures() {
    return Stream.of("ONE_PHASE", "TWO_PHASE").flatMap(phase ->
        Stream.of("TUMBLE", "HOP", "CUMULATE").flatMap(shape ->
            Stream.of(0, 3, 6, 9).map(precision -> Arguments.of(phase, shape, precision))));
  }

  static Stream<Arguments> zones() {
    return Stream.of("ONE_PHASE", "TWO_PHASE").flatMap(phase ->
        Stream.of("UTC", "Asia/Shanghai", "America/New_York").flatMap(zone ->
            Stream.of(3, 9).map(precision -> Arguments.of(phase, zone, precision))));
  }

  @ParameterizedTest
  @MethodSource("fixtures")
  void timestampExtremaKeepPrecisionAndBroadYears(String phase, String shape, int precision) {
    check(() -> environment(phase, precision, "UTC", false), sql(shape,
        "MIN(tv), MAX(tv), COUNT(*)"), phase);
  }

  @ParameterizedTest
  @MethodSource("zones")
  void localTimezoneExtremaCompareInstantsAcrossSessionZones(String phase, String zone, int precision) {
    check(() -> environment(phase, precision, zone, false), sql("TUMBLE",
        "MIN(lv), MAX(lv), MIN(tv), MAX(tv), COUNT(*)"), phase);
  }

  @ParameterizedTest
  @MethodSource("fixtures")
  void mixedStringAndTimestampExtremaPreserveSelectedRows(String phase, String shape, int precision) {
    String query = sql(shape,
        "MIN(tv), MAX(lv), MIN(CAST(k AS STRING)), MAX(CAST(ts AS STRING)), COUNT(*)");
    check(() -> environment(phase, precision, "UTC", false), query, phase);
  }

  @ParameterizedTest
  @MethodSource("zones")
  void independentFiltersPreserveNullOnlyGroupsAndMixedPartialPositions(
      String phase, String zone, int precision) {
    check(() -> environment(phase, precision, zone, false), sql(phase.equals("ONE_PHASE") ? "TUMBLE" : "HOP",
        "MIN(tv) FILTER (WHERE k = 1), AVG(ts), MAX(lv) FILTER (WHERE k = 2), "
        + "COUNT(DISTINCT tv), COUNT(*)"), phase);
  }

  @ParameterizedTest
  @CsvSource({"ONE_PHASE,false,TUMBLE", "TWO_PHASE,false,HOP", "ONE_PHASE,true,TUMBLE",
      "TWO_PHASE,true,HOP", "ONE_PHASE,false,SESSION", "ONE_PHASE,true,SESSION",
      "ONE_PHASE,false,HOP", "ONE_PHASE,true,HOP", "ONE_PHASE,false,CUMULATE",
      "ONE_PHASE,true,CUMULATE"})
  void checkpointRecoveryKeepsFractionalExtremaAndNullOnlyState(String phase, boolean rocks, String shape) {
    if (shape.equals("SESSION")) {
      tech.streamfusion.compat.FlinkTestCapabilities.requireSessionTableFunction();
    }
    var rows = java.util.stream.IntStream.range(0, 12).mapToObj(i -> {
      var value = i % 3 == 1 ? null : VALUES[i % VALUES.length];
      return Row.of(i % 3, value, value == null ? null : value.toInstant(ZoneOffset.UTC),
          LocalDateTime.ofInstant(Instant.ofEpochMilli(i < 10 ? 100 + i * 50L : i == 10 ? 1700 : 300), ZoneOffset.UTC));
    }).collect(java.util.stream.Collectors.toList());
    var schema = Schema.newBuilder().column("k", DataTypes.INT())
        .column("tv", DataTypes.TIMESTAMP(9)).column("lv", DataTypes.TIMESTAMP_LTZ(9))
        .column("rt", DataTypes.TIMESTAMP(3)).watermark("rt", "SOURCE_WATERMARK()").build();
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    try (var recovery = new PortableSqlRecovery(backend, rows,
        Types.ROW_NAMED(new String[] {"k", "tv", "lv", "rt"}, Types.INT,
            Types.LOCAL_DATE_TIME, Types.INSTANT, Types.LOCAL_DATE_TIME), schema, false, 4, 8)
        .withExecutionMetrics().withWatermarks(Map.of(10, 999L, 12, 4999L))) {
      java.util.function.UnaryOperator<TableEnvironment> configure = table -> {
        table.getConfig().setLocalTimeZone(ZoneId.of("America/New_York"));
        table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
        table.getConfig().set("streamfusion.transpose.batchRows", "3");
        return table;
      };
      String window = shape.equals("SESSION")
          ? "SESSION(TABLE recovery_input PARTITION BY k, DESCRIPTOR(rt), INTERVAL '1' SECOND)"
          : shape.equals("TUMBLE")
              ? "TUMBLE(TABLE recovery_input, DESCRIPTOR(rt), INTERVAL '2' SECOND)"
              : shape.equals("HOP")
                  ? "HOP(TABLE recovery_input, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)"
                  : "CUMULATE(TABLE recovery_input, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)";
      String query = "SELECT k, window_start, window_end, MIN(tv), MAX(lv), COUNT(*) FROM TABLE("
          + window + ") "
          + "GROUP BY k, window_start, window_end";
      var runs = NativeFailureParity.runRecovery(() -> configure.apply(recovery.uninterrupted()),
          () -> configure.apply(recovery.get()), query);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), shape.equals("SESSION")
          ? "NativeColumnarSessionWindowAggregate" : phase.equals("TWO_PHASE")
              ? "NativeColumnarGlobalWindowAggregate" : "NativeColumnarWindowAggregate");
      if (phase.equals("TWO_PHASE")) {
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarLocalWindowAggregate");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void emptyInputEmitsNoWindow(String phase) {
    check(() -> environment(phase, 9, "UTC", true), sql("TUMBLE", "MIN(tv), MAX(lv), COUNT(*)"), phase);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void ordinaryTemporalCountSharesTypedExtremaWindows(String phase) {
    var runs = NativeFailureParity.run(() -> environment(phase, 9, "UTC", false),
        sql("TUMBLE", "COUNT(tv), MIN(tv), MAX(lv)"));
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
  }

  @ParameterizedTest
  @ValueSource(strings = {"TUMBLE", "HOP", "SESSION"})
  void legacyWindowsKeepTimestampState(String shape) {
    String parameters = shape.equals("HOP")
        ? "rt, INTERVAL '2' SECOND, INTERVAL '10' SECOND" : "rt, INTERVAL '10' SECOND";
    check(() -> environment("ONE_PHASE", 9, "UTC", false),
        "SELECT k, " + shape + "_START(" + parameters + "), " + shape + "_END(" + parameters
            + "), MIN(tv), MAX(lv), COUNT(*) FROM src GROUP BY k, " + shape + "(" + parameters + ")",
        "ONE_PHASE");
  }

  @Test
  void sessionTableFunctionKeepsTimestampExtrema() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSessionTableFunction();
    var runs = NativeFailureParity.run(() -> environment("ONE_PHASE", 9, "UTC", false),
        "SELECT k, window_start, window_end, MIN(tv), MAX(lv) FROM TABLE("
            + "SESSION(TABLE src PARTITION BY k, DESCRIPTOR(rt), INTERVAL '10' SECOND)) "
            + "GROUP BY k, window_start, window_end");
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertTrue(runs.nativeRun().plan().contains("NativeColumnarSessionWindowAggregate"), runs.nativeRun().plan());
  }

  @Test
  void processingTimeAdmissionUsesTheTimestampAccumulator() {
    var table = environment("ONE_PHASE", 9, "UTC", false);
    table.executeSql("CREATE TEMPORARY VIEW timed_src AS SELECT *, PROCTIME() AS pt FROM src");
    var plan = NativePlanner.explain(table, "SELECT k, window_start, window_end, MIN(tv), MAX(lv) FROM TABLE("
        + "TUMBLE(TABLE timed_src, DESCRIPTOR(pt), INTERVAL '10' SECOND)) GROUP BY k, window_start, window_end");
    assertTrue(plan.contains("NativeColumnarWindowAggregate"), plan);
  }

  @ParameterizedTest
  @CsvSource({"ONE_PHASE,0", "TWO_PHASE,0", "ONE_PHASE,3", "TWO_PHASE,3", "ONE_PHASE,9", "TWO_PHASE,9"})
  void runtimeFractionsSurviveCompactLogicalPrecisionAndPartialMerging(String phase, int precision) {
    check(() -> {
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(2);
      var table = StreamTableEnvironment.create(env);
      table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
      table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
      var type = DataTypes.ROW(DataTypes.FIELD("k", DataTypes.INT()),
          DataTypes.FIELD("tv", DataTypes.TIMESTAMP(precision)), DataTypes.FIELD("rt", DataTypes.TIMESTAMP(3)));
      var rt = LocalDateTime.of(1970, 1, 1, 0, 0, 1);
      var rows = List.of(Row.of(1, LocalDateTime.parse("1969-12-31T23:59:59.999999999"), rt),
          Row.of(1, LocalDateTime.parse("1969-12-31T23:59:59.999999998"), rt), Row.of(1, null, rt));
      var source = tech.streamfusion.compat.FlinkTestSources.fromData(env, rows, ExternalTypeInfo.<Row>of(type))
          .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
              .withTimestampAssigner((row, previous) -> 1000L));
      table.createTemporaryView("src", source, Schema.newBuilder().fromRowDataType(type)
          .watermark("rt", "SOURCE_WATERMARK()").build());
      return table;
    }, sql("TUMBLE", "MIN(tv), MAX(tv), COUNT(*)"), phase);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void attachedWindowsKeepTemporalValuesAcrossBothPhases(String phase) {
    String query = "SELECT window_start, window_end, MIN(lo), MAX(hi), COUNT(*) FROM ("
        + "SELECT k, window_start, window_end, MIN(tv) AS lo, MAX(lv) AS hi FROM TABLE("
        + "TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '10' SECOND)) GROUP BY k, window_start, window_end) "
        + "GROUP BY window_start, window_end";
    check(() -> environment(phase, 9, "UTC", false), query, phase);
  }

  private static String sql(String shape, String aggregates) {
    String window = shape.equals("HOP")
        ? "HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)"
        : shape.equals("CUMULATE")
            ? "CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)"
            : "TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '10' SECOND)";
    return "SELECT k, window_start, window_end, " + aggregates + " FROM TABLE(" + window + ") "
        + "GROUP BY k, window_start, window_end";
  }

  private static void check(Supplier<TableEnvironment> factory, String query, String phase) {
    var runs = NativeFailureParity.run(factory, query);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertTrue(runs.nativeRun().plan().contains(phase.equals("TWO_PHASE")
        ? "NativeColumnarGlobalWindowAggregate" : query.contains("SESSION(")
            ? "NativeColumnarSessionWindowAggregate" : "NativeColumnarWindowAggregate"), runs.nativeRun().plan());
    if (phase.equals("TWO_PHASE")) {
      assertTrue(runs.nativeRun().plan().contains("NativeColumnarLocalWindowAggregate"), runs.nativeRun().plan());
    }
  }

  private static TableEnvironment environment(String phase, int precision, String zone, boolean empty) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneId.of(zone));
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    var type = DataTypes.ROW(DataTypes.FIELD("k", DataTypes.INT()),
        DataTypes.FIELD("raw_tv", DataTypes.TIMESTAMP(9)),
        DataTypes.FIELD("raw_lv", DataTypes.TIMESTAMP_LTZ(9)),
        DataTypes.FIELD("ts", DataTypes.BIGINT()), DataTypes.FIELD("rt", DataTypes.TIMESTAMP(3)));
    var source = env.fromSequence(0, 5002).filter(id -> !empty).map(id -> {
      LocalDateTime value = id % 5 == 4 || id % 7 == 0 ? null : VALUES[(int) (id % VALUES.length)];
      long millis = 1000L + id % 11000;
      return Row.of(id % 5 == 0 ? null : (int) (id % 5), value,
          value == null ? null : value.toInstant(ZoneOffset.UTC), millis,
          LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC));
    }).returns(ExternalTypeInfo.<Row>of(type))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
            .withTimestampAssigner((row, previous) -> (Long) row.getField(3)));
    table.createTemporaryView("src", source, Schema.newBuilder().fromRowDataType(type)
        .columnByExpression("tv", "CAST(raw_tv AS TIMESTAMP(" + precision + "))")
        .columnByExpression("lv", "CAST(raw_lv AS TIMESTAMP_LTZ(" + precision + "))")
        .watermark("rt", "SOURCE_WATERMARK()").build());
    return table;
  }
}
