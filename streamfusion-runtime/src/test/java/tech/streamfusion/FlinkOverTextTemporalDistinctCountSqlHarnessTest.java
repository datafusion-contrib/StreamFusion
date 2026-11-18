package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkOverTextTemporalDistinctCountSqlHarnessTest {
  static Stream<Arguments> cases() {
    return Stream.of("s", "CAST(s AS CHAR(8))", "t", "z").flatMap(column ->
        Stream.of("rt,ROWS UNBOUNDED PRECEDING", "rt,RANGE UNBOUNDED PRECEDING",
            "rt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
            "rt,RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW",
            "pt,ROWS UNBOUNDED PRECEDING", "pt,RANGE UNBOUNDED PRECEDING",
            "pt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW").map(shape -> {
              String[] parts = shape.split(",", 2);
              return Arguments.of(column, parts[0], parts[1]);
            }));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void distinctCountsPreserveStringBytesTemporalFractionsAndPeers(
      String column, String order, String frame) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison = NativeFailureParity.run(() -> environment(metrics),
          "SELECT id, k, s, COUNT(DISTINCT " + column + ") OVER w, COUNT(" + column + ") OVER w"
              + (column.contains("CHAR") ? "" : ", MIN(" + column + ") OVER w, MAX(" + column + ") OVER w")
              + " FROM src WINDOW w AS (PARTITION BY k ORDER BY " + order + " " + frame + ")");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  @ParameterizedTest
  @CsvSource({"0,false", "1,false", "2,false", "3,false", "0,true", "1,true", "2,true", "3,true"})
  void mixedTextAndTemporalMembershipSurvivesTwoRestores(int frame, boolean rocks) throws Exception {
    var rows = IntStream.range(0, 36).mapToObj(FlinkOverTextTemporalDistinctCountSqlHarnessTest::row)
        .collect(tech.streamfusion.compat.ListCollectors.toList());
    String[] bounds = {"RANGE UNBOUNDED PRECEDING", "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
      "RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW", "ROWS UNBOUNDED PRECEDING"};
    try (var recovery = new PortableSqlRecovery(
        rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap",
        rows, types(), schema(false).watermark("rt", "SOURCE_WATERMARK()").build(), false, 11, 23)
        .withExecutionMetrics().withWatermarks(java.util.Map.of(9, 1000L, 18, 1400L, 27, 1800L, 36, 5000L))) {
      var comparison = NativeFailureParity.runRecovery(recovery::uninterrupted, recovery,
          "SELECT id, k, s, COUNT(DISTINCT s) OVER w, COUNT(DISTINCT t) OVER w, COUNT(DISTINCT z) OVER w,"
              + " MIN(s) OVER w, MAX(t) OVER w, COUNT(*) OVER w FROM recovery_input"
              + " WINDOW w AS (PARTITION BY k ORDER BY rt " + bounds[frame] + ")");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeOverAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("text-temporal-distinct-over-recovery");
    }
  }

  @ParameterizedTest
  @CsvSource({"0,false", "3,false", "6,false", "9,false", "0,true", "3,true", "6,true", "9,true"})
  void runtimePrecisionMatchesTheReleasedTimestampSerializer(int precision, boolean ltz) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison = NativeFailureParity.run(() -> {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.getConfig().setLocalTimeZone(ZoneOffset.ofHoursMinutes(5, 45));
        var type = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("v", ltz ? DataTypes.TIMESTAMP_LTZ(precision) : DataTypes.TIMESTAMP(precision)),
            DataTypes.FIELD("ts", DataTypes.BIGINT()));
        var source = env.fromSequence(0, 23).map(id -> {
          var time = timeValue(id.intValue());
          return Row.of(id.intValue(), id % 7 == 0 ? null : ltz ? time.toInstant(ZoneOffset.UTC) : time,
              1000L + id / 3 * 400L);
        }).returns(org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type))
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                .withTimestampAssigner((value, old) -> (Long) value.getField(2)));
        table.createTemporaryView("precision_input", source, Schema.newBuilder().fromRowDataType(type)
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()").build());
        return table;
      }, "SELECT id, COUNT(DISTINCT v) OVER (ORDER BY rt ROWS UNBOUNDED PRECEDING) FROM precision_input");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void stringDistinctHashingPreservesMixedSelectionRepresentation(boolean distinctFirst) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      String count = "COUNT(DISTINCT s) OVER w";
      String selections = "FIRST_VALUE(s) OVER w, LAST_VALUE(s) OVER w, MIN(s) OVER w, MAX(s) OVER w";
      var comparison = NativeFailureParity.run(() -> environment(metrics),
          "SELECT id, " + (distinctFirst ? count + ", " + selections : selections + ", " + count)
              + " FROM src WINDOW w AS (PARTITION BY k ORDER BY rt ROWS UNBOUNDED PRECEDING)");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  static Stream<Arguments> charSelections() {
    return Stream.of("MIN", "MAX", "FIRST_VALUE", "LAST_VALUE").flatMap(function ->
        Stream.of(false, true).map(retracting -> Arguments.of(function, retracting)));
  }

  @ParameterizedTest
  @MethodSource("charSelections")
  void charSelectionsRetainReleasedFactoryBehavior(String function, boolean retracting) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      String bounds = retracting ? "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" : "ROWS UNBOUNDED PRECEDING";
      var comparison = NativeFailureParity.run(() -> environment(metrics),
          "SELECT id, COUNT(DISTINCT CAST(s AS CHAR(8))) OVER w, " + function
              + "(CAST(s AS CHAR(8))) OVER w FROM src"
              + " WINDOW w AS (PARTITION BY k ORDER BY rt " + bounds + ")");
      if (tech.streamfusion.compat.RuntimeCompat.supportsCharOverValueSelection(function, retracting)) {
        comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
      } else {
        assertNotNull(comparison.host().failure());
        assertNotNull(comparison.nativeRun().failure());
        assertEquals(comparison.host().rootCause().getClass(), comparison.nativeRun().rootCause().getClass());
        assertEquals(comparison.host().rootCause().getMessage(), comparison.nativeRun().rootCause().getMessage());
        assertEquals(comparison.host().phase(), comparison.nativeRun().phase());
        assertFalse(comparison.nativeRun().plan().contains("NativeOverAggregate"));
      }
    }
  }

  private static TableEnvironment environment(RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.ofHoursMinutes(5, 45));
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    var source = env.fromSequence(0, 95).map(id -> row(id.intValue())).returns(types())
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
            .withTimestampAssigner((value, old) -> ((LocalDateTime) value.getField(5))
                .toInstant(ZoneOffset.UTC).toEpochMilli()));
    table.createTemporaryView("src", source, schema(true).build());
    return table;
  }

  private static TypeInformation<Row> types() {
    return Types.ROW_NAMED(new String[] {"id", "k", "s", "t", "z", "rt"}, Types.INT, Types.INT,
        Types.STRING, Types.LOCAL_DATE_TIME, Types.INSTANT, Types.LOCAL_DATE_TIME);
  }

  private static Schema.Builder schema(boolean proctime) {
    var builder = Schema.newBuilder().column("id", DataTypes.INT()).column("k", DataTypes.INT())
        .column("s", DataTypes.STRING()).column("t", DataTypes.TIMESTAMP(9))
        .column("z", DataTypes.TIMESTAMP_LTZ(9)).column("rt", DataTypes.TIMESTAMP(3));
    if (proctime) builder.watermark("rt", "SOURCE_WATERMARK()").columnByExpression("pt", "PROCTIME()");
    return builder;
  }

  private static Row row(int id) {
    boolean missing = id % 5 == 0 || id % 3 == 2;
    String[] strings = {"", "?", "\ud800", "\udfff", "😀", "\ue000", "é", "tail"};
    var time = timeValue(id);
    return Row.of(id, id % 11 == 0 ? null : id % 3, missing ? null : strings[id / 6 % strings.length],
        missing ? null : time, missing ? null : time.toInstant(ZoneOffset.UTC),
        LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(1000L + id / 9 * 400L), ZoneOffset.UTC));
  }

  private static LocalDateTime timeValue(int id) {
    int slot = id / 6 % 6;
    return LocalDateTime.of(slot % 2 == 0 ? 1969 : 2024, 12, 31, 23, 59, 59,
        slot % 3 == 0 ? 999999999 : slot % 3 == 1 ? 999000001 : 999000000);
  }
}
