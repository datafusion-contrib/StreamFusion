package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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

class FlinkOverBasicPrimitiveSqlHarnessTest {
  static Stream<Arguments> cases() {
    return Stream.of("d", "tm", "b")
        .flatMap(
            column ->
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
                          var parts = frame.split(",", 2);
                          return Arguments.of(column, parts[0], parts[1]);
                        }));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void primitiveValuesPreserveTypedResultsPeersNullsAndEmptyPartitions(
      String column, String order, String frame) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison =
          NativeFailureParity.run(
              () -> environment(metrics),
              "SELECT id, k, "
                  + reductions(column)
                  + ", MIN(id) OVER w, COUNT(*) OVER w"
                  + " FROM src WINDOW w AS (PARTITION BY k ORDER BY "
                  + order
                  + " "
                  + frame
                  + ")");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      assertTrue(comparison.nativeRun().plan().contains("NativeOverAggregate"));
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  @ParameterizedTest
  @CsvSource({"0,false", "1,false", "2,false", "3,false", "0,true", "1,true", "2,true", "3,true"})
  void mixedPrimitiveFramesSurviveTwoRestores(int frame, boolean rocks) throws Exception {
    var rows =
        IntStream.range(0, 12)
            .mapToObj(FlinkOverBasicPrimitiveSqlHarnessTest::row)
            .collect(tech.streamfusion.compat.ListCollectors.toList());
    var schema = schema(false).watermark("rt", "SOURCE_WATERMARK()").build();
    String[] bounds = {
      "RANGE UNBOUNDED PRECEDING",
      "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
      "RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW",
      "ROWS UNBOUNDED PRECEDING"
    };
    try (var recovery =
        new PortableSqlRecovery(
                rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap",
                rows,
                types(),
                schema,
                false,
                4,
                8)
            .withExecutionMetrics()
            .withWatermarks(java.util.Map.of(3, 1000L, 6, 1400L, 9, 1800L, 12, 5000L))) {
      String query =
          "SELECT id, k, "
              + reductions("d")
              + ", "
              + reductions("tm")
              + ", "
              + reductions("b")
              + " FROM recovery_input WINDOW w AS (PARTITION BY k ORDER BY rt "
              + bounds[frame]
              + ")";
      var comparison = NativeFailureParity.runRecovery(recovery::uninterrupted, recovery, query);
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeOverAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("primitive-over-recovery");
    }
  }

  @ParameterizedTest
  @CsvSource({"d,FIRST_VALUE", "d,LAST_VALUE", "tm,FIRST_VALUE", "tm,LAST_VALUE"})
  void releasedTemporalFirstLastFailuresRemainHostFailures(String column, String function)
      throws Exception {
    org.junit.jupiter.api.Assumptions.assumeFalse(
        tech.streamfusion.compat.RuntimeCompat.supportsTemporalOverFirstLast());
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison =
          NativeFailureParity.run(
              () -> environment(metrics),
              "SELECT id, "
                  + function
                  + "("
                  + column
                  + ") OVER (PARTITION BY k ORDER BY rt ROWS UNBOUNDED PRECEDING) FROM src");
      org.junit.jupiter.api.Assertions.assertNotNull(comparison.host().failure());
      org.junit.jupiter.api.Assertions.assertNotNull(comparison.nativeRun().failure());
      assertEquals(
          comparison.host().rootCause().getClass(), comparison.nativeRun().rootCause().getClass());
      assertEquals(comparison.host().phase(), comparison.nativeRun().phase());
      org.junit.jupiter.api.Assertions.assertFalse(
          comparison.nativeRun().plan().contains("NativeOverAggregate"));
    }
  }

  private static String reductions(String column) {
    String result =
        "COUNT(" + column + ") OVER w, COUNT(DISTINCT " + column + ") OVER w, MIN(" + column + ") OVER w, MAX(" + column + ") OVER w";
    if (column.equals("b")
        || tech.streamfusion.compat.RuntimeCompat.supportsTemporalOverFirstLast()) {
      result += ", FIRST_VALUE(" + column + ") OVER w, LAST_VALUE(" + column + ") OVER w";
    }
    return result;
  }

  private static TableEnvironment environment(RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.ofHoursMinutes(5, 45));
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    var source =
        env.fromSequence(0, 95)
            .map(
                id -> {
                  var value = row(id.intValue());
                  value.setField(
                      5,
                      LocalDateTime.ofInstant(
                          java.time.Instant.ofEpochMilli(1000L + id / 8 * 400L), ZoneOffset.UTC));
                  return value;
                })
            .returns(types())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner(
                        (value, previous) ->
                            ((LocalDateTime) value.getField(5))
                                .toInstant(ZoneOffset.UTC)
                                .toEpochMilli()));
    table.createTemporaryView("src", source, schema(true).build());
    return table;
  }

  private static TypeInformation<Row> types() {
    return Types.ROW_NAMED(
        new String[] {"id", "k", "d", "tm", "b", "rt"},
        Types.INT,
        Types.INT,
        Types.LOCAL_DATE,
        Types.LOCAL_TIME,
        Types.BOOLEAN,
        Types.LOCAL_DATE_TIME);
  }

  private static Schema.Builder schema(boolean proctime) {
    var builder =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("k", DataTypes.INT())
            .column("d", DataTypes.DATE())
            .column("tm", DataTypes.TIME(3))
            .column("b", DataTypes.BOOLEAN())
            .column("rt", DataTypes.TIMESTAMP(3));
    if (proctime)
      builder.watermark("rt", "SOURCE_WATERMARK()").columnByExpression("pt", "PROCTIME()");
    return builder;
  }

  private static Row row(int id) {
    boolean missing = id % 5 == 0 || id % 3 == 2;
    return Row.of(
        id,
        id % 11 == 0 ? null : id % 3,
        missing
            ? null
            : LocalDate.of(new int[] {1, 1969, 2024, 9999}[id / 3 % 4], 1, 1).plusDays(id % 31),
        missing ? null : LocalTime.ofNanoOfDay((id % 2 == 0 ? 1L : 86_399_999L) * 1_000_000),
        missing ? null : id / 3 % 2 == 0,
        LocalDateTime.ofInstant(
            java.time.Instant.ofEpochMilli(1000L + id / 3 * 400L), ZoneOffset.UTC));
  }
}
