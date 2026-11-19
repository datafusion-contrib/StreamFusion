package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
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

class FlinkOverDecimalDistinctReductionsSqlHarnessTest {
  static Stream<Arguments> cases() {
    return Stream.of("DECIMAL(12,2)", "DECIMAL(38,0)", "DECIMAL(38,37)").flatMap(type ->
        Stream.of("rt,ROWS UNBOUNDED PRECEDING", "rt,RANGE UNBOUNDED PRECEDING",
            "rt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
            "rt,RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW",
            "pt,ROWS UNBOUNDED PRECEDING", "pt,RANGE UNBOUNDED PRECEDING",
            "pt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW").map(shape -> {
              String[] parts = shape.split(",", 2);
              return Arguments.of(type, parts[0], parts[1]);
            }));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void decimalDistinctArithmeticPreservesOverflowPeersAndNulls(String type, String order, String frame)
      throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison = NativeFailureParity.run(() -> environment(metrics, type),
          "SELECT id, k, SUM(DISTINCT v) OVER w, AVG(DISTINCT v) OVER w, COUNT(DISTINCT v) OVER w, SUM(v) OVER w"
              + " FROM src WINDOW w AS (PARTITION BY k ORDER BY " + order + " " + frame + ")");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  @ParameterizedTest
  @CsvSource({"0,false", "1,false", "2,false", "3,false", "0,true", "1,true", "2,true", "3,true"})
  void mixedDistinctArithmeticSurvivesTwoRestores(int frame, boolean rocks) throws Exception {
    var rows = IntStream.range(0, 36).mapToObj(id -> Row.of(id,
        id % 11 == 0 ? null : id % 3, value(id, 12, 2), value(id, 38, 0), value(id, 38, 37), time(id)))
        .collect(tech.streamfusion.compat.ListCollectors.toList());
    var types = Types.ROW_NAMED(new String[] {"id", "k", "n", "w", "f", "rt"},
        Types.INT, Types.INT, Types.BIG_DEC, Types.BIG_DEC, Types.BIG_DEC, Types.LOCAL_DATE_TIME);
    var schema = Schema.newBuilder().column("id", DataTypes.INT()).column("k", DataTypes.INT())
        .column("n", DataTypes.DECIMAL(12, 2)).column("w", DataTypes.DECIMAL(38, 0))
        .column("f", DataTypes.DECIMAL(38, 37)).column("rt", DataTypes.TIMESTAMP(3))
        .watermark("rt", "SOURCE_WATERMARK()").build();
    String[] bounds = {"RANGE UNBOUNDED PRECEDING", "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
      "RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW", "ROWS UNBOUNDED PRECEDING"};
    try (var recovery = new PortableSqlRecovery(
        rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap",
        rows, types, schema, false, 11, 23).withExecutionMetrics()
        .withWatermarks(java.util.Map.of(9, 1000L, 18, 1400L, 27, 1800L, 36, 5000L))) {
      var comparison = NativeFailureParity.runRecovery(recovery::uninterrupted, recovery,
          "SELECT id, k, SUM(DISTINCT n) OVER w, AVG(DISTINCT n) OVER w, SUM(DISTINCT w) OVER w, AVG(DISTINCT w) OVER w, SUM(DISTINCT f) OVER w, AVG(DISTINCT f) OVER w, COUNT(DISTINCT n) OVER w,"
              + " MIN(n) OVER w, MAX(w) OVER w, COUNT(*) OVER w FROM recovery_input"
              + " WINDOW w AS (PARTITION BY k ORDER BY rt " + bounds[frame] + ")");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeOverAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("decimal-distinct-reductions-over-recovery");
    }
  }

  static Stream<Arguments> scaleBoundaries() {
    return Stream.of("SUM", "AVG").flatMap(function -> Stream.of(37, 38).flatMap(scale ->
        Stream.of(false, true).flatMap(distinct -> Stream.of(false, true)
            .map(bounded -> Arguments.of(function, scale, distinct, bounded)))));
  }

  @ParameterizedTest
  @MethodSource("scaleBoundaries")
  void scaleBoundaryRetainsReleasedFactoryBehavior(String function, int scale, boolean distinct, boolean bounded) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison = NativeFailureParity.run(() -> environment(metrics, "DECIMAL(38," + scale + ")"),
          "SELECT id, " + function + "(" + (distinct ? "DISTINCT " : "") + "v) OVER"
              + " (PARTITION BY k ORDER BY rt "
              + (bounded ? "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" : "ROWS UNBOUNDED PRECEDING")
              + ") FROM src");
      if (scale == 38) {
        assertNotNull(comparison.host().failure());
        assertNotNull(comparison.nativeRun().failure());
        assertEquals(comparison.host().rootCause().getClass(), comparison.nativeRun().rootCause().getClass());
        assertEquals(comparison.host().rootCause().getMessage(), comparison.nativeRun().rootCause().getMessage());
        assertEquals(comparison.host().phase(), comparison.nativeRun().phase());
        assertFalse(comparison.nativeRun().plan().contains("NativeOverAggregate"));
      } else {
        comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
      }
    }
  }

  private static TableEnvironment environment(RecoveryExecutionMetrics metrics, String type) {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    int precision = type.equals("DECIMAL(12,2)") ? 12 : 38;
    int scale = type.equals("DECIMAL(12,2)") ? 2 : type.equals("DECIMAL(38,38)") ? 38 : type.equals("DECIMAL(38,37)") ? 37 : 0;
    var rowType = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()),
        DataTypes.FIELD("k", DataTypes.INT()), DataTypes.FIELD("v", DataTypes.DECIMAL(precision, scale)),
        DataTypes.FIELD("rt", DataTypes.TIMESTAMP(3)));
    var source = env.fromSequence(0, 95).map(id -> Row.of(id.intValue(),
        id % 11 == 0 ? null : (int) (id % 3), value(id.intValue(), precision, scale), time(id.intValue())))
        .returns(ExternalTypeInfo.<Row>of(rowType))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
            .withTimestampAssigner((row, previous) -> ((LocalDateTime) row.getField(3))
                .toInstant(ZoneOffset.UTC).toEpochMilli()));
    table.createTemporaryView("src", source, Schema.newBuilder().fromRowDataType(rowType)
        .watermark("rt", "SOURCE_WATERMARK()").columnByExpression("pt", "PROCTIME()").build());
    return table;
  }

  private static LocalDateTime time(int id) {
    return LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(1000L + id / 9 * 400L), ZoneOffset.UTC);
  }

  private static BigDecimal value(int id, int precision, int scale) {
    if (id % 5 == 0 || id % 3 == 2) return null;
    var maximum = java.math.BigInteger.TEN.pow(precision).subtract(java.math.BigInteger.ONE);
    switch (id / 6 % 9) {
      case 0: return new BigDecimal(maximum, scale);
      case 1: case 2: return BigDecimal.valueOf(1, scale);
      case 3: return BigDecimal.valueOf(7, scale);
      case 4: return new BigDecimal(maximum.negate(), scale);
      case 5: return null;
      case 6: return BigDecimal.valueOf(-1, scale);
      case 7: return new BigDecimal(maximum, scale);
      default: return BigDecimal.ZERO.setScale(scale);
    }
  }
}
