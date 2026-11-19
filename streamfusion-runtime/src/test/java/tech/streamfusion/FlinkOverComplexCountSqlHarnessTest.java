package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.compat.FlinkTestSources;

class FlinkOverComplexCountSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "rt,ROWS UNBOUNDED PRECEDING",
    "rt,RANGE UNBOUNDED PRECEDING",
    "rt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
    "rt,RANGE BETWEEN INTERVAL '0.3' SECOND PRECEDING AND CURRENT ROW",
    "pt,ROWS UNBOUNDED PRECEDING",
    "pt,RANGE UNBOUNDED PRECEDING",
    "pt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
  })
  void containersCountOnlyParentValidityAcrossAllFrames(String order, String frame) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var result = NativeFailureParity.run(() -> environment(metrics), sql(order, frame));
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
      assertFalse(result.host().rows().isEmpty());
      for (var row : result.host().rows()) {
        assertEquals(row.get(3), row.get(4));
        assertEquals(row.get(3), row.get(5));
        if (Integer.valueOf(2).equals(row.get(2))) assertEquals(0L, row.get(3));
      }
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  @ParameterizedTest
  @CsvSource({
    "rt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
    "pt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
  })
  void originalContainersRemainReadableAfterNativeEmission(String order, String frame) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var result =
          NativeFailureParity.run(
              () -> environment(metrics),
              sql(order, frame).replace("SELECT id, k,", "SELECT id, k, a, m, r,"));
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  @org.junit.jupiter.api.Test
  void numericResultsKeepColumnarSinksWhileMapsRequireARowSink(
      @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var table = environment(metrics);
      var scan = tech.streamfusion.planner.NativePlanner.install(table);
      table.executeSql(
          "CREATE TABLE numeric_sink (id BIGINT, k INT, ca BIGINT, cm BIGINT, cr BIGINT, n BIGINT,"
              + " mn BIGINT) WITH ('connector'='filesystem','format'='parquet','path'='"
              + directory.resolve("numeric").toUri()
              + "')");
      String plan =
          table.explainSql("INSERT INTO numeric_sink " + sql("rt", "ROWS UNBOUNDED PRECEDING"));
      assertTrue(plan.contains("NativeOverAggregate"), plan);
      assertTrue(plan.contains("NativeParquetSink"), plan);
    }
    try (var metrics = new RecoveryExecutionMetrics()) {
      var table = environment(metrics);
      var scan = tech.streamfusion.planner.NativePlanner.install(table);
      table.executeSql(
          "CREATE TABLE map_sink (m MAP<STRING,BIGINT>, n BIGINT)"
              + " WITH ('connector'='filesystem','format'='parquet','path'='"
              + directory.resolve("map").toUri()
              + "')");
      String plan =
          table.explainSql(
              "INSERT INTO map_sink SELECT m, COUNT(m) OVER (PARTITION BY k ORDER BY rt ROWS"
                  + " UNBOUNDED PRECEDING) FROM src");
      assertFalse(plan.contains("NativeParquetSink"), plan);
      assertEquals(0, scan.substitutions(), plan + scan.explainSummary());
      assertTrue(
          scan.fallbackReasons().stream()
              .anyMatch(reason -> reason.contains("requires a row sink")),
          scan.explainSummary());
    }
  }

  @org.junit.jupiter.api.Test
  void nullableMapKeysAndOriginalPayloadsCrossThreeWayShuffle() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var result =
          NativeFailureParity.run(
              () -> environment(metrics, 3),
              sql("rt", "ROWS UNBOUNDED PRECEDING")
                  .replace("SELECT id, k,", "SELECT id, k, a, m, r,"));
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "ROWS UNBOUNDED PRECEDING",
        "RANGE UNBOUNDED PRECEDING",
        "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
      })
  void processingTimeDefaultsBatchComplexValuesWithoutChangingScalarPlans(String frame) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      java.util.function.Supplier<StreamTableEnvironment> setup =
          () -> {
            var table = environment(metrics);
            table
                .getConfig()
                .getConfiguration()
                .removeConfig(
                    org.apache.flink.configuration.ConfigOptions.key(
                            "streamfusion.transpose.batchRows")
                        .intType()
                        .noDefaultValue());
            return table;
          };
      var table = setup.get();
      tech.streamfusion.planner.NativePlanner.install(table);
      assertTrue(table.explainSql(sql("pt", frame)).contains("preferredBatchRows=[4096]"));
      assertFalse(
          table
              .explainSql(
                  "SELECT COUNT(n) OVER (PARTITION BY k ORDER BY pt " + frame + ") FROM src")
              .contains("preferredBatchRows=[4096]"));
      var result = NativeFailureParity.run(() -> setup.get(), sql("pt", frame));
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "ROWS UNBOUNDED PRECEDING",
        "RANGE UNBOUNDED PRECEDING",
        "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
      })
  void processingTimeMapCountUsesNativeValidityState(String frame) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var table = environment(metrics);
      var scan = tech.streamfusion.planner.NativePlanner.install(table);
      String query = "SELECT COUNT(m) OVER (PARTITION BY k ORDER BY pt " + frame + ") FROM src";
      assertTrue(table.explainSql(query).contains("NativeOverAggregate"));
      var result = NativeFailureParity.run(() -> environment(metrics), query);
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  private static String sql(String order, String frame) {
    return "SELECT id, k, COUNT(a) OVER w, "
        + "COUNT(m) OVER w, "
        + "COUNT(r) OVER w, COUNT(*) OVER w,"
        + " MIN(n) OVER w FROM src WINDOW w AS (PARTITION BY k ORDER BY "
        + order
        + " "
        + frame
        + ")";
  }

  private static StreamTableEnvironment environment(RecoveryExecutionMetrics metrics) {
    return environment(metrics, 1);
  }

  private static StreamTableEnvironment environment(
      RecoveryExecutionMetrics metrics, int parallelism) {
    var env =
        metrics.environment(new org.apache.flink.configuration.Configuration(), parallelism, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    var rows = rows();
    var source =
        FlinkTestSources.fromData(
                env,
                Types.ROW_NAMED(
                    new String[] {"id", "k", "a", "m", "r", "n", "ts"},
                    Types.LONG,
                    Types.INT,
                    Types.OBJECT_ARRAY(Types.LONG),
                    Types.MAP(Types.STRING, Types.LONG),
                    Types.ROW_NAMED(new String[] {"child"}, Types.LONG),
                    Types.LONG,
                    Types.LONG),
                rows.toArray(new Row[0]))
            .setParallelism(1)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner((r, previous) -> (Long) r.getField(6)));
    table.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("k", DataTypes.INT())
            .column("a", DataTypes.ARRAY(DataTypes.BIGINT()))
            .column("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
            .column("r", DataTypes.ROW(DataTypes.FIELD("child", DataTypes.BIGINT())))
            .column("n", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .columnByExpression("pt", "PROCTIME()")
            .build());
    return table;
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> recoveryCases() {
    return java.util.stream.Stream.of(
            new String[] {"rt", "ROWS UNBOUNDED PRECEDING"},
            new String[] {"rt", "RANGE UNBOUNDED PRECEDING"},
            new String[] {"rt", "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"},
            new String[] {"rt", "RANGE BETWEEN INTERVAL '0.3' SECOND PRECEDING AND CURRENT ROW"},
            new String[] {"pt", "ROWS UNBOUNDED PRECEDING"},
            new String[] {"pt", "RANGE UNBOUNDED PRECEDING"},
            new String[] {"pt", "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"})
        .flatMap(
            frame ->
                java.util.stream.Stream.of(
                        "hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
                    .map(
                        backend ->
                            org.junit.jupiter.params.provider.Arguments.of(
                                frame[0], frame[1], backend)));
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("recoveryCases")
  void frameMembershipAndPendingContainersSurviveTwoRestores(
      String order, String frame, String backend) throws Exception {
    var rows = new java.util.ArrayList<Row>();
    for (var input : rows()) {
      Row row = Row.withPositions(8);
      for (int i = 0; i < 7; i++) row.setField(i, input.getField(i));
      row.setField(
          7,
          java.time.LocalDateTime.ofInstant(
              java.time.Instant.ofEpochMilli((Long) input.getField(6)), java.time.ZoneOffset.UTC));
      rows.add(row);
    }
    var types =
        Types.ROW_NAMED(
            new String[] {"id", "k", "a", "m", "r", "n", "ts", "rt"},
            Types.LONG,
            Types.INT,
            Types.OBJECT_ARRAY(Types.LONG),
            Types.MAP(Types.STRING, Types.LONG),
            Types.ROW_NAMED(new String[] {"child"}, Types.LONG),
            Types.LONG,
            Types.LONG,
            Types.LOCAL_DATE_TIME);
    var schema =
        Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("k", DataTypes.INT())
            .column("a", DataTypes.ARRAY(DataTypes.BIGINT()))
            .column("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
            .column("r", DataTypes.ROW(DataTypes.FIELD("child", DataTypes.BIGINT())))
            .column("n", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .column("rt", DataTypes.TIMESTAMP(3))
            .watermark("rt", "SOURCE_WATERMARK()")
            .columnByExpression("pt", "PROCTIME()")
            .build();
    try (var recovery =
        new PortableSqlRecovery(backend, rows, types, schema, false, 40, 80)
            .withExecutionMetrics()
            .withWatermarks(Map.of(24, 1100L, 60, 1400L, 96, 1700L, 120, 9999L))) {
      java.util.function.UnaryOperator<org.apache.flink.table.api.TableEnvironment> config =
          table -> {
            table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
            table.getConfig().set("streamfusion.transpose.batchRows", "7");
            return table;
          };
      var result =
          NativeFailureParity.runRecovery(
              () -> config.apply(recovery.uninterrupted()),
              () -> config.apply(recovery.get()),
              sql(order, frame).replace(" FROM src ", " FROM recovery_input "));
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeOverAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("over-complex-count-recovery");
    }
  }

  private static java.util.List<Row> rows() {
    var rows = new java.util.ArrayList<Row>();
    for (int i = 0; i < 120; i++) {
      boolean missing = i % 7 == 0 || i % 4 == 2;
      Long[] array = i % 3 == 0 ? new Long[0] : new Long[] {null, (long) i % 11};
      var map = new LinkedHashMap<String, Long>();
      if (i % 3 != 0) {
        map.put("nil", null);
        map.put(null, (long) i % 11);
      }
      rows.add(
          Row.of(
              (long) i,
              i % 11 == 0 ? null : i % 4,
              missing ? null : array,
              missing ? null : map,
              missing ? null : Row.of(i % 3 == 0 ? null : (long) i % 11),
              i % 5 == 0 ? null : (long) i % 11,
              1000L + i / 12 * 100));
    }
    return rows;
  }
}
