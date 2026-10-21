package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkUpdatingSessionWindowSqlHarnessTest {
  static Stream<Arguments> profiles() {
    return Stream.of(
            "TINYINT",
            "SMALLINT",
            "INT",
            "BIGINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL(12,2)",
            "DECIMAL(38,8)",
            "STRING",
            "CHAR(8)",
            "BOOLEAN",
            "DATE",
            "TIME(3)",
            "TIMESTAMP(9)",
            "TIMESTAMP_LTZ(9)")
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        legacy ->
                            Stream.of("ONE_PHASE", "TWO_PHASE")
                                .map(phase -> Arguments.of(type, legacy, phase))));
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void signedSessionsRetainFiltersNullsAndLiveCounts(String type, boolean legacy, String phase) {
    if (!legacy) tech.streamfusion.compat.FlinkTestCapabilities.requireSessionTableFunction();
    String value;
    switch (type) {
      case "BOOLEAN":
        value = "MOD(v,2)=0";
        break;
      case "DATE":
        value = "CAST(TO_TIMESTAMP_LTZ(v*86400000,3) AS DATE)";
        break;
      case "TIME(3)":
        value = "CAST(TO_TIMESTAMP_LTZ(v,3) AS TIME(3))";
        break;
      case "TIMESTAMP(9)":
        value = "CAST(rt AS TIMESTAMP(9))";
        break;
      case "TIMESTAMP_LTZ(9)":
        value = "CAST(rt AS TIMESTAMP_LTZ(9))";
        break;
      default:
        value = "CAST(v AS " + type + ")";
    }
    try (var metrics = new RecoveryExecutionMetrics()) {
      String sql =
          "SELECT k,"
              + (legacy ? "SESSION_END(rt,INTERVAL '5' SECOND)" : "window_end")
              + ",MIN("
              + value
              + "),MAX("
              + value
              + "),COUNT(*),MIN("
              + value
              + ") FILTER(WHERE v<0),MAX("
              + value
              + ") FILTER(WHERE v>3),COUNT(ARRAY[v,CAST(NULL AS BIGINT)]),SUM(v),AVG(CAST(v AS"
              + " DECIMAL(38,4))) "
              + (legacy
                  ? "FROM src GROUP BY k,SESSION(rt,INTERVAL '5' SECOND)"
                  : "FROM TABLE(SESSION(TABLE src PARTITION BY k,DESCRIPTOR(rt),INTERVAL '5'"
                      + " SECOND)) GROUP BY k,window_start,window_end");
      var comparison =
          NativeFailureParity.run(
              () -> {
                var table = FlinkLegacyUpdatingWindowSqlHarnessTest.environment(metrics);
                table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
                return table;
              },
              sql);
      if (type.equals("CHAR(8)")
          && !tech.streamfusion.compat.RuntimeCompat.supportsRetractingCharExtrema()) {
        comparison.assertFailure(
            org.apache.flink.table.api.TableException.class,
            NativeFailureParity.Phase.PLANNING,
            NativeFailureParity.Route.FALLBACK);
        return;
      }
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      assertTrue(comparison.nativeRun().plan().contains("NativeColumnarSessionWindowAggregate"));
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarSessionWindowAggregate");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "true,1",
    "true,2",
    "true,64",
    "false,1",
    "false,2",
    "false,64"
  })
  void arrivalOrderedBridgesKeepSignedCachedWinnersAndEmptyNamespaces(
      boolean legacy, int batchRows) {
    if (!legacy) tech.streamfusion.compat.FlinkTestCapabilities.requireSessionTableFunction();
    try (var metrics = new RecoveryExecutionMetrics()) {
      String sql =
          "SELECT k,"
              + (legacy
                  ? "SESSION_START(rt,INTERVAL '5' SECOND),SESSION_END(rt,INTERVAL '5' SECOND)"
                  : "window_start,window_end")
              + ",COUNT(*),MIN(v),MAX(v),SUM(v),AVG(v) "
              + (legacy
                  ? "FROM src GROUP BY k,SESSION(rt,INTERVAL '5' SECOND)"
                  : "FROM TABLE(SESSION(TABLE src PARTITION BY k,DESCRIPTOR(rt),INTERVAL '5'"
                        + " SECOND)) GROUP BY k,window_start,window_end");
      var comparison =
          NativeFailureParity.run(
              () -> {
                var env =
                    metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
                var table =
                    org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
                table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
                table
                    .getConfig()
                    .set("streamfusion.transpose.batchRows", Integer.toString(batchRows));
                var input = new java.util.ArrayList<org.apache.flink.types.Row>();
                long[][] rows = {
                  {3, 1, 0},
                  {0, 5, 10000},
                  {0, 9, 10000},
                  {0, 3, 5000},
                  {3, 3, 5000},
                  {0, 2, 30000},
                  {3, 5, 10000},
                  {0, 4, 20000},
                  {0, 7, 25000},
                  {0, 0, 15000},
                  {3, 9, 10000},
                  {0, 1, 15000}
                };
                for (var row : rows)
                  input.add(
                      org.apache.flink.types.Row.ofKind(
                          org.apache.flink.types.RowKind.fromByteValue((byte) row[0]),
                          1,
                          row[1],
                          java.time.LocalDateTime.ofInstant(
                              java.time.Instant.ofEpochMilli(row[2]), java.time.ZoneOffset.UTC)));
                var source =
                    env.fromCollection(
                            input,
                            org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(
                                new String[] {"k", "v", "rt"},
                                org.apache.flink.api.common.typeinfo.Types.INT,
                                org.apache.flink.api.common.typeinfo.Types.LONG,
                                org.apache.flink.api.common.typeinfo.Types.LOCAL_DATE_TIME))
                        .assignTimestampsAndWatermarks(
                            org.apache.flink.api.common.eventtime.WatermarkStrategy
                                .<org.apache.flink.types.Row>forBoundedOutOfOrderness(
                                    java.time.Duration.ofDays(1))
                                .withTimestampAssigner(
                                    (row, previous) ->
                                        ((java.time.LocalDateTime) row.getField(2))
                                            .toInstant(java.time.ZoneOffset.UTC)
                                            .toEpochMilli()));
                table.createTemporaryView(
                    "src",
                    table.fromChangelogStream(
                        source,
                        org.apache.flink.table.api.Schema.newBuilder()
                            .column("k", org.apache.flink.table.api.DataTypes.INT())
                            .column("v", org.apache.flink.table.api.DataTypes.BIGINT())
                            .column("rt", org.apache.flink.table.api.DataTypes.TIMESTAMP(3))
                            .watermark("rt", "SOURCE_WATERMARK()")
                            .build(),
                        org.apache.flink.table.connector.ChangelogMode.all()));
                return table;
              },
              sql);
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarSessionWindowAggregate");
    }
  }
}
