package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkLegacyUpdatingWindowSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
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
        "TIMESTAMP_LTZ(9)"
      })
  void typedSignedAggregatesRetainNullGroupsFiltersAndArbitraryFixedGrids(String type) {
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
      for (String window :
          List.of(
              "TUMBLE(rt,INTERVAL '5' SECOND)",
              "HOP(rt,INTERVAL '5' SECOND,INTERVAL '10' SECOND)",
              "HOP(rt,INTERVAL '5' SECOND,INTERVAL '7' SECOND)",
              "HOP(rt,INTERVAL '7' SECOND,INTERVAL '5' SECOND)")) {
        var previousJobs =
            metrics.nativeOperators().stream()
                .map(row -> row.get("jobId"))
                .collect(Collectors.toSet());
        String end = window.replaceFirst("\\(", "_END(");
        String sql =
            "SELECT k,"
                + end
                + ",MIN("
                + value
                + "),MAX("
                + value
                + "),COUNT(*),"
                + "MIN("
                + value
                + ") FILTER(WHERE v<0),COUNT(ARRAY[v,CAST(NULL AS BIGINT)]),"
                + "SUM(v),AVG(CAST(v AS DECIMAL(38,4))) FROM src GROUP BY k,"
                + window;
        var comparison = NativeFailureParity.run(() -> environment(metrics), sql);
        if (type.equals("CHAR(8)")
            && !tech.streamfusion.compat.RuntimeCompat.supportsRetractingCharExtrema()) {
          comparison.assertFailure(org.apache.flink.table.api.TableException.class,
              NativeFailureParity.Phase.PLANNING, NativeFailureParity.Route.FALLBACK);
          continue;
        }
        comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(
            metrics.nativeOperators().stream()
                .filter(row -> !previousJobs.contains(row.get("jobId")))
                .collect(Collectors.toList()),
            "NativeColumnarWindowAggregate");
      }
    }
  }

  static TableEnvironment environment(RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    var source =
        env.fromCollection(
                List.of(
                    row(RowKind.INSERT, 1, 3L, 1000),
                    row(RowKind.INSERT, 1, 5L, 1500),
                    row(RowKind.INSERT, 2, null, 1000),
                    row(RowKind.DELETE, 1, 3L, 1000),
                    row(RowKind.UPDATE_BEFORE, 1, 5L, 1500),
                    row(RowKind.UPDATE_AFTER, 1, 4L, 2000),
                    row(RowKind.INSERT, 3, 8L, 4000),
                    row(RowKind.DELETE, 3, 8L, 4000),
                    row(RowKind.INSERT, null, 6L, 1000),
                    row(RowKind.DELETE, null, 6L, 1000),
                    row(RowKind.INSERT, null, 7L, 1000),
                    row(RowKind.DELETE, 4, 99L, 1000),
                    row(RowKind.INSERT, 5, 12L, 6000)),
                Types.ROW_NAMED(
                    new String[] {"k", "v", "rt"}, Types.INT, Types.LONG, Types.LOCAL_DATE_TIME))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner(
                        (row, previous) ->
                            ((LocalDateTime) row.getField(2))
                                .toInstant(ZoneOffset.UTC)
                                .toEpochMilli()));
    table.createTemporaryView(
        "src",
        table.fromChangelogStream(
            source,
            Schema.newBuilder()
                .column("k", DataTypes.INT())
                .column("v", DataTypes.BIGINT())
                .column("rt", DataTypes.TIMESTAMP(3))
                .watermark("rt", "SOURCE_WATERMARK()")
                .build(),
            ChangelogMode.all()));
    return table;
  }

  private static Row row(RowKind kind, Integer key, Long value, long millis) {
    return Row.ofKind(
        kind,
        key,
        value,
        LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(millis), ZoneOffset.UTC));
  }
}
