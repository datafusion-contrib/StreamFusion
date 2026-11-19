package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.ZoneId;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkUnnestTemporalSqlHarnessTest {
  @Test
  void epochMillisConstructorKeepsSqlBoundsAndNativeEvaluation() {
    String sql =
        "SELECT k, e FROM src CROSS JOIN UNNEST(ARRAY[TO_TIMESTAMP_LTZ(v, 3),"
            + " TO_TIMESTAMP_LTZ(v, 3)]) AS u(e)";
    var comparison = NativeFailureParity.run(FlinkUnnestTemporalSqlHarnessTest::epochSource, sql);
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    var table = epochSource();
    var scan = NativePlanner.install(table);
    table.explainSql(sql);
    assertTrue(
        scan.functionExecutions().stream()
            .anyMatch(
                execution ->
                    execution.contains("NativeUnnestOperand")
                        && execution.contains("TO_TIMESTAMP_LTZ")
                        && execution.contains("jvm=[]")),
        scan.functionExecutions().toString());
  }

  @ParameterizedTest
  @CsvSource({
    "UTC,DATE",
    "UTC,TIME(0)",
    "Etc/UTC,DATE",
    "Etc/UTC,TIME(0)",
    "America/New_York,DATE",
    "America/New_York,TIME(0)",
    "Europe/Berlin,DATE",
    "Europe/Berlin,TIME(0)",
    "Pacific/Apia,DATE",
    "Pacific/Apia,TIME(0)",
    "Asia/Kathmandu,DATE",
    "Asia/Kathmandu,TIME(0)",
    "+05:30,DATE",
    "+05:30,TIME(0)"
  })
  void utcCastsKeepReleasedValuesAndPrecision(String zone, String target) {
    String sql =
        "SELECT k, e FROM src CROSS JOIN UNNEST(ARRAY[CAST(v AS "
            + target
            + "), CAST(v AS "
            + target
            + ")]) AS u(e)";
    var comparison =
        NativeFailureParity.run(() -> timestampSource(zone, !target.equals("DATE")), sql);
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    var table = timestampSource(zone, !target.equals("DATE"));
    var scan = NativePlanner.install(table);
    table.explainSql(sql);
    boolean utc = ZoneId.of(zone).normalized().equals(java.time.ZoneOffset.UTC);
    assertTrue(
        scan.functionExecutions().stream()
            .anyMatch(
                execution ->
                    execution.contains("NativeUnnestOperand")
                        && execution.contains("jvm=[]") == utc),
        scan.functionExecutions().toString());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"UTC", "Etc/UTC", "America/New_York"})
  void extremeDateRetainsReleasedExternalConversionFailure(String zone) {
    var comparison =
        NativeFailureParity.run(
            () -> timestampSource(zone, true),
            "SELECT k, e FROM src CROSS JOIN UNNEST(ARRAY[CAST(v AS DATE), CAST(v AS DATE)]) AS"
                + " u(e)");
    comparison.assertFailure(
        java.time.DateTimeException.class,
        "MonthOfYear",
        NativeFailureParity.Phase.COLLECTION,
        NativeFailureParity.Route.NATIVE);
    assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
  }

  @Test
  void invalidDynamicPrecisionRetainsHostFailure() {
    var comparison =
        NativeFailureParity.run(
            FlinkUnnestTemporalSqlHarnessTest::epochSource,
            "SELECT e FROM src CROSS JOIN UNNEST(ARRAY[TO_TIMESTAMP_LTZ(v, 3),"
                + " TO_TIMESTAMP_LTZ(v, CAST(k AS INT))]) AS u(e)");
    comparison.assertFailure(
        org.apache.flink.table.api.TableException.class,
        "precision",
        NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment epochSource() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    long[] values = {
      Long.MIN_VALUE,
      -62_167_219_200_001L,
      -62_167_219_200_000L,
      -1,
      1,
      253_402_300_799_999L,
      253_402_300_800_000L,
      Long.MAX_VALUE,
      java.time.Instant.parse("2026-03-08T07:00:00Z").toEpochMilli() - 1,
      java.time.Instant.parse("2026-03-08T07:00:00Z").toEpochMilli(),
      java.time.Instant.parse("2026-11-01T06:00:00Z").toEpochMilli(),
      java.time.Instant.parse("2026-11-01T06:00:00Z").toEpochMilli() - 1,
      java.time.Instant.parse("2011-12-30T10:00:00Z").toEpochMilli(),
      0
    };
    var source =
        env.fromSequence(0, values.length - 1)
            .map(i -> Row.of(i, i == values.length - 1 ? null : values[i.intValue()]))
            .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG));
    table.createTemporaryView("src", source);
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    return table;
  }

  private static TableEnvironment timestampSource(String zone, boolean fullRange) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneId.of(zone));
    long[] values = {
      fullRange ? Long.MIN_VALUE : -62_167_219_200_000L,
      -86_400_001,
      -1,
      1,
      86_400_001,
      fullRange ? Long.MAX_VALUE : 253_402_300_799_999L,
      java.time.Instant.parse("2026-03-08T07:00:00Z").toEpochMilli() - 1,
      java.time.Instant.parse("2026-03-08T07:00:00Z").toEpochMilli(),
      java.time.Instant.parse("2026-11-01T06:00:00Z").toEpochMilli(),
      java.time.Instant.parse("2026-11-01T06:00:00Z").toEpochMilli() - 1,
      java.time.Instant.parse("2011-12-30T10:00:00Z").toEpochMilli(),
      0
    };
    var type =
        RowType.of(
            new LogicalType[] {new BigIntType(), new LocalZonedTimestampType(9)},
            new String[] {"k", "v"});
    var source =
        env.fromSequence(0, values.length - 1)
            .map(
                i ->
                    (RowData)
                        GenericRowData.of(
                            i,
                            i == values.length - 1
                                ? null
                                : TimestampData.fromEpochMillis(values[i.intValue()], 987654)))
            .returns(InternalTypeInfo.of(type));
    table.createTemporaryView("src", source);
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    return table;
  }
}
