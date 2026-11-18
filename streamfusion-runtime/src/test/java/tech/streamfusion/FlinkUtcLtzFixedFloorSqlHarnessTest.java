package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
import tech.streamfusion.compat.FlinkCompat;
import tech.streamfusion.planner.NativePlanner;

class FlinkUtcLtzFixedFloorSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"UTC,0", "UTC,3", "UTC,9", "Z,9", "Etc/UTC,3", "Etc/GMT0,9",
      "Asia/Shanghai,9", "GMT+05:30,9", "America/New_York,9", "Europe/London,9"})
  void fixedUnitsPreserveSignedMillisFractionsNullsAndZoneAdmission(String zone, int precision)
      throws Exception {
    String query = "SELECT id,FLOOR(ts TO DAY),FLOOR(ts TO HOUR),"
        + (FlinkCompat.supportsSubHourLtzRounding()
            ? "FLOOR(ts TO MINUTE),FLOOR(ts TO SECOND),FLOOR(ts TO MILLISECOND)," : "")
        + "YEAR(FLOOR(ts TO DAY)),"
        + "FLOOR(ts TO HOUR) IS NULL,CASE WHEN MOD(id,3)=0 THEN FLOOR(ts TO HOUR) "
        + "ELSE CAST(NULL AS TIMESTAMP_LTZ(" + precision + ")) END FROM src";
    verify(zone, precision, query);
    verify(zone, precision, query + " WHERE id<0");
  }

  @Test
  void unsupportedReleasedSubHourFormsKeepTheirCodeGenerationFailure() throws Exception {
    if (FlinkCompat.supportsSubHourLtzRounding()) return;
    for (String unit : List.of("MINUTE", "SECOND", "MILLISECOND")) {
      NativeFailureParity.run(FlinkUtcLtzFixedFloorSqlHarnessTest::failureEnvironment,
          "SELECT FLOOR(ts TO " + unit + ") FROM src")
          .assertFailure(org.codehaus.commons.compiler.CompileException.class,
              "No applicable constructor/method", NativeFailureParity.Phase.INITIALIZATION,
              NativeFailureParity.Route.FALLBACK);
    }
  }

  private static TableEnvironment failureEnvironment() {
    var table = BuiltinFunctionParity.environment(ROW(FIELD("ts", TIMESTAMP_LTZ(9))),
        List.of(org.apache.flink.types.Row.of(java.time.Instant.ofEpochMilli(-1))));
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    return table;
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 3, 9})
  void plainTimestampScalarWidthsKeepFractionsOverflowAndNativeConsumption(int precision)
      throws Exception {
    var outputs = new ArrayList<String>();
    for (String unit : List.of("DAY", "HOUR", "MINUTE", "SECOND", "MILLISECOND")) {
      outputs.add("FLOOR(ts TO " + unit + ")");
      outputs.add("CEIL(ts TO " + unit + ")");
    }
    String sql = "SELECT id," + String.join(",", outputs) + " FROM src";
    for (String zone : List.of("UTC", "America/New_York")) {
      verify(zone, precision, sql, true);
      verify(zone, precision, sql + " WHERE id<0", true);
    }
  }

  private static void verify(String zone, int precision, String sql) throws Exception {
    verify(zone, precision, sql, false);
  }

  private static void verify(String zone, int precision, String sql, boolean plain) throws Exception {
    var type = ROW(FIELD("id", INT().notNull()),
        FIELD("ts", plain ? TIMESTAMP(precision) : TIMESTAMP_LTZ(precision)));
    long[] millis = {Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MIN_VALUE + 86400000,
        -62135596800000L, -12219292800000L, -2208988800000L,
        -86400001, -86400000, -86399999, -3600001, -3600000, -3599999,
        -60001, -60000, -59999, -1001, -1000, -999, -1, 0, 1, 999, 1000, 1001,
        59999, 60000, 60001, 3599999, 3600000, 3600001, 86399999, 86400000, 86400001,
        951827696123L, 1706659200000L, 1710037800000L, 253402300799999L,
        Long.MAX_VALUE - 86400000, Long.MAX_VALUE - 1, Long.MAX_VALUE};
    TreeMap<Integer, List<Object>> expected = null;
    List<DataType> expectedTypes = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.getConfig().setLocalTimeZone(ZoneId.of(zone));
        table.createTemporaryView("src", env.fromSequence(0, 5002)
            .map(index -> (RowData) GenericRowData.of(index.intValue(), index % 37 == 0 ? null
                : TimestampData.fromEpochMillis(millis[index.intValue() % millis.length],
                    (index.intValue() * 7919) % 1000000)))
            .returns(InternalTypeInfo.of((RowType) type.getLogicalType())));
        var scan = nativeRun ? NativePlanner.install(table) : null;
        var types = table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
        if (nativeRun) assertEquals(expectedTypes, types);
        else expectedTypes = types;
        var actual = new TreeMap<Integer, List<Object>>();
        try (var output = table.executeSql(sql).collect()) {
          while (output.hasNext()) {
            var row = output.next();
            var fields = new ArrayList<Object>();
            for (int i = 1; i < row.getArity(); i++) fields.add(row.getField(i));
            assertNull(actual.put((Integer) row.getField(0), fields));
          }
        }
        assertEquals(sql.contains("WHERE id<0") ? 0 : 5003, actual.size());
        if (nativeRun) {
          assertEquals(expected, actual, zone + " " + precision);
          assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
          assertFalse(scan.functionExecutions().isEmpty(), scan.explainSummary());
          boolean pure = plain || ZoneId.of(zone).normalized().equals(java.time.ZoneOffset.UTC);
          assertEquals(pure, scan.functionExecutions().stream().allMatch(s -> s.contains("jvm=[]")),
              scan.explainSummary());
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        } else expected = actual;
      }
    }
  }
}
