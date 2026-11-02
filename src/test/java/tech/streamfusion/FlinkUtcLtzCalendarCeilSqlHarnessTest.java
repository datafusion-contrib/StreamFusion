package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkUtcLtzCalendarCeilSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"UTC,3", "UTC,9", "Z,3", "Z,9", "Etc/UTC,3", "Etc/UTC,9",
      "Etc/GMT0,3", "Etc/GMT0,9", "Asia/Shanghai,3", "Asia/Shanghai,9",
      "GMT+05:30,3", "GMT+05:30,9", "America/New_York,3", "America/New_York,9",
      "Europe/London,3", "Europe/London,9"})
  void calendarCeilKeepsFullSignedMillisNullsAndZeroOffsetAdmission(String zone, int precision)
      throws Exception {
    var type = ROW(FIELD("id", INT().notNull()), FIELD("ts", TIMESTAMP_LTZ(precision)));
    long[] millis = {Long.MIN_VALUE, Long.MIN_VALUE + 1, -62135596800000L, -12219292800000L,
        -2208988800000L, -86400001, -86400000, -86399999, -1000, -1, 0, 1, 1000,
        951827696123L, 1706659200000L, 1710037800000L, 253402300799999L,
        Long.MAX_VALUE - 1, Long.MAX_VALUE};
    int count = 5003;
    String sql = "SELECT id,CEIL(ts TO MONTH),CEIL(ts TO YEAR),"
        + "YEAR(CEIL(ts TO MONTH)),CEIL(ts TO YEAR) IS NULL FROM src";
    TreeMap<Integer, List<Object>> expected = null;
    List<DataType> expectedTypes = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.getConfig().setLocalTimeZone(ZoneId.of(zone));
        table.createTemporaryView("src", env.fromSequence(0, count - 1)
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
        assertEquals(count, actual.size());
        if (nativeRun) {
          for (var entry : actual.entrySet()) {
            assertEquals(expected.get(entry.getKey()), entry.getValue(), zone + " id=" + entry.getKey());
          }
          assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
          assertFalse(scan.functionExecutions().isEmpty(), scan.explainSummary());
          boolean pure = ZoneId.of(zone).normalized().equals(java.time.ZoneOffset.UTC);
          assertEquals(pure, scan.functionExecutions().stream().allMatch(s -> s.contains("jvm=[]")),
              scan.explainSummary());
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        } else expected = actual;
      }
    }
  }
}
