package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import tech.streamfusion.planner.NativePlanner;

class FlinkDateFormatExtendedYearSqlHarnessTest {
  private static final int ROWS = 5003;
  private static final int[] YEARS = {0, -1, 1, -10000, 9999, 10000, 262142, 262143,
      -262144, 300000, -300000, -200000000, 200000000, 1500, 1582, 1969, 1970, 2000};

  @TestFactory
  Stream<DynamicTest> numericPatternsPreserveJavaCalendarAndYearOfEra() {
    return IntStream.of(0, 3, 6, 9).mapToObj(precision -> DynamicTest.dynamicTest(
        "TIMESTAMP(" + precision + ")", () -> {
          String sql = "SELECT id,DATE_FORMAT(ts,'yyyy-MM-dd HH:mm:ss'),"
              + "DATE_FORMAT(ts,'yy-MM-dd'),DATE_FORMAT(ts,'HH:mm:ss % yyyy') FROM src";
          verify(precision, sql, ROWS);
          verify(precision, sql + " WHERE MOD(id,3)=1", 1668);
          verify(precision, sql + " WHERE id<0", 0);
        }));
  }

  static void verify(int precision, String sql, int count) throws Exception {
    verify(precision, sql, count, true);
  }

  static void verify(int precision, String sql, int count, boolean pure) throws Exception {
    Map<Integer, List<Object>> expected = null;
    Object expectedSchema = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.getConfig().setLocalTimeZone(java.time.ZoneId.of("America/New_York"));
        var type = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT().notNull()),
            DataTypes.FIELD("ts", DataTypes.TIMESTAMP(precision)));
        table.createTemporaryView("src", env.fromSequence(0, ROWS - 1).map(index -> {
          long divisor = (long) Math.pow(10, 9 - precision);
          int nanos = (int) (123456789 / divisor * divisor);
          TimestampData timestamp = index % 37 == 0 ? null
              : TimestampData.fromLocalDateTime(LocalDateTime.of(
                  YEARS[(int) (index % YEARS.length)], index % 2 == 0 ? 2 : 12,
                  index % 2 == 0 ? 3 : 31, 4, 5, 6, nanos));
          if (index % 53 == 1 && precision >= 3) {
            timestamp = TimestampData.fromEpochMillis(index % 2 == 0 ? Long.MIN_VALUE : Long.MAX_VALUE);
          }
          return (RowData) GenericRowData.of(index.intValue(), timestamp);
        }).returns(InternalTypeInfo.of((RowType) type.getLogicalType())));
        var scan = nativeRun ? NativePlanner.install(table) : null;
        var result = table.executeSql(sql);
        Map<Integer, List<Object>> actual = new TreeMap<>();
        try (var rows = result.collect()) {
          while (rows.hasNext()) {
            var row = rows.next();
            var fields = new ArrayList<Object>();
            for (int field = 1; field < row.getArity(); field++) fields.add(row.getField(field));
            assertNull(actual.put((Integer) row.getField(0), fields), "duplicate input id");
          }
        }
        assertEquals(count, actual.size());
        if (nativeRun) {
          assertEquals(expectedSchema, result.getResolvedSchema());
          assertEquals(expected, actual);
          assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
          assertFalse(scan.functionExecutions().isEmpty());
          assertTrue(scan.functionExecutions().stream().allMatch(entry -> entry.contains("DATE_FORMAT")),
              scan.functionExecutions().toString());
          assertEquals(pure,
              scan.functionExecutions().stream().allMatch(entry -> entry.contains("jvm=[]")),
              scan.functionExecutions().toString());
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        } else {
          expected = actual;
          expectedSchema = result.getResolvedSchema();
        }
      }
    }
  }
}
