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
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkEpochMillisSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"UTC,true", "UTC,false", "Asia/Shanghai,true", "Asia/Shanghai,false",
      "GMT+05:30,true", "GMT+05:30,false", "America/New_York,true", "America/New_York,false"})
  void directMillisKeepReleasedBoundsNullsAndSessionZoneIndependence(String zone, boolean nullable)
      throws Exception {
    verify(zone, nullable, "SELECT id,TO_TIMESTAMP_LTZ(n,3),"
        + "TO_TIMESTAMP_LTZ(n,3) IS NULL FROM src");
  }

  @ParameterizedTest
  @CsvSource({"UTC,true", "UTC,false", "Asia/Shanghai,true", "Asia/Shanghai,false",
      "GMT+05:30,true", "GMT+05:30,false"})
  void nativeCalendarConsumersKeepInvalidValuesNull(String zone, boolean nullable) throws Exception {
    verify(zone, nullable, "SELECT id,YEAR(TO_TIMESTAMP_LTZ(n,3)),"
        + "MONTH(TO_TIMESTAMP_LTZ(n,3)),SECOND(TO_TIMESTAMP_LTZ(n,3)) FROM src");
  }

  @ParameterizedTest
  @CsvSource({"UTC,true", "UTC,false"})
  void conditionalSelectionKeepsTypedNulls(String zone, boolean nullable) throws Exception {
    verify(zone, nullable, "SELECT id,CASE WHEN MOD(id,3)=0 THEN CAST(NULL AS TIMESTAMP_LTZ(3)) "
        + "ELSE TO_TIMESTAMP_LTZ(n,3) END FROM src");
    verify(zone, nullable, "SELECT id,TO_TIMESTAMP_LTZ(n,3) FROM src WHERE id<0");
  }

  @ParameterizedTest
  @CsvSource({"UTC,true", "UTC,false"})
  void otherUnitsAndComputedInputsKeepTheirExistingEvaluators(String zone, boolean nullable)
      throws Exception {
    verify(zone, nullable, "SELECT id,TO_TIMESTAMP_LTZ(n,0) FROM src", false);
    verify(zone, nullable, "SELECT id,TO_TIMESTAMP_LTZ(n+CAST(1 AS BIGINT),3) FROM src", false);
  }

  private static void verify(String zone, boolean nullable, String sql) throws Exception {
    verify(zone, nullable, sql, true);
  }

  private static void verify(String zone, boolean nullable, String sql, boolean pure) throws Exception {
    var type = ROW(FIELD("id", INT().notNull()), FIELD("n", nullable ? BIGINT() : BIGINT().notNull()));
    long[] millis = {Long.MIN_VALUE, Long.MAX_VALUE, -62167219200001L, -62167219200000L,
        -62167219199999L, 253402300799999L, 253402300800000L, -62135596800000L,
        -12219292800000L, -86400001, -1, 0, 1, 951827696123L, 1706659200000L};
    int count = 5003;
    TreeMap<Integer, List<Object>> expected = null;
    List<DataType> expectedTypes = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.getConfig().setLocalTimeZone(ZoneId.of(zone));
        table.createTemporaryView("src", env.fromSequence(0, count - 1)
            .map(index -> (RowData) GenericRowData.of(index.intValue(), nullable && index % 37 == 0
                ? null : millis[index.intValue() % millis.length]))
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
        assertEquals(sql.contains("WHERE id<0") ? 0 : count, actual.size());
        if (nativeRun) {
          assertEquals(expected, actual, zone + " nullable=" + nullable + " " + sql);
          assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
          assertFalse(scan.functionExecutions().isEmpty(), scan.explainSummary());
          assertEquals(pure, scan.functionExecutions().stream().allMatch(s -> s.contains("jvm=[]")),
              scan.explainSummary());
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        } else expected = actual;
      }
    }
  }
}
