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

class FlinkIntegralEpochSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"TINYINT,true,UTC", "TINYINT,false,America/New_York",
      "SMALLINT,true,UTC", "SMALLINT,false,America/New_York",
      "INT,true,UTC", "INT,false,America/New_York",
      "BIGINT,true,UTC", "BIGINT,false,America/New_York"})
  void directIntegralSecondsAndMillisConsumeNativeWork(String type, boolean nullable, String zone)
      throws Exception {
    verify(type, nullable, zone, "SELECT id,TO_TIMESTAMP_LTZ(n,0),TO_TIMESTAMP_LTZ(n,3),"
        + "TO_TIMESTAMP_LTZ(n,0) IS NULL, CASE WHEN MOD(id,3)=0 THEN TO_TIMESTAMP_LTZ(n,3) "
        + "ELSE CAST(NULL AS TIMESTAMP_LTZ(3)) END FROM src", true);
    verify(type, nullable, zone,
        "SELECT id,TO_TIMESTAMP_LTZ(n,0),TO_TIMESTAMP_LTZ(n,3) FROM src WHERE id<0", true);
  }

  @ParameterizedTest
  @CsvSource({"TINYINT", "SMALLINT", "INT"})
  void unverifiedComputedOperandsKeepTheirGeneratedRoute(String type) throws Exception {
    verify(type, true, "UTC", "SELECT id,TO_TIMESTAMP_LTZ(n/CAST(2 AS " + type + "),3),"
        + "TO_TIMESTAMP_LTZ(n/CAST(2 AS " + type + "),0) FROM src", false);
  }

  static void verify(String name, boolean nullable, String zone, String sql, boolean pure)
      throws Exception {
    verify(name, nullable, zone, sql, pure, false);
  }

  static void verify(String name, boolean nullable, String zone, String sql, boolean pure,
      boolean legacy) throws Exception {
    DataType number;
    switch (name) {
      case "TINYINT": number = TINYINT(); break;
      case "SMALLINT": number = SMALLINT(); break;
      case "INT": number = INT(); break;
      case "BIGINT": number = BIGINT(); break;
      default: throw new IllegalArgumentException(name);
    }
    var type = ROW(FIELD("id", INT().notNull()), FIELD("n", nullable ? number : number.notNull()));
    TreeMap<Integer, List<Object>> expected = null;
    List<DataType> expectedTypes = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.getConfig().setLocalTimeZone(ZoneId.of(zone));
        table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
        table.createTemporaryView("src", env.fromSequence(0, 5002)
            .map(index -> (RowData) GenericRowData.of(index.intValue(), nullable && index % 37 == 0
                ? null : value(name, index.intValue())))
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
          assertEquals(expected, actual, name + " " + sql);
          assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
          assertFalse(scan.functionExecutions().isEmpty(), scan.explainSummary());
          assertEquals(pure, scan.functionExecutions().stream().allMatch(s -> s.contains("jvm=[]")),
              scan.explainSummary());
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        } else expected = actual;
      }
    }
  }

  private static Number value(String type, int index) {
    long[] boundaries = {Long.MIN_VALUE, Long.MAX_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE,
        Short.MIN_VALUE, Short.MAX_VALUE, Byte.MIN_VALUE, Byte.MAX_VALUE,
        -62167219200001L, -62167219200000L, -62167219199999L,
        253402300799998L, 253402300799999L, 253402300800000L,
        -62167219201L, -62167219200L, -62167219199L,
        253402300798L, 253402300799L, 253402300800L,
        Long.MIN_VALUE / 1000 - 1, Long.MAX_VALUE / 1000 + 1,
        -86400001, -1001, -1000, -999, -1, 0, 1, 999, 1000, 1001, 951827696, 1706659200};
    long value = index < boundaries.length ? boundaries[index]
        : (index % 2 == 0 ? -1L : 1L) * index * 7919;
    switch (type) {
      case "TINYINT": return (byte) value;
      case "SMALLINT": return (short) value;
      case "INT": return (int) value;
      case "BIGINT": return value;
      default: throw new IllegalArgumentException(type);
    }
  }
}
