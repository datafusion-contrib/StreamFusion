package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

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
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkDateClockExtractSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"HOUR"})
  void dateClockFieldsKeepReleasedSignedIntegerArithmetic(String unit) throws Exception {
    var type = ROW(FIELD("id", INT().notNull()), FIELD("d", DATE()));
    int[] days = {Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -86400001, -86400000, -86399999,
        -3600001, -3600000, -3599999, -60001, -60000, -59999, -1001, -1000, -999, -1,
        0, 1, 999, 1000, 1001, 59999, 60000, 60001, 3599999, 3600000, 3600001,
        86399999, 86400000, 86400001, Integer.MAX_VALUE - 1, Integer.MAX_VALUE, 19753,
        -719162, -141438, 2932896};
    int count = 5003;
    String expression = unit + "(d)";
    String sql = "SELECT id," + expression + ",EXTRACT(" + unit + " FROM d),"
        + expression + " IS NULL," + expression + "<0 FROM src";
    TreeMap<Integer, List<Object>> expected = null;
    List<DataType> expectedTypes = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.createTemporaryView("src", env.fromSequence(0, count - 1)
            .map(index -> (RowData) GenericRowData.of(index.intValue(),
                index % 37 == 0 ? null : days[index.intValue() % days.length]))
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
          assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
          assertTrue(scan.functionExecutions().stream().anyMatch(s -> s.contains("EXTRACT")),
              scan.explainSummary());
          assertTrue(scan.functionExecutions().stream().allMatch(s -> s.contains("jvm=[]")),
              scan.explainSummary());
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
          for (var entry : actual.entrySet()) {
            assertEquals(expected.get(entry.getKey()), entry.getValue(), unit + " id=" + entry.getKey());
          }
        } else expected = actual;
      }
    }
  }
}
