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
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.NativePlanner;

class FlinkDateComparisonSqlHarnessTest {
  @Test
  void leafComparisonsPreserveNullsAndTheFullSignedDayRange() throws Exception {
    int[] days = {Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -719162, -141438, -1, 0, 1,
        11016, 2932896, Integer.MAX_VALUE - 1, Integer.MAX_VALUE};
    var type = ROW(FIELD("id", INT().notNull()), FIELD("a", DATE()), FIELD("b", DATE()));
    int count = 5003;
    String sql = "SELECT id,a=b,a<>b,a<b,a<=b,a>b,a>=b,a<DATE '1970-01-01',"
        + "DATE '1970-01-01'>=b FROM dates";
    TreeMap<Integer, List<Object>> expected = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.getConfig().set("streamfusion.native.enabled", Boolean.toString(nativeRun));
        table.createTemporaryView("dates", env.fromSequence(0, count - 1)
            .map(index -> (RowData) GenericRowData.of(index.intValue(),
                index % 17 == 0 ? null : days[index.intValue() % days.length],
                index % 13 == 0 ? null : days[(index.intValue() / days.length) % days.length]))
            .returns(InternalTypeInfo.of((RowType) type.getLogicalType())));
        var scan = nativeRun ? NativePlanner.install(table) : null;
        if (nativeRun) assertFalse(table.explainSql(sql).contains("jvmExpressions"));
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
          assertEquals(expected, actual);
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        } else {
          assertTrue(metrics.nativeOperators().isEmpty(), metrics.nativeOperators().toString());
          expected = actual;
        }
      }
    }
  }
}
