package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import tech.streamfusion.planner.NativePlanner;

class FlinkPrimitivePrintfComparisonSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> provenAsciiComparisonsConsumeNativeWork() {
    try {
      BuiltInFunctionDefinitions.class.getField("PRINTF");
    } catch (NoSuchFieldException absent) {
      return new FlinkIntegerPrintfSqlHarnessTest().releasedPrintfContract();
    }
    var cases = new ArrayList<DynamicTest>();
    for (String type : List.of("BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT")) {
      for (boolean nullable : new boolean[] {false, true}) {
        cases.add(DynamicTest.dynamicTest(type + " nullable=" + nullable, () -> {
          var expressions = new ArrayList<String>();
          for (String comparison : List.of("=", "<>", "<", "<=", ">", ">=")) {
            expressions.add("PRINTF('%s',n)" + comparison + "'-1'");
            expressions.add("' 0'" + comparison + "PRINTF('%1$8.2S/%2$08x',n,id)");
            expressions.add("PRINTF('%b',n)" + comparison + "PRINTF('%s',n)");
            expressions.add("PRINTF('%s',n)" + comparison + "CAST(NULL AS STRING)");
            expressions.add("PRINTF('literal:%%',n)" + comparison + "''");
          }
          String sql = "SELECT id," + String.join(",", expressions) + " FROM src";
          FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql, true);
          FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql + " WHERE id<0", true);
        }));
      }
    }
    for (int[] type : List.of(new int[] {4, 2}, new int[] {18, 18},
        new int[] {38, 18}, new int[] {38, 38})) {
      cases.add(DynamicTest.dynamicTest("DECIMAL(" + type[0] + "," + type[1] + ")", () -> {
        var expressions = new ArrayList<String>();
        for (String comparison : List.of("=", "<>", "<", "<=", ">", ">=")) {
          expressions.add("PRINTF('%s',TRY_CAST(d AS BOOLEAN))" + comparison + "'false'");
          expressions.add("PRINTF('%1$8.2S/%2$+08d',d<e,id)" + comparison
              + "PRINTF('%s',e IS NULL)");
        }
        String sql = "SELECT id," + String.join(",", expressions) + " FROM src";
        for (boolean nullable : new boolean[] {false, true}) {
          for (boolean legacy : new boolean[] {false, true}) {
            FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
                type[0], type[1], nullable, legacy, sql, true);
            FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
                type[0], type[1], nullable, legacy, sql + " WHERE id<0", true);
          }
        }
      }));
    }
    cases.add(DynamicTest.dynamicTest("unverified strings formats and failing producers retain their routes", () -> {
      FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
          "SELECT id,PRINTF('%c',n)<'?',PRINTF('%s',n)<'\ue000',"
              + "PRINTF('%s',n)=CAST(n AS STRING),"
              + "PRINTF('%s',n)<PRINTF(CASE WHEN id<1000 THEN '%d' ELSE '%s' END,n),"
              + "PRINTF('%s',n)=PRINTF('%0$d',n) FROM src", false, false,
          "string representation before Arrow is not proven Java-backed");
      FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
          "SELECT id,PRINTF('%s',n/0)='0' FROM src WHERE id<0", false);
      FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
          "SELECT id,CASE WHEN id>=0 THEN PRINTF('%s',n)='0' "
              + "ELSE PRINTF('%s',n/0)='0' END FROM src", false);
      BuiltinFunctionParity.assertParity(() -> BuiltinFunctionParity.environment(
          DataTypes.ROW(DataTypes.FIELD("n", DataTypes.INT()), DataTypes.FIELD("s", DataTypes.STRING())),
          List.of(Row.of(63, "?"), Row.of(63, "\ud800"), Row.of(63, "\udc00"),
              Row.of(0, "\ue000"), Row.of(0, "\ud83d\ude00"), Row.of(null, null))),
          "SELECT PRINTF('%s',n)=s,PRINTF('%s',n)<>s,PRINTF('%s',n)<s,"
              + "PRINTF('%s',n)>=s FROM src");
    }));
    cases.add(DynamicTest.dynamicTest("a runtime native predicate filters formatted values", this::verifyFilter));
    return cases.stream();
  }

  private void verifyFilter() throws Exception {
    TreeMap<Integer, String> expected = null;
    var type = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT().notNull()),
        DataTypes.FIELD("n", DataTypes.INT()));
    String sql = "SELECT id,PRINTF('%s',n) FROM src WHERE PRINTF('%s',n)>='0'";
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.createTemporaryView("src", env.fromSequence(0, 5002)
            .map(i -> (RowData) GenericRowData.of(i.intValue(), i % 7 == 0 ? null : (int) (i % 7) - 3))
            .returns(InternalTypeInfo.of((RowType) type.getLogicalType())));
        var scan = nativeRun ? NativePlanner.install(table) : null;
        var actual = new TreeMap<Integer, String>();
        try (var output = table.executeSql(sql).collect()) {
          while (output.hasNext()) {
            var row = output.next();
            assertNull(actual.put((Integer) row.getField(0), (String) row.getField(1)));
          }
        }
        assertTrue(actual.size() > 1000 && actual.size() < 4500, actual.size() + " rows");
        if (nativeRun) {
          assertEquals(expected, actual);
          assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
          assertFalse(scan.functionExecutions().isEmpty(), scan.explainSummary());
          assertTrue(scan.functionExecutions().stream().allMatch(s -> s.contains("jvm=[]")),
              scan.explainSummary());
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        } else expected = actual;
      }
    }
  }
}
