package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class FlinkPrimitivePrintfLengthSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> primitiveAsciiLengthsConsumeNativeWork() {
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
          for (String length : List.of("CHAR_LENGTH", "CHARACTER_LENGTH")) {
            for (String format : List.of("", "literal:%%", "%s", "%1$b/%1$S/%<B",
                "%1$256.2S/%2$+020d", "%1$-8.3b/%2$08X")) {
              expressions.add(length + "(PRINTF('" + format + "',n,id))");
            }
            expressions.add(length + "(PRINTF('%1$s/%2$b',n,TRY_CAST(id AS BOOLEAN)))");
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
        String sql = "SELECT id,CHAR_LENGTH(PRINTF('%1$b/%1$S',TRY_CAST(d AS BOOLEAN))),"
            + "CHARACTER_LENGTH(PRINTF('%1$8.2S/%2$+08d',d<e,id)),"
            + "CHAR_LENGTH(PRINTF('%s',d IS NULL)) FROM src";
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
    cases.add(DynamicTest.dynamicTest("unqualified formats and failing or lazy producers keep their routes", () -> {
      FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
          "SELECT id,CHAR_LENGTH(PRINTF('%c',n)),CHARACTER_LENGTH(PRINTF('%+b',n)),"
              + "CHAR_LENGTH(PRINTF('%s',CAST(n AS STRING))),"
              + "CHAR_LENGTH(PRINTF(CASE WHEN id<1000 THEN '%d' ELSE '%s' END,n)),"
              + "CHAR_LENGTH(PRINTF('%0$d',n)),CHAR_LENGTH(PRINTF('%257d',n)),"
              + "CHAR_LENGTH(PRINTF('" + "a".repeat(513) + "%s',n)),"
              + "CHAR_LENGTH(PRINTF('\u00e9%s',n)),"
              + "CHAR_LENGTH(PRINTF(CAST(NULL AS STRING),n)) FROM src", false);
      FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
          "SELECT id,CHAR_LENGTH(PRINTF('%s',n/0)) FROM src WHERE id<0", false);
      FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
          "SELECT id,CASE WHEN id>=0 THEN CHAR_LENGTH(PRINTF('%s',n)) "
              + "ELSE CHAR_LENGTH(PRINTF('%s',n/0)) END FROM src", false);
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(18, 3, true, false,
          "SELECT id,CHAR_LENGTH(PRINTF('%s',d+d<e)) FROM src", false);
    }));
    cases.add(DynamicTest.dynamicTest("a catalog function named CHAR_LENGTH keeps its implementation", () -> {
      BuiltinFunctionParity.assertParity(() -> {
        var table = BuiltinFunctionParity.environment(
            DataTypes.ROW(DataTypes.FIELD("n", DataTypes.INT())),
            List.of(Row.of(0), Row.of(-1), Row.of(Integer.MIN_VALUE), Row.of((Object) null)));
        table.createTemporaryFunction("CHAR_LENGTH", new CustomLength());
        return table;
      }, "SELECT `default_catalog`.`default_database`.`CHAR_LENGTH`(PRINTF('%s',n)) FROM src");
    }));
    return cases.stream();
  }

  public static class CustomLength extends ScalarFunction {
    public Integer eval(String value) {
      return value == null ? -123 : 123;
    }
  }
}
