package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class FlinkIntegerTextPrintfSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> totalIntegerTextArgumentsConsumeNativeWork() {
    try {
      BuiltInFunctionDefinitions.class.getField("PRINTF");
    } catch (NoSuchFieldException absent) {
      return new FlinkIntegerPrintfSqlHarnessTest().releasedPrintfContract();
    }
    var cases = new ArrayList<DynamicTest>();
    for (String type : List.of("TINYINT", "SMALLINT", "INT", "BIGINT")) {
      for (boolean nullable : new boolean[] {false, true}) {
        cases.add(DynamicTest.dynamicTest(type + " nullable=" + nullable, () -> {
          var expressions = new ArrayList<String>();
          for (String cast : List.of("CAST", "TRY_CAST")) {
            String text = cast + "(n AS STRING)";
            for (String format : List.of("%s", "%S", "%b", "%B", "%.0s",
                "%1$12.3S/%1$b", "%2$+08d/%1$-12.4s/%<B")) {
              expressions.add("PRINTF('" + format + "'," + text + ",id)");
            }
            expressions.add("PRINTF('%1$b/%2$b/%2$s',n<CAST(0 AS " + type + ")," + text + ")");
            expressions.add("CHARACTER_LENGTH(PRINTF('%1$s/%1$B'," + text + "))");
            expressions.add("PRINTF('%s'," + text + ")<'0'");
            expressions.add("PRINTF('%1$s/%1$b'," + cast + "(n+CAST(1 AS " + type + ") AS STRING))");
          }
          String sql = "SELECT id," + String.join(",", expressions) + " FROM src";
          for (boolean legacy : new boolean[] {false, true}) {
            FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql, true, legacy);
            FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql + " WHERE id<0", true, legacy);
          }
        }));
      }
    }
    cases.add(DynamicTest.dynamicTest("numeric conversions bounded text and failing producers retain their routes", () -> {
      for (boolean legacy : new boolean[] {false, true}) {
        FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
            "SELECT id,PRINTF('%d',CAST(n AS STRING)),PRINTF('%x',TRY_CAST(n AS STRING)),"
                + "PRINTF('%s',CAST(n AS VARCHAR(2))),PRINTF('%b',TRY_CAST(n AS CHAR(8))),"
                + "PRINTF('%0$s',CAST(n AS STRING)) FROM src", false, legacy);
      }
      FlinkIntegralEpochSqlHarnessTest.verify("BOOLEAN", true, "UTC",
          "SELECT id,PRINTF('%b/%1$s',CAST(n AS STRING)),PRINTF('%b',n) FROM src", false);
      FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
          "SELECT id,PRINTF('unused',CAST(n/0 AS STRING)) FROM src WHERE id<0", false);
      FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
          "SELECT id,CASE WHEN id>=0 THEN PRINTF('%s',CAST(n AS STRING)) "
              + "ELSE PRINTF('unused',CAST(n/0 AS STRING)) END FROM src", false);
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(18, 3, true, false,
          "SELECT id,PRINTF('%s',CAST(d AS STRING)) FROM src", false);
    }));
    return cases.stream();
  }
}
