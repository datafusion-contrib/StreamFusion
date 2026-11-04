package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class FlinkBooleanPrintfSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> releasedBooleanPrintfContract() {
    try {
      BuiltInFunctionDefinitions.class.getField("PRINTF");
    } catch (NoSuchFieldException absent) {
      return new FlinkIntegerPrintfSqlHarnessTest().releasedPrintfContract();
    }
    var cases = new ArrayList<DynamicTest>();
    for (boolean nullable : new boolean[] {false, true}) {
      cases.add(DynamicTest.dynamicTest("BOOLEAN nullable=" + nullable, () -> {
        var expressions = new ArrayList<String>();
        for (String format : List.of("%s", "%S", "%b", "%B", "%.0s", "%.2s",
            "%8.2B", "%-8.3s", "%1$b/%1$s/%<B/%s", "%%:%2$b:%1$s:%<B")) {
          expressions.add("PRINTF('" + format + "',n,NOT n)");
        }
        expressions.add("PRINTF('%1$b/%2$+08d/%<x/%1$S/%b/%s',n,id)");
        expressions.add("PRINTF('%s/%2$B',n OR id<1000,n IS NOT TRUE)");
        String sql = "SELECT id," + String.join(",", expressions) + " FROM src";
        FlinkIntegralEpochSqlHarnessTest.verify("BOOLEAN", nullable, "UTC", sql, true);
        FlinkIntegralEpochSqlHarnessTest.verify("BOOLEAN", nullable, "UTC", sql + " WHERE id<0", true);
      }));
    }
    for (String type : List.of("TINYINT", "SMALLINT", "INT", "BIGINT")) {
      for (boolean nullable : new boolean[] {false, true}) {
        cases.add(DynamicTest.dynamicTest(type + "/BOOLEAN nullable=" + nullable, () -> {
          String condition = "n<CAST(0 AS " + type + ")";
          String sql = "SELECT id,PRINTF('%1$d:%2$b:%1$x:%2$S:%<b',n," + condition + "),"
              + "PRINTF('%2$8.2S|%1$+020d|%2$b|%1$b',n," + condition + "),"
              + "PRINTF('%1$b/%2$b/%2$s',n,(" + condition + ") IS NOT TRUE),"
              + "PRINTF('%1$d/%2$b/%2$s',n,TRUE),"
              + "PRINTF('%1$d/%2$b/%2$s',n,CAST(NULL AS BOOLEAN)),"
              + "PRINTF('%1$+08d/%2$B',n+CAST(1 AS " + type + "),n IS NOT NULL) FROM src";
          FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql, true);
          FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql + " WHERE id<0", true);
        }));
      }
    }
    cases.add(DynamicTest.dynamicTest("unverified formats consumers and failing inputs keep their routes", () -> {
      FlinkIntegralEpochSqlHarnessTest.verify("BOOLEAN", true, "UTC",
          "SELECT id,PRINTF('%d',n),PRINTF('%+b',n),PRINTF('%0$b',n),"
              + "PRINTF(CASE WHEN id<1000 THEN '%b' ELSE '%s' END,n),"
              + "CHAR_LENGTH(PRINTF('%s',n)) FROM src", false);
      FlinkIntegralEpochSqlHarnessTest.verify("BOOLEAN", true, "UTC",
          "SELECT id,PRINTF('%s',id/0>0) FROM src WHERE id<0", false);
      FlinkIntegralEpochSqlHarnessTest.verify("BOOLEAN", true, "UTC",
          "SELECT id,CASE WHEN id>=0 THEN PRINTF('%b',n) "
              + "ELSE PRINTF('%s',id/0>0) END FROM src", false);
    }));
    return cases.stream();
  }
}
