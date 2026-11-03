package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import tech.streamfusion.planner.NativePlanner;

class FlinkIntegerPrintfSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> releasedPrintfContract() {
    try {
      BuiltInFunctionDefinitions.class.getField("PRINTF");
    } catch (NoSuchFieldException absent) {
      return Stream.of(DynamicTest.dynamicTest("released Flink has no PRINTF", () -> {
        for (boolean nativeRun : new boolean[] {false, true}) {
          var table = BuiltinFunctionParity.environment(
              DataTypes.ROW(DataTypes.FIELD("n", DataTypes.INT())), List.of(Row.of(1)));
          if (nativeRun) NativePlanner.install(table);
          var failure = assertThrows(ValidationException.class,
              () -> table.sqlQuery("SELECT PRINTF('%d',n) FROM src"));
          assertTrue(failure.getMessage().contains("SQL validation failed"), failure.toString());
        }
      }));
    }
    var cases = new ArrayList<DynamicTest>();
    for (String type : List.of("TINYINT", "SMALLINT", "INT", "BIGINT")) {
      for (boolean nullable : new boolean[] {false, true}) {
        cases.add(DynamicTest.dynamicTest(type + " nullable=" + nullable, () -> {
          var expressions = new ArrayList<String>();
          for (String format : List.of("%d", "%+08d", "% 08d", "%-12d", "%s", "%.0s",
              "%12.3S", "%x", "%08X", "%o", "%2$s:%1$d:%<x:%s",
              "%%:%1$d/%2$08x/%<X")) {
            expressions.add("PRINTF('" + format + "',n,CAST(n AS BIGINT))");
          }
          expressions.add("PRINTF('%d/%1$x',n+CAST(1 AS " + type + "))");
          String query = "SELECT id," + String.join(",", expressions) + " FROM src";
          FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", query, true);
          FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", query + " WHERE id<0", true);
          FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC",
              "SELECT id,CHAR_LENGTH(PRINTF('%s/%d',n,n)) FROM src", false);
        }));
      }
    }
    cases.add(DynamicTest.dynamicTest("unverified formats and producers stay generated", () -> {
      FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
          "SELECT id,PRINTF('%n/%d',n),PRINTF('%#x',n),PRINTF('%b',n),PRINTF('%0$d',n),"
              + "PRINTF('%s',CAST(n AS STRING)),"
              + "PRINTF(CASE WHEN id<1000 THEN '%d' ELSE '%08d' END,n) FROM src", false);
      FlinkIntegralEpochSqlHarnessTest.verify("INT", true, "UTC",
          "SELECT id,PRINTF('%d',n/0) FROM src WHERE id<0", false);
    }));
    return cases.stream();
  }
}
