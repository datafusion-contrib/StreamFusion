package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class FlinkDecimalBooleanPrintfSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> verifiedDecimalBooleanArguments() {
    try {
      BuiltInFunctionDefinitions.class.getField("PRINTF");
    } catch (NoSuchFieldException absent) {
      return new FlinkIntegerPrintfSqlHarnessTest().releasedPrintfContract();
    }
    var cases = new ArrayList<DynamicTest>();
    for (int[] type : List.of(new int[] {1, 1}, new int[] {4, 2},
        new int[] {18, 0}, new int[] {18, 18}, new int[] {19, 0}, new int[] {19, 19},
        new int[] {38, 18}, new int[] {38, 38})) {
      int precision = type[0];
      int scale = type[1];
      cases.add(DynamicTest.dynamicTest("DECIMAL(" + precision + "," + scale + ")", () -> {
        var predicates = new ArrayList<>(List.of("TRY_CAST(d AS BOOLEAN)",
            "NOT TRY_CAST(d AS BOOLEAN)", "TRY_CAST(d AS BOOLEAN) IS NULL",
            "d=e", "d<>e", "d<e", "d<=e", "d>e", "d>=e", "d IS NULL", "e IS NOT NULL",
            "(d<e) IS NOT TRUE", "(d<e) OR id<7"));
        if (scale < precision) predicates.add("CAST(d AS BOOLEAN)");
        var outputs = new ArrayList<String>();
        for (String predicate : predicates) {
          outputs.add("PRINTF('%1$b/%1$B/%1$s/%1$S'," + predicate + ")");
          outputs.add("PRINTF('%1$8.2S:%1$-8.3b:%2$+08d'," + predicate + ",id)");
        }
        String sql = "SELECT id," + String.join(",", outputs) + " FROM src";
        for (boolean nullable : new boolean[] {false, true}) {
          for (boolean legacy : new boolean[] {false, true}) {
            FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
                precision, scale, nullable, legacy, sql, true);
            FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
                precision, scale, nullable, legacy, sql + " WHERE id<0", true);
          }
        }
      }));
    }
    cases.add(DynamicTest.dynamicTest("computed predicates and protected consumers retain their routes", () -> {
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(18, 3, true, false,
          "SELECT id,PRINTF('%s',d+d>e),CHAR_LENGTH(PRINTF('%s',d<e)),"
              + "PRINTF('%d',d<e),PRINTF('%+b',TRY_CAST(d AS BOOLEAN)) FROM src", false);
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(18, 3, true, false,
          "SELECT id,PRINTF('%s',d/(id-id)>e) FROM src WHERE id<0", false);
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(18, 3, true, false,
          "SELECT id,CASE WHEN id>=0 THEN PRINTF('%b',d<e) "
              + "ELSE PRINTF('%s',d/(id-id)>e) END FROM src", false);
    }));
    return cases.stream();
  }
}
