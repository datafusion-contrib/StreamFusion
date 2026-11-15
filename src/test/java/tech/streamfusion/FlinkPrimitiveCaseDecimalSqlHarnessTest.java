package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class FlinkPrimitiveCaseDecimalSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> directDecimalTruthAndPredicatesKeepNativeCase() {
    var cases = new ArrayList<DynamicTest>();
    for (String function : List.of("UPPER", "LOWER")) {
      for (int[] type : List.of(new int[] {4, 2}, new int[] {38, 18})) {
        for (boolean legacy : new boolean[] {false, true}) {
          cases.add(DynamicTest.dynamicTest(function + " DECIMAL(" + type[0] + "," + type[1]
              + ") legacy=" + legacy, () -> {
            String sql = "SELECT id," + function + "(CAST(TRY_CAST(d AS BOOLEAN) AS STRING)),"
                + function + "(TRY_CAST(d<e AS STRING)),"
                + function + "(CAST(d IS NULL AS STRING)) FROM src";
            FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
                type[0], type[1], true, legacy, sql, true);
            FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
                type[0], type[1], true, legacy, sql + " WHERE id<0", true);
          }));
        }
      }
    }
    return cases.stream();
  }
}
