package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.function.Supplier;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkUnnestProjectionSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"ARRAY", "STRING_ARRAY", "ROW_ARRAY", "MAP", "MULTISET"})
  void projectedFanoutRetainsReorderingRepeatedColumnsAndLeftPadding(String shape) {
    Supplier<TableEnvironment> source;
    String operand, alias, selected;
    switch (shape) {
      case "STRING_ARRAY":
        source = FlinkUnnestSqlHarnessTest::stringArrayEnvironment;
        operand = "ss";
        alias = "u(e)";
        selected = "e, k, e";
        break;
      case "ROW_ARRAY":
        source = FlinkUnnestSqlHarnessTest::rowArrayEnvironment;
        operand = "rs";
        alias = "u(a,b)";
        selected = "b, k, a, b";
        break;
      case "MAP":
        source = FlinkUnnestSqlHarnessTest::mapEnvironment;
        operand = "m";
        alias = "u(mk,mv)";
        selected = "mv, k, mk, mv";
        break;
      case "MULTISET":
        source = FlinkUnnestSqlHarnessTest::multisetEnvironment;
        operand = "ms";
        alias = "u(e)";
        selected = "e, k, e";
        break;
      default:
        source = FlinkUnnestSqlHarnessTest::environment;
        operand = "vs";
        alias = "u(e)";
        selected = "e, k, e";
    }
    for (boolean left : new boolean[] {false, true}) {
      String from =
          " FROM t "
              + (left ? "LEFT JOIN " : "CROSS JOIN ")
              + "UNNEST("
              + operand
              + ") AS "
              + alias
              + (left ? " ON TRUE" : "");
      for (String select : new String[] {selected, "COUNT(*)"}) {
        var comparison = NativeFailureParity.run(source, "SELECT " + select + from);
        comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
        assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      }
    }
  }
}
