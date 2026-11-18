package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkCollectionCastSemanticsTest {
  private static TableEnvironment environment(boolean legacy) {
    var table =
        BuiltinFunctionParity.environment(
            ROW(
                FIELD("id", INT()),
                FIELD("a", ARRAY(STRING())),
                FIELD("i", INT()),
                FIELD("j", INT())),
            List.of(
                Row.of(0, new String[] {"1", "bad", null, "2"}, 42, 7),
                Row.of(1, new String[] {"3", "4"}, 43, 8),
                Row.of(2, null, 44, 9)));
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    return table;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void arrayCastFailuresRetainTheReleasedContainerBoundary(boolean legacy) {
    for (String cast : List.of("CAST", "TRY_CAST")) {
      var table = environment(legacy);
      var scan = tech.streamfusion.planner.NativePlanner.install(table);
      table.explainSql("SELECT id," + cast + "(a AS ARRAY<INT>) FROM src");
      assertTrue(
          !scan.functionExecutions().isEmpty()
              && scan.functionExecutions().stream().allMatch(e -> e.contains("jvm=[]")),
          scan.functionExecutions().toString());
      var runs =
          NativeFailureParity.run(
              () -> environment(legacy), "SELECT id," + cast + "(a AS ARRAY<INT>) FROM src");
      System.out.println(
          "[collection-cast-boundary] legacy=" + legacy + " cast=" + cast + " host=" + runs.host());
      if (runs.host().failure() == null) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      else
        runs.assertFailure(
            runs.host().rootCause().getClass(),
            runs.host().phase(),
            NativeFailureParity.Route.NATIVE);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void numericBooleanArraysRetainTheConfiguredCharacterFormatting(boolean legacy) {
    String sql =
        "SELECT id,CAST(CAST(ARRAY[i,j,CAST(NULL AS INT)] AS ARRAY<BOOLEAN>) AS STRING) FROM src";
    var table = environment(legacy);
    var scan = tech.streamfusion.planner.NativePlanner.install(table);
    table.explainSql(sql);
    assertTrue(
        !scan.functionExecutions().isEmpty()
            && scan.functionExecutions().stream().allMatch(e -> e.contains("jvm=[]")),
        scan.functionExecutions().toString());
    NativeFailureParity.run(() -> environment(legacy), sql)
        .assertSuccess(NativeFailureParity.Route.NATIVE);
  }

  @Test
  void releasedMapConstructorGroupingOrder() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeFalse(
        tech.streamfusion.compat.FlinkCompat.ORDERED_MAP_CONSTRUCTORS);
    var observed = new java.util.LinkedHashSet<Object>();
    for (int i = 0; i < 16; i++) {
      var table = environment(false);
      try (var rows = table.executeSql("SELECT MAP_KEYS(MAP['a',i,'b',j]) FROM src").collect()) {
        while (rows.hasNext()) {
          Object result = NativeParity.comparableValue(rows.next().getField(0));
          observed.add(result);
          assertTrue(List.of(List.of("a", "b"), List.of("b", "a")).contains(result));
        }
      }
    }
    System.out.println(
        "[map-constructor-order] flink="
            + org.apache.flink.runtime.util.EnvironmentInformation.getVersion()
            + " observed="
            + observed);
  }
}
