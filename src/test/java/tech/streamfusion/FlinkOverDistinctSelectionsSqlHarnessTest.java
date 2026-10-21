package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkOverDistinctSelectionsSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "rt ROWS UNBOUNDED PRECEDING",
        "rt RANGE UNBOUNDED PRECEDING",
        "rt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
        "rt RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW",
        "pt ROWS UNBOUNDED PRECEDING",
        "pt RANGE UNBOUNDED PRECEDING",
        "pt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
      })
  void selectedDistinctValuesRetainTheirDeclaredTypesAndFrameHistory(String frame) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var calls = new ArrayList<String>();
      for (String value : new String[] {"s", "t", "z", "d", "tm", "b", "n"}) {
        for (String function : new String[] {"COUNT", "MIN", "MAX", "FIRST_VALUE", "LAST_VALUE"}) {
          if ((value.equals("t") || value.equals("z") || value.equals("d") || value.equals("tm"))
              && (function.equals("FIRST_VALUE") || function.equals("LAST_VALUE"))
              && !tech.streamfusion.compat.RuntimeCompat.supportsTemporalOverFirstLast()) continue;
          calls.add(function + "(DISTINCT " + value + ") OVER w");
        }
      }
      var comparison =
          NativeFailureParity.run(
              () -> {
                var table = FlinkOverTextTemporalDistinctCountSqlHarnessTest.environment(metrics);
                int timePrecision =
                    tech.streamfusion.compat.RuntimeCompat.supportsTemporalOverFirstLast() ? 3 : 0;
                table.createTemporaryView(
                    "typed",
                    table.sqlQuery(
                        "SELECT *, CAST(t AS DATE) AS d, CASE WHEN s IS NULL THEN CAST(NULL AS"
                            + " TIME("
                            + timePrecision
                            + ")) ELSE CAST(CASE WHEN MOD(id / 6, 2) = 0 THEN"
                            + " '00:00:00.123' ELSE '23:59:59.999' END AS TIME("
                            + timePrecision
                            + ")) END AS tm, CASE WHEN s IS"
                            + " NULL THEN CAST(NULL AS BOOLEAN) ELSE MOD(id / 6, 3) = 0 END AS b,"
                            + " CASE WHEN s IS NULL THEN CAST(NULL AS DECIMAL(20,3)) ELSE"
                            + " CAST(MOD(id / 6, 5) AS DECIMAL(20,3)) END AS n FROM src"));
                return table;
              },
              "SELECT id, k, "
                  + String.join(", ", calls)
                  + " FROM typed WINDOW w AS (PARTITION BY k ORDER BY "
                  + frame
                  + ")");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }
}
