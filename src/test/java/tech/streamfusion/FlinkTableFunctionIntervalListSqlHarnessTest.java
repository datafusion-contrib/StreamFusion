package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkTableFunctionIntervalListSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {"list:DAY_TIME", "list:YEAR_MONTH", "arraylist:DAY_TIME", "arraylist:YEAR_MONTH"})
  void scalarIntervalListsRetainReleasedConvertersAndNullableFanout(String name) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> FlinkTableFunctionConversionSqlHarnessTest.environment(name, 96, metrics),
              "SELECT s.id,s.p,t.v,t.pos FROM src s LEFT JOIN LATERAL"
                  + " TABLE(bridge_rows(s.id,s.p)) AS t(v,pos) ON TRUE");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }
}
