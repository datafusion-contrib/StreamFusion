package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkUpdatingAttachedWindowSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void explicitAssignedBoundsKeepSignedFiltersAndPartialMerges(String phase) {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.RETRACTING_WINDOW_TVF);
    try (var metrics = new RecoveryExecutionMetrics()) {
      String sql =
          "SELECT window_start,window_end,MIN(v),MAX(v),SUM(v),COUNT(*) FILTER(WHERE v>100 AND"
              + " window_start<TIMESTAMP '1970-01-01 00:00:05'),COUNT(v) FROM TABLE(TUMBLE(TABLE"
              + " src,DESCRIPTOR(rt),INTERVAL '5' SECOND)) GROUP BY window_start,window_end";
      var result =
          NativeFailureParity.run(
              () -> {
                var table = FlinkLegacyUpdatingWindowSqlHarnessTest.environment(metrics);
                table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
                return table;
              },
              sql);
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertTrue(
          result.nativeRun().plan().contains("NativeWindowTableFunction"),
          result.nativeRun().plan());
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeWindowTableFunction");
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(),
          phase.equals("TWO_PHASE")
              ? "NativeColumnarGlobalWindowAggregate"
              : "NativeColumnarWindowAggregate");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"TUMBLE", "HOP", "CUMULATE"})
  void standaloneAssignmentsPreserveTheReleasedChangelog(String shape) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      String window =
          shape.equals("TUMBLE")
              ? "TUMBLE(TABLE src,DESCRIPTOR(rt),INTERVAL '5' SECOND)"
              : shape + "(TABLE src,DESCRIPTOR(rt),INTERVAL '5' SECOND,INTERVAL '10' SECOND)";
      NativeParity.assertKindedParity(
          () -> FlinkLegacyUpdatingWindowSqlHarnessTest.environment(metrics),
          "SELECT k,v,window_start,window_end,window_time FROM TABLE(" + window + ")");
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeWindowTableFunction");
    }
  }
}
