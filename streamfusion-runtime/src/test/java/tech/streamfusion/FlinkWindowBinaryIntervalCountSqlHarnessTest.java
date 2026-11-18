package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkWindowBinaryIntervalCountSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "TUMBLE,ONE_PHASE,false", "TUMBLE,TWO_PHASE,false",
    "HOP,ONE_PHASE,false", "HOP,TWO_PHASE,false",
    "CUMULATE,ONE_PHASE,false", "CUMULATE,TWO_PHASE,false",
    "TUMBLE,ONE_PHASE,true", "TUMBLE,TWO_PHASE,true",
    "HOP,ONE_PHASE,true", "HOP,TWO_PHASE,true",
    "CUMULATE,ONE_PHASE,true", "CUMULATE,TWO_PHASE,true"
  })
  void countsPreserveEmptyBinaryNullsIntervalsFiltersAndPhase(
      String shape, String phase, boolean filtered) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      String window =
          shape
              + "(TABLE src, DESCRIPTOR(rt), "
              + (shape.equals("TUMBLE") ? "" : "INTERVAL '1' SECOND, ")
              + "INTERVAL '3' SECOND)";
      String reductions =
          java.util.stream.Stream.of("b", "d", "m")
              .map(
                  column ->
                      "COUNT(" + column + ")" + (filtered ? " FILTER (WHERE id % 3 = 0)" : ""))
              .collect(java.util.stream.Collectors.joining(", "));
      var result =
          NativeFailureParity.run(
              () -> {
                var table = FlinkOverBinaryIntervalCountSqlHarnessTest.environment(metrics, 1031);
                table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
                table.getConfig().set("table.exec.mini-batch.enabled", "true");
                table.getConfig().set("table.exec.mini-batch.allow-latency", "100 ms");
                table.getConfig().set("table.exec.mini-batch.size", "127");
                return table;
              },
              "SELECT k, window_start, window_end, "
                  + reductions
                  + ", COUNT(*) FROM TABLE("
                  + window
                  + ") GROUP BY k, window_start, window_end");
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
      assertFalse(result.host().rows().isEmpty());
      var operators = metrics.nativeOperators();
      RecoveryExecutionMetrics.assertConsumed(
          operators,
          phase.equals("TWO_PHASE")
              ? "NativeColumnarGlobalWindowAggregate"
              : "NativeColumnarWindowAggregate");
      if (phase.equals("TWO_PHASE"))
        RecoveryExecutionMetrics.assertConsumed(operators, "NativeColumnarLocalWindowAggregate");
    }
  }
}
