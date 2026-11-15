package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RecoveryExecutionMetricsTest {
  @ParameterizedTest
  @CsvSource({
    "NativeCalc, NativeCalc(select=[id])",
    "NativeCalc, NativeCalcExecNode[14]",
    "NativeFilter, NativeFilter(where=[true])",
    "NativeColumnarGroupAggregate, NativeColumnarGroupAggExecNode[1]"
  })
  void requiresConsumedRowsFromTheRequestedOperator(String planOperator, String metricOperator) {
    RecoveryExecutionMetrics.assertConsumed(observation(metricOperator, 5), planOperator);
    assertThrows(
        AssertionError.class,
        () -> RecoveryExecutionMetrics.assertConsumed(observation(metricOperator, 0), planOperator));
    assertThrows(
        AssertionError.class,
        () ->
            RecoveryExecutionMetrics.assertConsumed(
                observation("Calc(select=[id])", 5), planOperator));
    assertThrows(
        AssertionError.class,
        () ->
            RecoveryExecutionMetrics.assertConsumed(
                observation(planOperator + "Other", 5), planOperator));
    assertThrows(
        AssertionError.class,
        () ->
            RecoveryExecutionMetrics.assertConsumed(
                observation("NativeCalcExecNodeOther[14]", 5), planOperator));
  }

  private static List<Map<String, Object>> observation(String operator, long recordsIn) {
    return List.of(Map.of("operator", operator, "recordsIn", recordsIn));
  }
}
