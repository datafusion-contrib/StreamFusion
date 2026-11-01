package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class InferredNestedScalarBenchmark {
  @Test
  void completeJobsRetainInferredNestedConversionsAndBothTransposes() throws Exception {
    for (String mode : new String[] {"explicit", "delegating"}) {
      UdfTypeBoundaryBenchmark.run(
          "JVM_INFERRED_NESTED_" + mode,
          (rows, metrics) ->
              FlinkInferredNestedScalarSqlHarnessTest.environment(rows, mode, metrics),
          "SELECT id,inferred(r) FROM inputs",
          "NativeCalc",
          Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true")));
    }
  }
}
