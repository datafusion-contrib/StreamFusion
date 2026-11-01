package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class PrimitiveTemporalTableFunctionBenchmark {
  @Test
  void completeJobsRetainPrimitiveConversionsAndBothTransposes() throws Exception {
    for (String bridge :
        FlinkPrimitiveTemporalTableFunctionSqlHarnessTest.primitives().toArray(String[]::new)) {
      UdfTypeBoundaryBenchmark.run(
          "JVM_PRIMITIVE_TEMPORAL_" + bridge,
          (rows, metrics) ->
              FlinkPrimitiveTemporalTableFunctionSqlHarnessTest.environment(
                  bridge, rows, metrics, false),
          FlinkAlternateTemporalTableFunctionSqlHarnessTest.query("src", false),
          "NativeTableFunction",
          Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true")));
    }
  }
}
