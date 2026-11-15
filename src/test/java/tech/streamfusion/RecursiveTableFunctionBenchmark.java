package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class RecursiveTableFunctionBenchmark {
  @Test
  void completeJobsRetainRecursiveConversionsAndBothTransposes() throws Exception {
    for (String signature :
        System.getProperty(
                "recursive.bench.signatures",
                "list/row:default-STRING:both;row/primitive:primitive-DOUBLE:both;row/custom/map:default-DECIMAL(38,18):result")
            .split(";")) {
      String[] parts = signature.split(":");
      UdfTypeBoundaryBenchmark.run(
          "RECURSIVE_" + signature,
          (rows, metrics) ->
              FlinkRecursiveTableFunctionSqlHarnessTest.environment(
                  parts[0], parts[1], parts[2], rows, metrics),
          FlinkRecursiveTableFunctionSqlHarnessTest.query(false),
          "NativeTableFunction",
          Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true")));
    }
  }
}
