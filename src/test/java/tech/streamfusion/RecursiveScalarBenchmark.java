package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class RecursiveScalarBenchmark {
  @Test
  void completeJobsRetainRecursiveConversionsAndBothTransposes() throws Exception {
    for (String signature :
        System.getProperty(
                "recursive.scalar.bench.signatures",
                "list/row:default-STRING:both;row/primitive:primitive-DOUBLE:both;row/custom/map:default-DECIMAL(38,18):result")
            .split(";")) {
      String[] parts = signature.split(":");
      UdfTypeBoundaryBenchmark.run(
          "RECURSIVE_SCALAR_" + signature,
          (rows, metrics) ->
              FlinkRecursiveScalarSqlHarnessTest.environment(
                  parts[0], parts[1], parts[2], rows, metrics),
          "SELECT id,p,recursive_scalar(id,p) AS v FROM src",
          "NativeCalc",
          Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true")));
    }
  }
}
