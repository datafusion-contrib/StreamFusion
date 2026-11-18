package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class MixedPrimitiveTableFunctionBenchmark {
  @Test
  void completeJobsRetainPrimitiveArgumentsContainersAndBothTransposes() throws Exception {
    for (String signature :
        System.getProperty("boundary.signatures", "INT:array;INT:list;DOUBLE:map;FLOAT:row")
            .split(";")) {
      String[] parts = signature.split(":");
      UdfTypeBoundaryBenchmark.run(
          "JVM_MIXED_PRIMITIVE_" + signature,
          (rows, metrics) ->
              FlinkMixedPrimitiveTableFunctionSqlHarnessTest.environment(
                  parts[0], parts[1], rows, metrics),
          "SELECT s.x,t.v,t.m FROM src s,LATERAL TABLE(mixed_rows(s.x,s.p)) AS t(v,m)",
          "NativeTableFunction",
          Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true")));
    }
  }
}
