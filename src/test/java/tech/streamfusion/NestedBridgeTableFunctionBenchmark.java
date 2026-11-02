package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NestedBridgeTableFunctionBenchmark {
  @Test
  void completeJobsRetainNestedConversionsAndBothTransposes() throws Exception {
    for (String signature :
        System.getProperty(
                "boundary.nestedBridges",
                "row:primitive-FLOAT;row_map:date-sql;map_row:timestamp-sql;map_key:ltz-long")
            .split(";")) {
      String shape = signature.substring(0, signature.indexOf(':'));
      String bridge = signature.substring(signature.indexOf(':') + 1);
      UdfTypeBoundaryBenchmark.run(
          "NESTED_BRIDGE_" + signature,
          (rows, metrics) ->
              FlinkNestedBridgeTableFunctionSqlHarnessTest.environment(
                  shape, bridge, "both", rows, metrics),
          FlinkNestedBridgeTableFunctionSqlHarnessTest.query(false),
          "NativeTableFunction",
          Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true")));
    }
  }
}
