package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class AlternateTemporalTableFunctionBenchmark {
  @Test
  void completeJobsRetainReleasedTemporalConversionsAndBothTransposes() throws Exception {
    for (String signature :
        System.getProperty(
                "temporal.bridge.benchcases",
                "date-sql:array;time-integer:array;time-long:array;time-sql:array;timestamp-sql:array;"
                    + "ltz-integer:array;ltz-long:array;ltz-sql:array;date-sql:list;timestamp-sql:list")
            .split(";")) {
      var parts = signature.split(":");
      UdfTypeBoundaryBenchmark.run(
          "JVM_TEMPORAL_BRIDGE_" + signature,
          (rows, metrics) ->
              FlinkAlternateTemporalTableFunctionSqlHarnessTest.environment(
                  parts[0], parts[1], "America/New_York", rows, metrics, true, false),
          FlinkAlternateTemporalTableFunctionSqlHarnessTest.query("src", false),
          "NativeTableFunction",
          Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true")));
    }
  }
}
