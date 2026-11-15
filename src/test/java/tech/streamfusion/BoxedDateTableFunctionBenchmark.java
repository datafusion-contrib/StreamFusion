package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class BoxedDateTableFunctionBenchmark {
  @Test
  void completeJobsRetainBoxedDateConversionContainersAndBothTransposes() throws Exception {
    for (String container : System.getProperty("boxed.date.containers", "array;list").split(";"))
      UdfTypeBoundaryBenchmark.run(
          "JVM_BOXED_DATE_" + container,
          (rows, metrics) ->
              FlinkBoxedDateTableFunctionSqlHarnessTest.environment(
                  container, rows, metrics, false),
          "SELECT s.x,t.v,t.epoch_day,t.pos FROM src s"
              + FlinkBoxedDateTableFunctionSqlHarnessTest.join(false),
          "NativeTableFunction",
          Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true")));
  }
}
