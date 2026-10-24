package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class DateIntArgumentTableFunctionBenchmark {
  @Test
  void completeJobsRetainDateConversionContainersAndBothTransposes() throws Exception {
    for (String name :
        System.getProperty("date.boundaries", "array;list;map;CONTROL_array").split(";")) {
      boolean control = name.startsWith("CONTROL_");
      String container = control ? name.substring("CONTROL_".length()) : name;
      UdfTypeBoundaryBenchmark.run(
          "JVM_DATE_INT_ARGUMENT_" + name,
          (rows, metrics) ->
              FlinkDateIntArgumentTableFunctionSqlHarnessTest.environment(
                  container, rows, metrics, control),
          "SELECT s.x,t.v,t.epoch_day,t.pos FROM src s,LATERAL TABLE(day_rows(s.x,s.p)) AS"
              + " t(v,epoch_day,pos)",
          "NativeTableFunction",
          control || Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true")));
    }
  }
}
