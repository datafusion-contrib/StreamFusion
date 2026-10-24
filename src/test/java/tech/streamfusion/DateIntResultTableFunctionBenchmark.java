package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class DateIntResultTableFunctionBenchmark {
  @Test
  void completeJobsRetainResultConversionContainersAndBothTransposes() throws Exception {
    for (String name :
        System.getProperty(
                "date.result.boundaries", "row:array;row:list;atomic:array;CONTROL_array")
            .split(";")) {
      boolean control = name.startsWith("CONTROL_");
      boolean atomic = name.startsWith("atomic:");
      String container = control ? name.substring("CONTROL_".length()) : name.split(":")[1];
      UdfTypeBoundaryBenchmark.run(
          "JVM_DATE_INT_RESULT_" + name,
          (rows, metrics) ->
              control
                  ? FlinkDateIntArgumentTableFunctionSqlHarnessTest.environment(
                      container, rows, metrics, false)
                  : FlinkDateIntResultTableFunctionSqlHarnessTest.environment(
                      container, rows, metrics, atomic),
          control
              ? "SELECT s.x,t.v,t.epoch_day,t.pos FROM src s,LATERAL TABLE(day_rows(s.x,s.p)) AS"
                  + " t(v,epoch_day,pos)"
              : atomic
                  ? "SELECT s.x,t.epoch_day FROM src s,LATERAL TABLE(atomic_rows(s.x,s.p)) AS"
                      + " t(epoch_day)"
                  : "SELECT s.x,t.v,t.epoch_day,t.pos FROM src s,LATERAL"
                      + " TABLE(result_rows(s.x,s.p)) AS t(v,epoch_day,pos)",
          "NativeTableFunction",
          control || Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true")));
    }
  }
}
