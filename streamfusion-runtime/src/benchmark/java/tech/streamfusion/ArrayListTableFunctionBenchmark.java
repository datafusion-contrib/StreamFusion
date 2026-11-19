package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class ArrayListTableFunctionBenchmark {
  @Test
  void completeJobsRetainConcreteListResultsAndBothTransposes() throws Exception {
    for (String name :
        System.getProperty(
                "arraylist.boundaries",
                "arraylist:INT;arraylist:STRING;arraylist:DECIMAL(38,18);CONTROL_list:STRING")
            .split(";")) {
      boolean control = name.startsWith("CONTROL_");
      String conversion = control ? name.substring("CONTROL_".length()) : name;
      UdfTypeBoundaryBenchmark.run(
          "JVM_TABLE_ARRAYLIST_RESULT_" + name,
          (rows, metrics) ->
              FlinkTableFunctionConversionSqlHarnessTest.environment(conversion, rows, metrics),
          "SELECT s.id,t.v,t.pos FROM src s,LATERAL TABLE(bridge_rows(s.id,s.p)) AS t(v,pos)",
          "NativeTableFunction",
          control || Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true")));
    }
  }
}
