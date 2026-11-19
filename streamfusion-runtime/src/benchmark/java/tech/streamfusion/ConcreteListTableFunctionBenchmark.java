package tech.streamfusion;

@org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable(
    named = "SF_BENCHMARK",
    matches = "true")
class ConcreteListTableFunctionBenchmark {
  @org.junit.jupiter.api.Test
  void completeJobsRetainListConversionAndBothTransposes() throws Exception {
    for (String name :
        System.getProperty(
                "boundary.concreteLists",
                "linked:STRING;linked:DECIMAL(38,18);vector:TIMESTAMP(9);custom:STRING")
            .split(";")) {
      String bridge = name.substring(0, name.indexOf(':'));
      String type = name.substring(name.indexOf(':') + 1);
      UdfTypeBoundaryBenchmark.run(
          "CONCRETE_LIST_" + name,
          (rows, metrics) ->
              FlinkConcreteListTableFunctionSqlHarnessTest.environment(bridge, type, rows, metrics),
          "SELECT s.id,t.v,t.pos FROM src s,LATERAL TABLE(concrete_rows(s.id,s.p)) AS t(v,pos)",
          "NativeTableFunction",
          Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true")));
    }
  }
}
