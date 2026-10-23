package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class TableFunctionConversionBenchmark {
  @Test
  void completeJobsRetainExternalConversionsAndBothTransposes() throws Exception {
    for (String name :
        System.getProperty(
                "boundary.conversions",
                "primitive:INT;primitive:DOUBLE;list:STRING;list:DECIMAL(38,18)")
            .split(";")) {
      UdfTypeBoundaryBenchmark.run(
          "JVM_TABLE_CONVERSION_" + name,
          (rows, metrics) ->
              FlinkTableFunctionConversionSqlHarnessTest.environment(name, rows, metrics),
          "SELECT s.id,t.v,t.pos FROM src s,LATERAL TABLE(bridge_rows(s.id,s.p)) AS t(v,pos)",
          "NativeTableFunction",
          Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true")));
    }
  }
}
