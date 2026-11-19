package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class PrimitiveResultTableFunctionBenchmark {
  @Test
  void completeJobsRetainPrimitiveResultsContainersAndBothTransposes() throws Exception {
    for (String signature :
        System.getProperty(
                "boundary.signatures",
                "INT:array;INT:list;DOUBLE:map;FLOAT:row;ATOMIC_INT:array;ATOMIC_DOUBLE:map;CONTROL_INT:array")
            .split(";")) {
      String[] parts = signature.split(":");
      boolean control = parts[0].startsWith("CONTROL_");
      boolean atomic = parts[0].startsWith("ATOMIC_");
      String kind =
          control
              ? parts[0].substring("CONTROL_".length())
              : atomic ? parts[0].substring("ATOMIC_".length()) : parts[0];
      String functionName = control ? "mixed_rows" : atomic ? "atomic_rows" : "result_rows";
      UdfTypeBoundaryBenchmark.run(
          "JVM_PRIMITIVE_RESULT_" + signature,
          (rows, metrics) -> {
            var table =
                FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceEnvironment(
                    kind, parts[1], rows, metrics, false);
            if (control)
              return FlinkMixedPrimitiveTableFunctionSqlHarnessTest.register(table, kind, parts[1]);
            if (atomic)
              return FlinkPrimitiveResultTableFunctionSqlHarnessTest.registerAtomic(
                  table, kind, parts[1]);
            return FlinkPrimitiveResultTableFunctionSqlHarnessTest.register(table, kind, parts[1]);
          },
          "SELECT s.x,"
              + (atomic ? "t.m" : "t.v,t.m")
              + " FROM src s,LATERAL TABLE("
              + functionName
              + "(s.x,s.p)) AS t("
              + (atomic ? "m" : "v,m")
              + ")",
          "NativeTableFunction",
          control
              || (atomic && Boolean.getBoolean("boundary.atomicPreviouslyNative"))
              || Boolean.parseBoolean(System.getProperty("boundary.expectNative", "true")));
    }
  }
}
