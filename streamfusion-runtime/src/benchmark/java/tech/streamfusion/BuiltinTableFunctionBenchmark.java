package tech.streamfusion;

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class BuiltinTableFunctionBenchmark {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void completeJobsRetainJvmFanoutAndBothTransposes(boolean prior) throws Exception {
    UdfTypeBoundaryBenchmark.run(
        prior ? "BUILTIN_SPLIT_PRIOR_FALLBACK" : "BUILTIN_SPLIT_ARROW",
        (rows, metrics) -> {
          var table = FlinkBuiltinTableFunctionSqlHarnessTest.environment(metrics, rows, false);
          if (prior) table.getConfig().set("streamfusion.operator.tableFunction.enabled", "false");
          return table;
        },
        FlinkBuiltinTableFunctionSqlHarnessTest.query("BUILTIN_SPLIT", "s.delimiter_text", false),
        "NativeTableFunction",
        !prior);
  }
}
