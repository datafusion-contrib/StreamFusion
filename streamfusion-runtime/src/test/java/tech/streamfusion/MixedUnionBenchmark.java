package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class MixedUnionBenchmark {
  @Test
  void mixedChangelogRetainsBothTransposes() throws Exception {
    BasicTypeBenchmark.run(
        "mixed-union",
        FlinkMixedUnionSqlHarnessTest.query("(SELECT s AS k, d AS v FROM src) s", false),
        false,
        false,
        "NativeColumnarGroupAggregate");
  }
}
