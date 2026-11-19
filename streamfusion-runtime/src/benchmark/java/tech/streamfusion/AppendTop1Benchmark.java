package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class AppendTop1Benchmark {
  @Test
  void completeAppendFastTop1JobsKeepSourceShuffleAndBothTransposes() throws Exception {
    BasicTypeBenchmark.run("append-top1", "SELECT k, v, s, rn FROM (SELECT k, v, s, "
        + "ROW_NUMBER() OVER (PARTITION BY k ORDER BY v DESC, s ASC) AS rn FROM src) WHERE rn <= 1",
        false, false, "NativeColumnarTopN");
  }
}
