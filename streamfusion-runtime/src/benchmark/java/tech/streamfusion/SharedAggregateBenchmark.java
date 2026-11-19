package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class SharedAggregateBenchmark {
  @Test
  void sharedExactAggregates() throws Exception {
    for (String type : System.getProperty("basic.types", "BIGINT,DECIMAL").split(",")) {
      for (boolean twoPhase : new boolean[] {false, true}) {
        String value = type.equals("BIGINT") ? "v" : "d";
        BasicTypeBenchmark.run(type + (twoPhase ? "-two" : "-one"),
            "SELECT k, COUNT(" + value + "), AVG(" + value + "), SUM(" + value + ") FROM src GROUP BY k",
            false, twoPhase, "NativeColumnarGroupAggregate");
      }
    }
  }
}
