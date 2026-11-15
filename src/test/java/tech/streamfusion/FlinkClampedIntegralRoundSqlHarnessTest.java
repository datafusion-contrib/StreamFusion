package tech.streamfusion;

import java.util.List;

class FlinkClampedIntegralRoundSqlHarnessTest extends FlinkClampedIntegralRoundingSqlHarness {
  @Override
  String function() {
    return "ROUND";
  }

  @Override
  List<String> inputTypes() {
    return List.of("TINYINT", "SMALLINT", "INT", "BIGINT");
  }
}
