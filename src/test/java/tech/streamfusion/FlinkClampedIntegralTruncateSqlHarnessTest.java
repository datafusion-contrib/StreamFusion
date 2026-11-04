package tech.streamfusion;

import java.util.List;

class FlinkClampedIntegralTruncateSqlHarnessTest extends FlinkClampedIntegralRoundingSqlHarness {
  @Override
  String function() {
    return "TRUNCATE";
  }

  @Override
  List<String> inputTypes() {
    return List.of("INT", "BIGINT");
  }
}
