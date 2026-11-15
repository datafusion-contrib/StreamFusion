package tech.streamfusion;

class FlinkPrimitiveGreatestSqlHarnessTest extends FlinkPrimitiveStringExtremumSqlHarness {
  @Override
  String function() {
    return "GREATEST";
  }
}
