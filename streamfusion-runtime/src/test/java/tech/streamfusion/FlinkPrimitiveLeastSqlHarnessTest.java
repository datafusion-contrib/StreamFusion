package tech.streamfusion;

class FlinkPrimitiveLeastSqlHarnessTest extends FlinkPrimitiveStringExtremumSqlHarness {
  @Override
  String function() {
    return "LEAST";
  }
}
