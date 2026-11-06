package tech.streamfusion;

class FlinkPrimitiveLowerSqlHarnessTest extends FlinkPrimitiveCaseSqlHarness {
  @Override
  String function() {
    return "LOWER";
  }
}
