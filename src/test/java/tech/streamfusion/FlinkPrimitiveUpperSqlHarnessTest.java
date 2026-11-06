package tech.streamfusion;

class FlinkPrimitiveUpperSqlHarnessTest extends FlinkPrimitiveCaseSqlHarness {
  @Override
  String function() {
    return "UPPER";
  }
}
