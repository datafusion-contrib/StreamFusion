package tech.streamfusion;

class FlinkEpochLtzCeilSqlHarnessTest extends FlinkEpochLtzRoundSqlHarness {
  @Override
  String function() { return "CEIL"; }
}
