package tech.streamfusion;

class FlinkEpochLtzFloorSqlHarnessTest extends FlinkEpochLtzRoundSqlHarness {
  @Override
  String function() { return "FLOOR"; }
}
