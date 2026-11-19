package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkRightSqlHarnessTest {
  @Test
  void functionAlsoRunsInsideTheNativePredicate() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters, "SELECT id FROM inputs WHERE RIGHT(s, n) = 'b'");
  }

  @Test
  void rightHandlesDynamicNegativeAndNullCounts() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, RIGHT(s, n), RIGHT(s, -1), RIGHT(s, 2147483647) FROM inputs");
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"TINYINT", "SMALLINT"})
  void narrowIntegerParametersStayNative(String type) throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, RIGHT(s, CAST(n AS " + type + ")) FROM inputs");
  }
}
