package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkLeftSqlHarnessTest {
  @Test
  void functionAlsoRunsInsideTheNativePredicate() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters, "SELECT id FROM inputs WHERE LEFT(s, n) = 'a'");
  }

  @Test
  void leftHandlesDynamicNegativeAndNullCounts() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, LEFT(s, n), LEFT(s, -1), LEFT(s, 2147483647) FROM inputs");
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"TINYINT", "SMALLINT"})
  void narrowIntegerParametersStayNative(String type) throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, LEFT(s, CAST(n AS " + type + ")) FROM inputs");
  }
}
