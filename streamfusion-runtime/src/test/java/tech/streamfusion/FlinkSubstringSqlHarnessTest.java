package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkSubstringSqlHarnessTest {
  @Test
  void functionAlsoRunsInsideTheNativePredicate() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id FROM inputs WHERE SUBSTRING(s, n, 2) = 'ab'");
  }

  @Test
  void substringHandlesDynamicZeroNegativeAndNullParameters() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, SUBSTRING(s, n), SUBSTRING(s, n, n), SUBSTRING(s, n, 2), SUBSTRING(s, 0, n),"
            + " SUBSTRING(s, -2, n) FROM inputs");
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"TINYINT", "SMALLINT"})
  void narrowIntegerParametersStayNative(String type) throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, SUBSTRING(s, CAST(n AS " + type + "), CAST(n AS " + type + ")) FROM inputs");
  }
}
