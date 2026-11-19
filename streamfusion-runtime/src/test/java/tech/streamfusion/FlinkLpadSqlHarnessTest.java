package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkLpadSqlHarnessTest {
  @Test
  void functionAlsoRunsInsideTheNativePredicate() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters, "SELECT id FROM inputs WHERE LPAD(s, n, p) = 'xy'");
  }

  @Test
  void lpadMatchesUtf16LengthAndDynamicPadding() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, LPAD(s, n, p), LPAD(s, n, ''), LPAD(s, 0, p) FROM inputs");
  }
}
