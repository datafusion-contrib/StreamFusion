package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkRpadSqlHarnessTest {
  @Test
  void functionAlsoRunsInsideTheNativePredicate() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters, "SELECT id FROM inputs WHERE RPAD(s, n, p) = 'xy'");
  }

  @Test
  void rpadMatchesUtf16LengthAndDynamicPadding() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, RPAD(s, n, p), RPAD(s, n, ''), RPAD(s, 0, p) FROM inputs");
  }
}
