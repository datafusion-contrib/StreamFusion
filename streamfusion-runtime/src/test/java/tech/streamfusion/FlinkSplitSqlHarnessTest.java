package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkSplitSqlHarnessTest {
  @org.junit.jupiter.api.BeforeEach
  void requireReleasedHostFunction() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("SPLIT");
  }

  @Test
  void splitsLiteralSeparatorsAndPreservesEmptyTokens() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::strings,
        "SELECT id, SPLIT(s, '|'), SPLIT(s, '.*'), SPLIT(s, '\u4e2d') FROM inputs");
  }

  @Test
  void emptySeparatorsPreserveReleasedUtf16Units() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::strings, "SELECT id, SPLIT(s, '') FROM inputs");
  }
}
