package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkBtrimSqlHarnessTest {
  @org.junit.jupiter.api.BeforeEach
  void requireReleasedHostFunction() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("BTRIM");
  }

  @Test
  void btrimUsesCharacterSetsAndPreservesNonSpaceWhitespace() throws Exception {
    parity(
        "SELECT id, BTRIM(s), BTRIM(s, ' ab'), BTRIM(s, ''),"
            + " BTRIM(s, CAST(NULL AS STRING)) FROM texts");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::text, sql);
  }

  @Test
  void javaBackedDynamicTrimSetsMatchHost() throws Exception {
    BuiltinFunctionParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, BTRIM(s, p) FROM inputs");
  }
}
