package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkLeastSqlHarnessTest {
  @Test
  void strictNullsAndSupportedTypes() throws Exception {
    parity(
        "SELECT id, LEAST(i, n, 17), LEAST(b, FALSE), LEAST(d, CAST(-1 AS"
            + " DECIMAL(20, 3))) FROM texts");
  }

  @Test
  void predicatesAndNonNullableArguments() throws Exception {
    parity("SELECT id, LEAST(COALESCE(n, 0), CAST(1 AS BIGINT)) FROM texts WHERE LEAST(i, 10) < 4");
  }

  @Test
  void unverifiedOverloadsFallBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        StringFunctionTestInputs::text, "SELECT LEAST(CAST(n AS DOUBLE), 1E0) FROM texts", "LEAST");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::text, sql);
  }

  @Test
  void javaBackedUnicodeStringsMatchHost() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows("\ufffd", "\ud83d\ude00", null),
        "SELECT id, LEAST(s, '\ud83d\ude00') FROM inputs");
  }

  @Test
  void asciiLiteralBranchesRemainNative() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, LEAST(CASE WHEN n > 0 THEN 'a' ELSE 'z' END, 'm') FROM inputs");
  }
}
