package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkGreatestSqlHarnessTest {
  @Test
  void strictNullsAndSupportedTypes() throws Exception {
    parity(
        "SELECT id, GREATEST(n, i, -17), GREATEST(b, TRUE), GREATEST(d, CAST(1"
            + " AS DECIMAL(20, 3))) FROM texts");
  }

  @Test
  void predicatesAndNonNullableArguments() throws Exception {
    parity(
        "SELECT id, GREATEST(COALESCE(n, 0), CAST(1 AS BIGINT)) FROM texts WHERE GREATEST(i, 1) >"
            + " 0");
  }

  @Test
  void unverifiedOverloadsFallBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        StringFunctionTestInputs::text,
        "SELECT GREATEST(CAST(n AS DOUBLE), 1E0) FROM texts",
        "GREATEST");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::text, sql);
  }

  @Test
  void javaBackedUnicodeStringsMatchHost() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows("\ufffd", "\ud83d\ude00", null),
        "SELECT id, GREATEST(s, '\ud83d\ude00') FROM inputs");
  }

  @Test
  void asciiLiteralBranchesRemainNative() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, GREATEST(CASE WHEN n > 0 THEN 'a' ELSE 'z' END, 'm') FROM inputs");
  }
}
