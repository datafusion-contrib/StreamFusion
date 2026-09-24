package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkToBase64SqlHarnessTest {
  @Test
  void utf8AndPadding() throws Exception {
    parity("SELECT id, TO_BASE64(s) FROM encodings");
  }

  @Test
  void literalsNullsAndNonNullableArguments() throws Exception {
    parity(
        "SELECT id, TO_BASE64(CAST('ab' AS CHAR(4))), TO_BASE64(CAST(NULL AS STRING)),"
            + " TO_BASE64(COALESCE(s, '')) FROM encodings");
  }

  @Test
  void fromBase64NullableBranchesMatchHost() throws Exception {
    BuiltinFunctionParity.assertParity(
        StringFunctionTestInputs::encodings,
        "SELECT FROM_BASE64(CASE WHEN s IS NULL THEN CAST(NULL AS STRING) ELSE 'YQ==' END) FROM"
            + " encodings");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::encodings, sql);
  }

  @Test
  void binaryBytesNeedNoUtf8Conversion() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::bytes, "SELECT id, TO_BASE64(b) FROM inputs");
  }

  @Test
  void binaryLiteralsAndCompositions() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::bytes,
        "SELECT id, TO_BASE64(COALESCE(b, CAST(X'00FF' AS BYTES))),"
            + " TO_BASE64(CAST(X'00FF80' AS BYTES)),"
            + " TO_BASE64(CAST(NULL AS BYTES)), CHAR_LENGTH(TO_BASE64(b))"
            + " FROM inputs WHERE TO_BASE64(b) <> ''");
  }
}
