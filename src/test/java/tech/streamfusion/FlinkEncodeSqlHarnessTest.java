package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkEncodeSqlHarnessTest {
  @Test
  void encodesUnicodeNullsAndUnmappableCharacters() throws Exception {
    assertEncodingParity(
        StringFunctionTestInputs::encodings,
        "SELECT id, ENCODE(s, 'UTF-8'), ENCODE(s, 'ASCII'), ENCODE(s, 'latin1') FROM encodings");
  }

  @Test
  void unverifiedFormsFallBackBeforeExecution() throws Exception {
    NativeParity.assertFallback(
        StringFunctionTestInputs::encodings, "SELECT id, ENCODE(s, 'UTF-32') FROM encodings");
  }

  @Test
  void utf16EncodesBomEndianAliasesAndSupplementaryCharacters() throws Exception {
    assertEncodingParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                null,
                "",
                "a\u0000b",
                "\ufeff",
                "\ufffe",
                "\u007f\u0080",
                "\u4e2d\ud83d\ude00",
                "\ud7ff\ue000\uffff",
                "\ud83d\ude00".repeat(2048)),
        "SELECT id, ENCODE(s, 'UTF-16'), ENCODE(s, 'UnicodeBigUnmarked'),"
            + " ENCODE(s, 'UnicodeLittleUnmarked'), ENCODE(s, 'Unicode') FROM inputs");
  }

  @Test
  void dynamicCharsetMatchesHost() throws Exception {
    assertEncodingParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, ENCODE(s, CASE WHEN n > 0 THEN 'UTF-8' ELSE 'ASCII' END) FROM inputs");
  }

  private static void assertEncodingParity(
      java.util.function.Supplier<org.apache.flink.table.api.TableEnvironment> environment,
      String sql)
      throws Exception {
    if (tech.streamfusion.compat.FlinkTestCapabilities.VARIABLE_LENGTH_ENCODE) {
      NativeParity.assertParity(environment, sql);
    } else {
      NativeParity.assertFallbackReasonContains(environment, sql, "BINARY(1)");
    }
  }
}
