package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkUrlDecodeSqlHarnessTest {
  @org.junit.jupiter.api.BeforeEach
  void requireReleasedHostFunction() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("URL_DECODE");
  }

  @Test
  void formDecodingAndMalformedUtf8() throws Exception {
    parity("SELECT id, URL_DECODE(s), URL_DECODE(COALESCE(s, '')) FROM texts");
  }

  @Test
  void urlDecoderMatchesJavaAcrossByteAndUnicodeDigitBoundaries() throws Exception {
    NativeParity.assertParity(
        StringFunctionTestInputs::urlBoundaries, "SELECT id, URL_DECODE(s) FROM urls");
  }

  @Test
  void parseUrlHostMatchesHost() throws Exception {
    BuiltinFunctionParity.assertParity(
        StringFunctionTestInputs::text, "SELECT PARSE_URL(u, 'HOST') FROM texts");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::text, sql);
  }
}
