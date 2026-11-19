package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkUrlEncodeSqlHarnessTest {
  @org.junit.jupiter.api.BeforeEach
  void requireReleasedHostFunction() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("URL_ENCODE");
  }

  @Test
  void formEncodingAndUnicode() throws Exception {
    parity("SELECT id, URL_ENCODE(s), URL_ENCODE(COALESCE(s, '')) FROM texts");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::text, sql);
  }
}
