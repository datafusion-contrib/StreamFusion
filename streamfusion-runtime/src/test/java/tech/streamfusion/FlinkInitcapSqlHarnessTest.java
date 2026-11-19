package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkInitcapSqlHarnessTest {
  @Test
  void initcapUsesAsciiWordBoundaries() throws Exception {
    parity(
        "SELECT id, INITCAP(s), INITCAP(COALESCE(s, 'aBC')),"
            + " INITCAP(CAST('aB' AS CHAR(5))) FROM texts");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::text, sql);
  }
}
