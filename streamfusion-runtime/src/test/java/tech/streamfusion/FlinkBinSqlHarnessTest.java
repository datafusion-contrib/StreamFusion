package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkBinSqlHarnessTest {
  @Test
  void signedWidthsAndExtremes() throws Exception {
    parity("SELECT id, BIN(n), BIN(i), BIN(sh), BIN(t) FROM encodings");
  }

  @Test
  void literalsNullsAndNonNullableArguments() throws Exception {
    parity(
        "SELECT id, BIN(CAST(-1 AS TINYINT)), BIN(CAST(NULL AS BIGINT)), BIN(COALESCE(n, 0)) FROM"
            + " encodings");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::encodings, sql);
  }
}
