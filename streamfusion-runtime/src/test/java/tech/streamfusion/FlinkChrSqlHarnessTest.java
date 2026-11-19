package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkChrSqlHarnessTest {
  @Test
  void lowByteAndNegativeRulesCoverAllIntegerWidths() throws Exception {
    NativeParity.assertParity(
        StringFunctionTestInputs::encodings,
        "SELECT id, CHR(n), CHR(CAST(n AS INT)), CHR(CAST(n AS SMALLINT)),"
            + " CHR(CAST(n AS TINYINT)), CHR(353 + id) FROM encodings");
  }

  @Test
  void zeroBytesAndNegativeResultsComposeWithNativeOperators() throws Exception {
    NativeParity.assertParity(
        StringFunctionTestInputs::encodings,
        "SELECT CHR(n), COUNT(*) FROM encodings WHERE CHR(n) IS NOT NULL GROUP BY CHR(n)");
  }
}
