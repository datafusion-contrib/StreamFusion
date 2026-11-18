package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkHexSqlHarnessTest {
  @Test
  void signedWidthsAndUtf8Bytes() throws Exception {
    parity("SELECT id, HEX(n), HEX(i), HEX(sh), HEX(t), HEX(s) FROM encodings");
  }

  @Test
  void literalsNullsAndNonNullableArguments() throws Exception {
    parity(
        "SELECT id, HEX(CAST(-1 AS SMALLINT)), HEX(CAST('ab' AS CHAR(4))), HEX(CAST(NULL AS"
            + " BIGINT)), HEX(CAST(NULL AS STRING)), HEX(COALESCE(n, 0)), HEX(COALESCE(s, '')) FROM"
            + " encodings");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::encodings, sql);
  }

  @Test
  void booleanArgumentIsRejectedByFlinkValidation() {
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      var tables = StringFunctionTestInputs.text();
      if (nativeEnabled) {
        tech.streamfusion.planner.NativePlanner.install(tables);
      }
      var error =
          org.junit.jupiter.api.Assertions.assertThrows(
              org.apache.flink.table.api.ValidationException.class,
              () -> tables.explainSql("SELECT id, HEX(b) FROM texts"));
      org.junit.jupiter.api.Assertions.assertTrue(error.getMessage().contains("HEX(<BOOLEAN>)"));
    }
  }
}
