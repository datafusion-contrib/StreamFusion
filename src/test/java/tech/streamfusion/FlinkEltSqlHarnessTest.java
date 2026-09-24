package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkEltSqlHarnessTest {
  @org.junit.jupiter.api.BeforeEach
  void requireReleasedHostFunction() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ELT");
  }

  @Test
  void eltOnlyPropagatesSelectedNullAndBoundsChecksIndex() throws Exception {
    parity(
        "SELECT id, ELT(i, s, f, t), ELT(i, 'first', CAST(NULL AS STRING), 'third'),"
            + " ELT(1, s, CAST(NULL AS STRING)), ELT(CAST(NULL AS INT), s) FROM texts");
  }

  @Test
  void unverifiedOverloadsFallBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        StringFunctionTestInputs::text,
        "SELECT ELT(CAST(4294967297 AS BIGINT), s, f) FROM texts",
        "ELT");
    NativeParity.assertFallbackReasonContains(
        StringFunctionTestInputs::text,
        "SELECT ELT(i, X'AB', X'CD') FROM texts",
        "unsupported generated-expression result type BINARY(1)");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::text, sql);
  }
}
