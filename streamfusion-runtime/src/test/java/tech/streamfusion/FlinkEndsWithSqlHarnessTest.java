package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkEndsWithSqlHarnessTest {
  @org.junit.jupiter.api.BeforeEach
  void requireReleasedHostFunction() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ENDSWITH");
  }

  @Test
  void suffixArgumentsAndWildcards() throws Exception {
    parity(
        "SELECT id, ENDSWITH(s, needle), ENDSWITH(s, ''), ENDSWITH(s, 'bc'), ENDSWITH('abc',"
            + " needle), ENDSWITH(s, '_%'), ENDSWITH(s, '\\'), ENDSWITH(CAST(NULL AS STRING),"
            + " needle) FROM searches");
  }

  @Test
  void predicatesAndNonNullableArguments() throws Exception {
    parity(
        "SELECT id, ENDSWITH(COALESCE(s, ''), 'bc'), ENDSWITH(CAST('abc' AS CHAR(3)), 'c') FROM"
            + " searches WHERE ENDSWITH(s, 'bc')");
  }

  @Test
  void binarySuffixMatchesHost() throws Exception {
    BuiltinFunctionParity.assertParity(
        StringFunctionTestInputs::search,
        "SELECT ENDSWITH(binary_value, X'FF') FROM searches");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::search, sql);
  }
}
