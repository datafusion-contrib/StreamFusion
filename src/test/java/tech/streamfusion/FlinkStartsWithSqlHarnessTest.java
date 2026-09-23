package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkStartsWithSqlHarnessTest {
  @org.junit.jupiter.api.BeforeEach
  void requireReleasedHostFunction() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("STARTSWITH");
  }

  @Test
  void prefixArgumentsAndWildcards() throws Exception {
    parity(
        "SELECT id, STARTSWITH(s, needle), STARTSWITH(s, ''), STARTSWITH(s, 'ab'),"
            + " STARTSWITH('abc', needle), STARTSWITH(s, '%_'), STARTSWITH(s, '\\'), STARTSWITH(s,"
            + " CAST(NULL AS STRING)) FROM searches");
  }

  @Test
  void predicatesAndNonNullableArguments() throws Exception {
    parity(
        "SELECT id, STARTSWITH(COALESCE(s, ''), 'ab'), STARTSWITH(CAST('abc' AS CHAR(3)), 'a') FROM"
            + " searches WHERE STARTSWITH(s, 'ab')");
  }

  @Test
  void binaryPrefixMatchesHost() throws Exception {
    BuiltinFunctionParity.assertParity(
        StringFunctionTestInputs::search,
        "SELECT STARTSWITH(binary_value, X'FF') FROM searches");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::search, sql);
  }
}
