package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.NativePlanner;

class FlinkLocateSqlHarnessTest {
  @Test
  void positionsCountUnicodeCharacters() throws Exception {
    parity(
        "SELECT id, LOCATE(needle, s), LOCATE('', s), LOCATE('abc', s), LOCATE(needle, 'abc') FROM"
            + " searches");
  }

  @Test
  void predicatesAndNonNullableArguments() throws Exception {
    parity(
        "SELECT id, LOCATE('b', COALESCE(s, ''), 2), LOCATE('b', CAST('abc' AS CHAR(3)), 2),"
            + " LOCATE(needle, s, CAST(NULL AS INT)) FROM searches WHERE LOCATE('b', s, 1) > 0");
  }

  @Test
  void locateSupportsDynamicAndExtremeStartPositions() throws Exception {
    NativeParity.assertParity(
        StringFunctionTestInputs::search,
        "SELECT id, LOCATE(needle, s, start_pos), LOCATE('', s, start_pos),"
            + " LOCATE(needle, s, 0), LOCATE(needle, s, -1), LOCATE(needle, s, 2),"
            + " LOCATE(needle, s, 2147483647), LOCATE(needle, s, -2147483648),"
            + " LOCATE(needle, s, CAST(2 AS SMALLINT)),"
            + " LOCATE(needle, s, CAST(2 AS TINYINT)) FROM searches");
  }

  @Test
  void locateLiteralSearchersPreserveUnicodeAndNullStarts() throws Exception {
    NativeParity.assertParity(
        StringFunctionTestInputs::search,
        "SELECT id, LOCATE('abc', s, start_pos), LOCATE('\ud83d\ude00', s, start_pos),"
            + " LOCATE('\ud83d\ude00', s, 2), LOCATE('', s, start_pos),"
            + " LOCATE('abc', s, -2147483648), LOCATE(needle, 'abcabc', start_pos)"
            + " FROM searches");
  }

  @Test
  void bigintStartDoesNotSilentlyNarrow() {
    String plan =
        NativePlanner.explain(
            StringFunctionTestInputs.search(),
            "SELECT LOCATE(needle, s, CAST(4294967297 AS BIGINT)) FROM searches");
    assertFalse(plan.contains("NativeCalc"), plan);
    assertTrue(plan.contains("LOCATE start must be"), plan);
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::search, sql);
  }
}
