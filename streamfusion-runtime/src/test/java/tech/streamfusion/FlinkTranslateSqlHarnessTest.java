package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkTranslateSqlHarnessTest {
  @org.junit.jupiter.api.BeforeEach
  void requireReleasedHostFunction() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("TRANSLATE3");
  }

  @Test
  void translateUsesCodepointsFirstDuplicateAndSpecialNullRules() throws Exception {
    parity(
        "SELECT id, TRANSLATE(s, f, t), TRANSLATE(s, 'aab', '123'),"
            + " TRANSLATE(s, CAST(NULL AS STRING), 'x'),"
            + " TRANSLATE(s, 'ab', CAST(NULL AS STRING)), TRANSLATE(s, '', 'x') FROM texts");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::text, sql);
  }

  @Test
  void excessReplacementCharactersAreIgnored() throws Exception {
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows("abba", "a\ud83d\ude00", "", null),
        "SELECT id, TRANSLATE(s, 'ab', 'XYZ') FROM inputs");
  }
}
