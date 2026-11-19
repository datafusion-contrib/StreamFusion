package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkJsonQuoteSqlHarnessTest {
  @org.junit.jupiter.api.BeforeEach
  void requireReleasedHostFunction() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("JSON_QUOTE");
  }

  @Test
  void functionAlsoRunsInsideTheNativePredicate() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::strings, "SELECT id FROM inputs WHERE JSON_QUOTE(s) <> '\"\"'");
  }

  @Test
  void quotesUnicodeAndControlsExactlyAsFlink() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::strings, "SELECT id, JSON_QUOTE(s) FROM inputs");
  }

  @Test
  void delAndFirstNonAsciiCodepointMatchFlink() throws Exception {
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows("\u007f", "\u0080", "a\u007f\u0080b", null),
        "SELECT id, JSON_QUOTE(s) FROM inputs");
  }
}
