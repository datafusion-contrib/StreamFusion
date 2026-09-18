package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkJsonUnquoteSqlHarnessTest {
  @Test
  void jacksonPrefixValidationKeepsTheTrailingTextBehavior() throws Exception {
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                "\"a\" tail\\\"",
                "\"a\" \\u+123\"",
                "\"a\" \\u-123\"",
                "\"a\" \\u\uff11\uff12\uff13\uff14\"",
                "\"a\" \\u00\uff45\uff19\""),
        "SELECT id, JSON_UNQUOTE(s) FROM inputs");
  }

  @Test
  void functionAlsoRunsInsideTheNativePredicate() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::strings, "SELECT id FROM inputs WHERE JSON_UNQUOTE(s) <> 'ok'");
  }

  @Test
  void unquotesValidJsonAndPreservesInvalidInput() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::strings, "SELECT id, JSON_UNQUOTE(s) FROM inputs");
  }

  @Test
  void truncatedUnicodeAfterAValidFirstTokenFailsBothJobs() {
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      var tables = TextTimeFunctionTestInputs.textRows("\"a\" \\u1\"");
      tech.streamfusion.planner.PhysicalPlanScan scan =
          nativeEnabled ? tech.streamfusion.planner.NativePlanner.install(tables) : null;
      Exception error =
          org.junit.jupiter.api.Assertions.assertThrows(
              Exception.class,
              () -> {
                try (var rows = tables.executeSql("SELECT JSON_UNQUOTE(s) FROM inputs").collect()) {
                  while (rows.hasNext()) {
                    rows.next();
                  }
                }
              });
      StringBuilder causes = new StringBuilder();
      for (Throwable cause = error; cause != null; cause = cause.getCause()) {
        causes.append(cause).append('\n');
      }
      org.junit.jupiter.api.Assertions.assertTrue(
          causes
              .toString()
              .contains(
                  nativeEnabled
                      ? "JSON_UNQUOTE truncated Unicode escape"
                      : "StringIndexOutOfBoundsException"),
          causes.toString());
      if (scan != null) {
        org.junit.jupiter.api.Assertions.assertTrue(scan.substitutions() > 0);
      }
    }
  }
}
