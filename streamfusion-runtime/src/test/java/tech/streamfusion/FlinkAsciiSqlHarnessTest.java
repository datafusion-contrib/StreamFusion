package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkAsciiSqlHarnessTest {
  @Test
  void firstByteUsesJavaSignedByteSemantics() throws Exception {
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                null,
                "",
                "\u0000",
                "\u007f",
                "\u0080",
                "\u00e9",
                "\u07ff",
                "\u0800",
                "\u4e2d",
                "\uffff",
                "\ud800\udc00",
                "\ud83d\ude00",
                "\udbff\udfff",
                "a"),
        "SELECT id, ASCII(s), ASCII(COALESCE(s, '')) FROM inputs");
  }

  @Test
  void signedResultsComposeInPredicatesAndGroupKeys() throws Exception {
    NativeParity.assertParity(
        StringFunctionTestInputs::encodings,
        "SELECT ASCII(s), COUNT(*) FROM encodings WHERE ASCII(s) < 0 GROUP BY ASCII(s)");
  }
}
