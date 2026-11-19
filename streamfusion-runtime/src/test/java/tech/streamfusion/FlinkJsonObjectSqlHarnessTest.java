package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkJsonObjectSqlHarnessTest {
  @Test
  void scalarValuesNullPoliciesAndNonNullableInputs() throws Exception {
    NativeParity.assertParity(
        StringFunctionTestInputs::encodings,
        "SELECT id, JSON_OBJECT('z' VALUE s, 'b' VALUE n > 0, 'a' VALUE n),"
            + " JSON_OBJECT('s' VALUE s, 'n' VALUE n ABSENT ON NULL),"
            + " JSON_OBJECT('s' VALUE COALESCE(s, ''), 'n' VALUE COALESCE(n, 0)),"
            + " JSON_OBJECT() FROM encodings");
  }

  @Test
  void duplicatesKeepLastInsertedValueUnderEachNullPolicy() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::strings,
        "SELECT id, JSON_OBJECT('k' VALUE id, 'k' VALUE s),"
            + " JSON_OBJECT('k' VALUE id, 'k' VALUE s ABSENT ON NULL),"
            + " JSON_OBJECT('k' VALUE s, 'k' VALUE CAST(NULL AS STRING)),"
            + " JSON_OBJECT('k' VALUE s, 'k' VALUE CAST(NULL AS STRING) ABSENT ON NULL)"
            + " FROM inputs");
  }

  @Test
  void keysUseUtf16OrderAndValuesUseJacksonEscaping() throws Exception {
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                null,
                "",
                "\u0000\u001f\t/\\\"",
                "\ud83d\ude00\u4e2d",
                "\u007f\u0080",
                "\u4e2d\ud83d\ude00".repeat(2048)),
        "SELECT id, JSON_OBJECT('\ufffd' VALUE id, '\ud83d\ude00' VALUE s,"
            + " '' VALUE s, 'a\"b\\c' VALUE s, '\u001f' VALUE s) FROM inputs");
  }

  @Test
  void allIntegerWidthsPredicatesAndCaseExpressions() throws Exception {
    NativeParity.assertParity(
        StringFunctionTestInputs::encodings,
        "SELECT id, JSON_OBJECT('tiny' VALUE CAST(n AS TINYINT),"
            + " 'small' VALUE CAST(n AS SMALLINT), 'int' VALUE CAST(n AS INT),"
            + " 'long' VALUE n, 'value' VALUE CASE WHEN n > 0 THEN s ELSE '' END)"
            + " FROM encodings WHERE JSON_OBJECT('s' VALUE s ABSENT ON NULL) <> '{}'");
  }

  @Test
  void constructorInsideCaseIsAnOrdinaryStringValue() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::strings,
        "SELECT id, JSON_OBJECT('v' VALUE CASE WHEN s IS NULL THEN '{}'"
            + " ELSE JSON_OBJECT('s' VALUE s) END) FROM inputs");
  }

  @Test
  void decimalValuesShareExactScalarFormattingAndNullPolicies() throws Exception {
    for (int scale : new int[] {0, 2, 7, 9, 38}) {
      NativeParity.assertParity(
          () -> DecimalJsonTestInputs.decimals(38, scale),
          "SELECT id, JSON_OBJECT('n' VALUE n), JSON_OBJECT('n' VALUE n ABSENT ON NULL),"
              + " JSON_OBJECT('n' VALUE n, 'n' VALUE CAST(NULL AS DECIMAL(38,9)) ABSENT ON NULL)"
              + " FROM decimals");
    }
    NativeParity.assertParity(
        () -> DecimalJsonTestInputs.decimals(18, 2),
        "SELECT JSON_OBJECT('n' VALUE n), COUNT(*) FROM decimals GROUP BY JSON_OBJECT('n' VALUE"
            + " n)");
  }

  @Test
  void containersAndNestedRawJsonUseHostSemantics() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("JSON");
    for (String expression :
        new String[] {
          "CAST(n AS DOUBLE)",
          "ARRAY[n]",
          "JSON_OBJECT('n' VALUE n)",
          "JSON_ARRAY(n)",
          "JSON(CASE WHEN n > 0 THEN '{}' ELSE '[]' END)"
        }) {
      NativeParity.assertParity(
          StringFunctionTestInputs::encodings,
          "SELECT id, JSON_OBJECT('value' VALUE " + expression + ") FROM encodings");
    }
  }
}
