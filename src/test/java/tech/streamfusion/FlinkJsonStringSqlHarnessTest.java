package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkJsonStringSqlHarnessTest {
  @Test
  void stringsKeepJacksonEscapesAndUnicode() throws Exception {
    StringBuilder controls = new StringBuilder();
    for (char value = 0; value < 160; value++) {
      controls.append(value);
    }
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                null,
                "",
                controls.toString(),
                "a/b\\c\"d",
                "\ud83d\ude00\u4e2d",
                "\udbff\udfff\u2028\u2029",
                "a\ud83d\ude00\u0000".repeat(2048)),
        "SELECT id, JSON_STRING(s), JSON_STRING(COALESCE(s, '')),"
            + " JSON_STRING(JSON_STRING(s)) FROM inputs");
  }

  @Test
  void booleanAndAllIntegerWidthsIncludeExtremesAndNulls() throws Exception {
    NativeParity.assertParity(
        StringFunctionTestInputs::encodings,
        "SELECT id, JSON_STRING(n), JSON_STRING(CAST(n AS INT)),"
            + " JSON_STRING(CAST(n AS SMALLINT)), JSON_STRING(CAST(n AS TINYINT)),"
            + " JSON_STRING(n > 0), JSON_STRING(CAST(NULL AS BOOLEAN)) FROM encodings");
  }

  @Test
  void predicatesAndCaseResultsStayNative() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::strings,
        "SELECT id, CASE WHEN s IS NULL THEN JSON_STRING(id) ELSE JSON_STRING(s) END"
            + " FROM inputs WHERE JSON_STRING(s) <> '\"\"' OR s IS NULL");
  }

  @Test
  void serializedValuesComposeWithNativeGroupKeys() throws Exception {
    NativeParity.assertParity(
        StringFunctionTestInputs::encodings,
        "SELECT JSON_STRING(n), COUNT(*) FROM encodings GROUP BY JSON_STRING(n)");
  }

  @Test
  void decimalsPreserveTrailingZerosAndExponentBoundaries() throws Exception {
    for (int scale : new int[] {0, 2, 6, 7, 9, 18, 38}) {
      NativeParity.assertParity(
          () -> DecimalJsonTestInputs.decimals(38, scale),
          "SELECT id, JSON_STRING(n), JSON_STRING(COALESCE(n, 0)),"
              + " JSON_STRING(CAST(NULL AS DECIMAL(38,9))) FROM decimals");
    }
    NativeParity.assertParity(
        () -> DecimalJsonTestInputs.decimals(18, 2),
        "SELECT JSON_STRING(n), COUNT(*) FROM decimals GROUP BY JSON_STRING(n)");
  }

  @Test
  void additionalScalarAndContainerTypesUseHostSemantics() throws Exception {
    for (String expression :
        new String[] {"CAST(n AS DOUBLE)", "ARRAY[n]", "JSON_OBJECT('n' VALUE n)"}) {
      NativeParity.assertParity(
          StringFunctionTestInputs::encodings,
          "SELECT id, JSON_STRING(" + expression + ") FROM encodings");
    }
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::bytes, "SELECT id, JSON_STRING(b) FROM inputs");
  }
}
