package tech.streamfusion;

import static tech.streamfusion.NativeParity.assertFallback;
import static tech.streamfusion.NativeParity.assertParity;

import org.junit.jupiter.api.Test;

class FlinkJsonValueSqlHarnessTest {
  @Test
  void nativeAdmissionDoesNotRequireCompatibilityOptIn() throws Exception {
    NativeParity.assertParity(
        JsonFunctionTestInputs::documents, "SELECT id, JSON_VALUE(s, '$.a') FROM inputs");
  }

  @Test
  void strictAndLaxDistinguishMissingNullContainersAndErrors() throws Exception {
    assertParity(
        JsonFunctionTestInputs::documents,
        "SELECT id, JSON_VALUE(s, '$.a'), "
            + "JSON_VALUE(s, 'lax $.a' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), "
            + "JSON_VALUE(s, 'strict $.a' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), "
            + "JSON_VALUE(s, '$'), JSON_VALUE(s, 'lax $') FROM inputs");
  }

  @Test
  void nestedMembersQuotedMembersAndArrayIndexes() throws Exception {
    assertParity(
        JsonFunctionTestInputs::documents,
        "SELECT id, JSON_VALUE(s, '$.a.b[1]'), JSON_VALUE(s, '$[1].a'), "
            + "JSON_VALUE(s, '$[''a b'']'), JSON_VALUE(s, ' LaX $.a'), "
            + "JSON_VALUE(s, '$.a' RETURNING VARCHAR(3)) FROM inputs");
  }

  @Test
  void unicodeAndQuotedMemberNamesKeepPathPolicies() throws Exception {
    for (String path :
        new String[] {
          "$.\u7528\u6237['\u59d3.\u540d']", "$[\"O'Reilly\"]", "$['a\"b']", "$['a]b']",
          "$['\ud83d\ude00']", "$['*']", "$['$']", "$.\u00e9\u0661"
        }) {
      String literal = path.replace("'", "''");
      assertParity(
          JsonFunctionTestInputs::memberNames,
          "SELECT id, JSON_VALUE(s, '"
              + literal
              + "' DEFAULT 'error' ON ERROR), "
              + "JSON_VALUE(s, 'lax "
              + literal
              + "' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR) FROM inputs");
    }
    NativeParity.assertFallbackReasonContains(
        JsonFunctionTestInputs::memberNames,
        "SELECT JSON_VALUE(s, '$.\u7528\u6237[''\u59d3.\u540d'']'), COUNT(*) FROM inputs "
            + "GROUP BY JSON_VALUE(s, '$.\u7528\u6237[''\u59d3.\u540d'']')",
        "JSON string identity requires a final projection");
  }

  @Test
  void wideDocumentsKeepDuplicateKeysAndUnselectedFieldValidation() throws Exception {
    assertParity(
        JsonFunctionTestInputs::wideDocuments,
        "SELECT id, JSON_VALUE(s, '$.a'), "
            + "JSON_VALUE(s, 'lax $.a' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), "
            + "JSON_VALUE(s, '$.a[1].b'), JSON_VALUE(s, '$.field63') FROM inputs");
  }

  @Test
  void numbersKeepJacksonBigDecimalAndBigIntegerText() throws Exception {
    assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                "-0",
                "-0.0",
                "1.2300",
                "1e3",
                "100e-1",
                "0.000001",
                "0.0000001",
                "92233720368547758081234567890",
                "1e2147483647",
                "0e999999999",
                "1e2147483648",
                "1e-2147483648",
                "0.1e-2147483647",
                "1e+00000000000000000000001",
                "1".repeat(1001),
                "{\"a\":1,\"bad\":1e2147483648}"),
        "SELECT id, JSON_VALUE(s, '$'), JSON_VALUE(s, 'lax $' DEFAULT 'empty' ON EMPTY), "
            + "JSON_VALUE(s, '$.a' DEFAULT 'error' ON ERROR) FROM inputs");
  }

  @Test
  void downstreamStringGroupingFallsBackUntilIdentityCanCrossArrow() throws Exception {
    String sql =
        "SELECT JSON_VALUE(s, '$.a'), COUNT(*) FROM inputs "
            + "WHERE JSON_VALUE(s, '$.a') IS NOT NULL GROUP BY JSON_VALUE(s, '$.a')";
    NativeParity.assertFallbackReasonContains(
        JsonFunctionTestInputs::documents, sql, "JSON string identity requires a final projection");
  }

  @Test
  void jacksonNumericParserSwitchAndConstraints() throws Exception {
    assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                "1." + "0".repeat(490) + "e2147483648",
                "1." + "0".repeat(480) + "e2147483648",
                "-" + "1".repeat(1000),
                "1." + "1".repeat(999),
                "[1." + "1".repeat(1000) + "]",
                "[0." + "1".repeat(1000) + "]",
                "true\u007f",
                "null\u0301",
                "null\u0870",
                "true\ud83d\ude00"),
        "SELECT id, JSON_VALUE(s, '$' DEFAULT 'error' ON ERROR), JSON_VALUE(s, 'lax $' DEFAULT"
            + " 'empty' ON EMPTY DEFAULT 'error' ON ERROR) FROM inputs");
  }

  @Test
  void nullAndNonCharacterDefaultsFallBack() throws Exception {
    assertFallback(
        JsonFunctionTestInputs::documents,
        "SELECT id, JSON_VALUE(s, '$.a' DEFAULT CAST(NULL AS STRING) ON ERROR) FROM inputs");
    assertFallback(
        JsonFunctionTestInputs::documents,
        "SELECT id, JSON_VALUE(s, '$.a' DEFAULT 12 ON ERROR) FROM inputs");
  }

  @Test
  void emptyErrorIsNotSwallowedByOnErrorDefault() {
    JsonFunctionTestInputs.assertFails(
        "{}",
        "JSON_VALUE(s, 'lax $.a' ERROR ON EMPTY DEFAULT 'error' ON ERROR)",
        "JSON_VALUE EMPTY");
    JsonFunctionTestInputs.assertFails(
        "{\"a\":null}", "JSON_VALUE(s, 'strict $.a' ERROR ON ERROR)", "JSON_VALUE ERROR");
  }

  @Test
  void stringErrorPolicyKeepsFlinkRowShortCircuiting() throws Exception {
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows("{\"a\":\"ok\"}", "invalid"),
        "SELECT id, id = 1 OR JSON_VALUE(s, '$.a' ERROR ON ERROR) = 'ok' FROM inputs");
  }

  @Test
  void unverifiedPathsFallBack() throws Exception {
    assertFallback(
        JsonFunctionTestInputs::documents, "SELECT id, JSON_VALUE(s, '$[*][1:2]') FROM inputs");
  }
}
