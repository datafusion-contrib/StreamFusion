package tech.streamfusion;

import static tech.streamfusion.NativeParity.assertParity;

import org.junit.jupiter.api.Test;

class FlinkJsonExistsSqlHarnessTest {
  @Test
  void nativeAdmissionDoesNotRequireCompatibilityOptIn() throws Exception {
    NativeParity.assertParity(
        JsonFunctionTestInputs::documents, "SELECT id, JSON_EXISTS(s, '$.a') FROM inputs");
  }

  @Test
  void strictLaxAndAllNonThrowingErrorPolicies() throws Exception {
    assertParity(
        JsonFunctionTestInputs::documents,
        "SELECT id, JSON_EXISTS(s, '$.a'), JSON_EXISTS(s, '$.a' TRUE ON ERROR), "
            + "JSON_EXISTS(s, '$.a' UNKNOWN ON ERROR), JSON_EXISTS(s, 'lax $.a'), "
            + "JSON_EXISTS(s, 'lax $.a' TRUE ON ERROR), "
            + "JSON_EXISTS(s, 'lax $.a' UNKNOWN ON ERROR) FROM inputs");
  }

  @Test
  void containersRootAndNestedPaths() throws Exception {
    assertParity(
        JsonFunctionTestInputs::documents,
        "SELECT id, JSON_EXISTS(s, '$'), JSON_EXISTS(s, 'lax $'), "
            + "JSON_EXISTS(s, '$.a.b[1]' UNKNOWN ON ERROR), JSON_EXISTS(s, '$[1].a'), "
            + "JSON_EXISTS(s, '$[''a b'']'), JSON_EXISTS(s, ' StRiCt $.a') FROM inputs");
  }

  @Test
  void unicodeAndQuotedMemberNamesKeepExistencePolicies() throws Exception {
    for (String path :
        new String[] {
          "$.\u7528\u6237['\u59d3.\u540d']", "$[\"O'Reilly\"]", "$['a\"b']", "$['a]b']",
          "$['\ud83d\ude00']", "$['*']", "$['$']", "$.\u00e9\u0661"
        }) {
      String literal = path.replace("'", "''");
      assertParity(
          JsonFunctionTestInputs::memberNames,
          "SELECT id, JSON_EXISTS(s, '"
              + literal
              + "' UNKNOWN ON ERROR), "
              + "JSON_EXISTS(s, 'lax "
              + literal
              + "') FROM inputs");
    }
    assertParity(
        JsonFunctionTestInputs::memberNames,
        "SELECT id FROM inputs WHERE JSON_EXISTS(s, 'lax $.\u7528\u6237[''\u59d3.\u540d'']')");
    JsonFunctionTestInputs.assertFailsLikeFlink(
        "{}", "JSON_EXISTS(s, '$.\u7528\u6237[''\u59d3.\u540d'']' ERROR ON ERROR)");
  }

  @Test
  void wideDocumentsKeepDuplicateKeysAndUnselectedFieldValidation() throws Exception {
    assertParity(
        JsonFunctionTestInputs::wideDocuments,
        "SELECT id, JSON_EXISTS(s, '$.a' UNKNOWN ON ERROR), "
            + "JSON_EXISTS(s, 'lax $.a'), JSON_EXISTS(s, '$.a[1].b'), "
            + "JSON_EXISTS(s, '$.field63'), JSON_EXISTS(s, '$.absent' TRUE ON ERROR) FROM inputs");
  }

  @Test
  void unknownBooleanContextsKeepFlinkBoxedNullFailure() {
    NativeFailureParity.run(
            () -> TextTimeFunctionTestInputs.textRows("null"),
            "SELECT JSON_EXISTS(s, '$' UNKNOWN ON ERROR) IS TRUE FROM inputs")
        .assertFailure(
            NullPointerException.class,
            "booleanValue",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void errorPolicyKeepsFlinkRowShortCircuiting() throws Exception {
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows("{\"a\":1}", "invalid"),
        "SELECT id, id = 1 OR JSON_EXISTS(s, '$.a' ERROR ON ERROR) FROM inputs");
  }

  @Test
  void nativePredicateKeepsTheMatchingRows() throws Exception {
    assertParity(
        JsonFunctionTestInputs::documents, "SELECT id FROM inputs WHERE JSON_EXISTS(s, 'lax $.a')");
  }

  @Test
  void errorPolicyPreservesFlinkExceptionTypeAndMessage() {
    JsonFunctionTestInputs.assertFailsLikeFlink("{}", "JSON_EXISTS(s, '$.a' ERROR ON ERROR)");
    JsonFunctionTestInputs.assertFailsLikeFlink("null", "JSON_EXISTS(s, 'lax $' ERROR ON ERROR)");
    JsonFunctionTestInputs.assertFailsLikeFlink("{", "JSON_EXISTS(s, '$.a' ERROR ON ERROR)");
    JsonFunctionTestInputs.assertFailsLikeFlink(
        "{\"a\":1}", "JSON_EXISTS(s, '$.a.b' ERROR ON ERROR)");
  }

  @Test
  void errorPolicyPreservesSuccessfulAndNullRows() throws Exception {
    assertParity(
        () -> TextTimeFunctionTestInputs.textRows(null, "{\"a\":1}", "{\"a\":[]}", "{\"a\":{}}"),
        "SELECT id, JSON_EXISTS(s, '$.a' ERROR ON ERROR) FROM inputs");
  }

  @Test
  void slicedWildcardAndRecursivePathsMatchFlink() throws Exception {
    assertParity(
        JsonFunctionTestInputs::documents, "SELECT id, JSON_EXISTS(s, '$[*][1:2]') FROM inputs");
    assertParity(
        JsonFunctionTestInputs::documents, "SELECT id, JSON_EXISTS(s, '$..a') FROM inputs");
  }

  @Test
  void constraintsAndInvalidUnselectedValues() throws Exception {
    assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                "[".repeat(1000) + "0" + "]".repeat(1000),
                "[".repeat(1001) + "0" + "]".repeat(1001),
                "{\"a\":1,\"" + "k".repeat(50_001) + "\":2}",
                "{\"a\":1,\"bad\":1e2147483648}",
                "{\"a\":1,\"bad\":\"\\ud800\"}"),
        "SELECT id, JSON_EXISTS(s, '$' UNKNOWN ON ERROR), "
            + "JSON_EXISTS(s, '$.a' UNKNOWN ON ERROR), JSON_EXISTS(s, 'lax $.a') FROM inputs");
  }
}
