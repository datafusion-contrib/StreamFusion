package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkJsonEmptyMemberSqlHarnessTest {
  @Test
  void releasedFlinkSelectsEmptyNamesAsOrdinaryMembers() {
    for (String path : new String[] {"$['']", "$[\"\"]", "$[ '' ]", "$[ \"\" ]"}) {
      assertEquals(
          "last",
          SqlJsonUtils.jsonValue(
              "{\"\":\"first\",\" \":\"space\",\"\":\"last\"}",
              path,
              JsonValueOnEmptyOrError.DEFAULT,
              "empty",
              JsonValueOnEmptyOrError.DEFAULT,
              "error"));
      assertEquals(true, SqlJsonUtils.jsonExists("{\"\":[]}", path));
      assertEquals(false, SqlJsonUtils.jsonExists("{\" \":1}", "lax " + path));
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "$['']",
        "$[\"\"]",
        "$[ '' ]",
        "$[ \"\" ]  ",
        "$['']['']",
        "$[''].a",
        "$.a['']",
        "$[''][1]['']",
        "$[0]['']"
      })
  void emptyMembersPreserveSelectionAndPolicies(String path) throws Exception {
    String literal = path.replace("'", "''");
    String sql =
        "SELECT id, JSON_VALUE(s, '"
            + literal
            + "' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), "
            + "JSON_VALUE(s, 'lax "
            + literal
            + "' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), "
            + "JSON_EXISTS(s, '"
            + literal
            + "' UNKNOWN ON ERROR), "
            + "JSON_EXISTS(s, 'lax "
            + literal
            + "' UNKNOWN ON ERROR), "
            + "JSON_EXISTS(s, '"
            + literal
            + "' TRUE ON ERROR), "
            + "JSON_EXISTS(s, '"
            + literal
            + "') FROM inputs";
    assertTrue(NativePlanner.explain(documents(), sql).contains("NativeCalc"));
    NativeParity.assertParity(FlinkJsonEmptyMemberSqlHarnessTest::documents, sql);
  }

  @ParameterizedTest
  @ValueSource(strings = {"INTEGER", "BOOLEAN", "DOUBLE"})
  void typedReturningKeepsEmptyAndErrorPolicies(String type) throws Exception {
    String value;
    switch (type) {
      case "INTEGER":
        value = "-2147483648";
        break;
      case "BOOLEAN":
        value = "false";
        break;
      default:
        value = "-1.25e-10";
        break;
    }
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                null, "{}", "{\"\":null}", "{\"\":[]}", "invalid", "{\"\":" + value + "}"),
        "SELECT id, JSON_VALUE(s, '$['''']' RETURNING "
            + type
            + "), "
            + "JSON_VALUE(s, 'lax $[\"\"]' RETURNING "
            + type
            + ") FROM inputs");
  }

  @Test
  void adjacentEmptyAndSpaceMembersKeepIndependentSelectionState() throws Exception {
    NativeParity.assertParity(
        FlinkJsonEmptyMemberSqlHarnessTest::documents,
        "SELECT id, JSON_VALUE(s, '$['''']'), JSON_VALUE(s, '$['' '']'), "
            + "JSON_VALUE(s, '$.a'), JSON_VALUE(s, '$['''']['''']') FROM inputs");
    NativeParity.assertFallbackReasonContains(
        FlinkJsonEmptyMemberSqlHarnessTest::documents,
        "SELECT JSON_VALUE(s, '$['''']'), COUNT(*) FROM inputs "
            + "WHERE JSON_EXISTS(s, 'lax $['''']') GROUP BY JSON_VALUE(s, '$['''']')",
        "JSON string identity requires a final projection");
  }

  @Test
  void missingEmptyMemberPreservesErrorPolicies() {
    JsonFunctionTestInputs.assertFailsLikeFlink("{}", "JSON_EXISTS(s, '$['''']' ERROR ON ERROR)");
    JsonFunctionTestInputs.assertFails("{}", "JSON_VALUE(s, '$['''']' ERROR ON ERROR)");
    JsonFunctionTestInputs.assertFails("{}", "JSON_VALUE(s, 'lax $['''']' ERROR ON EMPTY)");
  }

  private static TableEnvironment documents() {
    String wide = ",\"a0\":0,\"a1\":1,\"a2\":2,\"a3\":3,\"a4\":4,\"a5\":5,\"a6\":6,\"a7\":7";
    return TextTimeFunctionTestInputs.textRows(
        null,
        "",
        "null",
        "{}",
        "[]",
        "true",
        "42",
        "\"root\"",
        "{\"\":\"value\",\" \":\"space\",\"a\":\"named\"}",
        "{\" \":\"space only\"}",
        "{\"\":null}",
        "{\"\":false}",
        "{\"\":17}",
        "{\"\":1.25}",
        "{\"\":\"\"}",
        "{\"\":[]}",
        "{\"\":{}}",
        "{\"\":{\"\":\"nested\",\"a\":\"named\"},\"a\":{\"\":\"child\"}}",
        "{\"\":[null,{\"\":\"array child\"}]}",
        "[{\"\":\"element\"}]",
        "{\"\":\"old\",\"\":\"new\"}",
        "{\"\":{\"\":\"removed\"},\"\":{}}",
        "{\"\":{},\"\":{\"\":\"added\"}}",
        "{\"\":\"valid\",\"bad\":[}",
        "{\"\":\"valid\",\"bad\":1e2147483648}",
        "{\"\":\"first document\"} trailing",
        "{\"\":\"old\"" + wide + ",\"\":\"new\"}",
        "{\"\":{\"\":\"removed\"}" + wide + ",\"\":{}}",
        "{\"\":\"valid\"" + wide + ",\"bad\":[}");
  }
}
