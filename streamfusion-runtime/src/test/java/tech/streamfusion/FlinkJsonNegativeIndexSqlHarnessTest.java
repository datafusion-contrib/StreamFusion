package tech.streamfusion;

import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkJsonNegativeIndexSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "$[-1]",
        "$[-2]",
        "$[-3]",
        "$[-4]",
        "$[-0]",
        "$[-0001]",
        "$[-2147483648]",
        "$[ -0002 ]",
        "$.a[-1]",
        "$[-1].q",
        "$[-1][-1]",
        "$[''][-1]['']",
        "$.a[-1].q[-1]",
        "$[ -1 ][ -1 ]"
      })
  void negativeSelectorsRetainEveryMissingNullAndErrorPolicy(String path) throws Exception {
    String literal = path.replace("'", "''");
    NativeParity.assertParity(
        () -> documents(71),
        "SELECT id, JSON_VALUE(s, '"
            + literal
            + "'), "
            + "JSON_VALUE(s, 'lax "
            + literal
            + "' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), "
            + "JSON_VALUE(s, 'strict "
            + literal
            + "' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), "
            + "JSON_EXISTS(s, '"
            + literal
            + "' UNKNOWN ON ERROR), "
            + "JSON_EXISTS(s, 'lax "
            + literal
            + "'), "
            + "JSON_EXISTS(s, '"
            + literal
            + "' TRUE ON ERROR) FROM inputs");
  }

  @Test
  void independentPathsReuseReadersAcrossBatches() throws Exception {
    NativeParity.assertParity(
        () -> documents(5003),
        "SELECT id, JSON_VALUE(s, '$[-1]'), JSON_VALUE(s, '$[-2]'), "
            + "JSON_VALUE(s, '$[-0]'), JSON_VALUE(s, '$.a[-1].q[-1]'), "
            + "JSON_VALUE(s, '$.a[-2]'), JSON_EXISTS(s, '$[-2147483648]') FROM inputs");
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {"INTEGER|-2147483648|7|9", "BOOLEAN|true|FALSE|TRUE", "DOUBLE|1.25|unused|unused"})
  void typedReturningSelectsBeforeConverting(String type, String value, String empty, String error)
      throws Exception {
    String defaults =
        type.equals("DOUBLE")
            ? ""
            : " DEFAULT " + empty + " ON EMPTY DEFAULT " + error + " ON ERROR";
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                null,
                "[]",
                "null",
                "[null]",
                "[{}," + value + "]",
                "[" + value + ",null]",
                "[null," + value + "]",
                "[[],{}]",
                "invalid"),
        "SELECT id, JSON_VALUE(s, '$[-1]' RETURNING "
            + type
            + defaults
            + "), "
            + "JSON_VALUE(s, 'lax $[-2]' RETURNING "
            + type
            + defaults
            + ") FROM inputs");
  }

  @Test
  void selectedConversionFailureStillEscapesOnError() {
    NativeFailureParity.run(
            () -> TextTimeFunctionTestInputs.textRows("[0,\"bad\"]"),
            "SELECT JSON_VALUE(s, '$[-1]' RETURNING INTEGER NULL ON ERROR) FROM inputs")
        .assertFailure(
            ClassCastException.class,
            "java.lang.String cannot be cast to class java.lang.Integer",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void missingNegativeIndexesKeepErrorBehavior() {
    JsonFunctionTestInputs.assertFailsLikeFlink("[]", "JSON_EXISTS(s, '$[-1]' ERROR ON ERROR)");
    JsonFunctionTestInputs.assertFails("[1]", "JSON_VALUE(s, '$[-2]' ERROR ON ERROR)");
    JsonFunctionTestInputs.assertFails("[1]", "JSON_VALUE(s, 'lax $[-2]' ERROR ON EMPTY)");
  }

  private static TableEnvironment documents(int count) {
    String fields =
        ",\"k0\":\"v0\",\"k1\":\"v1\",\"k2\":\"v2\",\"k3\":\"v3\","
            + "\"k4\":\"v4\",\"k5\":\"v5\",\"k6\":\"v6\",\"k7\":\"v7\"";
    String[] values = {
      null,
      "",
      "null",
      "[]",
      "{}",
      "true",
      "42",
      "\"text\"",
      "[null]",
      "[\"first\",\"second\",\"last\"]",
      "[1,2,3]",
      "[1.00,2e3,-0.0]",
      "[null,[],{}]",
      "[0,{\"q\":\"last\"}]",
      "[0,[\"x\",\"y\"]]",
      "{\"\":[0,{\"\":\"last\"}]}",
      "{\"a\":[\"first\",{\"q\":[\"x\",\"y\"]}]}",
      "{\"a\":[\"first\",{\"q\":[\"x\",\"y\"]}]" + fields + "}",
      "{\"a\":[\"old\"]" + fields + ",\"a\":[]}",
      "[\"?\",\"\\uD800\"]",
      "[\"\\uDC00\",\"?\"]",
      "[\"\\uD83D\\uDE00\"]",
      "[\"ok\",{\"bad\":1e2147483648}]",
      "[{\"bad\":1e2147483648},\"ok\"]",
      "[\"ok\",]",
      "[\"ok\"] trailing",
      "[[\"ok\"],{\"bad\":[}]"
    };
    String[] rows = new String[count];
    for (int i = 0; i < count; i++) rows[i] = values[i % values.length];
    return TextTimeFunctionTestInputs.textRows(rows);
  }
}
