package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParseException;
import org.apache.flink.table.api.TableEnvironment;

final class JsonFunctionTestInputs {
  private JsonFunctionTestInputs() {}

  static TableEnvironment documents() {
    return TextTimeFunctionTestInputs.textRows(
        null,
        "",
        "null",
        "{}",
        "[]",
        "{\"a\":null}",
        "{\"a\":false}",
        "{\"a\":\"\"}",
        "{\"a\":\"hello\"}",
        "{\"a\":{}}",
        "{\"a\":[]}",
        "{\"a\":{\"b\":[0,\"second\"]}}",
        "{\"a\":{\"b\":[null]}}",
        "{\"a\":{\"b\":[0,1]},\"a\":{}}",
        "{\"a\":1,\"a\":2}",
        "{\"\\u0061\":3}",
        "{\"a b\":4}",
        "{\"a\":\"\\ud800x\\udc00\\ud83d\\ude00\"}",
        "{\"a\":\"\\u0000\\b\\f\\n\\r\\t\\/\\\\\\\"\"}",
        "{\"a\":\"\u4e2d\ud83d\ude00\u00e9\"}",
        "{\"a\":7} ignored trailing content",
        "{\"a\":7,\"broken\":[}",
        "{\"a\":\"x\n\"}",
        "{\"a\":\"\\q\"}",
        "[0,{\"a\":\"array\"}]",
        "true",
        "false",
        "123",
        "\"root\"",
        "true trailing",
        "truex",
        "nullx",
        "1,",
        "{\"a\":\"" + "x".repeat(8193) + "\"}");
  }

  static TableEnvironment wideDocuments() {
    StringBuilder members = new StringBuilder();
    for (int i = 0; i < 64; i++) {
      members.append(",\"field").append(i).append("\":\"value").append(i).append("\"");
    }
    String prefix = "{\"a\":\"first\"" + members;
    return TextTimeFunctionTestInputs.textRows(
        null,
        prefix + "}",
        prefix + ",\"a\":\"last\"}",
        prefix + ",\"a\":\"line\\nquote\\\"\u4e2d\ud83d\ude00\"}",
        prefix + ",\"a\":null}",
        prefix + ",\"a\":false}",
        prefix + ",\"a\":{}}",
        prefix + ",\"a\":[null,{\"b\":\"nested\"}]}",
        prefix + ",\"a\":-0}",
        prefix + ",\"a\":1.2300}",
        prefix + ",\"bad\":tru}",
        prefix + ",\"bad\":1e2147483648}",
        prefix + ",\"bad\":\"\\ud800\"}",
        prefix + "} trailing");
  }

  static TableEnvironment memberNames() {
    return TextTimeFunctionTestInputs.textRows(
        null,
        "{}",
        "null",
        "{\"\u7528\u6237\":{\"\u59d3.\u540d\":\"old\",\"\u59d3.\u540d\":\"new\"}}",
        "{\"\u7528\u6237\":{\"\u59d3.\u540d\":null}}",
        "{\"\u7528\u6237\":{\"\u59d3.\u540d\":[]}}",
        "{\"\u7528\u6237\":{\"\u59d3.\u540d\":1},\"\u7528\u6237\":{}}",
        "{\"O'Reilly\":\"publisher\",\"a\\\"b\":true,\"a]b\":42}",
        "{\"\ud83d\ude00\":\"emoji\",\"*\":\"star\",\"$\":\"dollar\"}",
        "{\"\u00e9\u0661\":\"numbered\",\"\\u7528\\u6237\":{\"\u59d3.\u540d\":\"escaped\"}}",
        "{\"\u7528\u6237\":{\"\u59d3.\u540d\":\"ok\"},\"bad\":[}");
  }

  static void assertFailsLikeFlink(String document, String expression) {
    assertFails(document, expression);
  }

  static void assertFails(String document, String expression) {
    var comparison =
        NativeFailureParity.run(
            () -> TextTimeFunctionTestInputs.textRows(document),
            "SELECT " + expression + " FROM inputs");
    Throwable host = comparison.host().rootCause();
    Throwable nativeError = comparison.nativeRun().rootCause();
    assertNotNull(host, comparison.toString());
    assertNotNull(nativeError, comparison.toString());
    assertSameFailure(host, nativeError, comparison.toString());
    assertEquals(
        NativeFailureParity.Route.NATIVE, comparison.nativeRun().route(), comparison.toString());
  }

  static void assertSameFailure(Throwable expected, Throwable actual, String context) {
    assertEquals(expected.getClass(), actual.getClass(), context);
    if (!(expected instanceof JsonParseException)) {
      assertEquals(expected.getMessage(), actual.getMessage(), context);
      return;
    }
    var host = (JsonParseException) expected;
    var nativeError = (JsonParseException) actual;
    // Jackson's source reference is transient; Flink can return a local or serialized failure.
    assertEquals(host.getOriginalMessage(), nativeError.getOriginalMessage(), context);
    assertEquals(
        host.getRequestPayloadAsString(), nativeError.getRequestPayloadAsString(), context);
    var location = host.getLocation();
    var nativeLocation = nativeError.getLocation();
    if (location == null || nativeLocation == null) {
      assertEquals(location, nativeLocation, context);
      return;
    }
    assertEquals(location.getLineNr(), nativeLocation.getLineNr(), context);
    assertEquals(location.getColumnNr(), nativeLocation.getColumnNr(), context);
    assertEquals(location.getByteOffset(), nativeLocation.getByteOffset(), context);
    assertEquals(location.getCharOffset(), nativeLocation.getCharOffset(), context);
  }
}
