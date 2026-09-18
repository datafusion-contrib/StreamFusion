package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.table.api.TableEnvironment;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

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

  static void assertFails(String document, String expression, String nativeMessage) {
    assertFails(document, expression, nativeMessage, true);
  }

  static void assertFailsLikeFlink(String document, String expression) {
    Throwable expected = null;
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      TableEnvironment tables = TextTimeFunctionTestInputs.textRows(document);
      PhysicalPlanScan scan = nativeEnabled ? NativePlanner.install(tables) : null;
      Exception error =
          assertThrows(
              Exception.class,
              () -> {
                try (var rows =
                    tables.executeSql("SELECT " + expression + " FROM inputs").collect()) {
                  while (rows.hasNext()) rows.next();
                }
              });
      Throwable hostError = error;
      while (hostError != null
          && !(hostError instanceof org.apache.flink.table.api.TableRuntimeException)) {
        hostError = hostError.getCause();
      }
      org.junit.jupiter.api.Assertions.assertNotNull(hostError, error.toString());
      if (nativeEnabled) {
        assertEquals(expected.getClass(), hostError.getClass());
        assertEquals(expected.getMessage(), hostError.getMessage());
        assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
      } else expected = hostError;
    }
  }

  static void assertFallbackFails(String document, String expression, String message) {
    assertFails(document, expression, message, false);
  }

  private static void assertFails(
      String document, String expression, String nativeMessage, boolean admitted) {
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      TableEnvironment tables = TextTimeFunctionTestInputs.textRows(document);
      PhysicalPlanScan scan = nativeEnabled ? NativePlanner.install(tables) : null;
      Exception error =
          assertThrows(
              Exception.class,
              () -> {
                try (var rows =
                    tables.executeSql("SELECT " + expression + " FROM inputs").collect()) {
                  while (rows.hasNext()) {
                    rows.next();
                  }
                }
              });
      if (nativeEnabled) {
        StringBuilder causes = new StringBuilder();
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
          causes.append(cause).append('\n');
        }
        assertTrue(causes.toString().contains(nativeMessage), causes.toString());
        assertEquals(
            admitted, scan.substitutions() > 0, scan.fallbackReasons().toString());
      }
    }
  }
}
