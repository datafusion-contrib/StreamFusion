package tech.streamfusion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkIsJsonSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"VALUE", "OBJECT", "ARRAY", "SCALAR"})
  void matchesFlinkForValidInvalidNullAndTrailingDocuments(String type) throws Exception {
    NativeParity.assertParity(
        JsonFunctionTestInputs::documents,
        "SELECT id, s IS JSON " + type + ", s IS NOT JSON " + type + " FROM inputs");
    NativeParity.assertParity(
        JsonFunctionTestInputs::wideDocuments,
        "SELECT id, s IS JSON " + type + ", s IS NOT JSON " + type + " FROM inputs");
  }

  @Test
  void defaultFormAndPredicateComposeWithGrouping() throws Exception {
    NativeParity.assertParity(
        JsonFunctionTestInputs::documents,
        "SELECT s IS JSON, COUNT(*) FROM inputs WHERE s IS NOT JSON ARRAY GROUP BY s IS JSON");
  }

  @Test
  void tokenAndResourceBoundariesMatchJackson() throws Exception {
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                "null",
                " null ",
                "nullx",
                "null\u0301",
                "true\u007f",
                "true\ud83d\ude00",
                "1,",
                "1 trailing",
                "-0",
                "-0.0",
                "1e2147483648",
                "1".repeat(1001),
                "[".repeat(1000) + "0" + "]".repeat(1000),
                "[".repeat(1001) + "0" + "]".repeat(1001),
                "{\"" + "k".repeat(50001) + "\":0}",
                "\"\\ud800\""),
        "SELECT id, s IS JSON VALUE, s IS JSON SCALAR FROM inputs");
  }
}
