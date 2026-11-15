package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonLocation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParseException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.RequestPayload;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

class JsonFunctionTestInputsTest {
  @Test
  void parseFailureKeepsItsDiagnosticAndPositionAcrossFlinkSerialization() throws Exception {
    var local = assertThrows(JsonParseException.class, () -> new ObjectMapper().readTree("{"));
    var transported = InstantiationUtil.clone(local);
    assertNotEquals(local.getMessage(), transported.getMessage());
    JsonFunctionTestInputs.assertSameFailure(local, transported, "serialized parse failure");
    JsonFunctionTestInputs.assertSameFailure(transported, local, "local parse failure");
  }

  @Test
  void comparisonStillRejectsDifferentDiagnosticsPositionsAndExceptionTypes() {
    var location = new JsonLocation(null, 3, 5, 2, 4);
    var expected = new JsonParseException(null, "invalid document", location);
    for (Throwable different :
        new Throwable[] {
          new JsonParseException(null, "different error", location),
          new JsonParseException(null, "invalid document", location)
              .withRequestPayload(new RequestPayload("different payload")),
          new JsonParseException(null, "invalid document", new JsonLocation(null, 3, 5, 3, 4)),
          new JsonParseException(null, "invalid document", new JsonLocation(null, 3, 5, 2, 6)),
          new JsonParseException(null, "invalid document", new JsonLocation(null, 4, 5, 2, 4)),
          new JsonParseException(null, "invalid document", new JsonLocation(null, 3, 6, 2, 4)),
          new JsonParseException(null, "invalid document", (JsonLocation) null),
          new IllegalArgumentException(expected.getMessage())
        }) {
      assertThrows(
          AssertionError.class,
          () -> JsonFunctionTestInputs.assertSameFailure(expected, different, "mismatch"));
    }
    assertThrows(
        AssertionError.class,
        () ->
            JsonFunctionTestInputs.assertSameFailure(
                new IllegalArgumentException("first"),
                new IllegalArgumentException("second"),
                "non-JSON mismatch"));
  }
}
