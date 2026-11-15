package tech.streamfusion;

import java.util.Map;

class CanonicalBooleanOverDistinctCountJobTest extends CanonicalScalarOverDistinctCountJobTest {
  @Override
  String argumentType() {
    return "BOOLEAN";
  }

  @Override
  Map<String, String> expectedTable(String kind, String mode) {
    return mode.equals("rescale")
        ? super.expectedTable(kind, mode)
        : Map.of("1", "2|4", "2", "2|2", "null", "1|2");
  }
}
