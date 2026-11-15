package tech.streamfusion;

import java.util.Map;

class CanonicalUnboundedOverNullableDistinctJobTest extends CanonicalUnboundedOverDistinctJobTest {
  @Override
  boolean requiredArgument() {
    return false;
  }

  @Override
  Map<String, String> expectedTable(String kind, String mode) {
    return mode.equals("rescale")
        ? super.expectedTable(kind, mode)
        : Map.of("1", "4|4", "2", "2|2", "null", "2|2");
  }
}
