package tech.streamfusion;

import java.util.Map;

class CanonicalUnboundedOverNullableDistinctArithmeticJobTest
    extends CanonicalUnboundedOverDistinctArithmeticJobTest {
  @Override
  boolean requiredArgument() {
    return false;
  }

  @Override
  Map<String, String> expectedTable(String kind, String mode) {
    return mode.equals("rescale")
        ? super.expectedTable(kind, mode)
        : Map.of("1", "37|9", "2", "13|6", "null", "8|4");
  }
}
