package tech.streamfusion;

import java.util.HashMap;
import java.util.Map;

class CanonicalUnboundedOverDistinctRowKeyJobTest extends CanonicalUnboundedOverRowKeyJobTest {
  @Override
  boolean distinct() {
    return true;
  }

  @Override
  Map<String, String> expectedTable(String kind, String mode) {
    if (!mode.equals("rescale")) return super.expectedTable(kind, mode);
    var expected = new HashMap<String, String>();
    for (int key = 0; key < 64; key++)
      expected.put(key == 0 ? "null" : Integer.toString(key), "11|5");
    return expected;
  }
}
