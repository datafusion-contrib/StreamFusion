package tech.streamfusion;

import java.util.HashMap;
import java.util.Map;

class CanonicalUnboundedOverRowKeyStringJobTest extends CanonicalUnboundedOverRowKeyJobTest {
  @Override
  boolean stringExtrema() {
    return true;
  }

  @Override
  Map<String, String> expectedTable(String kind, String mode) {
    if (mode.equals("rescale")) {
      var expected = new HashMap<String, String>();
      for (int key = 0; key < 64; key++)
        expected.put(key == 0 ? "null" : Integer.toString(key), "5|6");
      return expected;
    }
    return Map.of("1", "17|9", "2", "11|2", "null", "3|5");
  }
}
