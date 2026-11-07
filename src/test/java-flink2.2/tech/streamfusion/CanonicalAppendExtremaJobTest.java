package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

class CanonicalAppendExtremaJobTest extends CanonicalGroupArithmeticAppendJobTest {
  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,MIN(v),MAX(v) FROM input_table GROUP BY k");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    if (mode.equals("rescale")) {
      var expected = new java.util.HashMap<String, String>();
      for (int key = 0; key < 64; key++)
        expected.put(key == 0 ? "null" : Integer.toString(key), "5|6");
      return expected;
    }
    return Map.of("null", "-20|20", "1", "-11|7", "2", "-10|10", "3", "5|6", "4", "null|null");
  }
}
