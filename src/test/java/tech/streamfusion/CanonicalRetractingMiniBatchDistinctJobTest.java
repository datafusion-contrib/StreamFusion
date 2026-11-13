package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

class CanonicalRetractingMiniBatchDistinctJobTest extends CanonicalRetractingMiniBatchGroupJobTest {
  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,COUNT(DISTINCT v),SUM(v) FROM input_table GROUP BY k");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    if (!mode.equals("rescale")) return Map.of("null", "0|null", "2", "1|11", "3", "2|11");
    var expected = new HashMap<String, String>();
    for (int key = 0; key < 64; key++)
      expected.put(key == 0 ? "null" : Integer.toString(key), "2|33");
    return expected;
  }
}
