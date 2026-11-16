package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

class CanonicalIndependentOverDistinctJobTest extends CanonicalScalarOverDistinctCountJobTest {
  @Override
  String argumentType() {
    return "DECIMAL(38,7)";
  }

  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, kind, mode, ordinary);
    var query = root.resolve("query.sql");
    Files.writeString(
        query, Files.readString(query).replace("COUNT(v) OVER w", "COUNT(DISTINCT k) OVER w"));
    Files.writeString(root.resolve("required-over-distinct-views"), "2");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String kind, String mode) {
    if (!mode.equals("rescale")) return Map.of("1", "4|1", "2", "2|1", "null", "2|0");
    var expected = new HashMap<String, String>();
    for (int key = 0; key < 64; key++)
      expected.put(key == 0 ? "null" : Integer.toString(key), key == 0 ? "2|0" : "2|1");
    return expected;
  }
}
