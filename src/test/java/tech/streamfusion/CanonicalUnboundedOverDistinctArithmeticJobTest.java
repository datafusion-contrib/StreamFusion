package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

class CanonicalUnboundedOverDistinctArithmeticJobTest
    extends CanonicalUnboundedOverDistinctJobTest {
  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, kind, mode, ordinary);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,SUM(DISTINCT v) OVER w,AVG(DISTINCT v) OVER w"
            + " FROM input_table WINDOW w AS (PARTITION BY k ORDER BY pt "
            + (kind.equals("rows") ? "ROWS" : "RANGE")
            + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String kind, String mode) {
    if (mode.equals("rescale")) {
      var expected = new HashMap<String, String>();
      for (int key = 0; key < 64; key++)
        expected.put(key == 0 ? "null" : Integer.toString(key), "11|5");
      return expected;
    }
    return Map.of("1", "37|7", "2", "13|4", "null", "8|4");
  }
}
