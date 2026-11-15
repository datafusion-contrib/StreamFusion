package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalUnboundedOverDistinctJobTest extends CanonicalUnboundedOverJobTest {
  boolean requiredArgument() {
    return true;
  }

  String argumentType() {
    return "BIGINT";
  }

  static Stream<Arguments> continuedCases() {
    return CanonicalTestMatrix.select(CanonicalUnboundedOverJobTest.continuedCases());
  }

  static Stream<Arguments> recoveryCases() {
    return CanonicalTestMatrix.select(CanonicalUnboundedOverJobTest.recoveryCases());
  }

  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, kind, mode, ordinary);
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT,v "
            + argumentType()
            + (requiredArgument() ? " NOT NULL" : "")
            + ",rt TIMESTAMP(3)");
    var rows = new ArrayList<String>();
    for (String line : Files.readAllLines(root.resolve("input.csv"))) {
      var cells = line.split(",", -1);
      if (requiredArgument() && cells[2].equals("null")) cells[2] = "0";
      rows.add(String.join(",", cells));
    }
    Files.write(root.resolve("input.csv"), rows);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,COUNT(DISTINCT v) OVER w,COUNT(v) OVER w"
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
        expected.put(key == 0 ? "null" : Integer.toString(key), "2|6");
      return expected;
    }
    return Map.of("1", "5|6", "2", "3|3", "null", "2|2");
  }
}
