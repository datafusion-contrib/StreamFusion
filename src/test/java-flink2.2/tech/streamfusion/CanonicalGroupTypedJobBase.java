package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

abstract class CanonicalGroupTypedJobBase extends CanonicalGroupArithmeticJobTest {
  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT CAST(CAST(k AS STRING) AS BIGINT),"
            + "COUNT(*)*CAST(1000000 AS BIGINT)+COUNT(DISTINCT CAST(v AS STRING)),"
            + "CAST(SUM(CAST(v AS INT)) AS BIGINT) FROM input_table "
            + "GROUP BY CAST(k AS STRING),TO_TIMESTAMP_LTZ(k,3)");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    var groups = new HashMap<String, ExpectedGroup>();
    var lines =
        assertDoesNotThrow(() -> Files.readAllLines(directory.resolve("switched/input.csv")));
    for (var line : lines) {
      var cells = line.split(",", -1);
      int sign = cells[0].equals("INSERT") || cells[0].equals("UPDATE_AFTER") ? 1 : -1;
      var group = groups.computeIfAbsent(cells[1], ignored -> new ExpectedGroup());
      group.rows += sign;
      assertTrue(group.rows >= 0, "Fixture must retract existing source rows");
      if (!cells[2].equals("null")) {
        int value = Math.toIntExact(Long.parseLong(cells[2]));
        group.nonnull += sign;
        group.sum += sign * value;
        long count = group.members.getOrDefault(cells[2], 0L) + sign;
        assertTrue(count >= 0);
        if (count == 0) group.members.remove(cells[2]);
        else group.members.put(cells[2], count);
      }
      if (group.rows == 0) groups.remove(cells[1]);
    }
    var expected = new HashMap<String, String>();
    groups.forEach(
        (key, group) ->
            expected.put(
                key,
                (group.rows * 1000000 + group.members.size())
                    + "|"
                    + (group.nonnull == 0 ? "null" : Integer.toString(group.sum))));
    return expected;
  }

  private static final class ExpectedGroup {
    long rows;
    long nonnull;
    int sum;
    final Map<String, Long> members = new HashMap<>();
  }
}
