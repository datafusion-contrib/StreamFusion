package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

abstract class CanonicalGroupTypedExtremaJobBase extends CanonicalGroupArithmeticAppendJobTest {
  abstract String valueType();

  static Stream<Arguments> cases() {
    boolean smoke = Boolean.getBoolean("sf.canonical.group.temporal.smoke");
    return CanonicalGroupArithmeticJobTest.cases()
        .filter(test -> test.get()[0].equals("continue"))
        .filter(
            test ->
                !smoke
                    || !(boolean) test.get()[1]
                        && !(boolean) test.get()[2]
                        && test.get()[3].equals("hashmap"));
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    Path root = super.prepare(name, mode, ordinary);
    Files.writeString(root.resolve("time-zone"), "UTC");
    String value;
    if (valueType().equals("BOOLEAN")) value = "MOD(v,CAST(2 AS BIGINT))=0";
    else
      value =
          "CAST(TO_TIMESTAMP_LTZ(v*CAST("
              + (valueType().equals("DATE") ? "86400000" : "1000")
              + " AS BIGINT),3) AS "
              + valueType()
              + ")";
    String minimum = projection("MIN(" + value + ")", true);
    String maximum = projection("MAX(" + value + ")", false);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT CAST(CAST(k AS STRING) AS BIGINT),"
            + "COUNT(*)*CAST(1000000 AS BIGINT)+"
            + minimum
            + ","
            + maximum
            + " FROM input_table GROUP BY CAST(k AS STRING),TO_TIMESTAMP_LTZ(k,3)");
    return root;
  }

  private String projection(String aggregate, boolean minimum) {
    if (valueType().equals("BOOLEAN"))
      return "CASE WHEN "
          + aggregate
          + " IS NULL THEN CAST("
          + (minimum ? "0" : "NULL")
          + " AS BIGINT) WHEN "
          + aggregate
          + " THEN CAST("
          + (minimum ? "2" : "1")
          + " AS BIGINT) ELSE CAST("
          + (minimum ? "1" : "0")
          + " AS BIGINT) END";
    String projected =
        "CAST(EXTRACT("
            + (valueType().equals("DATE") ? "DAY" : "SECOND")
            + " FROM "
            + aggregate
            + ") AS BIGINT)";
    return minimum ? "COALESCE(" + projected + ",CAST(0 AS BIGINT))" : projected;
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    var groups = new HashMap<String, ExpectedGroup>();
    var lines =
        assertDoesNotThrow(() -> Files.readAllLines(directory.resolve("switched/input.csv")));
    for (String line : lines) {
      String[] cells = line.split(",", -1);
      assertEquals("INSERT", cells[0], "Scalar extrema require append-only input");
      var group = groups.computeIfAbsent(cells[1], ignored -> new ExpectedGroup());
      group.rows++;
      if (!cells[2].equals("null")) {
        long value = Long.parseLong(cells[2]);
        if (valueType().equals("BOOLEAN")) value = value % 2 == 0 ? 1 : 0;
        else if (!valueType().equals("DATE")) value = Math.floorMod(value, 86400);
        group.minimum = group.minimum == null ? value : Math.min(group.minimum, value);
        group.maximum = group.maximum == null ? value : Math.max(group.maximum, value);
      }
    }
    var result = new HashMap<String, String>();
    groups.forEach(
        (key, group) ->
            result.put(
                key,
                (group.rows * 1000000 + (group.minimum == null ? 0 : display(group.minimum, true)))
                    + "|"
                    + (group.maximum == null ? "null" : display(group.maximum, false))));
    return result;
  }

  private long display(long value, boolean minimum) {
    if (valueType().equals("BOOLEAN")) return minimum ? value + 1 : value;
    if (valueType().equals("DATE")) return LocalDate.ofEpochDay(value).getDayOfMonth();
    return value % 60;
  }

  private static final class ExpectedGroup {
    long rows;
    Long minimum, maximum;
  }
}
