package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

abstract class CanonicalTypedRetractingExtremaJobBase extends CanonicalRetractingExtremaJobTest {
  abstract String valueType();

  static Stream<Arguments> cases() {
    boolean smoke = Boolean.getBoolean("sf.canonical.group.retracting.typed.job.smoke");
    return CanonicalGroupArithmeticJobTest.cases()
        .filter(
            test ->
                !smoke
                    || test.get()[0].equals("continue")
                        && !(boolean) test.get()[1]
                        && !(boolean) test.get()[2]
                        && test.get()[3].equals("hashmap"));
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(root.resolve("time-zone"), "UTC");
    String value;
    if (valueType().equals("BOOLEAN")) value = "MOD(v,CAST(2 AS BIGINT))=0";
    else if (valueType().equals("DATE") || valueType().startsWith("TIME("))
      value =
          "CAST(TO_TIMESTAMP_LTZ(v*CAST("
              + (valueType().equals("DATE") ? "86400000" : "1000")
              + " AS BIGINT),3) AS "
              + valueType()
              + ")";
    else value = "CAST(v AS " + valueType() + ")";
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,"
            + projection("MIN(" + value + ")")
            + ","
            + projection("MAX(" + value + ")")
            + " FROM input_table GROUP BY k");
    return root;
  }

  private String projection(String aggregate) {
    if (valueType().equals("BOOLEAN"))
      return "CASE WHEN "
          + aggregate
          + " IS NULL THEN CAST(NULL AS BIGINT) WHEN "
          + aggregate
          + " THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END";
    if (valueType().equals("DATE") || valueType().startsWith("TIME("))
      return "CAST(EXTRACT("
          + (valueType().equals("DATE") ? "DAY" : "SECOND")
          + " FROM "
          + aggregate
          + ") AS BIGINT)";
    return "CAST(" + aggregate + " AS BIGINT)";
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    var groups = new HashMap<String, List<Long>>();
    var lines =
        assertDoesNotThrow(() -> Files.readAllLines(directory.resolve("switched/input.csv")));
    for (String line : lines) {
      var cells = line.split(",", -1);
      Long value = cells[2].equals("null") ? null : Long.parseLong(cells[2]);
      if (value != null) {
        if (valueType().equals("BOOLEAN")) value = value % 2 == 0 ? 1L : 0L;
        else if (valueType().startsWith("TIME(")) value = Math.floorMod(value, 86400L);
      }
      var rows = groups.computeIfAbsent(cells[1], ignored -> new ArrayList<>());
      if (cells[0].equals("INSERT") || cells[0].equals("UPDATE_AFTER")) rows.add(value);
      else {
        assertTrue(cells[0].equals("DELETE") || cells[0].equals("UPDATE_BEFORE"));
        assertTrue(rows.remove((Object) value), "retract an existing logical value");
      }
      if (rows.isEmpty()) groups.remove(cells[1]);
    }
    var result = new HashMap<String, String>();
    groups.forEach(
        (key, rows) -> {
          Long min = null, max = null;
          for (Long value : rows)
            if (value != null) {
              min = min == null ? value : Math.min(min, value);
              max = max == null ? value : Math.max(max, value);
            }
          result.put(key, display(min) + "|" + display(max));
        });
    return result;
  }

  private String display(Long value) {
    if (value == null) return "null";
    if (valueType().equals("DATE"))
      return Integer.toString(LocalDate.ofEpochDay(value).getDayOfMonth());
    if (valueType().startsWith("TIME(")) return Long.toString(value % 60);
    return Long.toString(value);
  }
}
