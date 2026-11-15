package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalAppendStringExtremaJobTest extends CanonicalGroupArithmeticAppendJobTest {
  static Stream<Arguments> cases() {
    boolean smoke = Boolean.getBoolean("sf.canonical.group.string.job.smoke");
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
    var rows = Files.readAllLines(root.resolve("input.csv"));
    if (!mode.equals("rescale")) {
      rows.add("INSERT,1,12");
      rows.add("INSERT,2,2");
      Files.write(root.resolve("input.csv"), rows);
    }
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,CAST(MIN(CAST(v AS STRING)) AS BIGINT),"
            + "CAST(MAX(CAST(v AS STRING)) AS BIGINT) FROM input_table GROUP BY k");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    var groups = new HashMap<String, String[]>();
    var rows =
        assertDoesNotThrow(() -> Files.readAllLines(directory.resolve("switched/input.csv")));
    for (String row : rows) {
      var fields = row.split(",", -1);
      assertEquals("INSERT", fields[0]);
      var extrema = groups.computeIfAbsent(fields[1], ignored -> new String[2]);
      if (fields[2].equals("null")) continue;
      String value = fields[2];
      if (extrema[0] == null || value.compareTo(extrema[0]) < 0) extrema[0] = value;
      if (extrema[1] == null || value.compareTo(extrema[1]) > 0) extrema[1] = value;
    }
    var result = new HashMap<String, String>();
    groups.forEach((key, extrema) -> result.put(key, extrema[0] + "|" + extrema[1]));
    return result;
  }
}
