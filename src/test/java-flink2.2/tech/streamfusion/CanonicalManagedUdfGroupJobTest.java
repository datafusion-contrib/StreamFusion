package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalManagedUdfGroupJobTest extends CanonicalGroupArithmeticJobTest {
  static Stream<Arguments> cases() {
    return CanonicalGroupArithmeticJobTest.cases()
        .filter(
            test ->
                !Boolean.getBoolean("sf.canonical.jvmGroup.job.smoke")
                    || test.get()[0].equals("continue")
                        && !(boolean) test.get()[1]
                        && !(boolean) test.get()[2]
                        && test.get()[3].equals("hashmap"));
  }

  int cacheEntries() {
    return 0;
  }

  @Override
  String statefulFamily() {
    return "NativeColumnarJvmGroup";
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.createFile(root.resolve("history-aggregate"));
    Files.writeString(root.resolve("jvm-cache-entries"), Integer.toString(cacheEntries()));
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,fixture_history(v),COUNT(*) FROM input_table GROUP BY"
            + " k");
    Files.writeString(
        root.resolve("native-operator"), "NativeColumnarJvmGroup,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "NativeColumnarJvmGroup");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    var groups = new HashMap<String, List<Long>>();
    var input =
        assertDoesNotThrow(() -> Files.readAllLines(directory.resolve("switched/input.csv")));
    for (var row : input) {
      var cells = row.split(",", -1);
      var values = groups.computeIfAbsent(cells[1], ignored -> new ArrayList<>());
      Long value = cells[2].equals("null") ? null : Long.valueOf(cells[2]);
      if (cells[0].equals("INSERT") || cells[0].equals("UPDATE_AFTER")) values.add(value);
      else assertTrue(values.remove(value), "Oracle retracts an existing input");
    }
    var expected = new HashMap<String, String>();
    groups.forEach(
        (key, values) -> {
          if (values.isEmpty()) return;
          long sum = 0;
          var distinct = new HashSet<Long>();
          for (Long value : values)
            if (value != null) {
              sum += value;
              distinct.add(value);
            }
          expected.put(key, (sum + values.size() + 1000L * distinct.size()) + "|" + values.size());
        });
    return expected;
  }
}
