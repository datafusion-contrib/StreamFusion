package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalGroupSumDistinctJobTest extends CanonicalGroupArithmeticAppendJobTest {
  static Stream<Arguments> cases() {
    return CanonicalGroupArithmeticJobTest.cases()
        .filter(
            test ->
                !Boolean.getBoolean("sf.canonical.sumDistinct.job.smoke")
                    || test.get()[0].equals("continue")
                        && !(boolean) test.get()[1]
                        && !(boolean) test.get()[2]
                        && test.get()[3].equals("hashmap"));
  }

  boolean miniBatch() {
    return false;
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,SUM(DISTINCT v),COUNT(DISTINCT v) FROM input_table GROUP"
            + " BY k");
    if (miniBatch()) {
      Files.writeString(root.resolve("one-phase-mini-batch"), "5");
      Files.writeString(root.resolve("mini-batch-latency"), "1 h");
      Files.createFile(root.resolve("same-cut-stock-reference"));
    }
    return root;
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    var groups = new HashMap<String, Set<Long>>();
    for (var row :
        assertDoesNotThrow(() -> Files.readAllLines(directory.resolve("switched/input.csv")))) {
      var values = row.split(",", -1);
      assertEquals("INSERT", values[0]);
      var members = groups.computeIfAbsent(values[1], ignored -> new HashSet<>());
      if (!values[2].equals("null")) members.add(Long.parseLong(values[2]));
    }
    var result = new HashMap<String, String>();
    groups.forEach(
        (key, members) ->
            result.put(
                key,
                members.isEmpty()
                    ? "null|0"
                    : members.stream().mapToLong(Long::longValue).sum() + "|" + members.size()));
    return result;
  }
}
