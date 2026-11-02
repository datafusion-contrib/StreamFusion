package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalKeylessAntiJoinJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(first -> Stream.of("hashmap", "rocksdb").map(store -> Arguments.of(first, store)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = java.util.concurrent.TimeUnit.MINUTES)
  void nullAwareNotInRetractionsContinueAcrossCanonicalSwitches(boolean nativeFirst, String store)
      throws Exception {
    var runner = new CanonicalJobRunner(directory);
    var fixture = runner.fixtureJar();
    Path baseline = prepare("baseline");
    Path switched = prepare("switched");
    runner.run(fixture, false, baseline, Integer.MAX_VALUE, "none", "baseline", 3, "hashmap");
    runner.run(
        fixture, nativeFirst, switched, 5, "none", "prefix", 3, nativeFirst ? store : "hashmap");
    runner.run(
        fixture, !nativeFirst, switched, 9, "prefix", "middle", 3, nativeFirst ? "hashmap" : store);
    runner.run(
        fixture,
        nativeFirst,
        switched,
        Integer.MAX_VALUE,
        "middle",
        "suffix",
        3,
        nativeFirst ? store : "hashmap");
    var type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var actual = bag(CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type));
    assertEquals(
        Map.of("1|10|null", 1L, "7|70|null", 1L, "9|90|null", 1L, "11|110|null", 1L), actual);
    assertEquals(
        bag(CanonicalFixtureFactory.committedRows(baseline.resolve("committed"), type)), actual);
    assertFalse(actual.containsKey("3|30|null"));
    assertFalse(actual.containsKey("5|50|null"));
    for (String stage : List.of("prefix", "middle")) {
      Path savepoint =
          Path.of(
              java.net.URI.create(Files.readString(switched.resolve(stage + "-savepoint")).trim()));
      assertFalse(
          new String(
                  Files.readAllBytes(savepoint.resolve("_metadata")), StandardCharsets.ISO_8859_1)
              .contains("tech.streamfusion"));
    }
    assertTrue(
        Files.exists(
            switched.resolve((nativeFirst ? "middle" : "suffix") + "-stock-classpath-verified")));
  }

  private Path prepare(String name) throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    Files.createFile(root.resolve("insert-only"));
    Files.createFile(root.resolve("wait-native-input"));
    var left = new ArrayList<String>();
    var right = new ArrayList<String>();
    for (int i = 1; i <= 12; i++) {
      left.add("INSERT," + (i == 12 ? "null" : i) + "," + (10 * i));
      right.add("INSERT," + (i == 6 ? 3 : i == 7 ? 5 : 2 * i) + "," + (100 * i));
    }
    Files.write(root.resolve("input.csv"), left);
    Files.write(root.resolve("right-input.csv"), right);
    Files.writeString(root.resolve("right_input_table-columns.sql"), "k BIGINT NOT NULL,v BIGINT");
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT l.k,l.v,CAST(NULL AS BIGINT) FROM input_table l WHERE l.k"
            + " NOT IN (SELECT r.k FROM right_input_table r)");
    Files.writeString(
        root.resolve("native-operator"),
        "NativeColumnarUpdatingJoin,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "UpdatingJoin");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-join");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    return root;
  }

  private static Map<String, Long> bag(List<String> changes) {
    var result = new HashMap<String, Long>();
    for (String change : changes)
      result.merge(change.substring(3), change.charAt(0) == '-' ? -1L : 1L, Long::sum);
    result.values().removeIf(count -> count == 0);
    assertTrue(result.values().stream().allMatch(count -> count > 0), result.toString());
    return result;
  }
}
