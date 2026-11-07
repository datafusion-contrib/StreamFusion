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

class CanonicalSharedFirstNJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  boolean compiled() {
    return true;
  }

  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            same -> sharedCases().map(test -> Arguments.of(test.get()[0], test.get()[1], same)));
  }

  static Stream<Arguments> sharedCases() {
    return Stream.of(false, true)
        .flatMap(first -> Stream.of("hashmap", "rocksdb").map(store -> Arguments.of(first, store)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = java.util.concurrent.TimeUnit.MINUTES)
  void counterAndJoinStateContinueTogether(boolean nativeFirst, String store, boolean sameLimit)
      throws Exception {
    check(compiled(), nativeFirst, store, sameLimit, "continue");
  }

  @ParameterizedTest
  @MethodSource("sharedCases")
  @Timeout(value = 8, unit = java.util.concurrent.TimeUnit.MINUTES)
  void sharedStateRescalesWithoutDuplicatingOwners(boolean nativeFirst, String store)
      throws Exception {
    check(compiled(), nativeFirst, store, true, "rescale");
  }

  @ParameterizedTest
  @MethodSource("sharedCases")
  @Timeout(value = 8, unit = java.util.concurrent.TimeUnit.MINUTES)
  void sharedStateReexportsWithoutInput(boolean nativeFirst, String store) throws Exception {
    check(compiled(), nativeFirst, store, true, "no-input");
  }

  private void check(
      boolean compiled, boolean nativeFirst, String store, boolean sameLimit, String mode)
      throws Exception {
    var runner = new CanonicalJobRunner(directory);
    var fixture = runner.fixtureJar();
    Path baseline = prepare("baseline", compiled, sameLimit, mode);
    Path switched = prepare("switched", compiled, sameLimit, mode);
    int prefix = mode.equals("rescale") ? 64 : 5;
    int middle = mode.equals("rescale") ? 128 : 10;
    if (mode.equals("rescale")) {
      Files.createFile(switched.resolve("rescale"));
      Files.writeString(switched.resolve("native-rescale-family"), "FirstN,UpdatingJoin");
      Files.writeString(switched.resolve("prefix-parallelism"), "1");
      Files.writeString(switched.resolve("middle-parallelism"), "3");
      Files.writeString(switched.resolve("suffix-parallelism"), "2");
    }
    runner.run(fixture, false, baseline, Integer.MAX_VALUE, "none", "baseline", 3, "hashmap");
    runner.run(
        fixture,
        nativeFirst,
        switched,
        prefix,
        "none",
        "prefix",
        3,
        nativeFirst ? store : "hashmap");
    String restore = "prefix";
    if (mode.equals("no-input")) {
      runner.run(
          fixture,
          !nativeFirst,
          switched,
          prefix,
          restore,
          "no-input",
          3,
          nativeFirst ? "hashmap" : store);
      runner.run(
          fixture,
          nativeFirst,
          switched,
          prefix,
          "no-input",
          "return",
          3,
          nativeFirst ? store : "hashmap");
      restore = "return";
    }
    runner.run(
        fixture,
        !nativeFirst,
        switched,
        middle,
        restore,
        "middle",
        3,
        nativeFirst ? "hashmap" : store);
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
    assertEquals(expected(switched, sameLimit), actual);
    assertEquals(
        bag(CanonicalFixtureFactory.committedRows(baseline.resolve("committed"), type)), actual);
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

  private Path prepare(String name, boolean compiled, boolean sameLimit, String mode)
      throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    if (!compiled) Files.createFile(root.resolve("ordinary-sql"));
    Files.createFile(root.resolve("insert-only"));
    Files.createFile(root.resolve("wait-native-input"));
    Files.writeString(root.resolve("native-ready-families"), "FirstN");
    var input =
        new ArrayList<>(
            List.of(
                "INSERT,2,10",
                "INSERT,3,20",
                "INSERT,2,10",
                "INSERT,3,null",
                "INSERT,2,30",
                "INSERT,null,15",
                "INSERT,2,25",
                "INSERT,3,25",
                "INSERT,3,20",
                "INSERT,2,35",
                "INSERT,3,40",
                "INSERT,7,100",
                "INSERT,null,18",
                "INSERT,2,50",
                "INSERT,null,20",
                "INSERT,3,60"));
    if (mode.equals("rescale")) {
      Files.createFile(root.resolve("disable-chaining"));
      input.clear();
      for (int round = 0; round < 6; round++)
        for (int key = 0; key < 64; key++)
          input.add("INSERT," + (key == 0 ? "null" : key) + "," + (round * 1000 + key));
    }
    Files.write(root.resolve("input.csv"), input);
    Files.writeString(
        root.resolve("views.sql"),
        "CREATE TEMPORARY VIEW first_input AS SELECT k,v FROM (SELECT k,v,ROW_NUMBER() OVER"
            + " (PARTITION BY k ORDER BY pt) AS rn FROM input_table) WHERE rn<=3");
    String right =
        sameLimit
            ? "first_input"
            : "(SELECT k,v FROM (SELECT k,v,ROW_NUMBER() OVER (PARTITION BY k ORDER BY pt) AS rn"
                + " FROM input_table) WHERE rn<=2)";
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT l.k,l.v,r.v FROM first_input l JOIN "
            + right
            + " r ON l.k=r.k");
    Files.writeString(
        root.resolve("native-operator"),
        "NativeFirstN,NativeColumnarUpdatingJoin,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "FirstN,UpdatingJoin");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-rank");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    return root;
  }

  private static Map<String, Long> expected(Path root, boolean sameLimit) throws Exception {
    var rows = new HashMap<String, List<String>>();
    for (String line : Files.readAllLines(root.resolve("input.csv"))) {
      String[] fields = line.split(",");
      assertEquals("INSERT", fields[0]);
      if (!fields[1].equals("null")) {
        var values = rows.computeIfAbsent(fields[1], key -> new ArrayList<>());
        if (values.size() < 3) values.add(fields[2]);
      }
    }
    var result = new HashMap<String, Long>();
    rows.forEach(
        (key, values) -> {
          for (String left : values)
            for (String right : values.subList(0, Math.min(values.size(), sameLimit ? 3 : 2)))
              result.merge(key + "|" + left + "|" + right, 1L, Long::sum);
        });
    return result;
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
