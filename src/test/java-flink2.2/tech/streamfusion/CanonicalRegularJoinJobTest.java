package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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

class CanonicalRegularJoinJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String kind : List.of("INNER", "LEFT", "RIGHT", "FULL", "SEMI", "ANTI"))
      for (String shape : List.of("none", "join", "other"))
        for (boolean nativeFirst : new boolean[] {false, true})
          for (String backend : List.of("hashmap", "rocksdb"))
            cases.add(Arguments.of(kind, shape, nativeFirst, backend));
    return Boolean.getBoolean("streamfusion.test.regularJoinJobSmoke")
        ? cases.stream()
            .filter(
                test ->
                    test.get()[0].equals("INNER")
                        && test.get()[1].equals("none")
                        && (boolean) test.get()[2])
        : cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void regularStateContinuesAcrossCleanEngineProcesses(
      String kind, String shape, boolean nativeFirst, String backend) throws Exception {
    check(kind, shape, nativeFirst, backend, "continuation");
  }

  static Stream<Arguments> handoffs() {
    return Stream.of(false, true)
        .flatMap(
            first -> Stream.of("hashmap", "rocksdb").map(backend -> Arguments.of(first, backend)));
  }

  @ParameterizedTest
  @MethodSource("handoffs")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void retainedRegularRowsReexportWithoutNewInput(boolean nativeFirst, String backend)
      throws Exception {
    check("FULL", "none", nativeFirst, backend, "no-input");
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"hashmap", "rocksdb"})
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void nativeRegularCheckpointResumesBeforeCanonicalSwitch(String backend) throws Exception {
    check("INNER", "none", true, backend, "checkpoint");
  }

  @ParameterizedTest
  @MethodSource("handoffs")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void regularKeysRescaleThroughOneThreeAndTwoTasks(boolean nativeFirst, String backend)
      throws Exception {
    check("FULL", "none", nativeFirst, backend, "rescale");
  }

  @ParameterizedTest
  @MethodSource("handoffs")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void sharedCalculatedInputResumesWithOneLogicalOwner(boolean nativeFirst, String backend)
      throws Exception {
    check("INNER", "shared", nativeFirst, backend, "continuation");
  }

  private void check(String kind, String shape, boolean nativeFirst, String backend, String mode)
      throws Exception {
    var runner = new CanonicalJobRunner(directory);
    Path fixture = runner.fixtureJar();
    Path baseline = prepare("baseline", kind, shape);
    Path switched = prepare("switched", kind, shape);
    boolean rescale = mode.equals("rescale");
    if (rescale) {
      for (Path root : List.of(baseline, switched)) {
        Files.createFile(root.resolve("disable-chaining"));
        var left = new ArrayList<String>();
        var right = new ArrayList<String>();
        for (int round = 0; round < 6; round++)
          for (int key = 0; key < 64; key++) {
            left.add("INSERT," + (key + 1) + "," + (1000 * round + key + 1));
            right.add("INSERT," + (key + 1) + "," + (1000 * round + key + 2));
          }
        Files.write(root.resolve("input.csv"), left);
        Files.write(root.resolve("right-input.csv"), right);
      }
      Files.createFile(switched.resolve("rescale"));
      Files.writeString(switched.resolve("native-rescale-family"), "UpdatingJoin");
      Files.writeString(switched.resolve("prefix-parallelism"), "1");
      Files.writeString(switched.resolve("middle-parallelism"), "3");
      Files.writeString(switched.resolve("suffix-parallelism"), "2");
    }
    runner.run(fixture, false, baseline, Integer.MAX_VALUE, "none", "baseline", 3, "hashmap");
    if (mode.equals("checkpoint")) {
      Files.createFile(switched.resolve("checkpoint-native-checkpoint"));
      runner.run(fixture, true, switched, 4, "none", "checkpoint", 3, backend);
      runner.run(fixture, true, switched, 4, "checkpoint", "prefix", 3, backend);
    } else
      runner.run(
          fixture,
          nativeFirst,
          switched,
          rescale ? 128 : 4,
          "none",
          "prefix",
          3,
          nativeFirst ? backend : "hashmap");
    String restore = "prefix";
    if (mode.equals("no-input")) {
      runner.run(
          fixture,
          !nativeFirst,
          switched,
          4,
          restore,
          "no-input",
          3,
          nativeFirst ? "hashmap" : backend);
      runner.run(
          fixture,
          nativeFirst,
          switched,
          4,
          "no-input",
          "return",
          3,
          nativeFirst ? backend : "hashmap");
      restore = "return";
    }
    runner.run(
        fixture,
        !nativeFirst,
        switched,
        rescale ? 256 : 8,
        restore,
        "middle",
        3,
        nativeFirst ? "hashmap" : backend);
    runner.run(
        fixture,
        nativeFirst,
        switched,
        Integer.MAX_VALUE,
        "middle",
        "suffix",
        3,
        nativeFirst ? backend : "hashmap");
    var type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var actual =
        materialized(CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type));
    assertEquals(expected(switched, kind), actual);
    assertEquals(
        materialized(CanonicalFixtureFactory.committedRows(baseline.resolve("committed"), type)),
        actual);
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

  Path prepare(String name, String kind, String shape) throws Exception {
    Path root = directory.resolve(name);
    Files.createDirectories(root);
    var left =
        new ArrayList<>(
            List.of(
                "INSERT,1,10",
                "INSERT,2,20",
                "INSERT,3,30",
                "INSERT,4,40",
                "UPDATE_BEFORE,1,10",
                "UPDATE_AFTER,1,15",
                "DELETE,2,20",
                "INSERT,5,50",
                "INSERT,6,60",
                "DELETE,3,30",
                "INSERT,7,70",
                "INSERT,8,80"));
    var right =
        new ArrayList<>(
            List.of(
                "INSERT,1,100",
                "INSERT,2,200",
                "INSERT,3,300",
                "INSERT,9,900",
                "UPDATE_BEFORE,1,100",
                "UPDATE_AFTER,1,110",
                "DELETE,3,300",
                "INSERT,5,500",
                "INSERT,6,600",
                "DELETE,2,200",
                "INSERT,7,700",
                "INSERT,11,1100"));
    if (!shape.equals("join")) {
      left.set(3, "INSERT,null,40");
      right.set(3, "INSERT,null,900");
    }
    if (shape.equals("none")) {
      left.set(2, "INSERT,1,10");
      left.set(9, "DELETE,1,10");
      right.set(2, "INSERT,1,100");
      right.set(6, "DELETE,1,100");
    }
    Files.write(root.resolve("input.csv"), left);
    Files.write(root.resolve("right-input.csv"), right);
    String columns =
        "k BIGINT"
            + (shape.equals("join") ? " NOT NULL" : "")
            + ",v BIGINT"
            + (shape.equals("other") ? " NOT NULL" : "")
            + (shape.equals("none") || shape.equals("shared")
                ? ""
                : ",PRIMARY KEY(" + (shape.equals("join") ? "k" : "v") + ") NOT ENFORCED");
    Files.writeString(root.resolve("input-columns.sql"), columns);
    Files.writeString(root.resolve("right_input_table-columns.sql"), columns);
    String condition = "l.k=r.k AND l.v<r.v AND MOD(l.v,3)<>0";
    boolean semi = kind.equals("SEMI") || kind.equals("ANTI");
    String query =
        semi
            ? "INSERT INTO output_table SELECT l.k,l.v,CAST(NULL AS BIGINT) FROM input_table l"
                + " WHERE "
                + (kind.equals("ANTI") ? "NOT " : "")
                + "EXISTS (SELECT * FROM right_input_table r WHERE "
                + condition
                + ")"
            : "INSERT INTO output_table SELECT l.k,l.v,r.v FROM input_table l "
                + kind
                + " JOIN right_input_table r ON "
                + condition;
    Files.writeString(root.resolve("query.sql"), query);
    if (shape.equals("shared")) {
      Files.delete(root.resolve("right-input.csv"));
      Files.writeString(
          root.resolve("views.sql"),
          "CREATE TEMPORARY VIEW shared_input AS SELECT k*2 AS k,v+7 AS v FROM input_table");
      Files.writeString(
          root.resolve("query.sql"),
          "INSERT INTO output_table SELECT l.k,l.v,r.v FROM shared_input l"
              + " JOIN shared_input r ON l.k=r.k");
    }
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-join");
    Files.writeString(
        root.resolve("native-operator"),
        "NativeColumnarUpdatingJoin,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "UpdatingJoin");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    Files.createFile(root.resolve("wait-native-input"));
    return root;
  }

  private static Map<String, Long> materialized(List<String> rows) {
    var result = new HashMap<String, Long>();
    for (String row : rows)
      result.merge(row.substring(3), row.charAt(0) == '-' ? -1L : 1L, Long::sum);
    result.values().removeIf(count -> count == 0);
    assertTrue(result.values().stream().allMatch(count -> count > 0), result.toString());
    return result;
  }

  private static Map<String, Long> expected(Path root, String kind) throws Exception {
    var left = input(root.resolve("input.csv"));
    boolean shared = !Files.exists(root.resolve("right-input.csv"));
    var right = shared ? left : input(root.resolve("right-input.csv"));
    if (shared) {
      left = calculate(left);
      right = left;
    }
    var result = new HashMap<String, Long>();
    var matchedRight = new java.util.HashSet<String>();
    for (var l : left.entrySet()) {
      String[] lv = l.getKey().split("\\|");
      boolean found = false;
      for (var r : right.entrySet()) {
        String[] rv = r.getKey().split("\\|");
        if (!lv[0].equals("null")
            && lv[0].equals(rv[0])
            && (shared
                || Long.parseLong(lv[1]) < Long.parseLong(rv[1])
                    && Long.parseLong(lv[1]) % 3 != 0)) {
          found = true;
          matchedRight.add(r.getKey());
          if (!kind.equals("SEMI") && !kind.equals("ANTI"))
            result.merge(l.getKey() + "|" + rv[1], l.getValue() * r.getValue(), Long::sum);
        }
      }
      if ((kind.equals("LEFT") || kind.equals("FULL") || kind.equals("ANTI")) && !found
          || kind.equals("SEMI") && found)
        result.merge(l.getKey() + "|null", l.getValue(), Long::sum);
    }
    if (kind.equals("RIGHT") || kind.equals("FULL"))
      for (var r : right.entrySet())
        if (!matchedRight.contains(r.getKey()))
          result.merge("null|null|" + r.getKey().split("\\|")[1], r.getValue(), Long::sum);
    return result;
  }

  private static Map<String, Long> calculate(Map<String, Long> rows) {
    var result = new HashMap<String, Long>();
    rows.forEach(
        (row, count) -> {
          String[] fields = row.split("\\|");
          String key =
              fields[0].equals("null") ? "null" : Long.toString(2 * Long.parseLong(fields[0]));
          result.merge(key + "|" + (Long.parseLong(fields[1]) + 7), count, Long::sum);
        });
    return result;
  }

  private static Map<String, Long> input(Path path) throws Exception {
    var result = new HashMap<String, Long>();
    for (String line : Files.readAllLines(path)) {
      String[] row = line.split(",");
      result.merge(
          row[1] + "|" + row[2],
          row[0].equals("INSERT") || row[0].equals("UPDATE_AFTER") ? 1L : -1L,
          Long::sum);
    }
    result.values().removeIf(count -> count == 0);
    assertTrue(result.values().stream().allMatch(count -> count > 0));
    return result;
  }
}
