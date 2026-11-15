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

class CanonicalUpdateTopNJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> cases() {
    var continuation =
        Stream.of(
                "rank",
                "hidden",
                "top1",
                "top1-hidden",
                "keyless",
                "not-null",
                "two-sort",
                "two-sort-top1")
            .flatMap(
                variant ->
                    handoffs()
                        .map(
                            test ->
                                Arguments.of(variant, "continue", test.get()[0], test.get()[1])));
    var controls =
        Stream.of("rank", "top1")
            .flatMap(
                variant ->
                    Stream.of("no-input", "checkpoint", "rescale", "empty")
                        .flatMap(
                            mode ->
                                handoffs()
                                    .map(
                                        test ->
                                            Arguments.of(
                                                variant, mode, test.get()[0], test.get()[1]))))
            .filter(test -> !test.get()[1].equals("checkpoint") || (boolean) test.get()[2]);
    return Stream.concat(continuation, controls)
        .filter(
            test ->
                System.getProperty("streamfusion.test.updateTopNVariants", "").isEmpty()
                    || java.util.Arrays.asList(
                            System.getProperty("streamfusion.test.updateTopNVariants").split(","))
                        .contains(test.get()[0]))
        .filter(
            test ->
                !Boolean.getBoolean("streamfusion.test.updateTopNJobSmoke")
                    || (test.get()[0].equals("rank") || test.get()[0].equals("top1"))
                        && test.get()[1].equals("continue")
                        && (boolean) test.get()[2]);
  }

  static Stream<Arguments> handoffs() {
    return Stream.of(false, true)
        .flatMap(first -> Stream.of("hashmap", "rocksdb").map(store -> Arguments.of(first, store)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void retainedRowsContinueAcrossIsolatedEngineProcesses(
      String variant, String mode, boolean nativeFirst, String backend) throws Exception {
    check(variant, mode, nativeFirst, backend);
  }

  private void check(String variant, String mode, boolean nativeFirst, String backend)
      throws Exception {
    var runner = new CanonicalJobRunner(directory);
    var fixture = runner.fixtureJar();
    var baseline = prepare("baseline", variant, mode);
    var switched = prepare("switched", variant, mode);
    int prefix = mode.equals("empty") ? 0 : mode.equals("rescale") ? 128 : 5;
    int middle = mode.equals("rescale") ? 256 : 10;
    if (mode.equals("rescale")) {
      Files.createFile(switched.resolve("rescale"));
      Files.writeString(switched.resolve("native-rescale-family"), "TopN");
      Files.writeString(switched.resolve("prefix-parallelism"), "1");
      Files.writeString(switched.resolve("middle-parallelism"), "3");
      Files.writeString(switched.resolve("suffix-parallelism"), "2");
    }
    runner.run(fixture, false, baseline, Integer.MAX_VALUE, "none", "baseline", 3, "hashmap");
    if (mode.equals("checkpoint")) {
      Files.createFile(switched.resolve("checkpoint-native-checkpoint"));
      runner.run(fixture, true, switched, prefix, "none", "checkpoint", 3, backend);
      runner.run(fixture, true, switched, prefix, "checkpoint", "prefix", 3, backend);
    } else
      runner.run(
          fixture,
          nativeFirst,
          switched,
          prefix,
          "none",
          "prefix",
          3,
          nativeFirst ? backend : "hashmap");
    assertEquals(
        expected(switched, variant, prefix),
        materialized(
            CanonicalFixtureFactory.committedRows(switched.resolve("committed"), resultType()),
            variant));
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
          nativeFirst ? "hashmap" : backend);
      runner.run(
          fixture,
          nativeFirst,
          switched,
          prefix,
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
        middle,
        restore,
        "middle",
        3,
        nativeFirst ? "hashmap" : backend);
    assertEquals(
        expected(switched, variant, middle),
        materialized(
            CanonicalFixtureFactory.committedRows(switched.resolve("committed"), resultType()),
            variant));
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
    var switchedRows = CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type);
    var baselineRows = CanonicalFixtureFactory.committedRows(baseline.resolve("committed"), type);
    var actual = materialized(switchedRows, variant);
    assertEquals(expected(switched, variant, Integer.MAX_VALUE), actual);
    assertEquals(materialized(baselineRows, variant), actual);
    assertEquals(changelogBag(baselineRows), changelogBag(switchedRows));
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

  Path prepare(String name, String variant, String mode) throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    Files.createDirectories(root.resolve("committed"));
    Files.createFile(root.resolve("insert-only"));
    Files.createFile(root.resolve("wait-native-input"));
    var input =
        new ArrayList<>(
            List.of(
                "INSERT,2,10",
                "INSERT,3,20",
                "INSERT,2,30",
                "INSERT,2,10",
                "INSERT,3,20",
                "INSERT,2,40",
                "INSERT,2,30",
                "INSERT,2,30",
                "INSERT,3,null",
                "INSERT,null,15",
                "INSERT,2,40",
                "INSERT,2,40",
                "INSERT,3,null",
                "INSERT,null,15",
                "INSERT,3,20",
                "INSERT,2,50",
                "INSERT,2,50",
                "INSERT,3,null",
                "INSERT,2,50",
                "INSERT,2,50"));
    if (mode.equals("rescale")) {
      Files.createFile(root.resolve("disable-chaining"));
      input.clear();
      // Each partition has one aggregate key so rescaling does not introduce cross-channel
      // ordering ambiguity; continuation cases separately exercise competing ranked rows.
      for (int round = 0; round < 6; round++)
        for (int key = 0; key < 64; key++)
          input.add("INSERT," + (key == 0 ? "null" : key) + "," + key);
    }
    if (variant.equals("not-null")) {
      Files.writeString(root.resolve("input-columns.sql"), "k BIGINT NOT NULL,v BIGINT");
      input.replaceAll(line -> line.replace("INSERT,null,", "INSERT,4,"));
    }
    Files.write(root.resolve("input.csv"), input);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,v,"
            + (variant.contains("hidden") ? "CAST(0 AS BIGINT)" : "rn")
            + " FROM (SELECT k,v,c,ROW_NUMBER() OVER ("
            + (variant.equals("keyless") ? "" : "PARTITION BY k")
            + " ORDER BY c DESC"
            + (variant.startsWith("two-sort") ? ",v ASC NULLS FIRST" : "")
            + ") AS rn FROM (SELECT k,v,COUNT(*) AS c FROM input_table"
            + " GROUP BY k,v)) WHERE rn<="
            + (variant.contains("top1") ? 1 : 3));
    Files.writeString(
        root.resolve("native-operator"), "NativeColumnarTopN,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "TopN");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-rank");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    return root;
  }

  private static Map<String, Long> expected(Path root, String mode, int count) throws Exception {
    var counts = new HashMap<String, Long>();
    var arrival = new HashMap<String, Integer>();
    var groups = new HashMap<String, java.util.Set<String>>();
    int time = 0;
    for (String line :
        Files.readAllLines(root.resolve("input.csv")).stream()
            .limit(count)
            .collect(java.util.stream.Collectors.toList())) {
      String[] row = line.split(",");
      assertEquals("INSERT", row[0]);
      String id = row[1] + "|" + row[2];
      counts.merge(id, 1L, Long::sum);
      arrival.put(id, time++);
      groups
          .computeIfAbsent(
              mode.equals("keyless") ? "singleton" : row[1], ignored -> new java.util.HashSet<>())
          .add(id);
    }
    var result = new HashMap<String, Long>();
    for (var group : groups.values()) {
      var rows = new ArrayList<>(group);
      java.util.Comparator<String> ties =
          mode.startsWith("two-sort")
              ? (left, right) -> {
                String l = left.substring(left.indexOf('|') + 1);
                String r = right.substring(right.indexOf('|') + 1);
                if (l.equals("null")) return r.equals("null") ? 0 : -1;
                if (r.equals("null")) return 1;
                return Long.compare(Long.parseLong(l), Long.parseLong(r));
              }
              : java.util.Comparator.comparingInt(arrival::get);
      rows.sort(
          java.util.Comparator.<String>comparingLong(counts::get).reversed().thenComparing(ties));
      int limit = mode.contains("top1") ? 1 : 3;
      for (int rank = 0; rank < Math.min(limit, rows.size()); rank++)
        result.put(rows.get(rank) + "|" + (mode.contains("hidden") ? 0 : rank + 1), 1L);
    }
    return result;
  }

  private static RowType resultType() {
    return RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
  }

  private static Map<String, Long> materialized(List<String> rows, String variant) {
    if (!variant.contains("top1")) return bag(rows);
    var winners = new HashMap<String, String>();
    for (String row : rows) {
      String kind = row.substring(0, 2);
      String value = row.substring(3);
      String key = value.substring(0, value.indexOf('|'));
      assertTrue(List.of("+I", "+U", "-U", "-D").contains(kind), row);
      if (kind.equals("+I") || kind.equals("+U")) winners.put(key, value);
      else
        assertEquals(value, winners.remove(key), "Retraction must match the current Top-1 winner");
    }
    var result = new HashMap<String, Long>();
    for (String value : winners.values()) result.merge(value, 1L, Long::sum);
    return result;
  }

  private static Map<String, Long> changelogBag(List<String> rows) {
    var result = new HashMap<String, Long>();
    for (String row : rows) result.merge(row, 1L, Long::sum);
    return result;
  }

  private static Map<String, Long> bag(List<String> rows) {
    var result = new HashMap<String, Long>();
    for (String row : rows) {
      String kind = row.substring(0, 2);
      assertTrue(List.of("+I", "+U", "-U", "-D").contains(kind), row);
      result.merge(row.substring(3), kind.equals("+I") || kind.equals("+U") ? 1L : -1L, Long::sum);
    }
    result.values().removeIf(count -> count == 0);
    assertTrue(result.values().stream().allMatch(count -> count > 0), result.toString());
    return result;
  }
}
