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

class CanonicalRetractingTopNJobTest {
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
                "offset",
                "offset-hidden",
                "variable",
                "ttl",
                "mini-rank",
                "changelog",
                "large")
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
                System.getProperty("streamfusion.test.retractTopNVariants", "").isEmpty()
                    || java.util.Arrays.asList(
                            System.getProperty("streamfusion.test.retractTopNVariants").split(","))
                        .contains(test.get()[0]))
        .filter(
            test ->
                !Boolean.getBoolean("streamfusion.test.retractTopNJobSmoke")
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
    if (variant.equals("mini-rank")) {
      runner.run(fixture, false, baseline, prefix, "none", "baseline-prefix", 3, backend);
      runner.run(
          fixture, false, baseline, middle, "baseline-prefix", "baseline-middle", 3, backend);
      runner.run(
          fixture, false, baseline, Integer.MAX_VALUE, "baseline-middle", "baseline", 3, backend);
    } else runner.run(fixture, false, baseline, Integer.MAX_VALUE, "none", "baseline", 3, backend);
    if (mode.equals("checkpoint")) {
      Files.createFile(switched.resolve("checkpoint-native-checkpoint"));
      runner.run(fixture, true, switched, prefix, "none", "checkpoint", 3, backend);
      runner.run(fixture, true, switched, prefix, "checkpoint", "prefix", 3, backend);
    } else runner.run(fixture, nativeFirst, switched, prefix, "none", "prefix", 3, backend);
    assertEquals(
        expected(switched, variant, prefix, backend),
        materialized(
            CanonicalFixtureFactory.committedRows(switched.resolve("committed"), resultType()),
            variant));
    String restore = "prefix";
    if (mode.equals("no-input")) {
      runner.run(fixture, !nativeFirst, switched, prefix, restore, "no-input", 3, backend);
      runner.run(fixture, nativeFirst, switched, prefix, "no-input", "return", 3, backend);
      restore = "return";
    }
    runner.run(fixture, !nativeFirst, switched, middle, restore, "middle", 3, backend);
    assertEquals(
        expected(switched, variant, middle, backend),
        materialized(
            CanonicalFixtureFactory.committedRows(switched.resolve("committed"), resultType()),
            variant));
    runner.run(fixture, nativeFirst, switched, Integer.MAX_VALUE, "middle", "suffix", 3, backend);
    var type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var switchedRows = CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type);
    var baselineRows = CanonicalFixtureFactory.committedRows(baseline.resolve("committed"), type);
    var actual = materialized(switchedRows, variant);
    assertEquals(expected(switched, variant, Integer.MAX_VALUE, backend), actual);
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
    if (!variant.equals("changelog")) Files.createFile(root.resolve("insert-only"));
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
    if (variant.equals("changelog"))
      input =
          new ArrayList<>(
              List.of(
                  "INSERT,2,10",
                  "INSERT,3,20",
                  "INSERT,2,30",
                  "INSERT,2,10",
                  "INSERT,3,20",
                  "DELETE,2,10",
                  "INSERT,2,40",
                  "UPDATE_BEFORE,3,20",
                  "UPDATE_AFTER,3,25",
                  "DELETE,2,30",
                  "INSERT,null,15",
                  "INSERT,3,null",
                  "INSERT,2,40",
                  "DELETE,3,20",
                  "INSERT,2,50",
                  "UPDATE_BEFORE,2,40",
                  "UPDATE_AFTER,2,45",
                  "INSERT,null,15",
                  "DELETE,3,25",
                  "INSERT,3,20"));
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
    if (variant.equals("ttl")) Files.writeString(root.resolve("state-ttl"), "1 h");
    if (variant.equals("mini-rank")) Files.writeString(root.resolve("one-phase-mini-batch"), "3");
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,v,"
            + (variant.contains("hidden") ? "CAST(0 AS BIGINT)" : "rn")
            + " FROM (SELECT *,ROW_NUMBER() OVER ("
            + (variant.equals("keyless") ? "" : "PARTITION BY k ")
            + "ORDER BY c ASC"
            + (twoSorts(variant) ? ",v ASC NULLS FIRST" : "")
            + ") AS rn FROM (SELECT k,v,COUNT(*) AS c"
            + (variant.equals("variable") ? ",COALESCE(MOD(ABS(k),3)+1,2) AS b" : "")
            + " FROM input_table GROUP BY k,v)) WHERE "
            + (variant.startsWith("offset") ? "rn>=2 AND " : "")
            + "rn<="
            + (variant.equals("variable")
                ? "b"
                : variant.contains("top1") ? "1" : variant.equals("large") ? "2147483648" : "3"));
    Files.writeString(
        root.resolve("native-operator"), "NativeColumnarTopN,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "TopN");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-rank");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    return root;
  }

  private static Map<String, Long> expected(Path root, String mode, int count, String backend)
      throws Exception {
    if (mode.equals("offset-hidden")) return RetractingRankStockReference.table(count, backend);
    var counts = new HashMap<String, Long>();
    var arrival = new HashMap<String, Integer>();
    var groups = new HashMap<String, java.util.Set<String>>();
    int time = 0;
    for (String line :
        Files.readAllLines(root.resolve("input.csv")).stream()
            .limit(count)
            .collect(java.util.stream.Collectors.toList())) {
      String[] row = line.split(",");
      assertTrue(List.of("INSERT", "UPDATE_BEFORE", "UPDATE_AFTER", "DELETE").contains(row[0]));
      String id = row[1] + "|" + row[2];
      long delta = row[0].equals("INSERT") || row[0].equals("UPDATE_AFTER") ? 1 : -1;
      long remaining = counts.merge(id, delta, Long::sum);
      assertTrue(remaining >= 0);
      var group =
          groups.computeIfAbsent(
              mode.equals("keyless") ? "singleton" : row[1], ignored -> new java.util.HashSet<>());
      if (remaining == 0) {
        counts.remove(id);
        arrival.remove(id);
        group.remove(id);
      } else {
        arrival.put(id, time);
        group.add(id);
      }
      time++;
    }
    var result = new HashMap<String, Long>();
    for (var group : groups.values()) {
      var rows = new ArrayList<>(group);
      java.util.Comparator<String> ties =
          twoSorts(mode)
              ? (left, right) -> {
                String l = left.substring(left.indexOf('|') + 1);
                String r = right.substring(right.indexOf('|') + 1);
                if (l.equals("null")) return r.equals("null") ? 0 : -1;
                if (r.equals("null")) return 1;
                return Long.compare(Long.parseLong(l), Long.parseLong(r));
              }
              : java.util.Comparator.comparingInt(arrival::get);
      rows.sort(java.util.Comparator.<String>comparingLong(counts::get).thenComparing(ties));
      String key = rows.isEmpty() ? "null" : rows.get(0).substring(0, rows.get(0).indexOf('|'));
      int limit =
          mode.contains("top1")
              ? 1
              : mode.equals("large")
                  ? Integer.MAX_VALUE
                  : mode.equals("variable")
                      ? key.equals("null") ? 2 : (int) (Math.abs(Long.parseLong(key)) % 3) + 1
                      : 3;
      for (int rank = mode.startsWith("offset") ? 1 : 0;
          rank < Math.min(limit, rows.size());
          rank++) result.put(rows.get(rank) + "|" + (mode.contains("hidden") ? 0 : rank + 1), 1L);
    }
    return result;
  }

  private static boolean twoSorts(String variant) {
    return variant.equals("two-sort")
        || variant.contains("hidden")
        || variant.contains("top1")
        || variant.startsWith("offset")
        || variant.equals("mini-rank")
        || variant.equals("changelog");
  }

  static RowType resultType() {
    return RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
  }

  static Map<String, Long> materialized(List<String> rows, String variant) {
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
