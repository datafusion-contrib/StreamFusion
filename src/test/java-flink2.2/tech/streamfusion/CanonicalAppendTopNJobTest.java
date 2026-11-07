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

class CanonicalAppendTopNJobTest {
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
                "ttl",
                "mini-rank",
                "descending",
                "not-null",
                "ttl-top1",
                "mini-top1",
                "descending-top1",
                "variable")
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
                System.getProperty("streamfusion.test.appendTopNVariants", "").isEmpty()
                    || java.util.Arrays.asList(
                            System.getProperty("streamfusion.test.appendTopNVariants").split(","))
                        .contains(test.get()[0]))
        .filter(
            test ->
                !Boolean.getBoolean("streamfusion.test.appendTopNJobSmoke")
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

  static Stream<Arguments> concurrentRocksCases() {
    return Stream.of("hidden", "top1")
        .flatMap(
            variant ->
                java.util.stream.IntStream.range(0, 2)
                    .mapToObj(repeat -> Arguments.of(variant, repeat)));
  }

  @ParameterizedTest
  @MethodSource("concurrentRocksCases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void rocksStateInitializesAndContinuesAcrossIndependentConcurrentJobs(String variant, int repeat)
      throws Exception {
    check(variant, "continue", true, "rocksdb");
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
    if (variant.startsWith("mini")) {
      Files.writeString(root.resolve("one-phase-mini-batch"), "3");
      Files.writeString(root.resolve("mini-batch-latency"), "1000000 d");
    }
    Files.createFile(root.resolve("insert-only"));
    Files.createFile(root.resolve("wait-native-input"));
    if (variant.startsWith("ttl")) Files.writeString(root.resolve("state-ttl"), "1 h");
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
                "INSERT,2,null",
                "INSERT,null,18",
                "INSERT,2,50",
                "INSERT,null,20",
                "INSERT,3,60"));
    if (mode.equals("rescale")) {
      Files.createFile(root.resolve("disable-chaining"));
      input.clear();
      for (int round = 0; round < 6; round++)
        for (int key = 0; key < 64; key++)
          input.add("INSERT," + (key == 0 ? "null" : key) + "," + (1000 * round + key));
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
            + " FROM (SELECT k,v,ROW_NUMBER() OVER ("
            + (variant.equals("keyless") ? "" : "PARTITION BY k")
            + " ORDER BY v "
            + (variant.contains("descending") ? "DESC NULLS LAST" : "ASC NULLS FIRST")
            + ") AS rn FROM input_table) WHERE rn<="
            + (variant.contains("top1") ? 1 : 3));
    if (variant.equals("variable"))
      Files.writeString(
          root.resolve("query.sql"),
          "INSERT INTO output_table SELECT k,v,rn FROM (SELECT k,v,b,ROW_NUMBER() OVER "
              + "(PARTITION BY k ORDER BY v ASC NULLS FIRST) AS rn FROM "
              + "(SELECT k,v,COALESCE(MOD(ABS(k),3)+1,2) AS b FROM input_table)) WHERE rn<=b");
    Files.writeString(
        root.resolve("native-operator"), "NativeColumnarTopN,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "TopN");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-rank");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    return root;
  }

  private static Map<String, Long> expected(Path root, String mode, int count) throws Exception {
    var groups = new HashMap<String, List<String[]>>();
    for (String line :
        Files.readAllLines(root.resolve("input.csv")).stream()
            .limit(count)
            .collect(java.util.stream.Collectors.toList())) {
      String[] fields = line.split(",");
      assertEquals("INSERT", fields[0]);
      groups
          .computeIfAbsent(mode.equals("keyless") ? "singleton" : fields[1], k -> new ArrayList<>())
          .add(fields);
    }
    var result = new HashMap<String, Long>();
    java.util.Comparator<String[]> order =
        (a, b) -> {
          if (a[2].equals("null")) return b[2].equals("null") ? 0 : -1;
          if (b[2].equals("null")) return 1;
          return Long.compare(Long.parseLong(a[2]), Long.parseLong(b[2]));
        };
    if (mode.contains("descending")) order = order.reversed();
    int limit = mode.contains("top1") ? 1 : 3;
    for (var group : groups.entrySet()) {
      var rows = group.getValue();
      if (mode.equals("variable"))
        limit =
            group.getKey().equals("null")
                ? 2
                : (int) (Math.abs(Long.parseLong(group.getKey())) % 3 + 1);
      rows.sort(order);
      for (int i = 0; i < Math.min(limit, rows.size()); i++) {
        var row = rows.get(i);
        result.merge(
            row[1] + "|" + row[2] + "|" + (mode.contains("hidden") ? 0 : i + 1), 1L, Long::sum);
      }
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
