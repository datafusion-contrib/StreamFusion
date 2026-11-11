package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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

class CanonicalBufferedDedupJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> cases() {
    var continuation =
        Stream.of("mixed", "mixed-ttl", "not-null", "keyless", "keyless-ttl", "retained-late")
            .flatMap(
                variant ->
                    handoffs().map(a -> Arguments.of(variant, "continue", a.get()[0], a.get()[1])));
    var controls =
        Stream.of("mixed", "keyless")
            .flatMap(
                variant ->
                    Stream.of("empty", "no-input", "checkpoint", "rescale")
                        .filter(mode -> !mode.equals("rescale") || variant.equals("mixed"))
                        .flatMap(
                            mode ->
                                handoffs()
                                    .map(a -> Arguments.of(variant, mode, a.get()[0], a.get()[1]))))
            .filter(a -> !a.get()[1].equals("checkpoint") || (boolean) a.get()[2]);
    return Stream.concat(continuation, controls)
        .filter(
            a -> {
              String selection =
                  System.getProperty("streamfusion.test.bufferedDedupJobSelection", "all");
              return selection.equals("all")
                  || selection.equals("retained-only") && a.get()[0].equals("retained-late")
                  || selection.equals("empty-and-retained")
                      && (a.get()[1].equals("empty") || a.get()[0].equals("retained-late"));
            })
        .filter(
            a ->
                !Boolean.getBoolean("streamfusion.test.bufferedDedupJobSmoke")
                    || a.get()[0].equals("mixed") && a.get()[1].equals("continue"));
  }

  static Stream<Arguments> handoffs() {
    return Stream.of(false, true)
        .flatMap(first -> Stream.of("hashmap", "rocksdb").map(store -> Arguments.of(first, store)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void timersAndEmittedMarkersContinueAcrossIsolatedEngineProcesses(
      String variant, String mode, boolean nativeFirst, String backend) throws Exception {
    var runner = new CanonicalJobRunner(directory);
    var fixture = runner.fixtureJar();
    var baseline = prepare("baseline", variant, mode);
    var switched = prepare("switched", variant, mode);
    int prefix =
        mode.equals("empty")
            ? 0
            : Integer.parseInt(Files.readString(switched.resolve("prefix-boundary")));
    int middle = Integer.parseInt(Files.readString(switched.resolve("middle-boundary")));
    runner.run(fixture, false, baseline, Integer.MAX_VALUE, "none", "baseline", 3, "hashmap");
    if (mode.equals("rescale")) {
      Files.createFile(switched.resolve("rescale"));
      Files.writeString(switched.resolve("native-rescale-family"), "Deduplicate");
      Files.writeString(switched.resolve("prefix-parallelism"), "1");
      Files.writeString(switched.resolve("middle-parallelism"), "3");
      Files.writeString(switched.resolve("suffix-parallelism"), "2");
    }
    if (mode.equals("checkpoint")) {
      Files.createFile(switched.resolve("checkpoint-native-checkpoint"));
      runner.run(fixture, true, switched, prefix, "none", "checkpoint", 3, backend);
      runner.run(fixture, true, switched, prefix, "checkpoint", "prefix", 3, backend);
    } else {
      runner.run(
          fixture,
          nativeFirst,
          switched,
          prefix,
          "none",
          "prefix",
          3,
          nativeFirst ? backend : "hashmap");
    }
    assertEquals(expected(switched, variant, prefix), committed(switched));
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
      assertEquals(expected(switched, variant, prefix), committed(switched));
      runner.run(
          fixture,
          nativeFirst,
          switched,
          prefix,
          "no-input",
          "return",
          3,
          nativeFirst ? backend : "hashmap");
      assertEquals(expected(switched, variant, prefix), committed(switched));
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
    assertEquals(expected(switched, variant, middle), committed(switched));
    runner.run(
        fixture,
        nativeFirst,
        switched,
        Integer.MAX_VALUE,
        "middle",
        "suffix",
        3,
        nativeFirst ? backend : "hashmap");
    var actual = committed(switched);
    assertEquals(expected(switched, variant, Integer.MAX_VALUE), actual);
    assertEquals(committed(baseline), actual);
    var type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    assertEquals(
        CanonicalFixtureFactory.committedRows(baseline.resolve("committed"), type).stream()
            .sorted()
            .collect(java.util.stream.Collectors.toList()),
        CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type).stream()
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
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

  boolean ordinary() {
    return false;
  }

  private Path prepare(String name, String variant, String mode) throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    if (ordinary()) Files.createFile(root.resolve("ordinary-sql"));
    Files.createFile(root.resolve("insert-only"));
    Files.createFile(root.resolve("source-watermarks"));
    Files.createFile(root.resolve("wait-native-input"));
    Files.writeString(root.resolve("native-ready-families"), "Deduplicate");
    Files.writeString(root.resolve("native-input-families"), "Deduplicate");
    Files.writeString(
        root.resolve("native-operator"), "NativeDeduplicate,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-deduplicate");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    if (variant.endsWith("ttl")) Files.writeString(root.resolve("state-ttl"), "1 h");
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT"
            + (variant.equals("not-null") ? " NOT NULL" : "")
            + ",v BIGINT,rt TIMESTAMP(3),WATERMARK FOR rt AS SOURCE_WATERMARK()");
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,v,COALESCE(k,-1) FROM"
            + " (SELECT *,ROW_NUMBER() OVER ("
            + (variant.startsWith("keyless") ? "" : "PARTITION BY k ")
            + "ORDER BY rt ASC) AS rn FROM input_table) WHERE rn=1");
    int groups = mode.equals("rescale") ? 64 : 1;
    if (mode.equals("rescale")) Files.createFile(root.resolve("disable-chaining"));
    var rows = new ArrayList<String>();
    for (int group = 0; group < groups; group++) {
      add(rows, group, "1", 10, 100);
      add(rows, group, "2", 20, 300);
      add(rows, group, group == 0 && !variant.equals("not-null") ? "null" : "6", 30, 400);
      add(rows, group, "1", 11, 90);
    }
    rows.add("WATERMARK,100");
    for (int group = 0; group < groups; group++) {
      add(rows, group, "3", 40, 500);
      add(rows, group, "2", 18, 250);
    }
    Files.writeString(root.resolve("prefix-boundary"), Integer.toString(rows.size()));
    for (int group = 0; group < groups; group++) {
      add(rows, group, "1", 99, 150);
      add(rows, group, "2", 17, 200);
    }
    rows.add("WATERMARK,250");
    for (int group = 0; group < groups; group++) {
      add(
          rows,
          group,
          group == 0 && !variant.equals("not-null") ? "null" : "6",
          29,
          variant.equals("retained-late") ? 450 : 350);
      add(rows, group, "3", 39, variant.equals("retained-late") ? 550 : 450);
      add(rows, group, "4", 50, 700);
    }
    Files.writeString(root.resolve("middle-boundary"), Integer.toString(rows.size()));
    for (int group = 0; group < groups; group++) add(rows, group, "2", 99, 600);
    rows.add("WATERMARK,500");
    for (int group = 0; group < groups; group++) {
      add(rows, group, "4", 49, 650);
      add(rows, group, "5", 60, 800);
    }
    if (variant.equals("retained-late")) {
      add(rows, 0, "7", 70, 400);
      add(rows, 0, "8", 80, 500);
      add(rows, 0, "9", 90, 499);
      add(rows, 0, "9", 91, 501);
    }
    rows.add("WATERMARK,1000");
    Files.write(root.resolve("input.csv"), rows);
    return root;
  }

  private static void add(List<String> rows, int group, String key, long value, long time) {
    rows.add(
        "INSERT,"
            + (key.equals("null") ? key : Integer.toString(group * 10 + Integer.parseInt(key)))
            + ","
            + (group * 100 + value)
            + ","
            + time);
  }

  private static Map<String, Long> expected(Path root, String variant, int limit) throws Exception {
    if (tech.streamfusion.compat.FlinkCompat.EAGER_ROWTIME_KEEP_FIRST)
      return eagerExpected(root, variant, limit);
    var pending = new HashMap<String, String[]>();
    var emitted = new HashSet<String>();
    var result = new HashMap<String, Long>();
    long watermark = Long.MIN_VALUE;
    var lines = Files.readAllLines(root.resolve("input.csv"));
    for (int i = 0; i < Math.min(limit, lines.size()); i++) {
      String[] fields = lines.get(i).split(",");
      if (fields[0].equals("WATERMARK")) {
        watermark = Long.parseLong(fields[1]);
        var entries = pending.entrySet().iterator();
        while (entries.hasNext()) {
          var entry = entries.next();
          var row = entry.getValue();
          if (Long.parseLong(row[3]) <= watermark) {
            result.merge(
                row[1] + "|" + row[2] + "|" + (row[1].equals("null") ? "-1" : row[1]),
                1L,
                Long::sum);
            emitted.add(entry.getKey());
            entries.remove();
          }
        }
      } else {
        String key = variant.startsWith("keyless") ? "*" : fields[1];
        long time = Long.parseLong(fields[3]);
        if (time < watermark || emitted.contains(key)) continue;
        var old = pending.get(key);
        if (old == null || time < Long.parseLong(old[3])) pending.put(key, fields);
      }
    }
    return result;
  }

  private static Map<String, Long> eagerExpected(Path root, String variant, int limit)
      throws Exception {
    var winners = new HashMap<String, String[]>();
    var lines = Files.readAllLines(root.resolve("input.csv"));
    for (int i = 0; i < Math.min(limit, lines.size()); i++) {
      String[] fields = lines.get(i).split(",");
      if (fields[0].equals("WATERMARK")) continue;
      String key = variant.startsWith("keyless") ? "*" : fields[1];
      var old = winners.get(key);
      if (old == null || Long.parseLong(fields[3]) < Long.parseLong(old[3]))
        winners.put(key, fields);
    }
    var result = new HashMap<String, Long>();
    for (var row : winners.values())
      result.merge(
          row[1] + "|" + row[2] + "|" + (row[1].equals("null") ? "-1" : row[1]), 1L, Long::sum);
    return result;
  }

  private static Map<String, Long> committed(Path root) throws Exception {
    if (!Files.exists(root.resolve("committed"))) return Map.of();
    var type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var result = new HashMap<String, Long>();
    for (String row : CanonicalFixtureFactory.committedRows(root.resolve("committed"), type)) {
      if (tech.streamfusion.compat.FlinkCompat.EAGER_ROWTIME_KEEP_FIRST) {
        String kind = row.substring(0, 3);
        assertTrue(List.of("+I|", "+U|", "-U|", "-D|").contains(kind), row);
        result.merge(row.substring(3), kind.charAt(0) == '+' ? 1L : -1L, Long::sum);
      } else {
        assertTrue(row.startsWith("+I|"), row);
        result.merge(row.substring(3), 1L, Long::sum);
      }
    }
    result.values().removeIf(count -> count == 0);
    assertTrue(result.values().stream().allMatch(count -> count > 0), result.toString());
    return result;
  }
}
