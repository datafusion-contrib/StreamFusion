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

class CanonicalEagerDedupJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> cases() {
    var continuation =
        Stream.of(
                "proc-first",
                "proc-last",
                "row-last",
                "mini-proc-first",
                "mini-proc-last",
                "mini-row-last",
                "mini-row-first",
                "compact-row-last",
                "compact-row-first",
                "ttl-proc-last",
                "ttl-row-last",
                "keyless-row-last")
            .flatMap(
                variant ->
                    handoffs().map(a -> Arguments.of(variant, "continue", a.get()[0], a.get()[1])));
    var controls =
        Stream.of("proc-first", "row-last")
            .flatMap(
                variant ->
                    Stream.of("empty", "no-input", "checkpoint", "rescale")
                        .flatMap(
                            mode ->
                                handoffs()
                                    .map(a -> Arguments.of(variant, mode, a.get()[0], a.get()[1]))))
            .filter(a -> !a.get()[1].equals("checkpoint") || (boolean) a.get()[2]);
    return Stream.concat(continuation, controls)
        .filter(
            a ->
                !Boolean.getBoolean("streamfusion.test.eagerDedupJobSmoke")
                    || a.get()[0].equals("mini-proc-last")
                    || a.get()[0].equals("mini-row-first"));
  }

  static Stream<Arguments> handoffs() {
    return Stream.of(false, true)
        .flatMap(first -> Stream.of("hashmap", "rocksdb").map(store -> Arguments.of(first, store)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void retainedDeduplicationStateContinuesAcrossIsolatedEngineProcesses(
      String variant, String mode, boolean nativeFirst, String backend) throws Exception {
    var runner = new CanonicalJobRunner(directory);
    var fixture = runner.fixtureJar();
    var baseline = prepare("baseline", variant, mode);
    var switched = prepare("switched", variant, mode);
    int prefix = mode.equals("empty") ? 0 : mode.equals("rescale") ? 128 : 4;
    int middle = mode.equals("rescale") ? 256 : 8;
    if (mode.equals("rescale")) {
      Files.createFile(switched.resolve("rescale"));
      Files.writeString(switched.resolve("native-rescale-family"), "Deduplicate");
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
    var actual = bag(CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type));
    assertEquals(expected(switched, variant), actual);
    assertEquals(
        bag(CanonicalFixtureFactory.committedRows(baseline.resolve("committed"), type)), actual);
    if ((variant.startsWith("mini") || variant.startsWith("compact"))
        && !variant.endsWith("proc-first"))
      assertTrue(
          Files.exists(
              switched.resolve(
                  (nativeFirst ? "prefix" : "middle") + "-checkpoint-flush-verified")));
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
    Files.createFile(root.resolve("wait-native-input"));
    Files.writeString(root.resolve("native-ready-families"), "Deduplicate");
    boolean rowtime = variant.contains("row");
    boolean first = variant.endsWith("first");
    if (variant.startsWith("ttl")) Files.writeString(root.resolve("state-ttl"), "1 h");
    if (variant.startsWith("mini") || variant.startsWith("compact")) {
      Files.writeString(root.resolve("one-phase-mini-batch"), "3");
      Files.writeString(root.resolve("mini-batch-latency"), "1000000 d");
      Files.writeString(
          root.resolve("dedup-compact-changes"), Boolean.toString(variant.startsWith("compact")));
      if (!(first && !rowtime))
        Files.writeString(root.resolve("require-checkpoint-flush-families"), "Deduplicate");
    }
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT,v BIGINT,rt AS TO_TIMESTAMP_LTZ(v,3),WATERMARK FOR rt AS rt - INTERVAL '1' HOUR");
    var rows =
        new ArrayList<>(
            List.of(
                "INSERT,1,10",
                "INSERT,2,20",
                "INSERT,null,30",
                "INSERT,1,15",
                "INSERT,2,18",
                "INSERT,3,25",
                "INSERT,null,35",
                "INSERT,1,15",
                "INSERT,3,40",
                "INSERT,2,22",
                "INSERT,1,9",
                "INSERT,null,28"));
    if (mode.equals("rescale")) {
      Files.createFile(root.resolve("disable-chaining"));
      rows.clear();
      for (int round = 0; round < 6; round++)
        for (int key = 0; key < 128; key++)
          rows.add("INSERT," + key + "," + ((round % 2 == 0 ? round + 1 : 1) * 1000 + key));
    }
    Files.write(root.resolve("input.csv"), rows);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,v,COALESCE(k,-1) FROM"
            + " (SELECT *,ROW_NUMBER() OVER ("
            + (variant.startsWith("keyless") ? "" : "PARTITION BY k ")
            + "ORDER BY "
            + (rowtime ? "rt" : "pt")
            + (first ? " ASC" : " DESC")
            + ") AS rn FROM input_table) WHERE rn=1");
    Files.writeString(
        root.resolve("native-operator"), "NativeDeduplicate,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "Deduplicate");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-deduplicate");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    return root;
  }

  private static Map<String, Long> expected(Path root, String variant) throws Exception {
    var retained = new HashMap<String, String[]>();
    for (String line : Files.readAllLines(root.resolve("input.csv"))) {
      String[] fields = line.split(",");
      String key = variant.startsWith("keyless") ? "*" : fields[1];
      var previous = retained.get(key);
      boolean wins = previous == null;
      if (previous != null) {
        if (variant.contains("proc")) wins = variant.endsWith("last");
        else
          wins =
              variant.endsWith("first")
                  ? Long.parseLong(fields[2]) < Long.parseLong(previous[2])
                  : Long.parseLong(fields[2]) >= Long.parseLong(previous[2]);
      }
      if (wins) retained.put(key, fields);
    }
    var result = new HashMap<String, Long>();
    retained
        .values()
        .forEach(
            fields ->
                result.merge(
                    fields[1]
                        + "|"
                        + fields[2]
                        + "|"
                        + (fields[1].equals("null") ? "-1" : fields[1]),
                    1L,
                    Long::sum));
    return result;
  }

  private static Map<String, Long> bag(List<String> rows) {
    var result = new HashMap<String, Long>();
    for (String row : rows)
      result.merge(row.substring(3), row.charAt(0) == '-' ? -1L : 1L, Long::sum);
    result.values().removeIf(count -> count == 0);
    assertTrue(result.values().stream().allMatch(count -> count > 0), result.toString());
    return result;
  }
}
