package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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

class CanonicalRetractingProctimeJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            compiled ->
                Stream.of("continue", "no-input", "checkpoint", "rescale")
                    .flatMap(
                        mode ->
                            Stream.of(false, true)
                                .filter(first -> !mode.equals("checkpoint") || first)
                                .flatMap(
                                    first ->
                                        Stream.of("hashmap", "rocksdb")
                                            .map(
                                                store ->
                                                    Arguments.of(compiled, mode, first, store)))))
        .filter(
            test ->
                !Boolean.getBoolean("streamfusion.test.retractProctimeJobSmoke")
                    || (boolean) test.get()[0]
                        && test.get()[1].equals("continue")
                        && !(boolean) test.get()[2]
                        && test.get()[3].equals("rocksdb"));
  }

  @ParameterizedTest(name = "compiled={0}, {1}, native-first={2}, {3}")
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void projectedClockSurvivesCanonicalSwitchesWithCleanStockWorkers(
      boolean compiled, String mode, boolean nativeFirst, String backend) throws Exception {
    var runner = new CanonicalJobRunner(directory);
    var fixture = runner.fixtureJar();
    var baseline = prepare("baseline", compiled, mode);
    var switched = prepare("switched", compiled, mode);
    long earliest = Math.floorDiv(System.currentTimeMillis(), 1000) - 5;
    execute(runner, fixture, baseline, false, false, mode, backend, earliest);
    execute(runner, fixture, switched, true, nativeFirst, mode, backend, earliest);
    var expectedRows = rows(baseline, earliest);
    var actualRows = rows(switched, earliest);
    assertEquals(changes(expectedRows), changes(actualRows));
    assertEquals(materialized(expectedRows), materialized(actualRows));
    assertEquals(expected(switched, Integer.MAX_VALUE), materialized(actualRows));
    for (String stage : List.of("prefix", "middle")) {
      var savepoint =
          Path.of(
              java.net.URI.create(Files.readString(switched.resolve(stage + "-savepoint")).trim()));
      assertFalse(
          new String(
                  Files.readAllBytes(savepoint.resolve("_metadata")), StandardCharsets.ISO_8859_1)
              .contains("tech.streamfusion"));
    }
    String stockStage = nativeFirst ? "middle" : "suffix";
    assertTrue(Files.exists(switched.resolve(stockStage + "-stock-classpath-verified")));
    if (!compiled)
      assertTrue(Files.exists(switched.resolve(stockStage + "-ordinary-sql-verified")));
  }

  private void execute(
      CanonicalJobRunner runner,
      Path fixture,
      Path root,
      boolean switching,
      boolean nativeFirst,
      String mode,
      String backend,
      long earliest)
      throws Exception {
    int prefix = mode.equals("rescale") ? 128 : 4;
    int middle = mode.equals("rescale") ? 256 : 8;
    if (mode.equals("rescale")) {
      Files.createFile(root.resolve("rescale"));
      Files.writeString(root.resolve("native-rescale-family"), "TopN");
      Files.writeString(root.resolve("prefix-parallelism"), "1");
      Files.writeString(root.resolve("middle-parallelism"), "3");
      Files.writeString(root.resolve("suffix-parallelism"), "2");
    }
    if (mode.equals("checkpoint")) {
      Files.createFile(root.resolve("checkpoint-native-checkpoint"));
      runner.run(fixture, switching, root, prefix, "none", "checkpoint", 3, backend);
      runner.run(fixture, switching, root, prefix, "checkpoint", "prefix", 3, backend);
    } else
      runner.run(fixture, switching && nativeFirst, root, prefix, "none", "prefix", 3, backend);
    assertEquals(expected(root, prefix), materialized(rows(root, earliest)));
    String restore = "prefix";
    if (mode.equals("no-input")) {
      runner.run(fixture, switching && !nativeFirst, root, prefix, restore, "no-input", 3, backend);
      runner.run(fixture, switching && nativeFirst, root, prefix, "no-input", "return", 3, backend);
      restore = "return";
      assertEquals(expected(root, prefix), materialized(rows(root, earliest)));
    }
    runner.run(fixture, switching && !nativeFirst, root, middle, restore, "middle", 3, backend);
    assertEquals(expected(root, middle), materialized(rows(root, earliest)));
    runner.run(
        fixture, switching && nativeFirst, root, Integer.MAX_VALUE, "middle", "suffix", 3, backend);
    assertEquals(expected(root, Integer.MAX_VALUE), materialized(rows(root, earliest)));
  }

  private Path prepare(String name, boolean compiled, String mode) throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root.resolve("committed"));
    Files.createFile(root.resolve("wait-native-input"));
    if (!compiled) Files.createFile(root.resolve("ordinary-sql"));
    Files.writeString(root.resolve("time-zone"), "UTC");
    Files.writeString(root.resolve("uid-generation"), "ALWAYS");
    Files.writeString(root.resolve("input-columns.sql"), "k BIGINT,v BIGINT,pt AS PROCTIME()");
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,v,UNIX_TIMESTAMP(CAST(pt AS STRING))"
            + " FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY k ORDER BY v ASC) AS rn"
            + " FROM input_table) WHERE rn<=3");
    Files.writeString(
        root.resolve("native-operator"), "NativeColumnarTopN,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "TopN");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-rank");
    var input =
        new ArrayList<>(
            List.of(
                "INSERT,1,20",
                "INSERT,1,10",
                "INSERT,1,30",
                "INSERT,2,8",
                "DELETE,1,20",
                "INSERT,1,15",
                "UPDATE_BEFORE,2,8",
                "UPDATE_AFTER,2,9",
                "INSERT,1,5",
                "DELETE,1,30",
                "INSERT,2,7",
                "INSERT,1,40"));
    if (mode.equals("rescale")) {
      Files.createFile(root.resolve("disable-chaining"));
      input.clear();
      for (int round = 0; round < 6; round++)
        for (int key = 0; key < 64; key++)
          input.add(
              (round == 3 || round == 5 ? "DELETE" : "INSERT")
                  + ","
                  + key
                  + ","
                  + new long[] {20, 10, 30, 20, 15, 30}[round]);
    }
    Files.write(root.resolve("input.csv"), input);
    return root;
  }

  private static List<String> rows(Path root, long earliest) throws Exception {
    var values =
        CanonicalFixtureFactory.committedRows(
            root.resolve("committed"),
            RowType.of(new BigIntType(), new BigIntType(), new BigIntType()));
    long latest = Math.floorDiv(System.currentTimeMillis(), 1000) + 5;
    for (String row : values) {
      var fields = row.split("\\|", -1);
      assertEquals(4, fields.length, row);
      assertNotEquals("null", fields[3], "Projected processing time must materialize");
      long clock = Long.parseLong(fields[3]);
      assertTrue(clock >= earliest && clock <= latest, "Clock outside actual execution: " + row);
    }
    return values;
  }

  private static Map<String, Long> changes(List<String> rows) {
    var result = new HashMap<String, Long>();
    for (String row : rows) {
      var fields = row.split("\\|", -1);
      result.merge(fields[0] + "|" + fields[1] + "|" + fields[2], 1L, Long::sum);
    }
    return result;
  }

  private static Map<String, Long> materialized(List<String> rows) {
    var result = new HashMap<String, Long>();
    for (String row : rows) {
      var fields = row.split("\\|", -1);
      result.merge(
          fields[1] + "|" + fields[2],
          fields[0].equals("+I") || fields[0].equals("+U") ? 1L : -1L,
          Long::sum);
    }
    result.values().removeIf(count -> count == 0);
    return result;
  }

  private static Map<String, Long> expected(Path root, int limit) throws Exception {
    var groups = new HashMap<Long, TreeMap<Long, Long>>();
    for (String row :
        Files.readAllLines(root.resolve("input.csv")).stream()
            .limit(limit)
            .collect(java.util.stream.Collectors.toList())) {
      var fields = row.split(",");
      long key = Long.parseLong(fields[1]), value = Long.parseLong(fields[2]);
      var values = groups.computeIfAbsent(key, ignored -> new TreeMap<>());
      values.merge(
          value,
          fields[0].equals("INSERT") || fields[0].equals("UPDATE_AFTER") ? 1L : -1L,
          Long::sum);
      assertTrue(values.get(value) >= 0, row);
      values.values().removeIf(count -> count == 0);
    }
    var result = new HashMap<String, Long>();
    for (var group : groups.entrySet()) {
      long remaining = 3;
      for (var value : group.getValue().entrySet()) {
        long count = Math.min(remaining, value.getValue());
        if (count > 0) result.put(group.getKey() + "|" + value.getKey(), count);
        remaining -= count;
      }
    }
    return result;
  }
}
