package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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

class CanonicalBareUpdateDedupJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            mini ->
                Stream.of(false, true)
                    .flatMap(
                        ttl ->
                            Stream.of(false, true)
                                .flatMap(
                                    first ->
                                        Stream.of("hashmap", "rocksdb")
                                            .map(store -> Arguments.of(mini, ttl, first, store)))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void upsertEffectsContinueWithoutInventedRetainedRows(
      boolean mini, boolean ttl, boolean nativeFirst, String backend) throws Exception {
    var runner = new CanonicalJobRunner(directory);
    var fixture = runner.fixtureJar();
    var baseline = prepare("baseline", mini, ttl);
    var switched = prepare("switched", mini, ttl);
    runner.run(fixture, false, baseline, Integer.MAX_VALUE, "none", "baseline", 3, "hashmap");
    runner.run(
        fixture, nativeFirst, switched, 4, "none", "prefix", 3, nativeFirst ? backend : "hashmap");
    runner.run(
        fixture,
        !nativeFirst,
        switched,
        8,
        "prefix",
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
    var actual = CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type);
    var stock = CanonicalFixtureFactory.committedRows(baseline.resolve("committed"), type);
    assertTrue(actual.stream().allMatch(row -> row.startsWith("+U|")), actual.toString());
    assertEquals(Map.of("1", "1|9|1", "2", "2|22|2", "3", "3|40|3"), upserts(actual));
    assertEquals(upserts(stock), upserts(actual));
    if (!mini) {
      var expected = new HashMap<String, Long>();
      for (String line : Files.readAllLines(switched.resolve("input.csv"))) {
        var fields = line.split(",");
        expected.merge("+U|" + fields[1] + "|" + fields[2] + "|" + fields[1], 1L, Long::sum);
      }
      assertEquals(expected, bag(actual));
      assertEquals(bag(stock), bag(actual));
    } else
      assertTrue(
          Files.exists(
              switched.resolve(
                  (nativeFirst ? "prefix" : "middle") + "-checkpoint-flush-verified")));
    for (String stage : List.of("prefix", "middle")) {
      var savepoint =
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

  private Path prepare(String name, boolean mini, boolean ttl) throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    if (ordinary()) Files.createFile(root.resolve("ordinary-sql"));
    Files.createFile(root.resolve("insert-only"));
    Files.createFile(root.resolve("upsert-sink"));
    Files.createFile(root.resolve("wait-native-input"));
    Files.writeString(root.resolve("dedup-insert-sensitive"), "false");
    Files.writeString(root.resolve("input-columns.sql"), "k BIGINT NOT NULL,v BIGINT");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    if (ttl) Files.writeString(root.resolve("state-ttl"), "1 h");
    if (mini) {
      Files.writeString(root.resolve("one-phase-mini-batch"), "3");
      Files.writeString(root.resolve("mini-batch-latency"), "1000000 d");
      Files.writeString(root.resolve("require-checkpoint-flush-families"), "Deduplicate");
    }
    Files.write(
        root.resolve("input.csv"),
        List.of(
            "INSERT,1,10",
            "INSERT,2,20",
            "INSERT,1,10",
            "INSERT,1,15",
            "INSERT,2,18",
            "INSERT,3,25",
            "INSERT,2,18",
            "INSERT,1,15",
            "INSERT,3,40",
            "INSERT,2,22",
            "INSERT,1,9",
            "INSERT,3,40"));
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,v,k FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY k"
            + " ORDER BY pt DESC) AS rn FROM input_table) WHERE rn=1");
    Files.writeString(
        root.resolve("native-operator"), "NativeDeduplicate,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "Deduplicate");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-deduplicate");
    return root;
  }

  private static Map<String, String> upserts(List<String> rows) {
    var result = new HashMap<String, String>();
    for (var row : rows) {
      assertTrue(row.startsWith("+U|"));
      result.put(row.split("\\|")[1], row.substring(3));
    }
    return result;
  }

  private static Map<String, Long> bag(List<String> rows) {
    var result = new HashMap<String, Long>();
    rows.forEach(row -> result.merge(row, 1L, Long::sum));
    return result;
  }
}
