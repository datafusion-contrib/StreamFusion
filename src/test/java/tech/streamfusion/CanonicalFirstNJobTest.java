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
import org.junit.jupiter.params.provider.ValueSource;

class CanonicalFirstNJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> cases() {
    return Stream.of("rank", "hidden", "keyless", "ttl")
        .flatMap(mode -> handoffs().map(test -> Arguments.of(mode, test.get()[0], test.get()[1])))
        .filter(
            test ->
                !Boolean.getBoolean("streamfusion.test.firstNJobSmoke")
                    || test.get()[0].equals("rank") && (boolean) test.get()[1]);
  }

  static Stream<Arguments> handoffs() {
    return Stream.of(false, true)
        .flatMap(first -> Stream.of("hashmap", "rocksdb").map(store -> Arguments.of(first, store)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void countersContinueAcrossIsolatedEngineProcesses(
      String mode, boolean nativeFirst, String backend) throws Exception {
    check(mode, nativeFirst, backend);
  }

  @ParameterizedTest
  @MethodSource("handoffs")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void retainedCountersReexportWithoutNewInput(boolean nativeFirst, String backend)
      throws Exception {
    check("no-input", nativeFirst, backend);
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "rocksdb"})
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void nativeCheckpointRecoveryPrecedesTheCanonicalSwitch(String backend) throws Exception {
    check("checkpoint", true, backend);
  }

  @ParameterizedTest
  @MethodSource("handoffs")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void countersRescaleThroughOneThreeAndTwoTasks(boolean nativeFirst, String backend)
      throws Exception {
    check("rescale", nativeFirst, backend);
  }

  @ParameterizedTest
  @MethodSource("handoffs")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void emptyCountersAcceptTheFirstRowsAfterAnEngineSwitch(boolean nativeFirst, String backend)
      throws Exception {
    check("empty", nativeFirst, backend);
  }

  private void check(String mode, boolean nativeFirst, String backend) throws Exception {
    var runner = new CanonicalJobRunner(directory);
    var fixture = runner.fixtureJar();
    var baseline = prepare("baseline", mode);
    var switched = prepare("switched", mode);
    int prefix = mode.equals("empty") ? 0 : mode.equals("rescale") ? 128 : 5;
    int middle = mode.equals("rescale") ? 256 : 10;
    if (mode.equals("rescale")) {
      Files.createFile(switched.resolve("rescale"));
      Files.writeString(switched.resolve("native-rescale-family"), "FirstN");
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
    assertEquals(expected(switched, mode), actual);
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

  Path prepare(String name, String mode) throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    Files.createFile(root.resolve("insert-only"));
    Files.createFile(root.resolve("wait-native-input"));
    if (mode.equals("ttl")) Files.writeString(root.resolve("state-ttl"), "1 h");
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
    Files.write(root.resolve("input.csv"), input);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,v,"
            + (mode.equals("hidden") ? "CAST(0 AS BIGINT)" : "rn")
            + " FROM (SELECT k,v,ROW_NUMBER() OVER ("
            + (mode.equals("keyless") ? "" : "PARTITION BY k")
            + " ORDER BY pt) AS rn FROM input_table) WHERE rn<=3");
    Files.writeString(
        root.resolve("native-operator"), "NativeFirstN,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "FirstN");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-rank");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    return root;
  }

  private static Map<String, Long> expected(Path root, String mode) throws Exception {
    var counts = new HashMap<String, Integer>();
    var result = new HashMap<String, Long>();
    for (String line : Files.readAllLines(root.resolve("input.csv"))) {
      String[] fields = line.split(",");
      assertEquals("INSERT", fields[0]);
      int rank = counts.merge(mode.equals("keyless") ? "singleton" : fields[1], 1, Integer::sum);
      if (rank <= 3)
        result.merge(
            fields[1] + "|" + fields[2] + "|" + (mode.equals("hidden") ? 0 : rank), 1L, Long::sum);
    }
    return result;
  }

  private static Map<String, Long> bag(List<String> rows) {
    var result = new HashMap<String, Long>();
    for (String row : rows) {
      assertTrue(row.startsWith("+I|"), row);
      result.merge(row.substring(3), 1L, Long::sum);
    }
    return result;
  }
}
