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

class CanonicalNormalizerJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> cases() {
    return Stream.of("immediate", "mini", "ttl")
        .flatMap(mode -> handoffs().map(test -> Arguments.of(mode, test.get()[0], test.get()[1])))
        .filter(
            test ->
                !Boolean.getBoolean("streamfusion.test.normalizerJobSmoke")
                    || test.get()[0].equals("mini") && (boolean) test.get()[1]);
  }

  static Stream<Arguments> handoffs() {
    return Stream.of(false, true)
        .flatMap(first -> Stream.of("hashmap", "rocksdb").map(store -> Arguments.of(first, store)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void normalizerAndJoinContinueAcrossIsolatedEngineProcesses(
      String mode, boolean nativeFirst, String backend) throws Exception {
    check(mode, nativeFirst, backend);
  }

  @ParameterizedTest
  @MethodSource("handoffs")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void retainedRowsReexportWithoutNewInput(boolean nativeFirst, String backend) throws Exception {
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
  void retainedRowsRescaleThroughOneThreeAndTwoTasks(boolean nativeFirst, String backend)
      throws Exception {
    check("rescale", nativeFirst, backend);
  }

  @ParameterizedTest
  @MethodSource("handoffs")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void emptyRowsAcceptTheFirstUpdatesAfterAnEngineSwitch(boolean nativeFirst, String backend)
      throws Exception {
    check("empty", nativeFirst, backend);
  }

  private void check(String mode, boolean nativeFirst, String backend) throws Exception {
    var runner = new CanonicalJobRunner(directory);
    var fixture = runner.fixtureJar();
    var baseline = prepare("baseline", mode);
    var switched = prepare("switched", mode);
    int prefix = mode.equals("empty") ? 0 : mode.equals("rescale") ? 64 : 5;
    int middle = mode.equals("rescale") ? 128 : 9;
    if (mode.equals("rescale")) {
      Files.createFile(switched.resolve("rescale"));
      Files.writeString(switched.resolve("native-rescale-family"), rescaleFamilies());
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
    if (mode.equals("mini"))
      assertTrue(
          Files.exists(
              switched.resolve(
                  (nativeFirst ? "prefix" : "middle") + "-checkpoint-flush-verified")));
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

  Path oracleInput(Path root) {
    return root.resolve("input.csv");
  }

  String rescaleFamilies() {
    return "Normalize,UpdatingJoin";
  }

  boolean ordinary() {
    return false;
  }

  Path prepare(String name, String mode) throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    if (ordinary()) Files.createFile(root.resolve("ordinary-sql"));
    Files.createFile(root.resolve("upsert-source"));
    Files.createFile(root.resolve("right_input_table-insert-only"));
    Files.writeString(
        root.resolve("input_table-columns.sql"),
        "k BIGINT NOT NULL,v BIGINT,PRIMARY KEY(k) NOT ENFORCED");
    Files.writeString(root.resolve("right_input_table-columns.sql"), "k BIGINT,v BIGINT");
    Files.createFile(root.resolve("wait-native-input"));
    Files.writeString(root.resolve("native-ready-families"), "Normalize");
    if (mode.equals("ttl")) Files.writeString(root.resolve("state-ttl"), "1 h");
    if (mode.equals("mini")) {
      Files.writeString(root.resolve("one-phase-mini-batch"), "3");
      Files.writeString(root.resolve("mini-batch-latency"), "1000000 d");
      Files.writeString(root.resolve("require-checkpoint-flush-families"), "Normalize");
    }
    var left =
        new ArrayList<>(
            List.of(
                "INSERT,1,10",
                "INSERT,2,20",
                "UPDATE_AFTER,1,10",
                "UPDATE_AFTER,1,15",
                "INSERT,3,null",
                "INSERT,4,40",
                "DELETE,2,null",
                "UPDATE_AFTER,3,30",
                "DELETE,4,null",
                "INSERT,2,22",
                "UPDATE_AFTER,1,17",
                "DELETE,3,null"));
    var right = new ArrayList<String>();
    long[] values = {10, 20, 15, 40, 30, 22, 17, 10, 20, 30, 40, 15};
    for (int i = 0; i < values.length; i++) right.add("INSERT," + values[i] + "," + (100 + i));
    if (mode.equals("rescale")) {
      Files.createFile(root.resolve("disable-chaining"));
      left.clear();
      right.clear();
      for (int round = 0; round < 6; round++)
        for (int key = 0; key < 64; key++) {
          String kind =
              round == 0 || round == 4 ? "INSERT" : round == 3 ? "DELETE" : "UPDATE_AFTER";
          String value =
              round == 3 ? "null" : Integer.toString((round == 2 ? 2 : round + 1) * 1000 + key);
          left.add(kind + "," + key + "," + value);
          right.add("INSERT," + ((round + 1) * 1000 + key) + "," + (round * 10000 + key));
        }
    }
    Files.write(root.resolve("input.csv"), left);
    Files.write(root.resolve("right-input.csv"), right);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT l.k,l.v,r.v FROM input_table l JOIN right_input_table r ON"
            + " l.v=r.k");
    Files.writeString(
        root.resolve("native-operator"),
        "NativeChangelogNormalize,NativeColumnarUpdatingJoin,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "Normalize,UpdatingJoin");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-changelog-normalize");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    return root;
  }

  Map<String, Long> expected(Path root, String mode) throws Exception {
    var left = new HashMap<String, String>();
    for (String line : Files.readAllLines(oracleInput(root))) {
      String[] fields = line.split(",");
      if (fields[0].equals("DELETE")) left.remove(fields[1]);
      else left.put(fields[1], fields[2]);
    }
    var result = new HashMap<String, Long>();
    for (String line : Files.readAllLines(root.resolve("right-input.csv"))) {
      String[] fields = line.split(",");
      assertEquals("INSERT", fields[0]);
      left.forEach(
          (key, value) -> {
            if (!value.equals("null") && value.equals(fields[1]))
              result.merge(key + "|" + value + "|" + fields[2], 1L, Long::sum);
          });
    }
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
