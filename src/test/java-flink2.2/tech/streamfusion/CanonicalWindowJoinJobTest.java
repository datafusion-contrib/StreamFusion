package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.*;
import java.util.*;
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

class CanonicalWindowJoinJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String kind : List.of("INNER", "LEFT", "RIGHT", "FULL"))
      for (String shape : List.of("TUMBLE", "HOP", "CUMULATE"))
        for (boolean keyless : new boolean[] {false, true}) {
          if (!kind.equals("INNER") && (!shape.equals("TUMBLE") || keyless)) continue;
          for (boolean nativeFirst : new boolean[] {false, true})
            for (String backend : List.of("hashmap", "rocksdb"))
              cases.add(Arguments.of(kind, shape, keyless, nativeFirst, backend));
        }
    return Boolean.getBoolean("streamfusion.test.windowJoinJobSmoke")
        ? cases.stream()
            .filter(
                test ->
                    test.get()[0].equals("INNER")
                        && test.get()[1].equals("TUMBLE")
                        && (boolean) test.get()[3])
        : cases.stream();
  }

  static Stream<Arguments> handoffCases() {
    return Stream.of(
        Arguments.of(true, "hashmap"),
        Arguments.of(true, "rocksdb"),
        Arguments.of(false, "hashmap"),
        Arguments.of(false, "rocksdb"));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void pendingWindowJoinsContinueAcrossCleanProcesses(
      String kind, String shape, boolean keyless, boolean nativeFirst, String backend)
      throws Exception {
    check(kind, shape, keyless, nativeFirst, backend, "continuation");
  }

  @ParameterizedTest
  @MethodSource("handoffCases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void pendingWindowJoinsReexportWithoutNewInput(boolean nativeFirst, String backend)
      throws Exception {
    check("FULL", "TUMBLE", false, nativeFirst, backend, "no-input");
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"hashmap", "rocksdb"})
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void nativeCheckpointsResumeBeforeCanonicalHandoff(String backend) throws Exception {
    check("INNER", "HOP", false, true, backend, "checkpoint");
  }

  @ParameterizedTest
  @MethodSource("handoffCases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void actualWindowJoinKeysRescaleThroughOneThreeAndTwoTasks(boolean nativeFirst, String backend)
      throws Exception {
    check("FULL", "TUMBLE", false, nativeFirst, backend, "rescale");
  }

  private void check(
      String kind, String shape, boolean keyless, boolean nativeFirst, String backend, String mode)
      throws Exception {
    var runner = new CanonicalJobRunner(directory);
    var fixture = runner.fixtureJar();
    var baseline = prepare("baseline", kind, shape, keyless);
    if (mode.equals("rescale")) rescaleInput(baseline);
    runner.run(fixture, false, baseline, Integer.MAX_VALUE, "none", "baseline", 3, "hashmap");
    var switched = prepare("switched", kind, shape, keyless);
    if (mode.equals("rescale")) {
      rescaleInput(switched);
      Files.createFile(switched.resolve("rescale"));
      Files.writeString(switched.resolve("native-rescale-family"), "WindowJoin");
      Files.writeString(switched.resolve("prefix-parallelism"), "1");
      Files.writeString(switched.resolve("middle-parallelism"), "3");
      Files.writeString(switched.resolve("suffix-parallelism"), "2");
    }
    if (mode.equals("checkpoint")) Files.createFile(switched.resolve("prefix-native-checkpoint"));
    runner.run(
        fixture,
        nativeFirst,
        switched,
        mode.equals("rescale") ? 128 : 4,
        "none",
        "prefix",
        3,
        nativeFirst ? backend : "hashmap");
    String restore = "prefix";
    if (mode.equals("checkpoint")) {
      runner.run(fixture, true, switched, 4, restore, "canonical", 3, backend);
      restore = "canonical";
    } else if (mode.equals("no-input")) {
      runner.run(fixture, !nativeFirst, switched, 4, restore, "no-input", 3, backend);
      runner.run(fixture, nativeFirst, switched, 4, "no-input", "return", 3, backend);
      restore = "return";
    }
    runner.run(
        fixture,
        !nativeFirst,
        switched,
        mode.equals("rescale") ? 256 : 8,
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
    var expected =
        CanonicalFixtureFactory.committedRows(baseline.resolve("committed"), type).stream()
            .sorted()
            .toArray();
    assertTrue(expected.length > 0);
    assertArrayEquals(
        expected,
        CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type).stream()
            .sorted()
            .toArray());
    for (String stage : List.of("prefix", "middle")) {
      if (mode.equals("checkpoint") && stage.equals("prefix")) continue;
      var savepoint =
          Path.of(
              java.net.URI.create(Files.readString(switched.resolve(stage + "-savepoint")).trim()));
      var metadata =
          new String(
              Files.readAllBytes(savepoint.resolve("_metadata")),
              java.nio.charset.StandardCharsets.ISO_8859_1);
      assertFalse(metadata.contains("tech.streamfusion"));
    }
    assertTrue(
        Files.exists(
            switched.resolve((nativeFirst ? "middle" : "suffix") + "-stock-classpath-verified")));
  }

  private static void rescaleInput(Path root) throws Exception {
    var left = new ArrayList<String>();
    var right = new ArrayList<String>();
    for (int round = 0; round < 6; round++)
      for (int key = 0; key < 64; key++) {
        left.add("INSERT," + (-key - 1) + "," + ((round % 3) * 1000 + 100 + key));
        right.add("INSERT," + (-key - 1) + "," + ((round % 3) * 1000 + 110 + key));
      }
    Files.write(root.resolve("input.csv"), left);
    Files.write(root.resolve("right-input.csv"), right);
  }

  private Path prepare(String name, String kind, String shape, boolean keyless) throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    Files.write(
        root.resolve("input.csv"),
        List.of(
            "INSERT,1,110",
            "INSERT,1,110",
            "INSERT,2,130",
            "INSERT,null,140",
            "INSERT,3,1150",
            "INSERT,2,1250",
            "INSERT,9,2400",
            "INSERT,1,2500",
            "INSERT,2,3100",
            "INSERT,5,3200",
            "INSERT,null,3300",
            "INSERT,1,3400"));
    Files.write(
        root.resolve("right-input.csv"),
        List.of(
            "INSERT,1,210",
            "INSERT,1,210",
            "INSERT,4,230",
            "INSERT,null,240",
            "INSERT,3,1250",
            "INSERT,2,1300",
            "INSERT,9,2450",
            "INSERT,7,2600",
            "INSERT,2,3150",
            "INSERT,6,3200",
            "INSERT,null,3350",
            "INSERT,1,3450"));
    Files.createFile(root.resolve("insert-only"));
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT,v BIGINT,rt AS TO_TIMESTAMP_LTZ(v,3),WATERMARK FOR rt AS rt - INTERVAL '1' DAY");
    String window =
        shape.equals("TUMBLE") ? "INTERVAL '3' SECOND" : "INTERVAL '1' SECOND, INTERVAL '3' SECOND";
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT l.k,l.v,r.v FROM TABLE("
            + shape
            + "(TABLE input_table, DESCRIPTOR(rt), "
            + window
            + ")) l "
            + kind
            + " JOIN TABLE("
            + shape
            + "(TABLE right_input_table, DESCRIPTOR(rt), "
            + window
            + ")) r ON l.window_start=r.window_start AND l.window_end=r.window_end"
            + (keyless ? "" : " AND l.k=r.k"));
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-window-join");
    Files.writeString(
        root.resolve("native-operator"), "NativeWindowJoin,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "WindowJoin");
    Files.createFile(root.resolve("wait-native-input"));
    Files.writeString(root.resolve("time-zone"), "America/New_York");
    return root;
  }
}
