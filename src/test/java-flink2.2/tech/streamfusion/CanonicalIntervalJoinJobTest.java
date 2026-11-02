package tech.streamfusion;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalIntervalJoinJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String kind : List.of("INNER", "LEFT", "RIGHT", "FULL"))
      for (boolean nativeFirst : new boolean[] {false, true})
        for (String backend : List.of("hashmap", "rocksdb"))
          for (boolean processingTime : new boolean[] {false, true})
            cases.add(Arguments.of(kind, nativeFirst, backend, processingTime));
    return Boolean.getBoolean("streamfusion.test.intervalJoinJobSmoke")
        ? cases.stream().filter(test -> test.get()[0].equals("INNER") && (boolean) test.get()[1])
        : cases.stream();
  }

  static Stream<Arguments> handoffs() {
    return Stream.of(false, true)
        .flatMap(
            processingTime ->
                Stream.of(
                    Arguments.of(true, "hashmap", processingTime),
                    Arguments.of(true, "rocksdb", processingTime),
                    Arguments.of(false, "hashmap", processingTime),
                    Arguments.of(false, "rocksdb", processingTime)));
  }

  static Stream<Arguments> checkpoints() {
    return Stream.of(false, true)
        .flatMap(
            processingTime ->
                Stream.of("hashmap", "rocksdb")
                    .map(backend -> Arguments.of(backend, processingTime)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void cachedRowsContinueAcrossCleanEngineProcesses(
      String kind, boolean nativeFirst, String backend, boolean processingTime) throws Exception {
    check(kind, nativeFirst, backend, "continuation", processingTime, false);
  }

  @ParameterizedTest
  @MethodSource("handoffs")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void cachesReexportWithoutNewInput(boolean nativeFirst, String backend, boolean processingTime)
      throws Exception {
    check("FULL", nativeFirst, backend, "no-input", processingTime, false);
  }

  @ParameterizedTest
  @MethodSource("checkpoints")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void nativeCheckpointsResumeBeforeCanonicalSwitch(String backend, boolean processingTime)
      throws Exception {
    check("INNER", true, backend, "checkpoint", processingTime, false);
  }

  @ParameterizedTest
  @MethodSource("handoffs")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void actualJoinKeysRescaleThroughOneThreeAndTwoTasks(
      boolean nativeFirst, String backend, boolean processingTime) throws Exception {
    check("FULL", nativeFirst, backend, "rescale", processingTime, false);
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void residualPredicatesRetainTheirLogicalInputPositions(
      String kind, boolean nativeFirst, String backend, boolean processingTime) throws Exception {
    check(kind, nativeFirst, backend, "continuation", processingTime, true);
  }

  private void check(
      String kind,
      boolean nativeFirst,
      String backend,
      String mode,
      boolean processingTime,
      boolean residual)
      throws Exception {
    var baseline = prepare("baseline", kind, processingTime, residual);
    var switched = prepare("switched", kind, processingTime, residual);
    if (mode.equals("rescale")) {
      rescaleInput(baseline);
      rescaleInput(switched);
    }
    new CanonicalJobRunner(directory)
        .verifyJoinSwitch(baseline, switched, nativeFirst, backend, mode, "IntervalJoin");
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

  private Path prepare(String name, String kind, boolean processingTime, boolean residual)
      throws Exception {
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
    String clock = processingTime ? "pt" : "rt";
    String range = processingTime ? "INTERVAL '1' DAY" : "INTERVAL '1' SECOND";
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT l.k,l.v,r.v FROM input_table l "
            + kind
            + " JOIN right_input_table r ON l.k=r.k AND l."
            + clock
            + " BETWEEN r."
            + clock
            + " - "
            + range
            + " AND r."
            + clock
            + " + "
            + range
            + (residual ? " AND l.v<r.v" : ""));
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-interval-join");
    Files.writeString(
        root.resolve("native-operator"), "NativeIntervalJoin,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "IntervalJoin");
    Files.createFile(root.resolve("wait-native-input"));
    Files.writeString(root.resolve("time-zone"), "America/New_York");
    return root;
  }
}
