package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

class CanonicalTemporalJoinJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String kind : List.of("INNER", "LEFT"))
      for (boolean nativeFirst : new boolean[] {false, true})
        for (String backend : List.of("hashmap", "rocksdb"))
          for (boolean residual : new boolean[] {false, true})
            cases.add(Arguments.of(kind, nativeFirst, backend, residual));
    return Boolean.getBoolean("streamfusion.test.temporalJoinJobSmoke")
        ? cases.stream()
            .filter(
                test ->
                    test.get()[0].equals("INNER")
                        && (boolean) test.get()[1]
                        && !(boolean) test.get()[3])
        : cases.stream();
  }

  static Stream<Arguments> handoffs() {
    return Stream.of(false, true)
        .flatMap(
            nativeFirst ->
                Stream.of("hashmap", "rocksdb").map(backend -> Arguments.of(nativeFirst, backend)));
  }

  static Stream<String> backends() {
    return Stream.of("hashmap", "rocksdb");
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void pendingProbesAndVersionsContinueAcrossCleanEngineProcesses(
      String kind, boolean nativeFirst, String backend, boolean residual) throws Exception {
    check(kind, nativeFirst, backend, "continuation", residual);
  }

  @ParameterizedTest
  @MethodSource("handoffs")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void pendingProbesAndVersionsReexportWithoutNewInput(boolean nativeFirst, String backend)
      throws Exception {
    check("LEFT", nativeFirst, backend, "no-input", true);
  }

  @ParameterizedTest
  @MethodSource("backends")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void nativeCheckpointResumesBeforeCanonicalSwitch(String backend) throws Exception {
    check("INNER", true, backend, "checkpoint", true);
  }

  @ParameterizedTest
  @MethodSource("handoffs")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void actualTemporalKeysRescaleThroughOneThreeAndTwoTasks(boolean nativeFirst, String backend)
      throws Exception {
    check("LEFT", nativeFirst, backend, "rescale", true);
  }

  private void check(
      String kind, boolean nativeFirst, String backend, String mode, boolean residual)
      throws Exception {
    var baseline = prepare("baseline", kind, residual);
    var switched = prepare("switched", kind, residual);
    if (mode.equals("rescale")) {
      rescaleInput(baseline);
      rescaleInput(switched);
    }
    new CanonicalJobRunner(directory)
        .verifyJoinSwitch(baseline, switched, nativeFirst, backend, mode, "TemporalJoin");
    var type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var actual = CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type);
    actual.sort(String::compareTo);
    assertEquals(expectedRows(switched, kind, residual), actual);
    try (var files = Files.list(switched)) {
      for (Path marker :
          files
              .filter(path -> path.toString().endsWith("-savepoint"))
              .collect(java.util.stream.Collectors.toList())) {
        if (mode.equals("checkpoint") && marker.getFileName().toString().equals("prefix-savepoint"))
          continue;
        Path savepoint = Path.of(java.net.URI.create(Files.readString(marker).trim()));
        String metadata =
            new String(
                Files.readAllBytes(savepoint.resolve("_metadata")), StandardCharsets.ISO_8859_1);
        assertFalse(metadata.contains("tech.streamfusion"), marker.toString());
      }
    }
  }

  static List<String> expectedRows(Path root, String kind, boolean residual) throws Exception {
    var versions = new HashMap<String, TreeMap<Long, Long>>();
    for (String line : Files.readAllLines(root.resolve("right-input.csv"))) {
      String[] row = line.split(",");
      long timestamp = Long.parseLong(row[2]);
      versions.computeIfAbsent(row[1], ignored -> new TreeMap<>()).put(timestamp, timestamp);
    }
    var expected = new ArrayList<String>();
    for (String line : Files.readAllLines(root.resolve("input.csv"))) {
      String[] row = line.split(",");
      var history = versions.get(row[1]);
      var version = history == null ? null : history.floorEntry(Long.parseLong(row[2]));
      Long value = version == null ? null : version.getValue();
      if (value != null && residual && Long.parseLong(row[2]) <= value) value = null;
      if (value != null || kind.equals("LEFT"))
        expected.add("+I|" + row[1] + "|" + row[2] + "|" + value);
    }
    expected.sort(String::compareTo);
    return expected;
  }

  void rescaleInput(Path root) throws Exception {
    var left = new ArrayList<String>();
    var right = new ArrayList<String>();
    for (int round = 0; round < 6; round++)
      for (int key = 0; key < 64; key++) {
        left.add("INSERT," + (-key - 1) + "," + (round * 1000 + 100 + key));
        right.add("INSERT," + (-key - 1) + "," + (round * 1000 + 90 + key));
      }
    Files.write(root.resolve("input.csv"), left);
    Files.write(root.resolve("right-input.csv"), right);
  }

  Path prepare(String name, String kind, boolean residual) throws Exception {
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
            "INSERT,1,100",
            "INSERT,1,110",
            "INSERT,4,120",
            "INSERT,2,125",
            "INSERT,3,1100",
            "INSERT,2,1250",
            "INSERT,9,2400",
            "INSERT,1,2600",
            "INSERT,2,3000",
            "INSERT,6,3200",
            "INSERT,1,3350",
            "INSERT,1,3450"));
    Files.createFile(root.resolve("insert-only"));
    String timeColumns =
        ",v BIGINT,rt AS TO_TIMESTAMP_LTZ(v,3),WATERMARK FOR rt AS rt - INTERVAL '1' DAY";
    Files.writeString(root.resolve("input-columns.sql"), "k BIGINT" + timeColumns);
    Files.writeString(
        root.resolve("right_input_table-columns.sql"),
        "k BIGINT NOT NULL" + timeColumns + ",PRIMARY KEY(k) NOT ENFORCED");
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT l.k,l.v,r.v FROM input_table l "
            + kind
            + " JOIN right_input_table FOR SYSTEM_TIME AS OF l.rt AS r ON l.k=r.k"
            + (residual ? " AND l.v>r.v" : ""));
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-temporal-join");
    Files.writeString(
        root.resolve("native-operator"), "NativeTemporalJoin,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "TemporalJoin");
    Files.createFile(root.resolve("wait-native-input"));
    Files.writeString(root.resolve("time-zone"), "America/New_York");
    return root;
  }
}
