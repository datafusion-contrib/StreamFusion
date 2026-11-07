package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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

class CanonicalTemporalSortJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> continuedCases() {
    return cases(false).filter(args -> !(boolean) args.get()[1]);
  }

  static Stream<Arguments> recoveryCases() {
    return cases(true).filter(args -> !(boolean) args.get()[1]);
  }

  static Stream<Arguments> cases(boolean recovery) {
    return CanonicalGroupArithmeticJobTest.cases()
        .filter(args -> recovery != args.get()[0].equals("continue"));
  }

  @ParameterizedTest
  @MethodSource("continuedCases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void continuedJobs(String mode, boolean ordinary, boolean first, String backend)
      throws Exception {
    verify(mode, ordinary, first, backend);
  }

  @ParameterizedTest
  @MethodSource("recoveryCases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void recoveryJobs(String mode, boolean ordinary, boolean first, String backend) throws Exception {
    verify(mode, ordinary, first, backend);
  }

  private void verify(String mode, boolean ordinary, boolean first, String backend)
      throws Exception {
    var baseline = prepare("baseline", mode, ordinary);
    var switched = prepare("switched", mode, ordinary);
    new CanonicalJobRunner(directory)
        .verifyJoinSwitch(baseline, switched, first, backend, mode, "TemporalSort");
    var type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var rows = CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type);
    assertEquals(
        expected(mode),
        rows,
        "global timestamp order and equal-time arrival order survive handoff");
    assertEquals(CanonicalFixtureFactory.committedRows(baseline.resolve("committed"), type), rows);
    String maximum = Files.readString(baseline.resolve("baseline-singleton-verified"));
    try (var files = Files.list(switched)) {
      var proofs =
          files
              .filter(path -> path.getFileName().toString().endsWith("-singleton-verified"))
              .collect(java.util.stream.Collectors.toList());
      assertEquals(mode.equals("no-input") ? 5 : mode.equals("checkpoint") ? 4 : 3, proofs.size());
      for (var proof : proofs) assertEquals(maximum, Files.readString(proof));
    }
    if (ordinary) {
      for (String stage : List.of("prefix", "middle", "suffix"))
        assertTrue(Files.exists(switched.resolve(stage + "-ordinary-sql-verified")));
      assertFalse(Files.exists(switched.resolve("plan.json")));
    } else
      assertFalse(Files.readString(switched.resolve("plan.json")).contains("tech.streamfusion"));
  }

  private Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    if (ordinary) Files.createFile(root.resolve("ordinary-sql"));
    Files.createFile(root.resolve("insert-only"));
    Files.createFile(root.resolve("source-watermarks"));
    if (mode.equals("rescale")) Files.createFile(root.resolve("disable-chaining"));
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT,v BIGINT,t BIGINT,rt TIMESTAMP(3),WATERMARK FOR rt AS SOURCE_WATERMARK()");
    Files.write(root.resolve("input.csv"), input(mode));
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,v,t FROM input_table ORDER BY rt");
    Files.writeString(
        root.resolve("native-operator"), "NativeTemporalSort,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "TemporalSort");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-temporal-sort");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    Files.writeString(root.resolve("singleton-owner"), "TemporalSort");
    return root;
  }

  private static List<String> input(String mode) {
    if (mode.equals("rescale")) {
      var rows = new ArrayList<String>();
      for (int round = 0; round < 6; round++)
        for (int key = 0; key < 64; key++) {
          long time = 1000 + (5 - round) * 64 + key;
          rows.add(
              "INSERT,"
                  + (key == 0 ? "null" : key)
                  + ","
                  + (round % 2 == 0 ? 6 : 5)
                  + ","
                  + time
                  + ","
                  + time);
        }
      rows.add("WATERMARK,2000");
      return rows;
    }
    return List.of(
        "INSERT,1,null,1000,1000",
        "INSERT,2,11,1000,1000",
        "INSERT,null,5,1000,1000",
        "INSERT,1,7,1001,1001",
        "WATERMARK,1000",
        "INSERT,1,4,1002,1002",
        "INSERT,2,2,1003,1003",
        "WATERMARK,1002",
        "INSERT,2,null,1004,1004",
        "INSERT,1,17,1005,1005",
        "WATERMARK,1006",
        "INSERT,null,3,1007,1007",
        "WATERMARK,1010",
        "INSERT,999,99,1006,1006",
        "INSERT,998,88,1008,1008",
        "INSERT,1,null,1011,1011",
        "INSERT,1,9,1012,1012",
        "WATERMARK,1100");
  }

  private static List<String> expected(String mode) {
    var rows = new ArrayList<String>();
    for (String line : input(mode)) {
      var cells = line.split(",");
      if (cells[0].equals("INSERT") && !cells[1].equals("999"))
        rows.add("+I|" + cells[1] + "|" + cells[2] + "|" + cells[3]);
    }
    rows.sort(Comparator.comparingLong(row -> Long.parseLong(row.split("\\|", -1)[3])));
    assertEquals(mode.equals("rescale") ? 384 : 12, rows.size());
    return rows;
  }
}
