package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

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

class CanonicalBoundedOverJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> continuedCases() {
    return cases(false);
  }

  static Stream<Arguments> recoveryCases() {
    return cases(true);
  }

  private static Stream<Arguments> cases(boolean recovery) {
    return Stream.of("processing-rows", "event-rows", "event-range")
        .flatMap(
            kind ->
                CanonicalGroupArithmeticJobTest.cases()
                    .filter(args -> recovery != args.get()[0].equals("continue"))
                    .map(
                        args ->
                            Arguments.of(
                                kind, args.get()[0], args.get()[1], args.get()[2], args.get()[3])));
  }

  @ParameterizedTest
  @MethodSource("continuedCases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void continuedJobs(String kind, String mode, boolean ordinary, boolean first, String backend)
      throws Exception {
    assertJob(kind, mode, ordinary, first, backend);
  }

  @ParameterizedTest
  @MethodSource("recoveryCases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void recoveryJobs(String kind, String mode, boolean ordinary, boolean first, String backend)
      throws Exception {
    assertJob(kind, mode, ordinary, first, backend);
  }

  private void assertJob(String kind, String mode, boolean ordinary, boolean first, String backend)
      throws Exception {
    var baseline = prepare("baseline", kind, mode, ordinary);
    var switched = prepare("switched", kind, mode, ordinary);
    new CanonicalJobRunner(directory)
        .verifyJoinSwitch(baseline, switched, first, backend, mode, "OverAgg");
    var type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var committed = CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type);
    assertEquals(mode.equals("rescale") ? 384 : 11, committed.size());
    var materialized = new HashMap<String, String>();
    for (String row : committed) {
      String[] cells = row.split("\\|", -1);
      assertEquals("+I", cells[0]);
      materialized.put(cells[1], cells[2] + "|" + cells[3]);
    }
    boolean range = kind.equals("event-range");
    Map<String, String> expected;
    if (mode.equals("rescale")) {
      expected = new HashMap<>();
      for (int key = 0; key < 64; key++)
        expected.put(key == 0 ? "null" : Integer.toString(key), range ? "33|5" : "16|5");
    } else expected = Map.of("1", range ? "37|9" : "26|13", "2", "13|6", "null", "8|4");
    assertEquals(expected, materialized);
    if (ordinary) {
      for (String stage : List.of("prefix", "middle", "suffix"))
        assertTrue(Files.exists(switched.resolve(stage + "-ordinary-sql-verified")));
      assertFalse(Files.exists(switched.resolve("plan.json")));
    } else
      assertFalse(Files.readString(switched.resolve("plan.json")).contains("tech.streamfusion"));
  }

  private Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    Path root = directory.resolve(name);
    Files.createDirectories(root);
    if (ordinary) Files.createFile(root.resolve("ordinary-sql"));
    Files.createFile(root.resolve("insert-only"));
    boolean processing = kind.equals("processing-rows"), range = kind.equals("event-range");
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT,v BIGINT,rt TIMESTAMP(3)"
            + (processing ? "" : ",WATERMARK FOR rt AS SOURCE_WATERMARK()"));
    if (!processing) Files.createFile(root.resolve("source-watermarks"));
    List<String> rows =
        new ArrayList<>(
            List.of(
                "INSERT,1,null,1000",
                "INSERT,2,11,1000",
                "INSERT,null,5,1000",
                "INSERT,1,7,1001",
                "WATERMARK,1000",
                "INSERT,1,4,1002",
                "INSERT,2,2,1003",
                "WATERMARK,1002",
                "INSERT,2,null,1004",
                "INSERT,1,17,1005",
                "WATERMARK,1006",
                "INSERT,null,3,1007",
                "WATERMARK,1010",
                "INSERT,1,null,1011",
                "INSERT,1,9,1012",
                "WATERMARK,1100"));
    if (processing) {
      // Keep the same first and second savepoint cuts while replacing watermark controls with rows.
      rows =
          new ArrayList<>(
              List.of(
                  "INSERT,1,null,1000",
                  "INSERT,2,11,1000",
                  "INSERT,null,5,1000",
                  "INSERT,1,7,1001",
                  "INSERT,1,4,1002",
                  "INSERT,2,2,1003",
                  "INSERT,2,null,1004",
                  "INSERT,1,17,1005",
                  "INSERT,null,3,1007",
                  "INSERT,1,null,1011",
                  "INSERT,1,9,1012"));
    }
    if (mode.equals("rescale")) {
      Files.createFile(root.resolve("disable-chaining"));
      rows.clear();
      for (int round = 0; round < 6; round++)
        for (int key = 0; key < 64; key++)
          rows.add(
              "INSERT,"
                  + (key == 0 ? "null" : key)
                  + ","
                  + (round % 2 == 0 ? 6 : 5)
                  + ","
                  + (1000 + round));
      if (!processing) rows.add("WATERMARK,1100");
    }
    Files.write(root.resolve("input.csv"), rows);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,SUM(v) OVER w,AVG(v) OVER w FROM input_table WINDOW w AS"
            + " (PARTITION BY k ORDER BY "
            + (processing ? "pt" : "rt")
            + (range ? " RANGE BETWEEN INTERVAL '0.032' SECOND" : " ROWS BETWEEN 2")
            + " PRECEDING AND CURRENT ROW)");
    Files.writeString(
        root.resolve("native-operator"), "NativeOverAgg,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "OverAgg");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-over-aggregate");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    return root;
  }
}
