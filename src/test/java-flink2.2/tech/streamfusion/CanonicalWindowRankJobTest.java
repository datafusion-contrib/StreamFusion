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

class CanonicalWindowRankJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> continuedCases() {
    return cases(false);
  }

  static Stream<Arguments> recoveryCases() {
    return cases(true);
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
        .verifyJoinSwitch(baseline, switched, first, backend, mode, "WindowRank");
    var type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var rows = CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type);
    assertEquals(
        expected(mode),
        rows.stream().sorted().collect(java.util.stream.Collectors.toList()),
        "independent per-key window selection must agree with the stock baseline");
    if (ordinary) {
      for (String stage : List.of("prefix", "middle", "suffix"))
        assertTrue(Files.exists(switched.resolve(stage + "-ordinary-sql-verified")));
      assertFalse(Files.exists(switched.resolve("plan.json")));
    } else
      assertFalse(Files.readString(switched.resolve("plan.json")).contains("tech.streamfusion"));
  }

  boolean deduplicate() {
    return false;
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
        "INSERT INTO output_table SELECT k,v,t FROM (SELECT k,v,t,ROW_NUMBER() OVER (PARTITION BY"
            + " window_start,window_end,k ORDER BY "
            + (deduplicate() ? "rt ASC" : "v ASC NULLS LAST")
            + ") AS rn FROM TABLE(TUMBLE(TABLE input_table,DESCRIPTOR(rt),INTERVAL '3' SECOND)))"
            + " WHERE rn <= "
            + (deduplicate() ? "1" : "3"));
    Files.writeString(
        root.resolve("native-operator"),
        (ordinary ? "NativeWindowRankExecNode" : "NativeColumnarWindowRank")
            + ",RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "WindowRank");
    Files.writeString(
        root.resolve("required-plan-node"),
        deduplicate() ? "stream-exec-window-deduplicate" : "stream-exec-window-rank");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    return root;
  }

  private static List<String> input(String mode) {
    if (mode.equals("rescale")) {
      var rows = new ArrayList<String>();
      for (int round = 0; round < 6; round++)
        for (int key = 0; key < 64; key++) {
          rows.add(
              "INSERT,"
                  + (key == 0 ? "null" : key)
                  + ","
                  + (round % 2 == 0 ? 6 : 5)
                  + ","
                  + (round * 64 + key)
                  + ","
                  + (1000 + round));
        }
      rows.add("WATERMARK,2999");
      return rows;
    }
    return List.of(
        "INSERT,1,10,100,1000",
        "INSERT,1,10,101,1000",
        "INSERT,null,5,102,1001",
        "INSERT,2,8,103,1002",
        "INSERT,1,1,104,1003",
        "WATERMARK,2999",
        "INSERT,1,9,105,3100",
        "INSERT,2,4,106,3200",
        "INSERT,999,99,999,1004",
        "INSERT,1,0,107,3300",
        "INSERT,1,null,108,3301",
        "INSERT,null,2,109,3302",
        "INSERT,2,4,110,3200",
        "WATERMARK,5999");
  }

  private List<String> expected(String mode) {
    var windows = new java.util.HashMap<String, List<String[]>>();
    long watermark = Long.MIN_VALUE;
    for (String line : input(mode)) {
      var cells = line.split(",");
      if (cells[0].equals("WATERMARK")) {
        watermark = Long.parseLong(cells[1]);
        continue;
      }
      long time = Long.parseLong(cells[4]);
      long end = Math.floorDiv(time, 3000) * 3000 + 3000;
      if (end - 1 <= watermark) continue;
      windows.computeIfAbsent(cells[1] + ":" + end, ignored -> new ArrayList<>()).add(cells);
    }
    var result = new ArrayList<String>();
    for (var rows : windows.values()) {
      rows.sort(
          Comparator.comparing(
              (String[] cells) ->
                  deduplicate()
                      ? Long.valueOf(cells[4])
                      : cells[2].equals("null") ? null : Long.valueOf(cells[2]),
              Comparator.nullsLast(Comparator.naturalOrder())));
      for (int i = 0; i < Math.min(rows.size(), deduplicate() ? 1 : 3); i++) {
        var row = rows.get(i);
        result.add("+I|" + row[1] + "|" + row[2] + "|" + row[3]);
      }
    }
    result.sort(Comparator.naturalOrder());
    return result;
  }
}
