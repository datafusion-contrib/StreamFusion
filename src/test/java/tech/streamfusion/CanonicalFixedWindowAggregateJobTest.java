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

class CanonicalFixedWindowAggregateJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  static Stream<Arguments> continuedCases() {
    return cases(false);
  }

  static Stream<Arguments> recoveryCases() {
    return cases(true);
  }

  static Stream<Arguments> cases(boolean recovery) {
    if (recovery && Boolean.getBoolean("streamfusion.test.groupArithmeticJobSmoke"))
      return Stream.of(Arguments.of("no-input", false, true, "rocksdb"));
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
    var baseline = prepare("baseline", mode, ordinary, backend);
    var switched = prepare("switched", mode, ordinary, backend);
    new CanonicalJobRunner(directory)
        .verifyJoinSwitch(baseline, switched, first, backend, mode, family());
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

  boolean session() {
    return false;
  }

  boolean distinct() {
    return false;
  }

  boolean twoPhase() {
    return false;
  }

  boolean legacySession() {
    return session() && !tech.streamfusion.compat.CanonicalTestCompat.flink22();
  }

  String family() {
    return session() ? "SessionWindowAgg" : twoPhase() ? "GlobalWindowAgg" : "WindowAgg";
  }

  private Path prepare(String name, String mode, boolean ordinary, String backend)
      throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    if (ordinary) Files.createFile(root.resolve("ordinary-sql"));
    if (legacySession()) Files.createFile(root.resolve("same-cut-stock-reference"));
    Files.createFile(root.resolve("insert-only"));
    Files.createFile(root.resolve("source-watermarks"));
    if (mode.equals("rescale")) Files.createFile(root.resolve("disable-chaining"));
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT,v BIGINT,t BIGINT,rt TIMESTAMP(3),WATERMARK FOR rt AS SOURCE_WATERMARK()");
    Files.write(root.resolve("input.csv"), input(mode));
    if (twoPhase()) Files.createFile(root.resolve("window-two-phase"));
    String aggregates = "k,COUNT(" + (distinct() ? "DISTINCT v" : "v") + "),SUM(v)";
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT "
            + aggregates
            + (legacySession()
                ? " FROM input_table GROUP BY k,SESSION(rt,INTERVAL '1' SECOND)"
                : " FROM TABLE("
                    + (session()
                        ? "SESSION(TABLE input_table PARTITION BY k,DESCRIPTOR(rt),INTERVAL '1'"
                            + " SECOND)"
                        : "TUMBLE(TABLE input_table,DESCRIPTOR(rt),INTERVAL '3' SECOND)")
                    + ") GROUP BY k,window_start,window_end"));
    String expectedOperator =
        twoPhase()
            ? "NativeColumnarLocalWindowAggregate,NativeColumnarGlobalWindowAggregate"
            : session()
                ? (ordinary
                    ? "NativeColumnarSessionWindowAggExecNode"
                    : "NativeColumnarSessionWindowAggregate")
                : ordinary ? "NativeColumnarWindowAggExecNode" : "NativeColumnarWindowAggregate";
    Files.writeString(
        root.resolve("native-operator"), expectedOperator + ",RowDataToArrow,ArrowToRowData");
    Files.writeString(
        root.resolve("native-input-families"),
        twoPhase() ? "LocalWindowAgg,GlobalWindowAgg" : family());
    Files.writeString(
        root.resolve("required-plan-node"),
        twoPhase()
            ? "stream-exec-global-window-aggregate"
            : legacySession()
                ? "stream-exec-group-window-aggregate"
                : "stream-exec-window-aggregate");
    if (backend.equals("rocksdb") && distinct()) {
      int[] kinds = {7, 0}, types = {0, 0}, keys = {0};
      boolean direct =
          session()
              ? tech.streamfusion.Native.rocksdbSessionAggregatorSupported(types, kinds, keys)
              : tech.streamfusion.Native.rocksdbWindowAggregatorSupported(types, kinds, keys);
      assertTrue(direct, "Canonical DISTINCT windows must execute on native RocksDB");
    }
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
    var groups = new java.util.HashMap<String, List<Long>>();
    long watermark = Long.MIN_VALUE;
    int offset = 0;
    for (String line : input(mode)) {
      int firstCut = mode.equals("rescale") ? 128 : 4;
      int secondCut = mode.equals("rescale") ? 256 : 8;
      if (legacySession() && (offset == firstCut || offset == secondCut))
        watermark = Long.MIN_VALUE;
      offset++;
      var cells = line.split(",");
      if (cells[0].equals("WATERMARK")) {
        watermark = Long.parseLong(cells[1]);
        continue;
      }
      long time = Long.parseLong(cells[4]);
      long end = session() ? time + 1000 : Math.floorDiv(time, 3000) * 3000 + 3000;
      if (end - 1 <= watermark) continue;
      String bucket = cells[1] + ":" + (time < 3000 ? 0 : 1);
      groups
          .computeIfAbsent(bucket, ignored -> new ArrayList<>())
          .add(cells[2].equals("null") ? null : Long.valueOf(cells[2]));
    }
    var result = new ArrayList<String>();
    for (var group : groups.entrySet()) {
      var nonnull =
          group.getValue().stream()
              .filter(java.util.Objects::nonNull)
              .collect(java.util.stream.Collectors.toList());
      long count = distinct() ? nonnull.stream().distinct().count() : nonnull.size();
      String sum =
          nonnull.isEmpty()
              ? "null"
              : Long.toString(nonnull.stream().mapToLong(Long::longValue).sum());
      result.add("+I|" + group.getKey().split(":")[0] + "|" + count + "|" + sum);
    }
    result.sort(Comparator.naturalOrder());
    return result;
  }
}
