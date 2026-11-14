package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalScalarRankJobTest {
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    return CanonicalTestMatrix.select(
        Stream.of("append", "update", "retract")
            .flatMap(
                algorithm ->
                    Stream.of("DOUBLE", "DECIMAL(38,4)")
                        .filter(type -> !algorithm.equals("retract") || !type.equals("DOUBLE"))
                        .flatMap(
                            type ->
                                CanonicalGroupArithmeticJobTest.cases()
                                    .map(
                                        args ->
                                            Arguments.of(
                                                algorithm,
                                                type,
                                                args.get()[0],
                                                args.get()[1],
                                                args.get()[2],
                                                args.get()[3])))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void scalarSortAndPayloadStateContinuesAcrossBothEnginesAndRescaling(
      String algorithm,
      String type,
      String mode,
      boolean ordinary,
      boolean nativeFirst,
      String backend)
      throws Exception {
    var baseline = prepare("baseline", algorithm, type, mode, ordinary);
    var switched = prepare("switched", algorithm, type, mode, ordinary);
    new CanonicalJobRunner(directory)
        .verifyJoinSwitch(
            baseline,
            switched,
            nativeFirst,
            backend,
            mode,
            algorithm.equals("update") ? "GroupAgg,TopN" : "TopN");
    var output = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var actual = new HashMap<String, Long>();
    for (String row :
        CanonicalFixtureFactory.committedRows(switched.resolve("committed"), output)) {
      var cells = row.split("\\|", -1);
      long delta = cells[0].equals("+I") || cells[0].equals("+U") ? 1 : -1;
      actual.merge(cells[1] + "|" + cells[2] + "|" + cells[3], delta, Long::sum);
    }
    actual.values().removeIf(count -> count == 0);
    assertEquals(expected(switched, algorithm), actual);
    if (ordinary) {
      for (String stage : List.of("prefix", "middle", "suffix"))
        assertTrue(Files.exists(switched.resolve(stage + "-ordinary-sql-verified")));
      assertFalse(Files.exists(switched.resolve("plan.json")));
    } else
      assertFalse(Files.readString(switched.resolve("plan.json")).contains("tech.streamfusion"));
  }

  int firstRank() {
    return 1;
  }

  int rankEnd(String key) {
    return 3;
  }

  String rankPredicate() {
    return "rn<=3 AND rn>=" + firstRank();
  }

  String updatingInput() {
    return "(SELECT k,v,id,p,COUNT(*) AS c FROM input_table GROUP BY k,v,id,p)";
  }

  Path oracleInput(Path root) {
    return root.resolve("input.csv");
  }

  Path prepare(String name, String algorithm, String type, String mode, boolean ordinary)
      throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    if (ordinary) Files.createFile(root.resolve("ordinary-sql"));
    if (!algorithm.equals("retract")) Files.createFile(root.resolve("insert-only"));
    Files.writeString(root.resolve("input-columns.sql"), "k BIGINT,v BIGINT,id BIGINT,p BIGINT");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    Files.writeString(
        root.resolve("native-operator"), "NativeColumnarTopN,RowDataToArrow,ArrowToRowData");
    Files.writeString(
        root.resolve("native-input-families"),
        algorithm.equals("update") ? "GroupAgg,TopN" : "TopN");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-rank");
    Files.writeString(
        root.resolve("required-rank-strategy"),
        algorithm.equals("append")
            ? "AppendFast"
            : algorithm.equals("update") ? "UpdateFast" : "Retract");
    String source = algorithm.equals("update") ? updatingInput() : "input_table";
    String order =
        (algorithm.equals("update") ? "c DESC," : "")
            + "CAST(v AS "
            + type
            + ") ASC NULLS LAST,p DESC,id ASC"
            + (algorithm.equals("update") ? ",v ASC NULLS LAST" : "");
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,id,rn FROM (SELECT *,ROW_NUMBER() OVER"
            + " (PARTITION BY k ORDER BY "
            + order
            + ") AS rn FROM "
            + source
            + ") WHERE "
            + rankPredicate());
    var rows = new ArrayList<String>();
    int keys = mode.equals("rescale") ? 64 : 2;
    if (mode.equals("rescale")) Files.createFile(root.resolve("disable-chaining"));
    for (int round = 0; round < 6; round++) {
      for (int key = 0; key < keys; key++) {
        int member = round % 3;
        String change = "INSERT";
        int score = member == 1 ? 20 : 10;
        if (algorithm.equals("retract") && round >= 3) {
          member = round == 3 ? 0 : 1;
          score = member == 1 ? 20 : 10;
          change = round == 3 ? "DELETE" : round == 4 ? "UPDATE_BEFORE" : "UPDATE_AFTER";
          if (round == 5) score = 5;
        }
        int id = key * 10 + (algorithm.equals("append") ? round : member);
        int secondary = (member + key) % 5;
        rows.add(
            change + "," + (key == 0 ? "null" : key) + "," + score + "," + id + "," + secondary);
      }
    }
    Files.write(root.resolve("input.csv"), rows);
    return root;
  }

  private Map<String, Long> expected(Path root, String algorithm) throws Exception {
    var multiplicities = new HashMap<List<String>, Long>();
    for (String line : Files.readAllLines(oracleInput(root))) {
      var cells = line.split(",", -1);
      var row = List.of(cells[1], cells[2], cells[3], cells[4]);
      long delta = cells[0].equals("INSERT") || cells[0].equals("UPDATE_AFTER") ? 1 : -1;
      long count = multiplicities.merge(row, delta, Long::sum);
      assertTrue(count >= 0, line);
      if (count == 0) multiplicities.remove(row);
    }
    var groups = new HashMap<String, List<List<String>>>();
    multiplicities.forEach(
        (row, count) -> {
          int copies = algorithm.equals("update") ? 1 : Math.toIntExact(count);
          for (int copy = 0; copy < copies; copy++)
            groups.computeIfAbsent(row.get(0), ignored -> new ArrayList<>()).add(row);
        });
    Comparator<List<String>> order = Comparator.comparingLong(row -> Long.parseLong(row.get(1)));
    order =
        order
            .thenComparing(
                Comparator.<List<String>>comparingLong(row -> Long.parseLong(row.get(3)))
                    .reversed())
            .thenComparingLong(row -> Long.parseLong(row.get(2)));
    if (algorithm.equals("update"))
      order =
          Comparator.<List<String>>comparingLong(multiplicities::get)
              .reversed()
              .thenComparing(order);
    var result = new HashMap<String, Long>();
    for (var rows : groups.values()) {
      rows.sort(order);
      for (int index = firstRank() - 1;
          index < Math.min(rankEnd(rows.get(0).get(0)), rows.size());
          index++) {
        var row = rows.get(index);
        result.merge(row.get(0) + "|" + row.get(2) + "|" + (index + 1), 1L, Long::sum);
      }
    }
    return result;
  }
}
