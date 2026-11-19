package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalNestedRankJobTest {
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    return CanonicalTestMatrix.select(
        Stream.of("append-map", "append-row", "retract-row")
            .flatMap(
                shape ->
                    CanonicalGroupArithmeticJobTest.cases()
                        .map(
                            args ->
                                Arguments.of(
                                    shape,
                                    args.get()[0],
                                    args.get()[1],
                                    args.get()[2],
                                    args.get()[3]))));
  }

  @ParameterizedTest
  @CanonicalCases(
      value = "cases",
      recoveryAxes = {1, 3, 4})
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void nestedRetainedRowsContinueAcrossCanonicalAndNativeRecovery(
      String shape, String mode, boolean ordinary, boolean nativeFirst, String backend)
      throws Exception {
    var baseline = prepare("baseline", shape, mode, ordinary);
    var switched = prepare("switched", shape, mode, ordinary);
    new CanonicalJobRunner(directory)
        .verifyJoinSwitch(baseline, switched, nativeFirst, backend, mode, "TopN");
    var output = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    for (var root : List.of(baseline, switched)) {
      var actual = new HashMap<String, Long>();
      for (String line : CanonicalFixtureFactory.committedRows(root.resolve("committed"), output)) {
        String[] cells = line.split("\\|", -1);
        actual.merge(
            cells[1] + "|" + cells[2] + "|" + cells[3],
            cells[0].equals("+I") || cells[0].equals("+U") ? 1L : -1L,
            Long::sum);
      }
      actual.values().removeIf(count -> count == 0);
      assertEquals(expected(root, shape.endsWith("map")), actual);
    }
  }

  private Path prepare(String name, String shape, String mode, boolean ordinary) throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    boolean maps = shape.endsWith("map"), retract = shape.startsWith("retract");
    if (ordinary) Files.createFile(root.resolve("ordinary-sql"));
    if (!retract) Files.createFile(root.resolve("insert-only"));
    if (mode.equals("rescale")) Files.createFile(root.resolve("disable-chaining"));
    Files.writeString(root.resolve("nested-rank-checksum"), maps ? "map" : "row");
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT,v BIGINT,id BIGINT,p "
            + (maps
                ? CanonicalFixtureFactory.NestedJoinChecksum.TYPE
                : CanonicalFixtureFactory.NestedRowRankChecksum.TYPE));
    Files.writeString(
        root.resolve("native-operator"), "NativeColumnarTopN,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "TopN");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-rank");
    Files.writeString(root.resolve("required-rank-strategy"), retract ? "Retract" : "AppendFast");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,id,rank_checksum(p,rn) FROM (SELECT *,ROW_NUMBER() OVER"
            + " (PARTITION BY k ORDER BY v ASC,id ASC) AS rn FROM input_table) WHERE rn<=3");
    var payload =
        maps
            ? CanonicalNestedJoinFixture.PAYLOAD
            : RowType.of(
                new LogicalType[] {
                  CanonicalNestedJoinFixture.PAYLOAD.getTypeAt(0),
                  CanonicalNestedJoinFixture.PAYLOAD.getTypeAt(2)
                },
                new String[] {"a", "r"});
    var input = RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), payload);
    var serializer = new RowDataSerializer(input);
    var encoded = new ArrayList<String>();
    var reference = new ArrayList<String>();
    for (int round = 0; round < 6; round++)
      for (int key = 0; key < (mode.equals("rescale") ? 64 : 2); key++) {
        int member = round;
        int score = new int[] {20, 10, 10, 5, 30, 8}[round];
        RowKind kind = RowKind.INSERT;
        if (retract && round >= 3) {
          member = round == 3 ? 0 : 1;
          score = round == 3 ? 20 : round == 4 ? 10 : 5;
          kind =
              round == 3
                  ? RowKind.DELETE
                  : round == 4 ? RowKind.UPDATE_BEFORE : RowKind.UPDATE_AFTER;
        }
        long id = key * 10L + member;
        var nested = CanonicalNestedJoinFixture.payload(id);
        Object value =
            nested == null || maps
                ? nested
                : GenericRowData.of(nested.getArray(0), nested.getRow(2, 2));
        var row = GenericRowData.of(key == 0 ? null : (long) key, (long) score, id, value);
        row.setRowKind(kind);
        var bytes = new DataOutputSerializer(128);
        serializer.serialize(row, bytes);
        encoded.add("BINARY," + Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer()));
        reference.add(kind.name() + "," + (key == 0 ? "null" : key) + "," + score + "," + id);
      }
    Files.write(root.resolve("input.csv"), encoded);
    Files.write(root.resolve("reference.csv"), reference);
    return root;
  }

  private static Map<String, Long> expected(Path root, boolean maps) throws Exception {
    var counts = new HashMap<List<String>, Long>();
    for (String line : Files.readAllLines(root.resolve("reference.csv"))) {
      String[] fields = line.split(",");
      var row = List.of(fields[1], fields[2], fields[3]);
      counts.merge(
          row,
          fields[0].equals("INSERT") || fields[0].equals("UPDATE_AFTER") ? 1L : -1L,
          Long::sum);
    }
    var partitions = new HashMap<String, List<List<String>>>();
    counts.forEach(
        (row, count) -> {
          for (long copy = 0; copy < count; copy++)
            partitions.computeIfAbsent(row.get(0), ignored -> new ArrayList<>()).add(row);
        });
    var result = new HashMap<String, Long>();
    for (var rows : partitions.values()) {
      rows.sort(
          Comparator.<List<String>>comparingLong(row -> Long.parseLong(row.get(1)))
              .thenComparingLong(row -> Long.parseLong(row.get(2))));
      for (int rank = 1; rank <= Math.min(rows.size(), 3); rank++) {
        var row = rows.get(rank - 1);
        long id = Long.parseLong(row.get(2));
        long checksum = -7;
        if (id % 11 != 0) {
          checksum = ((17 * 37 + id) * 37 - 1) * 37 - id;
          if (maps) checksum += "λ".hashCode() * 19L + id + 7 + "empty".hashCode() * 19L - 3;
          checksum = checksum * 41 + id * 2 + ("nested-" + id).hashCode();
        }
        result.merge(row.get(0) + "|" + id + "|" + (checksum * 31 + rank), 1L, Long::sum);
      }
    }
    return result;
  }
}
