package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.*;
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
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.operator.CanonicalRowKeyFixture;

class CanonicalGroupRowKeyJobTest {
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    return CanonicalTestMatrix.select(
        Stream.of("eager", "mini", "global")
            .flatMap(
                variant ->
                    Stream.of(false, true)
                        .flatMap(
                            nested ->
                                CanonicalGroupArithmeticJobTest.cases()
                                    .map(
                                        args ->
                                            Arguments.of(
                                                variant,
                                                nested,
                                                args.get()[0],
                                                args.get()[1],
                                                args.get()[2],
                                                args.get()[3])))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void binaryGroupingKeysRetainAccumulatorsViewsAndCommittedEffects(
      String variant, boolean nested, String mode, boolean ordinary, boolean first, String backend)
      throws Exception {
    var baseline = prepare("baseline", variant, nested, mode, ordinary);
    var switched = prepare("switched", variant, nested, mode, ordinary);
    new CanonicalJobRunner(directory)
        .verifyJoinSwitch(baseline, switched, first, backend, mode, "GroupAgg");
    var output = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    for (var root : List.of(baseline, switched)) {
      var actual = new HashMap<String, String>();
      for (String line : CanonicalFixtureFactory.committedRows(root.resolve("committed"), output)) {
        var fields = line.split("\\|", -1);
        if (fields[0].equals("+I") || fields[0].equals("+U"))
          actual.put(fields[1], fields[2] + "|" + fields[3]);
        else actual.remove(fields[1]);
      }
      var expected = new HashMap<String, String>();
      int keys = mode.equals("rescale") ? 64 : 4;
      for (int key = 0; key < keys; key++)
        expected.put(
            key == 0 ? "null" : Integer.toString(key),
            variant.equals("global") ? mode.equals("rescale") ? "21|6" : "10|4" : "6|3");
      assertEquals(expected, actual);
    }
  }

  private Path prepare(String name, String variant, boolean nested, String mode, boolean ordinary)
      throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    if (ordinary) Files.createFile(root.resolve("ordinary-sql"));
    if (variant.equals("global")) Files.createFile(root.resolve("insert-only"));
    Files.createFile(root.resolve("wait-native-input"));
    Files.writeString(root.resolve("native-ready-families"), "GroupAgg");
    Files.writeString(
        root.resolve("native-operator"),
        (variant.equals("global") ? "NativeColumnarLocalGroupAgg," : "")
            + "NativeColumnarGroupAgg,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "GroupAgg");
    Files.writeString(
        root.resolve("required-plan-node"),
        variant.equals("global")
            ? "stream-exec-global-group-aggregate"
            : "stream-exec-group-aggregate");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    Files.writeString(
        root.resolve("input-columns.sql"), "k " + CanonicalRowKeyFixture.sql(nested) + ",v BIGINT");
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT q.group_key.i,q.s,q.n FROM (SELECT k AS group_key,SUM(v)"
            + " AS s,COUNT(DISTINCT v) AS n FROM input_table GROUP BY k) q");
    if (!variant.equals("eager")) {
      Files.writeString(
          root.resolve(variant.equals("global") ? "two-phase-mini-batch" : "one-phase-mini-batch"),
          "3");
      Files.writeString(root.resolve("mini-batch-latency"), "1000000 d");
      Files.createFile(root.resolve("same-cut-stock-reference"));
    }
    int keys = mode.equals("rescale") ? 64 : 4;
    if (mode.equals("rescale")) Files.createFile(root.resolve("disable-chaining"));
    var rows = new ArrayList<String>();
    var serializer =
        new RowDataSerializer(RowType.of(CanonicalRowKeyFixture.type(nested), new BigIntType()));
    int rounds = variant.equals("global") ? mode.equals("rescale") ? 6 : 4 : 5;
    for (int round = 0; round < rounds; round++)
      for (int key = 0; key < keys; key++) {
        Long id = key == 0 ? null : (long) key;
        var row =
            GenericRowData.of(
                CanonicalRowKeyFixture.key(nested, id, RowKind.values()[key % 4], true),
                !variant.equals("global") && round == 4 ? 4L : (long) round + 1);
        if (!variant.equals("global") && round == 4) row.setRowKind(RowKind.DELETE);
        var bytes = new DataOutputSerializer(256);
        serializer.serialize(row, bytes);
        rows.add("BINARY," + Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer()));
      }
    Files.write(root.resolve("input.csv"), rows);
    return root;
  }
}
