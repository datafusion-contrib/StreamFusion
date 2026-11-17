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
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import tech.streamfusion.operator.CanonicalRowKeyFixture;

class CanonicalDedupRowKeyJobTest {
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    return CanonicalTestMatrix.select(
        Stream.of(
                "proc-first",
                "proc-last",
                "row-first",
                "row-last",
                "mini-proc-last",
                "mini-row-first")
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
  @CanonicalCases(
      value = "cases",
      recoveryAxes = {2, 4, 5})
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void actualRowKeysKeepSourceOffsetsStateAndCommittedEffects(
      String variant, boolean nested, String mode, boolean ordinary, boolean first, String backend)
      throws Exception {
    var baseline = prepare("baseline", variant, nested, mode, ordinary);
    var switched = prepare("switched", variant, nested, mode, ordinary);
    new CanonicalJobRunner(directory)
        .verifyJoinSwitch(baseline, switched, first, backend, mode, "Deduplicate");
    var output = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    for (var root : List.of(baseline, switched)) {
      var actual = new HashMap<String, Long>();
      for (String line : CanonicalFixtureFactory.committedRows(root.resolve("committed"), output)) {
        String[] fields = line.split("\\|", -1);
        actual.merge(
            fields[1] + "|" + fields[2] + "|" + fields[3],
            fields[0].equals("+I") || fields[0].equals("+U") ? 1L : -1L,
            Long::sum);
      }
      actual.values().removeIf(count -> count == 0);
      assertEquals(expected(root, variant), actual);
    }
  }

  private Path prepare(String name, String variant, boolean nested, String mode, boolean ordinary)
      throws Exception {
    var root = directory.resolve(name);
    Files.createDirectories(root);
    if (ordinary) Files.createFile(root.resolve("ordinary-sql"));
    Files.createFile(root.resolve("insert-only"));
    Files.createFile(root.resolve("wait-native-input"));
    Files.writeString(root.resolve("native-ready-families"), "Deduplicate");
    Files.writeString(
        root.resolve("native-operator"), "NativeDeduplicate,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "Deduplicate");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-deduplicate");
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k "
            + CanonicalRowKeyFixture.sql(nested)
            + ",v BIGINT,rt AS TO_TIMESTAMP_LTZ(v,3),WATERMARK FOR rt AS rt - INTERVAL '1' HOUR");
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k.i,v,COALESCE(k.i,-1) FROM (SELECT *,ROW_NUMBER() OVER"
            + " (PARTITION BY k ORDER BY "
            + (variant.contains("proc") ? "pt" : "rt")
            + (variant.endsWith("first") ? " ASC" : " DESC")
            + ") AS rn FROM input_table) WHERE rn=1");
    if (variant.startsWith("mini")) {
      Files.writeString(root.resolve("one-phase-mini-batch"), "3");
      Files.writeString(root.resolve("mini-batch-latency"), "1000000 d");
    }
    Files.writeString(root.resolve("state-ttl"), "1 h");
    Files.writeString(root.resolve("uid-generation"), "DISABLED");
    var reference = new ArrayList<String>();
    if (mode.equals("rescale")) {
      Files.createFile(root.resolve("disable-chaining"));
      for (int round = 0; round < 6; round++)
        for (int key = 0; key < 64; key++)
          reference.add(key + "," + ((round % 2 == 0 ? round + 1 : 1) * 1000 + key));
    } else
      reference.addAll(
          List.of(
              "1,10", "2,20", "null,30", "1,15", "2,18", "3,25", "null,35", "1,15", "3,40", "2,22",
              "1,9", "null,28"));
    var encoded = new ArrayList<String>();
    var serializer =
        new RowDataSerializer(RowType.of(CanonicalRowKeyFixture.type(nested), new BigIntType()));
    for (String line : reference) {
      String[] fields = line.split(",");
      Long id = fields[0].equals("null") ? null : Long.valueOf(fields[0]);
      var key =
          CanonicalRowKeyFixture.key(
              nested, id, id == null ? RowKind.INSERT : RowKind.values()[(int) (id % 4)], true);
      var row = GenericRowData.of(key, Long.valueOf(fields[1]));
      var bytes = new DataOutputSerializer(256);
      serializer.serialize(row, bytes);
      encoded.add("BINARY," + Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer()));
    }
    Files.write(root.resolve("input.csv"), encoded);
    Files.write(root.resolve("reference.csv"), reference);
    return root;
  }

  private static Map<String, Long> expected(Path root, String variant) throws Exception {
    var selected = new HashMap<String, Long>();
    for (String line : Files.readAllLines(root.resolve("reference.csv"))) {
      String[] fields = line.split(",");
      long value = Long.parseLong(fields[1]);
      selected.compute(
          fields[0],
          (key, old) ->
              old == null
                  ? value
                  : variant.contains("proc")
                      ? variant.endsWith("first") ? old : value
                      : variant.endsWith("first") ? Math.min(old, value) : Math.max(old, value));
    }
    var result = new HashMap<String, Long>();
    selected.forEach(
        (key, value) ->
            result.put(key + "|" + value + "|" + (key.equals("null") ? "-1" : key), 1L));
    return result;
  }
}
