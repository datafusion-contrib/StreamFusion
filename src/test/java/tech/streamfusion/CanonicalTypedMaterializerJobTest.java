package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

abstract class CanonicalTypedMaterializerJobTest {
  @TempDir Path directory;

  static Stream<Arguments> casesForShape(String shape, boolean inputKey) {
    return CanonicalTestMatrix.select(
        CanonicalSinkMaterializerJobTest.allCases()
            .filter(args -> (boolean) args.get()[1] == inputKey)
            .map(
                args ->
                    Arguments.of(
                        shape,
                        args.get()[0],
                        args.get()[1],
                        args.get()[2],
                        args.get()[3],
                        args.get()[4],
                        args.get()[5],
                        args.get()[6])));
  }

  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void typedCandidateRowsReachReleasedStockWritersAfterHandoff(
      String shape,
      String strategy,
      boolean inputKey,
      String uids,
      String mode,
      boolean ordinary,
      boolean first,
      String backend)
      throws Exception {
    var baseline = prepare("baseline", shape, strategy, inputKey, uids, mode, ordinary);
    var switched = prepare("switched", shape, strategy, inputKey, uids, mode, ordinary);
    new CanonicalJobRunner(directory)
        .verifyJoinSwitch(
            baseline,
            switched,
            first,
            backend,
            mode,
            "SinkMaterializer",
            root ->
                CanonicalFixtureFactory.committedRecords(root.resolve("committed")).stream()
                    .map(bytes -> Base64.getEncoder().encodeToString(bytes))
                    .collect(java.util.stream.Collectors.toList()));
    var type =
        RowType.of(
            CanonicalMaterializerTypeFixture.keyType(shape),
            CanonicalMaterializerTypeFixture.type(shape),
            new BigIntType());
    var serializer = new RowDataSerializer(type);
    for (var root : List.of(baseline, switched)) {
      var actual = new HashMap<Long, String>();
      for (byte[] bytes : CanonicalFixtureFactory.committedRecords(root.resolve("committed"))) {
        RowData row = serializer.deserialize(new DataInputDeserializer(bytes));
        long id = CanonicalMaterializerTypeFixture.keyId(shape, row);
        if (row.getRowKind() == RowKind.DELETE) actual.remove(id);
        else {
          row.setRowKind(RowKind.INSERT);
          actual.put(id, encode(serializer, row));
        }
      }
      var expected = new HashMap<Long, String>();
      for (long key = mode.equals("rescale") ? 0 : 1;
          key < (mode.equals("rescale") ? 64 : 2);
          key++) {
        long value = mode.equals("rescale") ? 30 : 10;
        expected.put(
            key,
            encode(
                serializer,
                GenericRowData.of(
                    CanonicalMaterializerTypeFixture.key(shape, key),
                    CanonicalMaterializerTypeFixture.value(shape, key == 2 ? null : value),
                    value * 10)));
      }
      assertEquals(expected, actual);
    }
  }

  private Path prepare(
      String name,
      String shape,
      String strategy,
      boolean inputKey,
      String uids,
      String mode,
      boolean ordinary)
      throws Exception {
    var root =
        CanonicalSinkMaterializerJobTest.prepareFixture(
            directory, name, strategy, inputKey, uids, mode, ordinary);
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k "
            + CanonicalMaterializerTypeFixture.keySql(shape)
            + " NOT NULL,v "
            + CanonicalMaterializerTypeFixture.sql(shape)
            + ",n BIGINT"
            + (inputKey ? ",PRIMARY KEY(k,n) NOT ENFORCED" : ""));
    Files.writeString(
        root.resolve("output-columns.sql"),
        "k "
            + CanonicalMaterializerTypeFixture.keySql(shape)
            + ",n "
            + CanonicalMaterializerTypeFixture.sql(shape)
            + ",total BIGINT,PRIMARY KEY(k) NOT ENFORCED");
    var input =
        RowType.of(
            CanonicalMaterializerTypeFixture.keyType(shape),
            CanonicalMaterializerTypeFixture.type(shape),
            new BigIntType());
    var serializer = new RowDataSerializer(input);
    var encoded = new ArrayList<String>();
    for (String line : Files.readAllLines(root.resolve("input.csv"))) {
      String[] fields = line.split(",");
      var row =
          GenericRowData.ofKind(
              RowKind.valueOf(fields[0]),
              CanonicalMaterializerTypeFixture.key(shape, Long.parseLong(fields[1])),
              CanonicalMaterializerTypeFixture.value(
                  shape, fields[1].equals("2") ? null : Long.valueOf(fields[2])),
              Long.valueOf(fields[3]));
      encoded.add("BINARY," + encode(serializer, row));
    }
    Files.write(root.resolve("input.csv"), encoded);
    return root;
  }

  private static String encode(RowDataSerializer serializer, RowData row) throws Exception {
    var bytes = new DataOutputSerializer(128);
    serializer.serialize(row, bytes);
    return Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer());
  }
}
