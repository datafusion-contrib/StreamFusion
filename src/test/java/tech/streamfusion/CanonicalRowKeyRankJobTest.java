package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.stream.Stream;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.provider.Arguments;
import tech.streamfusion.operator.CanonicalRowKeyFixture;

class CanonicalRowKeyRankJobTest extends CanonicalScalarRankJobTest {
  static Stream<Arguments> cases() {
    return CanonicalTestMatrix.select(
        Stream.of("append", "update", "retract")
            .flatMap(
                algorithm ->
                    CanonicalGroupArithmeticJobTest.cases()
                        .map(
                            args ->
                                Arguments.of(
                                    algorithm,
                                    "BIGINT",
                                    args.get()[0],
                                    args.get()[1],
                                    args.get()[2],
                                    args.get()[3]))));
  }

  @Override
  Path oracleInput(Path root) {
    return root.resolve("oracle-input.csv");
  }

  @Override
  Path prepare(String name, String algorithm, String type, String mode, boolean ordinary)
      throws Exception {
    var root = super.prepare(name, algorithm, type, mode, ordinary);
    Files.copy(root.resolve("input.csv"), oracleInput(root));
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k " + CanonicalRowKeyFixture.sql(true) + ",v BIGINT,id BIGINT,p BIGINT");
    var serializer =
        new RowDataSerializer(
            RowType.of(
                CanonicalRowKeyFixture.type(true),
                new BigIntType(),
                new BigIntType(),
                new BigIntType()));
    var encoded = new ArrayList<String>();
    for (String line : Files.readAllLines(oracleInput(root))) {
      var cells = line.split(",", -1);
      Long key = cells[1].equals("null") ? null : Long.valueOf(cells[1]);
      var row =
          GenericRowData.ofKind(
              RowKind.valueOf(cells[0]),
              CanonicalRowKeyFixture.key(
                  true,
                  key,
                  key == null
                      ? RowKind.INSERT
                      : RowKind.fromByteValue((byte) Math.floorMod(key, 4)),
                  true),
              Long.valueOf(cells[2]),
              Long.valueOf(cells[3]),
              Long.valueOf(cells[4]));
      var buffer = new DataOutputSerializer(256);
      serializer.serialize(row, buffer);
      encoded.add("BINARY," + Base64.getEncoder().encodeToString(buffer.getCopyOfBuffer()));
    }
    Files.write(root.resolve("input.csv"), encoded);
    Files.writeString(
        root.resolve("query.sql"),
        Files.readString(root.resolve("query.sql")).replace("SELECT k,id,rn", "SELECT k.i,id,rn"));
    return root;
  }
}
