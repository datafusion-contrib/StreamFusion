package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalCountPayloadGroupJobTest extends CanonicalGroupArithmeticJobTest {
  private String phase, shape;

  static Stream<Arguments> cases() {
    return CanonicalTestMatrix.select(
        Stream.of("eager", "mini", "global")
            .flatMap(
                phase ->
                    Stream.of("scalar-varbinary", "array", "row", "map")
                        .flatMap(
                            shape ->
                                CanonicalGroupArithmeticJobTest.cases()
                                    .map(
                                        args ->
                                            Arguments.of(
                                                phase + ":" + shape + ":" + args.get()[0],
                                                args.get()[1],
                                                args.get()[2],
                                                args.get()[3])))));
  }

  @Override
  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void generatedArithmeticStateContinuesAcrossIsolatedEngines(
      String spec, boolean ordinary, boolean first, String backend) throws Exception {
    var parts = spec.split(":");
    phase = parts[0];
    shape = parts[1];
    super.generatedArithmeticStateContinuesAcrossIsolatedEngines(
        parts[2], ordinary, first, backend);
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    var input =
        RowType.of(
            new BigIntType(), CanonicalMaterializerTypeFixture.type(shape), new BooleanType());
    var serializer = new RowDataSerializer(input);
    var encoded = new ArrayList<String>();
    for (String line : Files.readAllLines(root.resolve("input.csv"))) {
      var cells = line.split(",", -1);
      Long value = cells[2].equals("null") ? null : Long.valueOf(cells[2]);
      var row =
          GenericRowData.ofKind(
              RowKind.valueOf(cells[0]),
              cells[1].equals("null") ? null : Long.valueOf(cells[1]),
              CanonicalMaterializerTypeFixture.value(shape, value),
              value == null ? null : value % 2 == 0);
      var buffer = new DataOutputSerializer(256);
      serializer.serialize(row, buffer);
      encoded.add("BINARY," + Base64.getEncoder().encodeToString(buffer.getCopyOfBuffer()));
    }
    Files.write(root.resolve("input.csv"), encoded);
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT,v " + CanonicalMaterializerTypeFixture.sql(shape) + ",f BOOLEAN");
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,COUNT(v),COUNT(v) FILTER(WHERE f)"
            + " FROM input_table GROUP BY k");
    if (!phase.equals("eager")) {
      Files.createFile(root.resolve("same-cut-stock-reference"));
      Files.writeString(
          root.resolve(phase.equals("mini") ? "one-phase-mini-batch" : "two-phase-mini-batch"),
          "5");
      Files.writeString(root.resolve("mini-batch-latency"), "1 h");
    }
    if (phase.equals("global"))
      Files.writeString(root.resolve("required-plan-node"), "stream-exec-global-group-aggregate");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    if (mode.equals("rescale")) {
      var expected = new HashMap<String, String>();
      for (int key = 0; key < 64; key++)
        expected.put(key == 0 ? "null" : Integer.toString(key), "6|3");
      return expected;
    }
    return Map.of("null", "0|0", "2", shape.equals("map") ? "0|0" : "1|0", "3", "2|1");
  }
}
