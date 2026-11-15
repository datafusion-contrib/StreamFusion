package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;

class CanonicalFloatingOverDistinctArithmeticJobTest
    extends CanonicalUnboundedOverNullableDistinctArithmeticJobTest {
  @Override
  String argumentType() {
    return "DOUBLE";
  }

  @Override
  RowType outputType() {
    return RowType.of(new BigIntType(), new DoubleType(), new DoubleType());
  }

  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, kind, mode, ordinary);
    Files.writeString(root.resolve("output-columns.sql"), "k BIGINT,s DOUBLE,a DOUBLE");
    var serializer =
        new RowDataSerializer(RowType.of(new BigIntType(), new DoubleType(), new TimestampType(3)));
    var encoded = new ArrayList<String>();
    for (String line : Files.readAllLines(root.resolve("input.csv"))) {
      var cells = line.split(",", -1);
      var row =
          GenericRowData.ofKind(
              RowKind.valueOf(cells[0]),
              cells[1].equals("null") ? null : Long.valueOf(cells[1]),
              cells[2].equals("null") ? null : Double.valueOf(cells[2]),
              TimestampData.fromEpochMillis(Long.parseLong(cells[3])));
      var buffer = new DataOutputSerializer(256);
      serializer.serialize(row, buffer);
      encoded.add("BINARY," + Base64.getEncoder().encodeToString(buffer.getCopyOfBuffer()));
    }
    Files.write(root.resolve("input.csv"), encoded);
    return root;
  }

  @Override
  Map<String, String> expectedTable(String kind, String mode) {
    if (!mode.equals("rescale"))
      return Map.of("1", "37.0|9.25", "2", "13.0|6.5", "null", "8.0|4.0");
    var expected = new HashMap<String, String>();
    for (int key = 0; key < 64; key++)
      expected.put(key == 0 ? "null" : Integer.toString(key), "11.0|5.5");
    return expected;
  }
}
