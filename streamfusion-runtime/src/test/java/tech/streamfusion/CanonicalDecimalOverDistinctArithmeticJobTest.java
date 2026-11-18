package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;

class CanonicalDecimalOverDistinctArithmeticJobTest
    extends CanonicalUnboundedOverNullableDistinctArithmeticJobTest {
  @Override
  String argumentType() {
    return "DECIMAL(38,7)";
  }

  @Override
  RowType outputType() {
    return RowType.of(new BigIntType(), new DecimalType(38, 7), new DecimalType(38, 7));
  }

  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, kind, mode, ordinary);
    Files.writeString(
        root.resolve("output-columns.sql"), "k BIGINT,s DECIMAL(38,7),a DECIMAL(38,7)");
    var serializer =
        new RowDataSerializer(
            RowType.of(new BigIntType(), new DecimalType(38, 7), new TimestampType(3)));
    var encoded = new ArrayList<String>();
    for (String line : Files.readAllLines(root.resolve("input.csv"))) {
      var cells = line.split(",", -1);
      var row =
          GenericRowData.ofKind(
              RowKind.valueOf(cells[0]),
              cells[1].equals("null") ? null : Long.valueOf(cells[1]),
              cells[2].equals("null")
                  ? null
                  : DecimalData.fromBigDecimal(new java.math.BigDecimal(cells[2]), 38, 7),
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
      return Map.of(
          "1", "37.0000000|9.2500000", "2", "13.0000000|6.5000000", "null", "8.0000000|4.0000000");
    var expected = new HashMap<String, String>();
    for (int key = 0; key < 64; key++)
      expected.put(key == 0 ? "null" : Integer.toString(key), "11.0000000|5.5000000");
    return expected;
  }
}
