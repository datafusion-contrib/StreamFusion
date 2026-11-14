package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.types.RowKind;
import tech.streamfusion.operator.CanonicalOverValueFixture;

abstract class CanonicalScalarOverDistinctCountJobTest
    extends CanonicalUnboundedOverNullableDistinctJobTest {
  @Override
  abstract String argumentType();

  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, kind, mode, ordinary);
    var valueType = LogicalTypeParser.parse(argumentType());
    var serializer =
        new RowDataSerializer(RowType.of(new BigIntType(), valueType, new TimestampType(3)));
    var encoded = new ArrayList<String>();
    for (String line : Files.readAllLines(root.resolve("input.csv"))) {
      var cells = line.split(",", -1);
      var row =
          GenericRowData.ofKind(
              RowKind.valueOf(cells[0]),
              cells[1].equals("null") ? null : Long.valueOf(cells[1]),
              CanonicalOverValueFixture.number(
                  valueType, cells[2].equals("null") ? null : Long.valueOf(cells[2])),
              TimestampData.fromEpochMillis(Long.parseLong(cells[3])));
      var buffer = new DataOutputSerializer(256);
      serializer.serialize(row, buffer);
      encoded.add("BINARY," + Base64.getEncoder().encodeToString(buffer.getCopyOfBuffer()));
    }
    Files.write(root.resolve("input.csv"), encoded);
    return root;
  }
}
