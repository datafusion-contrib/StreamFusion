package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.operator.CanonicalRowKeyFixture;

class CanonicalFirstNRowKeyJobTest extends CanonicalFirstNJobTest {
  @Override
  Path oracleInput(Path root) {
    return root.resolve("oracle-input.csv");
  }

  @Override
  Path prepare(String name, String mode) throws Exception {
    var root = super.prepare(name, mode);
    Files.copy(root.resolve("input.csv"), oracleInput(root));
    Files.writeString(
        root.resolve("input-columns.sql"), "k " + CanonicalRowKeyFixture.sql(true) + ",v BIGINT");
    var serializer =
        new RowDataSerializer(RowType.of(CanonicalRowKeyFixture.type(true), new BigIntType()));
    var encoded = new ArrayList<String>();
    for (String line : Files.readAllLines(oracleInput(root))) {
      var cells = line.split(",", -1);
      Long id = cells[1].equals("null") ? null : Long.valueOf(cells[1]);
      var row =
          GenericRowData.of(
              CanonicalRowKeyFixture.key(
                  true,
                  id,
                  id == null ? RowKind.INSERT : RowKind.fromByteValue((byte) Math.floorMod(id, 4)),
                  true),
              cells[2].equals("null") ? null : Long.valueOf(cells[2]));
      var buffer = new DataOutputSerializer(256);
      serializer.serialize(row, buffer);
      encoded.add("BINARY," + Base64.getEncoder().encodeToString(buffer.getCopyOfBuffer()));
    }
    Files.write(root.resolve("input.csv"), encoded);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k.i,v,"
            + (mode.equals("hidden") ? "CAST(0 AS BIGINT)" : "rn")
            + " FROM (SELECT k,v,ROW_NUMBER() OVER ("
            + (mode.equals("keyless") ? "" : "PARTITION BY k")
            + " ORDER BY pt) AS rn FROM input_table) WHERE rn<=3");
    return root;
  }
}
