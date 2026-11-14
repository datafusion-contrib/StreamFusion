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
import org.apache.flink.types.RowKind;
import tech.streamfusion.operator.CanonicalRowKeyFixture;

class CanonicalUnboundedOverRowKeyJobTest extends CanonicalUnboundedOverJobTest {
  boolean stringExtrema() {
    return false;
  }

  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, kind, mode, ordinary);
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k "
            + CanonicalRowKeyFixture.sql(true)
            + ",v "
            + (stringExtrema() ? "STRING" : "BIGINT")
            + ",rt TIMESTAMP(3)");
    var serializer =
        new RowDataSerializer(
            RowType.of(
                CanonicalRowKeyFixture.type(true),
                stringExtrema()
                    ? new org.apache.flink.table.types.logical.VarCharType(100)
                    : new BigIntType(),
                new TimestampType(3)));
    var encoded = new ArrayList<String>();
    for (String line : Files.readAllLines(root.resolve("input.csv"))) {
      var cells = line.split(",", -1);
      Long id = cells[1].equals("null") ? null : Long.valueOf(cells[1]);
      var row =
          GenericRowData.of(
              CanonicalRowKeyFixture.key(
                  true,
                  id,
                  id == null ? RowKind.INSERT : RowKind.fromByteValue((byte) Math.floorMod(id, 4)),
                  true),
              cells[2].equals("null")
                  ? null
                  : stringExtrema()
                      ? org.apache.flink.table.data.StringData.fromString(cells[2])
                      : Long.valueOf(cells[2]),
              TimestampData.fromEpochMillis(Long.parseLong(cells[3])));
      var buffer = new DataOutputSerializer(256);
      serializer.serialize(row, buffer);
      encoded.add("BINARY," + Base64.getEncoder().encodeToString(buffer.getCopyOfBuffer()));
    }
    Files.write(root.resolve("input.csv"), encoded);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k.i,"
            + (stringExtrema()
                ? "CAST(MIN(v) OVER w AS BIGINT),CAST(MAX(v) OVER w AS BIGINT)"
                : "SUM(v) OVER w,AVG(v) OVER w")
            + " FROM input_table"
            + " WINDOW w AS (PARTITION BY k ORDER BY pt "
            + (kind.equals("rows") ? "ROWS" : "RANGE")
            + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
    return root;
  }
}
