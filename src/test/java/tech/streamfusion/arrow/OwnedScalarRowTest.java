package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class OwnedScalarRowTest {
  @Test
  void representedScalarRowsOwnTheirValuesBeforeTheReaderMovesOrTheBatchCloses() {
    var fields = RowType.of(new VarCharType(32768), new VarBinaryType(32768),
        new DecimalType(38, 9), new TimestampType(9));
    var schema = RowType.of(fields);
    byte[] bytes = new byte[16384];
    for (int i = 0; i < bytes.length; i++) bytes[i] = (byte) (i * 31);
    byte[] expectedBytes = bytes.clone();
    var decimal = DecimalData.fromBigDecimal(new BigDecimal("-98765432101234567890.000000001"), 38, 9);
    var timestamp = TimestampData.fromEpochMillis(Long.MIN_VALUE, 999999);
    RowData first, second;
    try (var allocator = new RootAllocator();
        var root = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(schema, true), allocator)) {
      var writer = ArrowConversion.createRowDataArrowWriter(root, schema, 1);
      var input = GenericRowData.of(StringData.fromString("\ud800😀\0"), bytes, decimal, timestamp);
      input.setRowKind(RowKind.DELETE);
      writer.write(GenericRowData.of(input));
      Arrays.fill(bytes, (byte) -1);
      writer.write(GenericRowData.of(GenericRowData.of(StringData.fromString("next"), new byte[0], null, null)));
      writer.finish();
      var reader = ArrowConversion.createArrowReader(root, schema);
      first = reader.read(0).getRow(0, 4);
      second = reader.read(1).getRow(0, 4);
    }
    assertEquals(RowKind.DELETE, first.getRowKind());
    assertEquals("\ud800😀\0", first.getString(0).toString());
    assertArrayEquals(expectedBytes, first.getBinary(1));
    assertEquals(decimal, first.getDecimal(2, 38, 9));
    assertEquals(timestamp, first.getTimestamp(3, 9));
    assertEquals(RowKind.INSERT, second.getRowKind());
    assertEquals("next", second.getString(0).toString());
    assertArrayEquals(new byte[0], second.getBinary(1));
    assertTrue(second.isNullAt(2));
    assertTrue(second.isNullAt(3));
  }
}
