package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.Native;

class SerializedRowKeySelectorTest {
  @Test
  void wideNullableDecimalKeysMatchExchangeHashesAndOwnTheirStorage() {
    var type = RowType.of(new IntType(), new DecimalType(38, 18));
    List<RowData> rows = new ArrayList<>();
    for (String number :
        new String[] {
          null,
          "0",
          "-0.000000000000000001",
          "99999999999999999999.999999999999999999",
          "-99999999999999999999.999999999999999999",
          "1"
        }) {
      for (RowKind kind : RowKind.values()) {
        var row =
            GenericRowData.of(
                7,
                number == null ? null : DecimalData.fromBigDecimal(new BigDecimal(number), 38, 18));
        row.setRowKind(kind);
        rows.add(row);
      }
    }
    for (int[] fields : new int[][] {{1}, {1, 0}, {1, 1}}) {
      var selector = new SerializedRowKeySelector(type, fields);
      var retained = new ArrayList<RowData>();
      for (RowData row : rows) {
        RowKind original = row.getRowKind();
        retained.add(selector.getKey(row));
        assertEquals(original, row.getRowKind());
        assertEquals(RowKind.INSERT, retained.get(retained.size() - 1).getRowKind());
      }
      try (var allocator = new RootAllocator();
          var root = RowDataArrowConverter.write(rows, type, allocator);
          var array = ArrowArray.allocateNew(allocator);
          var schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
        int[] expected =
            Native.flinkBinaryRowHashes(
                array.memoryAddress(),
                schema.memoryAddress(),
                fields,
                java.util.stream.IntStream.of(fields).map(i -> -1).toArray());
        assertArrayEquals(expected, retained.stream().mapToInt(Object::hashCode).toArray());
        selector.getKey(GenericRowData.of(9, DecimalData.fromBigDecimal(BigDecimal.TEN, 38, 18)));
        assertArrayEquals(expected, retained.stream().mapToInt(Object::hashCode).toArray());
      }
    }
  }
}
