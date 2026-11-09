package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;

class StockRowBatchTest {
  @Test
  void countAndByteLimitsPreserveOwnedRowsAndAllowOneOversizedValue() {
    var type = RowType.of(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH));
    var counts = new ArrayList<Integer>();
    var ids = new ArrayList<Long>();
    var lengths = new ArrayList<Integer>();
    var batch =
        new StockRowBatch(
            type,
            (a, s) -> {
              try (var array = ArrowArray.wrap(a);
                  var schema = ArrowSchema.wrap(s);
                  var root =
                      Data.importVectorSchemaRoot(
                          NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
                assertTrue(root.getRowCount() <= 1024);
                counts.add(root.getRowCount());
                var reader = ArrowConversion.createArrowReader(root, type);
                for (int i = 0; i < root.getRowCount(); i++) {
                  var row = reader.read(i);
                  ids.add(row.getLong(0));
                  lengths.add(row.isNullAt(1) ? -1 : row.getString(1).toBytes().length);
                }
              }
            });
    var row = new GenericRowData(2);
    for (long i = 0; i < 2051; i++) {
      row.setField(0, i);
      row.setField(1, null);
      batch.append(row);
      row.setField(0, -1L);
    }
    batch.flush();
    assertEquals(java.util.List.of(1024, 1024, 3), counts);
    counts.clear();
    for (long i = 2051; i < 2055; i++) {
      row.setField(0, i);
      row.setField(1, StringData.fromString("x".repeat(i == 2054 ? 5 * 1024 * 1024 : 1536 * 1024)));
      batch.append(row);
    }
    batch.flush();
    assertEquals(java.util.List.of(2, 1, 1), counts);
    for (int i = 0; i < ids.size(); i++) assertEquals((long) i, ids.get(i));
    assertEquals(-1, lengths.get(2050));
    assertEquals(1536 * 1024, lengths.get(2051));
    assertEquals(5 * 1024 * 1024, lengths.get(2054));
  }
}
