package tech.streamfusion.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.flink.sink.Conversions;
import io.delta.flink.sink.KernelBatchRowData;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class ArrowKernelRowsTest {

  @Test
  void oneRowViewReadsArrowDirectlyAndRetainsTheBatchUntilEveryRowCloses() {
    try (RootAllocator allocator = new RootAllocator();
        BigIntVector ids = new BigIntVector("id", allocator);
        VarCharVector names = new VarCharVector("name", allocator)) {
      ids.allocateNew(2);
      names.allocateNew();
      ids.setSafe(0, 10L);
      ids.setSafe(1, 20L);
      names.setSafe(0, "first".getBytes(StandardCharsets.UTF_8));
      names.setSafe(1, "second".getBytes(StandardCharsets.UTF_8));
      ids.setValueCount(2);
      names.setValueCount(2);
      VectorSchemaRoot root = new VectorSchemaRoot(List.of(ids, names));
      root.setRowCount(2);
      RowType rowType =
          (RowType)
              DataTypes.ROW(
                      DataTypes.FIELD("id", DataTypes.BIGINT()),
                      DataTypes.FIELD("name", DataTypes.STRING()))
                  .getLogicalType();
      ArrowKernelRows rows =
          new ArrowKernelRows(root, rowType, Conversions.FlinkToDelta.schema(rowType));
      ArrowBuf idData = ids.getDataBuffer();

      assertEquals(2, rows.rowCount());
      RowData cursor = rows.rowView(0);
      assertSame(cursor, rows.rowView(1), "batch inspection must reuse one non-retained cursor");
      assertEquals(20L, cursor.getLong(0));

      KernelBatchRowData first = rows.row(0, RowKind.INSERT);
      KernelBatchRowData second = rows.row(1, RowKind.UPDATE_AFTER);
      assertSame(first.batchIdentity(), second.batchIdentity());
      assertEquals(0, first.rowId());
      assertEquals(1, second.rowId());
      assertEquals(10L, first.getLong(0));
      assertEquals("first", first.getString(1).toString());
      assertEquals(20L, second.getLong(0));
      assertEquals("second", second.getString(1).toString());
      assertEquals(RowKind.UPDATE_AFTER, second.getRowKind());

      first.close();
      assertTrue(idData.getReferenceManager().getRefCount() > 0);
      second.close();
      assertTrue(idData.getReferenceManager().getRefCount() > 0);
      rows.close();
      assertEquals(0, idData.getReferenceManager().getRefCount());
    }
  }
}
