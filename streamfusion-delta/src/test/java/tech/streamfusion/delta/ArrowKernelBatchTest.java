package tech.streamfusion.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.types.StructType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

class ArrowKernelBatchTest {

  @Test
  void selectedRowsRemainViewsUntilTheNativeGather() {
    try (RootAllocator allocator = new RootAllocator();
        BigIntVector values = new BigIntVector("value", allocator)) {
      values.allocateNew(3);
      values.setSafe(0, 10);
      values.setSafe(1, 20);
      values.setSafe(2, 30);
      values.setValueCount(3);
      VectorSchemaRoot source = new VectorSchemaRoot(List.of(values));
      source.setRowCount(3);

      try (ArrowKernelBatch batch =
              new ArrowKernelBatch(
                  source, new StructType().add("value", LongType.LONG), new int[] {2, 0, 2});
          VectorSchemaRoot retained = batch.retainedRoot()) {
        assertEquals(3, batch.getSize());
        assertEquals(30, batch.getColumnVector(0).getLong(0));
        assertEquals(10, batch.getColumnVector(0).getLong(1));
        assertEquals(30, batch.getColumnVector(0).getLong(2));
        assertEquals(3, retained.getRowCount(), "the Java side retains buffers without gathering");
        assertFalse(batch.hasSparseSelection());
        org.junit.jupiter.api.Assertions.assertArrayEquals(
            new int[] {2, 0, 2}, batch.selectedRows());
      }
    }
  }

  @Test
  void sparseSelectedRowsRetainTheirHighestSourceIndex() {
    try (RootAllocator allocator = new RootAllocator();
        BigIntVector values = new BigIntVector("value", allocator)) {
      values.allocateNew(10);
      for (int row = 0; row < 10; row++) {
        values.setSafe(row, row * 10L);
      }
      values.setValueCount(10);
      VectorSchemaRoot source = new VectorSchemaRoot(List.of(values));
      source.setRowCount(10);

      try (ArrowKernelBatch batch =
              new ArrowKernelBatch(
                  source, new StructType().add("value", LongType.LONG), new int[] {9, 0});
          VectorSchemaRoot retained = batch.retainedRoot()) {
        assertEquals(2, batch.getSize());
        assertTrue(batch.hasSparseSelection());
        assertEquals(90, batch.getColumnVector(0).getLong(0));
        assertEquals(10, retained.getRowCount(), "Rust must receive the full source before gather");
        assertEquals(90, ((BigIntVector) retained.getVector(0)).get(9));
      }
    }
  }

  @Test
  void retainedRootRestoresDeltaTimestampTimezoneMetadata() {
    try (RootAllocator allocator = new RootAllocator();
        TimeStampNanoVector instant = new TimeStampNanoVector("instant", allocator);
        TimeStampNanoVector local = new TimeStampNanoVector("local", allocator)) {
      instant.allocateNew(1);
      instant.setSafe(0, 123L);
      instant.setValueCount(1);
      local.allocateNew(1);
      local.setSafe(0, 456L);
      local.setValueCount(1);
      VectorSchemaRoot source = new VectorSchemaRoot(List.of(instant, local));
      source.setRowCount(1);
      StructType deltaSchema =
          new StructType()
              .add("instant", TimestampType.TIMESTAMP)
              .add("local", TimestampNTZType.TIMESTAMP_NTZ);

      try (ArrowKernelBatch batch = new ArrowKernelBatch(source, deltaSchema);
          VectorSchemaRoot retained = batch.retainedRoot()) {
        ArrowType.Timestamp instantType =
            (ArrowType.Timestamp) retained.getSchema().getFields().get(0).getType();
        ArrowType.Timestamp localType =
            (ArrowType.Timestamp) retained.getSchema().getFields().get(1).getType();
        assertEquals("UTC", instantType.getTimezone());
        assertNull(localType.getTimezone());
      }
    }
  }
}
