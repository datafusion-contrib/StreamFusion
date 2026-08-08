package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class ArrowBatchSerializerTest {

  private static final RowType SCHEMA =
      RowType.of(new LogicalType[] {new BigIntType(), new IntType()}, new String[] {"k", "v"});

  private static RowData row(long k, int v) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, k);
    row.setField(1, v);
    return row;
  }

  @Test
  void roundTripsABatchThroughArrowIpc() throws Exception {
    ArrowBatchSerializer serializer = new ArrowBatchSerializer();
    try (BufferAllocator allocator = new RootAllocator()) {
      List<RowData> rows = List.of(row(1L, 10), row(2L, 20), row(3L, 30));
      VectorSchemaRoot root = RowDataArrowConverter.write(rows, SCHEMA, allocator);

      DataOutputSerializer out = new DataOutputSerializer(256);
      serializer.serialize(new ArrowBatch(root, 3), out);
      root.close();

      DataOutputSerializer copied = new DataOutputSerializer(256);
      serializer.copy(new DataInputDeserializer(out.getCopyOfBuffer()), copied);
      ArrowBatch back = serializer.deserialize(new DataInputDeserializer(copied.getCopyOfBuffer()));
      try (VectorSchemaRoot result = back.root()) {
        assertEquals(3, back.keyGroup());
        assertEquals(3, back.rowCount());
        List<RowData> readBack = RowDataArrowConverter.read(result, SCHEMA);
        assertEquals(3, readBack.size());
        for (int i = 0; i < rows.size(); i++) {
          assertEquals(rows.get(i).getLong(0), readBack.get(i).getLong(0), "k row " + i);
          assertEquals(rows.get(i).getInt(1), readBack.get(i).getInt(1), "v row " + i);
        }
      }
    }
  }

  @Test
  void zeroCopyHandsTheSameBatchAcrossTheWire() throws Exception {
    ArrowBatchSerializer serializer = new ArrowBatchSerializer(true);
    try (BufferAllocator allocator = new RootAllocator()) {
      VectorSchemaRoot root =
          RowDataArrowConverter.write(List.of(row(1L, 10), row(2L, 20)), SCHEMA, allocator);
      ArrowBatch batch = new ArrowBatch(root, 2);

      DataOutputSerializer out = new DataOutputSerializer(64);
      serializer.serialize(batch, out);
      assertEquals(1, ArrowBatchHandles.inFlight());

      // The frame survives a buffer-to-buffer copy and claims back the identical batch: the Arrow
      // buffers never left the JVM, so nothing was encoded or reallocated.
      DataOutputSerializer copied = new DataOutputSerializer(64);
      serializer.copy(new DataInputDeserializer(out.getCopyOfBuffer()), copied);
      ArrowBatch back = serializer.deserialize(new DataInputDeserializer(copied.getCopyOfBuffer()));
      assertSame(batch, back);
      assertEquals(2, back.keyGroup());
      assertEquals(0, ArrowBatchHandles.inFlight());
      root.close();
    }
  }

  @Test
  void zeroCopyHandleRejectsAForeignProcessToken() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      VectorSchemaRoot root = RowDataArrowConverter.write(List.of(row(1L, 10)), SCHEMA, allocator);
      long handle = ArrowBatchHandles.register(new ArrowBatch(root));
      assertThrows(
          IllegalStateException.class,
          () -> ArrowBatchHandles.claim(ArrowBatchHandles.TOKEN_HI + 1, ArrowBatchHandles.TOKEN_LO, handle));
      // The right token claims it; claiming again is the already-consumed error.
      ArrowBatchHandles.claim(ArrowBatchHandles.TOKEN_HI, ArrowBatchHandles.TOKEN_LO, handle);
      assertThrows(
          IllegalStateException.class,
          () -> ArrowBatchHandles.claim(ArrowBatchHandles.TOKEN_HI, ArrowBatchHandles.TOKEN_LO, handle));
      root.close();
    }
  }

  @Test
  void failedOwnerReleasesOnlyItsUnclaimedBatches() throws Exception {
    long failedOwner = ArrowBatchHandles.newOwner();
    long liveOwner = ArrowBatchHandles.newOwner();
    try (BufferAllocator allocator = new RootAllocator()) {
      VectorSchemaRoot failedRoot =
          RowDataArrowConverter.write(List.of(row(1L, 10)), SCHEMA, allocator);
      VectorSchemaRoot liveRoot =
          RowDataArrowConverter.write(List.of(row(2L, 20)), SCHEMA, allocator);
      long failedHandle =
          ArrowBatchHandles.register(new ArrowBatch(failedRoot, 0, failedOwner));
      long liveHandle = ArrowBatchHandles.register(new ArrowBatch(liveRoot, 1, liveOwner));

      assertEquals(1, ArrowBatchHandles.releaseOwner(failedOwner));
      assertThrows(
          IllegalStateException.class,
          () ->
              ArrowBatchHandles.claim(
                  ArrowBatchHandles.TOKEN_HI, ArrowBatchHandles.TOKEN_LO, failedHandle));
      ArrowBatch live =
          ArrowBatchHandles.claim(
              ArrowBatchHandles.TOKEN_HI, ArrowBatchHandles.TOKEN_LO, liveHandle);
      live.root().close();
    }
  }

  @Test
  void copyIsIdentity() {
    ArrowBatchSerializer serializer = new ArrowBatchSerializer();
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = RowDataArrowConverter.write(List.of(row(1L, 10)), SCHEMA, allocator)) {
      ArrowBatch batch = new ArrowBatch(root);
      // A fresh batch is handed off, never retained or mutated, so copy returns the same instance.
      assertEquals(batch, serializer.copy(batch));
    }
  }
}
