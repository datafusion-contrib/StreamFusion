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
import tech.streamfusion.compat.ListCollectors;

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
  void roundTripsOrderedRecoveryMetadataAndBufferCopy() throws Exception {
    ArrowBatchSerializer serializer = new ArrowBatchSerializer();
    try (BufferAllocator allocator = new RootAllocator()) {
      VectorSchemaRoot root =
          RowDataArrowConverter.write(List.of(row(1L, 10), row(1L, 30)), SCHEMA, allocator);
      ArrowBatch fragment =
          new ArrowBatch(
              root,
              7,
              ArrowBatch.NO_HANDLE_OWNER,
              null,
              11,
              12,
              13,
              new int[] {0, 2},
              new int[] {7, 9});
      DataOutputSerializer encoded = new DataOutputSerializer(256);
      serializer.serialize(fragment, encoded);
      DataOutputSerializer copied = new DataOutputSerializer(256);
      serializer.copy(new DataInputDeserializer(encoded.getCopyOfBuffer()), copied);
      ArrowBatch restored =
          serializer.deserialize(new DataInputDeserializer(copied.getCopyOfBuffer()));
      assertEquals(7, restored.keyGroup());
      assertEquals(11, restored.parentEpochHigh());
      assertEquals(12, restored.parentEpochLow());
      assertEquals(13, restored.parentSequence());
      assertEquals(
          List.of(0, 2),
          java.util.Arrays.stream(restored.rowOrdinals()).boxed().collect(ListCollectors.toList()));
      assertEquals(
          List.of(7, 9),
          java.util.Arrays.stream(restored.parentKeyGroups())
              .boxed()
              .collect(ListCollectors.toList()));
      restored.root().close();
    }
  }

  @Test
  void ipcFramesLeaveFollowingRecordsUnreadIncludingLegacyFrames() throws Exception {
    ArrowBatchSerializer serializer = new ArrowBatchSerializer();
    try (BufferAllocator allocator = new RootAllocator()) {
      DataOutputSerializer encoded = new DataOutputSerializer(256);
      serializer.serialize(
          new ArrowBatch(RowDataArrowConverter.write(List.of(row(5, 6)), SCHEMA, allocator), 7),
          encoded);
      byte[] tagged = encoded.getCopyOfBuffer();
      DataOutputSerializer records = new DataOutputSerializer(256);
      // The legacy wire shape starts at the IPC length, after the tag and key group.
      records.write(tagged, 8, tagged.length - 8);
      records.write(tagged);
      records.writeInt(0x12345678);
      DataInputDeserializer input = new DataInputDeserializer(records.getCopyOfBuffer());
      for (int keyGroup : new int[] {-1, 7}) {
        ArrowBatch batch = serializer.deserialize(input);
        assertEquals(keyGroup, batch.keyGroup());
        try (VectorSchemaRoot root = batch.root()) {
          assertEquals(5, RowDataArrowConverter.read(root, SCHEMA).get(0).getLong(0));
        }
      }
      assertEquals(0x12345678, input.readInt());
    }
  }

  @Test
  void serializerReuseResetsGrowingAndShrinkingPayloads() throws Exception {
    ArrowBatchSerializer serializer = new ArrowBatchSerializer();
    try (BufferAllocator allocator = new RootAllocator()) {
      DataOutputSerializer output = new DataOutputSerializer(256);
      for (int size : new int[] {1, 100_000, 2}) {
        List<RowData> rows =
            java.util.stream.IntStream.range(0, size)
                .mapToObj(i -> row(size, i))
                .collect(ListCollectors.toList());
        serializer.serialize(
            new ArrowBatch(RowDataArrowConverter.write(rows, SCHEMA, allocator)), output);
      }
      DataInputDeserializer input = new DataInputDeserializer(output.getCopyOfBuffer());
      for (int size : new int[] {1, 100_000, 2}) {
        try (VectorSchemaRoot root = serializer.deserialize(input).root()) {
          List<RowData> rows = RowDataArrowConverter.read(root, SCHEMA);
          assertEquals(size, rows.size());
          assertEquals(size, rows.get(0).getLong(0));
          assertEquals(size - 1, rows.get(size - 1).getInt(1));
        }
      }
    }
  }

  @Test
  void truncatedIpcCannotReadIntoTheNextFrame() throws Exception {
    DataOutputSerializer bytes = new DataOutputSerializer(32);
    bytes.writeInt(1);
    bytes.writeByte(0);
    bytes.writeInt(0x12345678);
    DataInputDeserializer input = new DataInputDeserializer(bytes.getCopyOfBuffer());
    assertThrows(Exception.class, () -> new ArrowBatchSerializer().deserialize(input));
    assertEquals(0x12345678, input.readInt());
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
      assertEquals(0, ArrowBatchHandles.releaseOwner(failedOwner));
      assertEquals(1, ArrowBatchHandles.inFlight());
      assertThrows(
          org.apache.flink.runtime.execution.CancelTaskException.class,
          () ->
              ArrowBatchHandles.claim(
                  ArrowBatchHandles.TOKEN_HI, ArrowBatchHandles.TOKEN_LO, failedHandle));
      ArrowBatch live =
          ArrowBatchHandles.claim(
              ArrowBatchHandles.TOKEN_HI, ArrowBatchHandles.TOKEN_LO, liveHandle);
      live.root().close();
      assertThrows(IllegalStateException.class,
          () -> ArrowBatchHandles.claim(
              ArrowBatchHandles.TOKEN_HI, ArrowBatchHandles.TOKEN_LO, failedHandle));
      ArrowBatchHandles.forgetOwner(failedOwner);
      ArrowBatchHandles.forgetOwner(liveOwner);
    }
  }

  @Test
  void jobReleaseForgetsUnconsumedCancellationMarkers() throws Exception {
    long owner = ArrowBatchHandles.newOwner();
    try (BufferAllocator allocator = new RootAllocator()) {
      VectorSchemaRoot root = RowDataArrowConverter.write(List.of(row(1L, 10)), SCHEMA, allocator);
      long handle = ArrowBatchHandles.register(new ArrowBatch(root, 0, owner));
      ArrowBatchHandles.releaseOwner(owner);
      assertEquals(0, allocator.getAllocatedMemory());
      ArrowBatchHandles.forgetOwner(owner);
      assertThrows(IllegalStateException.class,
          () -> ArrowBatchHandles.claim(ArrowBatchHandles.TOKEN_HI, ArrowBatchHandles.TOKEN_LO, handle));
    }
  }

  @Test
  void claimedBatchSurvivesProducerAndJobCleanup() throws Exception {
    long owner = ArrowBatchHandles.newOwner();
    try (BufferAllocator allocator = new RootAllocator()) {
      VectorSchemaRoot root = RowDataArrowConverter.write(List.of(row(1L, 10)), SCHEMA, allocator);
      long handle = ArrowBatchHandles.register(new ArrowBatch(root, 0, owner));
      ArrowBatch claimed = ArrowBatchHandles.claim(
          ArrowBatchHandles.TOKEN_HI, ArrowBatchHandles.TOKEN_LO, handle);
      assertEquals(0, ArrowBatchHandles.releaseOwner(owner));
      ArrowBatchHandles.forgetOwner(owner);
      assertSame(root, claimed.root());
      assertEquals(1, root.getRowCount());
      root.close();
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void jobReleaseClosesBatchesThatWereNeverClaimed() throws Exception {
    long owner = ArrowBatchHandles.newOwner();
    try (BufferAllocator allocator = new RootAllocator()) {
      VectorSchemaRoot root = RowDataArrowConverter.write(List.of(row(1L, 10)), SCHEMA, allocator);
      ArrowBatchHandles.register(new ArrowBatch(root, 0, owner));
      ArrowBatchHandles.forgetOwner(owner);
      assertEquals(0, allocator.getAllocatedMemory());
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
