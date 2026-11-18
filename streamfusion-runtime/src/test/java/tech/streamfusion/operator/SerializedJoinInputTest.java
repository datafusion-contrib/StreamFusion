package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import tech.streamfusion.NativeMemoryLimitException;

class SerializedJoinInputTest {
  private static final RowType TYPE =
      RowType.of(new BigIntType(), RowType.of(new BigIntType(), new VarCharType(100)));

  @BeforeAll
  static void memory() {
    CanonicalTemporalSortInteropTest.memory();
  }

  @Test
  void deniedWorkspaceLeavesTheBorrowedInputAvailableForRetry() throws Exception {
    try (var allocator = NativeAllocator.newAllocatorForTests();
        var input = input(allocator)) {
      long allocated = allocator.getAllocatedMemory();
      long reserved = TaskOffHeapMemory.reservedBytes();
      byte[] expected = bytes(RowDataArrowConverter.read(input, TYPE).get(0));
      try (var blocked =
          TaskOffHeapMemory.reserve(
              "state", "join conversion denial", TaskOffHeapMemory.capacityBytes() - reserved)) {
        assertThrows(
            NativeMemoryLimitException.class, () -> SerializedJoinInput.prepare(input, TYPE));
        assertEquals(allocated, allocator.getAllocatedMemory());
        assertEquals(TaskOffHeapMemory.capacityBytes(), TaskOffHeapMemory.reservedBytes());
      }
      assertEquals(reserved, TaskOffHeapMemory.reservedBytes());
      try (var prepared = SerializedJoinInput.prepare(input, TYPE)) {
        assertNotSame(input, prepared.root());
        assertArrayEquals(
            expected, bytes(RowDataArrowConverter.read(prepared.root(), TYPE).get(0)));
        assertTrue(TaskOffHeapMemory.reservedBytes() > reserved);
      }
      assertEquals(allocated, allocator.getAllocatedMemory());
      assertEquals(reserved, TaskOffHeapMemory.reservedBytes());
      assertArrayEquals(expected, bytes(RowDataArrowConverter.read(input, TYPE).get(0)));
    }
  }

  @Test
  void malformedRowKindCarrierClosesPartialConversionWithoutClosingInput() {
    try (var allocator = NativeAllocator.newAllocatorForTests();
        var input = malformedInput(allocator)) {
      long allocated = allocator.getAllocatedMemory();
      long reserved = TaskOffHeapMemory.reservedBytes();
      assertThrows(ClassCastException.class, () -> SerializedJoinInput.prepare(input, TYPE));
      assertEquals(allocated, allocator.getAllocatedMemory());
      assertEquals(reserved, TaskOffHeapMemory.reservedBytes());
      assertEquals(1, input.getRowCount());
      assertEquals(2, ((IntVector) input.getVector(RowDataArrowConverter.ROW_KIND_COLUMN)).get(0));
    }
  }

  @Test
  void primitiveAndEmptyInputsKeepTheirBorrowedOwnership() {
    try (var allocator = NativeAllocator.newAllocatorForTests()) {
      for (boolean empty : new boolean[] {false, true}) {
        var type = empty ? TYPE : RowType.of(new BigIntType());
        List<RowData> rows = empty ? List.of() : List.of(GenericRowData.of(1L));
        try (var input = RowDataArrowConverter.write(rows, type, allocator, true)) {
          long allocated = allocator.getAllocatedMemory();
          long reserved = TaskOffHeapMemory.reservedBytes();
          try (var prepared = SerializedJoinInput.prepare(input, type)) {
            assertSame(input, prepared.root());
          }
          assertEquals(allocated, allocator.getAllocatedMemory());
          assertEquals(reserved, TaskOffHeapMemory.reservedBytes());
          assertEquals(empty ? 0 : 1, input.getRowCount());
        }
      }
    }
  }

  private static VectorSchemaRoot input(BufferAllocator allocator) {
    var nested = GenericRowData.of(7L, StringData.fromString("owned-λ"));
    nested.setRowKind(RowKind.DELETE);
    return RowDataArrowConverter.write(
        List.of(GenericRowData.ofKind(RowKind.UPDATE_AFTER, 1L, nested)), TYPE, allocator, true);
  }

  private static VectorSchemaRoot malformedInput(BufferAllocator allocator) {
    var input = input(allocator);
    var malformed = new IntVector(RowDataArrowConverter.ROW_KIND_COLUMN, allocator);
    try {
      malformed.allocateNew(1);
      malformed.setSafe(0, 2);
      malformed.setValueCount(1);
      // Move the data vectors to a root with an intentionally invalid row-kind vector.
      var vectors = new java.util.ArrayList<>(input.getFieldVectors());
      vectors.remove(vectors.size() - 1).close();
      vectors.add(malformed);
      return new VectorSchemaRoot(vectors);
    } catch (RuntimeException | Error failure) {
      input.close();
      malformed.close();
      throw failure;
    }
  }

  private static byte[] bytes(RowData row) throws Exception {
    var output = new DataOutputSerializer(256);
    new RowDataSerializer(TYPE).serialize(row, output);
    return output.getCopyOfBuffer();
  }
}
