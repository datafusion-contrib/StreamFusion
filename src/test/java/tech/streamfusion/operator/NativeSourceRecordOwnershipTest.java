package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class NativeSourceRecordOwnershipTest {
  @Test
  void failedEmissionBeforeConsumptionReleasesOnce() {
    try (RootAllocator allocator = new RootAllocator()) {
      var record = record(allocator);
      record.discardUnclaimed();
      record.discardUnclaimed();
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void failureAfterConsumerClosesDoesNotCloseAgain() {
    try (RootAllocator allocator = new RootAllocator()) {
      var record = record(allocator);
      try (var consumed = record.batch().root()) {
        assertEquals(7, consumed.getVector(0).getObject(0));
      }
      record.discardUnclaimed();
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  private static NativeSourceRecord record(RootAllocator allocator) {
    Schema schema = new Schema(List.of(Field.nullable("id", new ArrowType.Int(32, true))));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    ((IntVector) root.getVector(0)).setSafe(0, 7);
    root.setRowCount(1);
    return new NativeSourceRecord(new ArrowBatch(root), 1, Long.MIN_VALUE);
  }
}
