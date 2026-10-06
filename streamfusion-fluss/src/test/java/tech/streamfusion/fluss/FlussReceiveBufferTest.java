package tech.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

class FlussReceiveBufferTest {
  @Test
  void decodedVectorsRetainReceivedAllocationWithoutCopyingBody() throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot input =
            VectorSchemaRoot.create(FlussArrowLogBatchTest.SCHEMA, allocator)) {
      input.allocateNew();
      ((BigIntVector) input.getVector(0)).setSafe(0, 123);
      input.setRowCount(1);
      byte[] bytes = FlussArrowLogBatch.encode(input, 0, -1, -1);
      var received = Unpooled.directBuffer(bytes.length).writeBytes(bytes);
      long address = received.memoryAddress();
      long before = allocator.getAllocatedMemory();
      FlussArrowLogBatch.Decoded decoded;
      try (var borrowed = FlussReceiveBuffer.borrow(received, allocator)) {
        assertEquals(before + received.capacity(), allocator.getAllocatedMemory());
        decoded =
            FlussArrowLogBatch.decode(
                received.nioBuffer(), FlussArrowLogBatchTest.SCHEMA, allocator, true, borrowed);
      }
      received.release();
      try (decoded) {
        var vector = (BigIntVector) decoded.root().getVector(0);
        assertEquals(123, vector.get(0));
        assertTrue(vector.getDataBuffer().memoryAddress() >= address);
        assertTrue(
            vector.getDataBuffer().memoryAddress() + vector.getDataBuffer().capacity()
                <= address + bytes.length);
        assertEquals(1, received.refCnt());
      }
      assertEquals(0, received.refCnt());
      assertEquals(before, allocator.getAllocatedMemory());
    }
  }

  @Test
  void failedReservationReleasesOnlyTheBorrowedReference() {
    var received = Unpooled.directBuffer(64);
    try (RootAllocator allocator = new RootAllocator(1)) {
      assertThrows(
          org.apache.arrow.memory.OutOfMemoryException.class,
          () -> FlussReceiveBuffer.borrow(received, allocator));
      assertEquals(1, received.refCnt());
      assertEquals(0, allocator.getAllocatedMemory());
    } finally {
      received.release();
    }
  }
}
