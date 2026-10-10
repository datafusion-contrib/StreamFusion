package tech.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

class FlussReceiveBufferTest {
  @Test
  void interruptedRpcReleasesItsLateResponseWhileTransportRemainsOpen() throws Exception {
    var responseBuffer = org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled.directBuffer(8);
    var response =
        (org.apache.fluss.rpc.messages.ApiMessage)
            java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {org.apache.fluss.rpc.messages.ApiMessage.class},
                (proxy, method, args) -> {
                  switch (method.getName()) {
                    case "isLazilyParsed":
                      return true;
                    case "getParsedByteBuf":
                      return responseBuffer;
                    default:
                      throw new UnsupportedOperationException(method.getName());
                  }
                });
    var pending =
        new java.util.concurrent.CompletableFuture<org.apache.fluss.rpc.messages.ApiMessage>();
    var interrupted = new java.util.concurrent.atomic.AtomicBoolean();
    var started = new java.util.concurrent.CountDownLatch(1);
    Thread reader =
        new Thread(
            () -> {
              started.countDown();
              try {
                FlussArrowClient.await(pending);
              } catch (java.io.IOException expected) {
                interrupted.set(
                    expected.getCause() instanceof InterruptedException
                        && Thread.currentThread().isInterrupted());
              }
            });
    try {
      reader.start();
      assertTrue(started.await(10, java.util.concurrent.TimeUnit.SECONDS));
      reader.interrupt();
      reader.join(10000);
      assertFalse(reader.isAlive());
      assertTrue(interrupted.get());
      assertEquals(1, responseBuffer.refCnt());
      pending.complete(response);
      assertEquals(0, responseBuffer.refCnt());
    } finally {
      reader.interrupt();
      if (responseBuffer.refCnt() > 0) responseBuffer.release();
    }
  }

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
