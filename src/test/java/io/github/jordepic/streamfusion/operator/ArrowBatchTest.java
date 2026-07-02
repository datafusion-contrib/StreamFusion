package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * The abandoned-batch backstop: a batch Flink drops in flight (no consumer ever takes its root) must
 * be reclaimed once unreachable, and taking the root must disarm the backstop so buffers in use are
 * never freed underneath their consumer.
 */
class ArrowBatchTest {

  @Test
  void abandonedBatchIsReclaimedOnceUnreachable() throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    newBatchAndDropIt();
    assertTrue(
        drainsTo(before),
        () -> "abandoned batch not reclaimed; allocator holds "
            + (NativeAllocator.SHARED.getAllocatedMemory() - before)
            + " extra bytes");
  }

  @Test
  void takingTheRootDisarmsTheBackstop() throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    VectorSchemaRoot root = newBatch().root(); // the batch wrapper is dropped here, disarmed
    for (int i = 0; i < 10; i++) {
      System.gc();
      Thread.sleep(20);
    }
    assertTrue(
        NativeAllocator.SHARED.getAllocatedMemory() > before,
        "backstop freed a handed-over root");
    assertEquals(3, root.getRowCount());
    root.close();
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  /** In its own frame so the batch is unquestionably unreachable when the caller polls. */
  private static void newBatchAndDropIt() {
    newBatch();
  }

  private static ArrowBatch newBatch() {
    BigIntVector values = new BigIntVector("v", NativeAllocator.SHARED);
    values.allocateNew(3);
    for (int i = 0; i < 3; i++) {
      values.setSafe(i, i);
    }
    values.setValueCount(3);
    VectorSchemaRoot root = new VectorSchemaRoot(List.of(values));
    root.setRowCount(3);
    return new ArrowBatch(root);
  }

  private static boolean drainsTo(long target) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      if (NativeAllocator.SHARED.getAllocatedMemory() == target) {
        return true;
      }
      System.gc();
      Thread.sleep(20);
    }
    return NativeAllocator.SHARED.getAllocatedMemory() == target;
  }
}
