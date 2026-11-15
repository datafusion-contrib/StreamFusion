package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.streamfusion.NativeMemoryLimitException;

class NativeAllocatorTest {

  private long previousCapacity;

  @BeforeEach
  void savePoolConfiguration() {
    previousCapacity = TaskOffHeapMemory.capacityBytes();
  }

  @AfterEach
  void reset() {
    TaskOffHeapMemory.resetForTests(previousCapacity == 0 ? -1 : previousCapacity);
  }

  @Test
  void sharedAllocatorIsUncappedByDefault() {
    assertEquals(Long.MAX_VALUE, NativeAllocator.SHARED.getLimit());
  }

  @Test
  void sharedPoolRejectsArrowAllocationPastTaskOffHeapCap() {
    TaskOffHeapMemory.resetForTests(1 << 20);
    try (BufferAllocator allocator = NativeAllocator.newAllocatorForTests()) {
      try (ArrowBuf withinBudget = allocator.buffer(512 << 10)) {
        NativeMemoryLimitException error =
            assertThrows(NativeMemoryLimitException.class, () -> allocator.buffer(1 << 20));
        assertTrue(error.getMessage().contains("taskmanager.memory.task.off-heap.size"));
        assertEquals(512 << 10, TaskOffHeapMemory.reservedBytes());
      }
    }
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void adoptsLiveArrowBuffersAllocatedBeforeTaskInitialization() {
    TaskOffHeapMemory.resetForTests(-1);
    try (BufferAllocator allocator = NativeAllocator.newAllocatorForTests();
        ArrowBuf early = allocator.buffer(128)) {
      TaskOffHeapMemory.initialize(budget(1024));
      assertEquals(128, TaskOffHeapMemory.reservedBytes());
      try (ArrowBuf later = allocator.buffer(256)) {
        assertEquals(384, TaskOffHeapMemory.reservedBytes());
      }
      assertEquals(128, TaskOffHeapMemory.reservedBytes());
    }
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void doesNotAdoptBuffersReleasedBeforeTaskInitialization() {
    TaskOffHeapMemory.resetForTests(-1);
    try (BufferAllocator allocator = NativeAllocator.newAllocatorForTests()) {
      try (ArrowBuf early = allocator.buffer(128)) {
        assertEquals(128, allocator.getAllocatedMemory());
      }
      TaskOffHeapMemory.initialize(budget(1024));
      assertEquals(0, TaskOffHeapMemory.reservedBytes());
      try (ArrowBuf later = allocator.buffer(256)) {
        assertEquals(256, TaskOffHeapMemory.reservedBytes());
      }
    }
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void rejectsAnInsufficientInitialBudgetWithoutLosingEarlyBufferOwnership() {
    TaskOffHeapMemory.resetForTests(-1);
    try (BufferAllocator allocator = NativeAllocator.newAllocatorForTests();
        ArrowBuf early = allocator.buffer(2048)) {
      assertThrows(
          NativeMemoryLimitException.class, () -> TaskOffHeapMemory.initialize(budget(1024)));
      assertEquals(0, TaskOffHeapMemory.capacityBytes());
      TaskOffHeapMemory.initialize(budget(4096));
      assertEquals(2048, TaskOffHeapMemory.reservedBytes());
    }
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void releasesEarlyBuffersOnTheirSourceThreadAfterAnotherTaskInitializesThePool()
      throws Exception {
    TaskOffHeapMemory.resetForTests(-1);
    var allocated = new java.util.concurrent.CountDownLatch(1);
    var initialized = new java.util.concurrent.CountDownLatch(1);
    var result = new java.util.concurrent.CompletableFuture<Void>();
    var reader =
        new Thread(
            () -> {
              try (BufferAllocator allocator = NativeAllocator.newAllocatorForTests();
                  ArrowBuf early = allocator.buffer(128)) {
                allocated.countDown();
                if (!initialized.await(10, java.util.concurrent.TimeUnit.SECONDS))
                  throw new AssertionError("Task pool initialization did not complete");
              } catch (Throwable failure) {
                result.completeExceptionally(failure);
                allocated.countDown();
                return;
              }
              result.complete(null);
            });
    reader.start();
    try {
      assertTrue(allocated.await(10, java.util.concurrent.TimeUnit.SECONDS));
      TaskOffHeapMemory.initialize(budget(1024));
      assertEquals(128, TaskOffHeapMemory.reservedBytes());
      initialized.countDown();
      result.get(10, java.util.concurrent.TimeUnit.SECONDS);
      assertEquals(0, TaskOffHeapMemory.reservedBytes());
    } finally {
      initialized.countDown();
      reader.join(10000);
    }
  }

  private static Configuration budget(long bytes) {
    var configuration = new Configuration();
    configuration.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, new MemorySize(bytes));
    return configuration;
  }
}
