package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.NativeMemoryLimitException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class NativeAllocatorTest {

  @AfterEach
  void reset() {
    TaskOffHeapMemory.resetForTests(-1);
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
}
