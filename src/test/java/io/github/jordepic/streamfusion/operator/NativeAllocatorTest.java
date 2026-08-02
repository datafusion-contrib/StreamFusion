package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.NativeMemoryLimitException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Test;

class NativeAllocatorTest {

  @Test
  void sharedAllocatorIsUncappedByDefault() {
    assertEquals(Long.MAX_VALUE, NativeAllocator.SHARED.getLimit());
  }

  @Test
  void cappedAllocatorFailsOverBudgetNamingTheKnob() {
    try (BufferAllocator capped = NativeAllocator.newAllocator(1)) {
      try (ArrowBuf withinBudget = capped.buffer(512 << 10)) {
        NativeMemoryLimitException error =
            assertThrows(NativeMemoryLimitException.class, () -> capped.buffer(1 << 20));
        assertTrue(error.getMessage().contains("streamfusion.memory.arrow.max-mb"));
      }
    }
  }
}
