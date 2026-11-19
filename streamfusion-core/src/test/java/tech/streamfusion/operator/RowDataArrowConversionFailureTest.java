package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RowDataArrowConversionFailureTest {
  private static final RowType TYPE = RowType.of(new IntType(), new IntType());

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  void rejectedDataOrChangelogAllocationReleasesEarlierVectors(int rejectedAllocation) {
    var calls = new AtomicInteger();
    var rejected = new IllegalStateException("task budget exhausted");
    var listener =
        new AllocationListener() {
          @Override
          public void onPreAllocation(long size) {
            if (calls.incrementAndGet() == rejectedAllocation) throw rejected;
          }
        };
    try (var allocator = new RootAllocator(listener, Long.MAX_VALUE)) {
      var failure =
          assertThrows(
              IllegalStateException.class,
              () ->
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(1, 2)), TYPE, allocator, true));
      assertSame(rejected, failure);
      assertEquals(rejectedAllocation, calls.get());
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void invalidRowReleasesAllocatedVectorsBeforePropagatingConversionFailure() {
    try (var allocator = new RootAllocator()) {
      assertThrows(
          ClassCastException.class,
          () ->
              RowDataArrowConverter.write(
                  List.of(GenericRowData.of(1, "wrong internal type")), TYPE, allocator));
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }
}
