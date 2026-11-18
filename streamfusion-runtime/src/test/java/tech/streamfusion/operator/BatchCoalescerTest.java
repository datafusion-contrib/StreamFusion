package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * The post-exchange coalescer's buffering contract: sub-batches accumulate until the row target,
 * merge natively in arrival order, and everything pending is either delivered on flush or freed on
 * close. Latency 0 disables the processing-time backstop so no timer service is needed.
 */
class BatchCoalescerTest {

  @Test
  void mixedAppendAndChangelogBatchesPreserveAllKindsInEitherOrder() {
    for (boolean appendFirst : new boolean[] {true, false}) {
      List<VectorSchemaRoot> delivered = new ArrayList<>();
      long allocatedBefore = NativeAllocator.SHARED.getAllocatedMemory();
      try (BatchCoalescer coalescer = coalescer(100, delivered)) {
        VectorSchemaRoot append = rootOf(10, 11);
        VectorSchemaRoot changes = kindedRoot(new int[] {0, 1, 2, 3}, 20, 21, 22, 23);
        coalescer.add(appendFirst ? append : changes);
        coalescer.add(appendFirst ? changes : append);
        coalescer.flush();
        VectorSchemaRoot merged = delivered.get(0);
        assertValues(
            merged,
            appendFirst
                ? new long[] {10, 11, 20, 21, 22, 23}
                : new long[] {20, 21, 22, 23, 10, 11});
        TinyIntVector kinds =
            (TinyIntVector) merged.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
        int[] expected = appendFirst ? new int[] {0, 0, 0, 1, 2, 3} : new int[] {0, 1, 2, 3, 0, 0};
        for (int i = 0; i < expected.length; i++) assertEquals(expected[i], kinds.get(i));
      } finally {
        delivered.forEach(VectorSchemaRoot::close);
      }
      assertEquals(allocatedBefore, NativeAllocator.SHARED.getAllocatedMemory());
    }
  }

  @Test
  void failedMergeReleasesInputsAndDoesNotLeaveClosedRootsPending() {
    List<VectorSchemaRoot> delivered = new ArrayList<>();
    long allocatedBefore = NativeAllocator.SHARED.getAllocatedMemory();
    try (BatchCoalescer coalescer = coalescer(100, delivered)) {
      coalescer.add(rootOf(1));
      coalescer.add(kindedRoot(new int[] {4}, 2));
      assertTrue(
          assertThrows(tech.streamfusion.NativeException.class, coalescer::flush)
              .getMessage()
              .contains("RowKind"));
      coalescer.flush();
      coalescer.add(rootOf(3));
      coalescer.flush();
      assertEquals(1, delivered.size());
      assertValues(delivered.get(0), 3);
    } finally {
      delivered.forEach(VectorSchemaRoot::close);
    }
    assertEquals(allocatedBefore, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static VectorSchemaRoot kindedRoot(int[] tags, long... values) {
    VectorSchemaRoot data = rootOf(values);
    TinyIntVector kinds =
        new TinyIntVector(RowDataArrowConverter.ROW_KIND_COLUMN, NativeAllocator.SHARED);
    kinds.allocateNew(tags.length);
    for (int i = 0; i < tags.length; i++) kinds.set(i, tags[i]);
    kinds.setValueCount(tags.length);
    VectorSchemaRoot root = VectorSchemaRoot.of(data.getVector(0), kinds);
    root.setRowCount(values.length);
    return root;
  }

  @Test
  void buffersUntilFlushAndMergesInOrder() {
    List<VectorSchemaRoot> delivered = new ArrayList<>();
    try (BatchCoalescer coalescer = coalescer(100, delivered)) {
      long mergedBefore = BatchCoalescer.merged();
      coalescer.add(rootOf(1, 2));
      coalescer.add(rootOf(3));
      assertTrue(delivered.isEmpty());
      coalescer.flush();
      assertEquals(1, delivered.size());
      assertValues(delivered.get(0), 1, 2, 3);
      assertEquals(mergedBefore + 2, BatchCoalescer.merged());
    } finally {
      delivered.forEach(VectorSchemaRoot::close);
    }
  }

  @Test
  void reachingTheRowTargetDelivers() {
    List<VectorSchemaRoot> delivered = new ArrayList<>();
    try (BatchCoalescer coalescer = coalescer(3, delivered)) {
      coalescer.add(rootOf(1, 2));
      assertTrue(delivered.isEmpty());
      coalescer.add(rootOf(3, 4));
      assertEquals(1, delivered.size());
      assertValues(delivered.get(0), 1, 2, 3, 4);
    } finally {
      delivered.forEach(VectorSchemaRoot::close);
    }
  }

  @Test
  void batchAtTargetPassesThroughUnmerged() {
    List<VectorSchemaRoot> delivered = new ArrayList<>();
    try (BatchCoalescer coalescer = coalescer(2, delivered)) {
      VectorSchemaRoot large = rootOf(1, 2, 3);
      coalescer.add(large);
      assertEquals(1, delivered.size());
      assertSame(large, delivered.get(0));
    } finally {
      delivered.forEach(VectorSchemaRoot::close);
    }
  }

  @Test
  void emptyBatchIsDropped() {
    List<VectorSchemaRoot> delivered = new ArrayList<>();
    try (BatchCoalescer coalescer = coalescer(100, delivered)) {
      coalescer.add(rootOf());
      coalescer.flush();
      assertTrue(delivered.isEmpty());
    }
  }

  @Test
  void closeFreesPendingWithoutDelivering() {
    List<VectorSchemaRoot> delivered = new ArrayList<>();
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    BatchCoalescer coalescer = coalescer(100, delivered);
    coalescer.add(rootOf(1, 2));
    coalescer.close();
    assertTrue(delivered.isEmpty());
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static BatchCoalescer coalescer(int targetRows, List<VectorSchemaRoot> delivered) {
    return new BatchCoalescer(
        targetRows, 0, NativeAllocator.SHARED, NativeAllocator.DICTIONARIES, null, delivered::add);
  }

  private static VectorSchemaRoot rootOf(long... values) {
    BigIntVector vector = new BigIntVector("v", NativeAllocator.SHARED);
    vector.allocateNew(values.length);
    for (int i = 0; i < values.length; i++) {
      vector.set(i, values[i]);
    }
    vector.setValueCount(values.length);
    VectorSchemaRoot root = VectorSchemaRoot.of(vector);
    root.setRowCount(values.length);
    return root;
  }

  private static void assertValues(VectorSchemaRoot root, long... expected) {
    assertEquals(expected.length, root.getRowCount());
    BigIntVector vector = (BigIntVector) root.getVector("v");
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], vector.get(i));
    }
  }
}
