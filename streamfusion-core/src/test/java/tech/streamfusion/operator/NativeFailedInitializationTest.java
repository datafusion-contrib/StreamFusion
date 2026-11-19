package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class NativeFailedInitializationTest {
  private static final RowType SCHEMA =
      RowType.of(new BigIntType(), new BigIntType(), new LocalZonedTimestampType(3));

  enum Operator { DEDUP, GROUP, NORMALIZE, TOP_N, LOCAL_GROUP }

  @ParameterizedTest
  @EnumSource(Operator.class)
  void missingMemoryConfigurationPreservesFailureThroughCleanup(Operator kind) throws Exception {
    long capacity = TaskOffHeapMemory.capacityBytes();
    TaskOffHeapMemory.resetForTests(-1);
    try {
      IllegalStateException failure = assertThrows(IllegalStateException.class, () -> {
        try (var harness = harness(operator(kind))) {
          harness.open();
        }
      });
      assertTrue(failure.getMessage().contains("taskmanager.memory.task.off-heap.size"));
      assertEquals(0, failure.getSuppressed().length, "cleanup must not add another failure");
      assertEquals(0, TaskOffHeapMemory.reservedBytes());
    } finally {
      TaskOffHeapMemory.resetForTests(capacity == 0 ? -1 : capacity);
    }
  }

  @Test
  void partiallyInitializedDedupReleasesItsHandleWithoutFlushing() throws Exception {
    var operator = new NativeColumnarKeepLastDeduplicateOperator(
        new int[] {0}, new int[] {-1}, 2, SCHEMA,
        true, true, true, false, true, false, 4, 0, 128) {
      @Override
      public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        assertTrue(handle != 0);
        throw new IllegalStateException("deliberate failure after native state creation");
      }
    };
    IllegalStateException failure = assertThrows(IllegalStateException.class, () -> {
      try (var harness = harness(operator)) {
        harness.open();
      }
    });
    assertEquals("deliberate failure after native state creation", failure.getMessage());
    assertEquals(0, failure.getSuppressed().length);
    operator.finish();
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
    // SharedFlinkCluster also checks live JNI handles and Arrow allocations after this test.
  }

  private static OneInputStreamOperator<ArrowBatch, ArrowBatch> operator(Operator kind) {
    switch (kind) {
      case DEDUP:
        return new NativeColumnarKeepLastDeduplicateOperator(
            new int[] {0}, new int[] {-1}, 2, SCHEMA,
            true, true, true, false, true, false, 4, 0, 128);
      case GROUP:
        return new NativeColumnarGroupAggregateOperator(
            new int[] {0}, new int[] {0}, new int[] {1}, new int[] {0},
            new int[] {-1}, new int[] {-1}, new int[] {-1}, -1,
            true, true, 4, 0, new int[] {-1}, 128);
      case NORMALIZE:
        return new NativeColumnarChangelogNormalizeOperator(
            new int[] {0}, new int[] {-1}, SCHEMA, true, true, 4, 0, 128);
      case TOP_N:
        return new NativeColumnarTopNOperator(
            new int[] {0}, new int[] {-1}, SCHEMA, new int[] {1}, new int[] {1},
            new int[] {0}, 0, 2, false, false, null, null, true, true, 4, 0, 128);
      case LOCAL_GROUP:
        return new NativeColumnarLocalGroupAggregateOperator(
            new int[] {0}, new int[] {0}, new int[] {1}, new int[] {-1},
            new int[] {0}, new int[] {-1}, new int[] {-1}, 4);
      default:
        throw new AssertionError(kind);
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      OneInputStreamOperator<ArrowBatch, ArrowBatch> operator) throws Exception {
    var harness = new KeyedOneInputStreamOperatorTestHarness<>(operator, batch -> 0, Types.INT, 128, 1, 0);
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }
}
