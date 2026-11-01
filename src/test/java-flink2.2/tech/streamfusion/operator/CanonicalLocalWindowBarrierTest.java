package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.Native;

class CanonicalLocalWindowBarrierTest {
  private static final RowType INPUT = RowType.of(new BigIntType(), new TimestampType(3));

  @BeforeAll
  static void initializeMemory() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void localPartialsMustCrossTheBarrierAndLeaveNoStateHandles(boolean canonical, boolean counted)
      throws Exception {
    var operator =
        new NativeColumnarLocalWindowAggregateOperator(
            1000,
            1000,
            1,
            -1,
            -1,
            new int[] {0},
            null,
            new int[0],
            new int[0],
            new int[] {0},
            new int[] {counted ? NativeWindowOperatorCore.KIND_COUNT_DISTINCT_WITH_COUNTS : 3},
            "UTC",
            new int[] {Native.STOCK_KEY_PROJECTION},
            128);
    operator.useStockLocalCheckpointing();
    try (var allocator = new RootAllocator();
        var harness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                operator, new ConstantArrowBatchKeySelector(0), Types.INT, 128, 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      var row = GenericRowData.of(7L, TimestampData.fromEpochMillis(500));
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(List.of(row, row, row), INPUT, allocator))));
      var failure = assertThrows(IllegalStateException.class, () -> snapshot(harness, canonical));
      assertTrue(
          failure.getMessage().contains("not drained before the barrier"), failure.toString());
      harness.prepareSnapshotPreBarrier(1);
      assertPartials(harness, counted, 3);
      var saved = snapshot(harness, canonical);
      assertTrue(saved.getManagedKeyedState().isEmpty());
      assertTrue(saved.getRawKeyedState().isEmpty());
      assertTrue(saved.getManagedOperatorState().isEmpty());
      assertTrue(saved.getRawOperatorState().isEmpty());
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(List.of(row), INPUT, allocator))));
      harness.prepareSnapshotPreBarrier(2);
      assertPartials(harness, counted, 1);
    }
  }

  private static OperatorSubtaskState snapshot(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      boolean canonical)
      throws Exception {
    return canonical
        ? harness
            .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
            .getJobManagerOwnedState()
        : harness.snapshot(1, 0);
  }

  private static void assertPartials(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      boolean counted,
      long frequency) {
    int batches = 0;
    for (Object item; (item = harness.getOutput().poll()) != null; ) {
      if (!(item instanceof StreamRecord)) continue;
      var batch = (ArrowBatch) ((StreamRecord<?>) item).getValue();
      try (var root = batch.root()) {
        assertEquals(1, root.getRowCount());
        assertEquals(1000, ((BigIntVector) root.getVector("slice_end")).get(0));
        if (counted) {
          assertEquals(List.of(7L), ((ListVector) root.getVector("partial0")).getObject(0));
          assertEquals(List.of(frequency), ((ListVector) root.getVector("partial1")).getObject(0));
          assertEquals(1, ((BigIntVector) root.getVector("partial2")).get(0));
        } else assertEquals(frequency, ((BigIntVector) root.getVector("partial0")).get(0));
        batches++;
      }
    }
    assertEquals(1, batches);
  }
}
