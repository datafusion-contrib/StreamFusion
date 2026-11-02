package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class NativeTemporalCleanupTimerTest {
  private static final RowType TYPE =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType());

  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  private static NativeTemporalJoinOperator operator() {
    return new NativeTemporalJoinOperator(
        new int[] {0},
        new int[] {0},
        2,
        2,
        1,
        TYPE,
        TYPE,
        EncodedPredicate.NONE,
        new int[] {-1},
        100,
        128);
  }

  private static KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      harness(NativeTemporalJoinOperator operator, boolean disk) throws Exception {
    var harness =
        new KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>(
            operator, batch -> 0, batch -> 0, Types.INT, 128, 1, 0);
    if (disk)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), NativeTemporalCleanupTimerTest.class.getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  private static ArrowBatch batch(RootAllocator allocator, long key) {
    return new ArrowBatch(
        RowDataArrowConverter.write(List.of(GenericRowData.of(key, 10L, 100L)), TYPE, allocator));
  }

  private static void deadline(NativeTemporalJoinOperator operator, Long expected) {
    assertArrayEquals(
        expected == null ? new long[0] : new long[] {expected},
        Native.nextTemporalJoinCleanup(operator.handle));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void finishWithPendingBuildCallbacksDoesNotRegisterAnotherProcessingTimer(boolean disk)
      throws Exception {
    var operator = operator();
    try (var allocator = new RootAllocator();
        var harness = harness(operator, disk)) {
      harness.open();
      harness.setProcessingTime(500);
      harness.processElement2(new StreamRecord<>(batch(allocator, 1)));
      harness.setProcessingTime(600);
      deadline(operator, 650L);
      // Harness close stops its timer service before finish. The final event callback refreshes
      // the native clock to 750, which must not schedule another callback on that stopped service.
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void cleanupDeadlinePreservesJavaLongOverflow(boolean disk) throws Exception {
    var operator = operator();
    try (var allocator = new RootAllocator();
        var harness = harness(operator, disk)) {
      harness.open();
      long now = Long.MAX_VALUE - 50;
      harness.setProcessingTime(now);
      harness.processElement1(new StreamRecord<>(batch(allocator, 1)));
      deadline(operator, now + 150);
      assertEquals(1, harness.getProcessingTimeService().getNumActiveTimers());
      harness.setProcessingTime(now);
      deadline(operator, null);
      assertTrue(harness.getOutput().isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void idleExpiryNeedsNeitherInputNorWatermarks(boolean disk) throws Exception {
    var operator = operator();
    try (var allocator = new RootAllocator();
        var harness = harness(operator, disk)) {
      harness.open();
      assertEquals(disk, operator.directRocksDBState());
      harness.processElement1(new StreamRecord<>(batch(allocator, 1)));
      deadline(operator, 150L);
      assertEquals(1, harness.getProcessingTimeService().getNumActiveTimers());
      harness.setProcessingTime(149);
      deadline(operator, 150L);
      harness.setProcessingTime(150);
      deadline(operator, null);
      assertEquals(0, harness.getProcessingTimeService().getNumActiveTimers());
      assertTrue(harness.getOutput().isEmpty());
      // The orphan event callback survives expiry, but its probe rows must never be emitted.
      harness.processBothWatermarks(new Watermark(100));
      assertTrue(harness.extractOutputStreamRecords().isEmpty());
      deadline(operator, null);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void replacedMinimumCancelsTheOldCallbackAndRearmsTheNextKey(boolean disk) throws Exception {
    var operator = operator();
    try (var allocator = new RootAllocator();
        var harness = harness(operator, disk)) {
      harness.open();
      harness.processElement1(new StreamRecord<>(batch(allocator, 1)));
      harness.setProcessingTime(40);
      harness.processElement1(new StreamRecord<>(batch(allocator, 2)));
      harness.setProcessingTime(80);
      harness.processElement1(new StreamRecord<>(batch(allocator, 1)));
      deadline(operator, 190L);
      assertEquals(1, harness.getProcessingTimeService().getNumActiveTimers());
      harness.setProcessingTime(150);
      deadline(operator, 190L);
      harness.setProcessingTime(190);
      deadline(operator, 230L);
      harness.setProcessingTime(230);
      deadline(operator, null);
      assertEquals(0, harness.getProcessingTimeService().getNumActiveTimers());
      assertTrue(harness.getOutput().isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void buildOnlyEventCallbackRefreshesTheIdleDeadline(boolean disk) throws Exception {
    var operator = operator();
    try (var allocator = new RootAllocator();
        var harness = harness(operator, disk)) {
      harness.open();
      harness.setProcessingTime(500);
      harness.processElement2(new StreamRecord<>(batch(allocator, 1)));
      deadline(operator, 650L);
      harness.setProcessingTime(600);
      harness.processBothWatermarks(new Watermark(100));
      deadline(operator, 750L);
      harness.setProcessingTime(650);
      deadline(operator, 750L);
      harness.setProcessingTime(750);
      deadline(operator, null);
      assertTrue(harness.extractOutputStreamRecords().isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nativeRecoveryRearmsOriginalAndAlreadyOverdueDeadlines(boolean disk) throws Exception {
    OperatorSubtaskState snapshot;
    try (var allocator = new RootAllocator();
        var source = harness(operator(), disk)) {
      source.open();
      source.setProcessingTime(500);
      source.processElement1(new StreamRecord<>(batch(allocator, 1)));
      snapshot = source.snapshot(1, 0);
    }
    try {
      for (long clock : new long[] {600, 700}) {
        var operator = operator();
        try (var restored = harness(operator, disk)) {
          restored.initializeState(snapshot);
          restored.setProcessingTime(clock);
          restored.open();
          deadline(operator, 650L);
          assertEquals(1, restored.getProcessingTimeService().getNumActiveTimers());
          restored.setProcessingTime(Math.max(clock, 650));
          deadline(operator, null);
          assertEquals(0, restored.getProcessingTimeService().getNumActiveTimers());
          assertTrue(restored.getOutput().isEmpty());
        }
      }
    } finally {
      snapshot.discardState();
    }
  }
}
