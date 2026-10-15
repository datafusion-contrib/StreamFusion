package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

@ExtendWith(CoalescingOff.class)
class NativeRetractingSumMergeStateTest {
  private static final RowType PARTIAL =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());
  private static final RowType RESULT = RowType.of(new BigIntType(), new BigIntType());

  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  @ParameterizedTest
  @CsvSource({
    "false,false,false",
    "false,true,false",
    "true,false,false",
    "true,true,false",
    "false,false,true",
    "true,true,true"
  })
  void contributionCountsAndNegativeRecordCountsSurviveRestore(
      boolean writeRocks, boolean readRocks, boolean mini) throws Exception {
    OperatorSubtaskState snapshot;
    try (var allocator = new RootAllocator();
        var before = harness(writeRocks, mini, 0, 1, 0)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      push(before, allocator, partial(1, 0, 0, 1), partial(1, 10, 1, 0));
      assertEquals(
          mini
              ? List.of(change(RowKind.INSERT, 1, 10L))
              : List.of(
                  change(RowKind.INSERT, 1, null),
                  change(RowKind.UPDATE_BEFORE, 1, null),
                  change(RowKind.UPDATE_AFTER, 1, 10L)),
          drain(before));
      snapshot = snapshot(before, writeRocks != readRocks);
    }
    OperatorSubtaskState negative;
    try (var allocator = new RootAllocator();
        var restored = harness(readRocks, mini, 0, 1, 0)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      push(restored, allocator, partial(1, 10, 0, 0));
      flush(restored);
      assertEquals(
          List.of(change(RowKind.UPDATE_BEFORE, 1, 10L), change(RowKind.UPDATE_AFTER, 1, 20L)),
          drain(restored));
      push(restored, allocator, partial(1, -20, -1, -1));
      flush(restored);
      assertEquals(List.of(change(RowKind.DELETE, 1, 20L)), drain(restored));
      push(restored, allocator, partial(1, -7, -1, -1));
      flush(restored);
      assertEquals(List.of(change(RowKind.INSERT, 1, -7L)), drain(restored));
      negative = snapshot(restored, false);
    }
    try (var allocator = new RootAllocator();
        var restored = harness(readRocks, mini, 0, 1, 0)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(negative);
      restored.open();
      push(restored, allocator, partial(1, 0, 0, 1));
      flush(restored);
      assertEquals(List.of(change(RowKind.DELETE, 1, -7L)), drain(restored));
      push(restored, allocator, partial(1, 0, 1, 1));
      flush(restored);
      assertEquals(List.of(change(RowKind.INSERT, 1, 0L)), drain(restored));
      push(restored, allocator, partial(1, 0, -1, 0));
      flush(restored);
      assertEquals(
          List.of(change(RowKind.UPDATE_BEFORE, 1, 0L), change(RowKind.UPDATE_AFTER, 1, null)),
          drain(restored));
      push(restored, allocator, partial(1, 0, 0, -1));
      flush(restored);
      assertEquals(List.of(change(RowKind.DELETE, 1, null)), drain(restored));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void sumAndContributionCountsRescaleTogether(boolean rocks) throws Exception {
    OperatorSubtaskState snapshot;
    try (var allocator = new RootAllocator();
        var before = harness(rocks, false, 0, 1, 0)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      for (long key = 1; key <= 16; key++) push(before, allocator, partial(key, key * 10, 2, 2));
      drain(before);
      snapshot = snapshot(before, false);
    }
    for (int subtask = 0; subtask < 2; subtask++) {
      try (var allocator = new RootAllocator();
          var restored = harness(rocks, false, 0, 2, subtask)) {
        restored.setup(new ArrowBatchSerializer());
        restored.initializeState(
            AbstractStreamOperatorTestHarness.repartitionOperatorState(
                snapshot, 128, 1, 2, subtask));
        restored.open();
        for (long key = 1; key <= 16; key++) {
          int hash =
              new RowDataSerializer(RowType.of(new BigIntType()))
                  .toBinaryRow(GenericRowData.of(key))
                  .hashCode();
          int group =
              org.apache.flink.runtime.state.KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
                  hash, 128);
          if (org.apache.flink.runtime.state.KeyGroupRangeAssignment
                  .computeOperatorIndexForKeyGroup(128, 2, group)
              != subtask) continue;
          push(restored, allocator, partial(key, -key * 10, -2, -1));
          assertEquals(
              List.of(
                  change(RowKind.UPDATE_BEFORE, key, key * 10),
                  change(RowKind.UPDATE_AFTER, key, null)),
              drain(restored));
          push(restored, allocator, partial(key, 0, 0, -1));
          assertEquals(List.of(change(RowKind.DELETE, key, null)), drain(restored));
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void ttlUsesWriteTimeAndResetsBothCounts(boolean rocks) throws Exception {
    try (var allocator = new RootAllocator();
        var harness = harness(rocks, false, 1000, 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.setProcessingTime(5000);
      harness.open();
      push(harness, allocator, partial(1, 10, 1, 1));
      drain(harness);
      harness.setProcessingTime(5500);
      push(harness, allocator, partial(1, 0, 0, 0));
      assertEquals(
          List.of(change(RowKind.UPDATE_BEFORE, 1, 10L), change(RowKind.UPDATE_AFTER, 1, 10L)),
          drain(harness));
      harness.setProcessingTime(6000);
      push(harness, allocator, partial(1, 5, 0, 0));
      assertEquals(
          List.of(change(RowKind.UPDATE_BEFORE, 1, 10L), change(RowKind.UPDATE_AFTER, 1, 15L)),
          drain(harness));
      harness.setProcessingTime(7000);
      push(harness, allocator, partial(1, 0, 0, 1));
      assertEquals(List.of(change(RowKind.INSERT, 1, null)), drain(harness));
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      boolean rocks, boolean mini, long ttl, int parallelism, int subtask) throws Exception {
    var operator =
        new NativeColumnarGroupAggregateOperator(
            new int[] {0},
            new int[] {0},
            new int[] {1},
            new int[] {0},
            new int[] {-1},
            new int[] {2},
            new int[] {-1},
            3,
            true,
            mini,
            mini ? 2 : -1,
            ttl,
            new int[] {-1},
            128);
    int[] stateKeys = FlinkKeyGroupUtils.stateKeysForSubtasks(128, parallelism);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            operator,
            (ArrowBatch batch) -> stateKeys[subtask],
            Types.INT,
            128,
            parallelism,
            subtask);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), NativeRetractingSumMergeStateTest.class.getClassLoader()));
    return harness;
  }

  private static OperatorSubtaskState snapshot(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      boolean canonical)
      throws Exception {
    harness.prepareSnapshotPreBarrier(1);
    if (!canonical) return harness.snapshot(1, 1);
    return harness
        .snapshotWithLocalState(
            1,
            1,
            org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  private static GenericRowData partial(long key, long sum, long count, long records) {
    return GenericRowData.of(key, sum, count, records);
  }

  private static List<Object> change(RowKind kind, long key, Long sum) {
    return Arrays.asList(kind, key, sum);
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RootAllocator allocator,
      RowData... rows)
      throws Exception {
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(Arrays.asList(rows), PARTIAL, allocator, false))));
  }

  private static void flush(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness)
      throws Exception {
    harness.prepareSnapshotPreBarrier(1);
  }

  private static List<List<Object>> drain(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    var result = new ArrayList<List<Object>>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (!(event instanceof StreamRecord<?>)) continue;
      try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
        for (var row : RowDataArrowConverter.read(root, RESULT))
          result.add(
              change(row.getRowKind(), row.getLong(0), row.isNullAt(1) ? null : row.getLong(1)));
      }
    }
    return result;
  }
}
