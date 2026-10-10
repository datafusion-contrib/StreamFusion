package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

@ExtendWith(CoalescingOff.class)
class NativeGroupedValueStateTest {
  private static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType());
  private static final RowType OUTPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType());

  @BeforeAll
  static void memoryAuthority() {
    Configuration config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void orderedOccurrencesSurviveCheckpointAndBackendChanges(boolean writeRocks, boolean readRocks)
      throws Exception {
    OperatorSubtaskState snapshot;
    try (var allocator = new RootAllocator();
        var before = harness(writeRocks, 15, 16, 0)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      push(before, allocator, row(RowKind.INSERT, 10L), row(RowKind.INSERT, 20L));
      drain(before, OUTPUT);
      push(
          before,
          allocator,
          row(RowKind.INSERT, 10L),
          row(RowKind.INSERT, 30L),
          row(RowKind.INSERT, null));
      drain(before, OUTPUT);
      snapshot =
          writeRocks == readRocks
              ? before.snapshot(1, 1)
              : before
                  .snapshotWithLocalState(
                      1,
                      1,
                      org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                          org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
    }
    try (var allocator = new RootAllocator();
        var restored = harness(readRocks, 15, 16, 0)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      push(restored, allocator, row(RowKind.DELETE, 10L));
      assertEquals(
          List.of(change(RowKind.UPDATE_BEFORE, 10L, 30L), change(RowKind.UPDATE_AFTER, 20L, 30L)),
          drain(restored, OUTPUT));
      push(restored, allocator, row(RowKind.DELETE, 30L), row(RowKind.DELETE, 10L));
      assertEquals(
          List.of(
              change(RowKind.UPDATE_BEFORE, 20L, 30L),
              change(RowKind.UPDATE_AFTER, 20L, 10L),
              change(RowKind.UPDATE_BEFORE, 20L, 10L),
              change(RowKind.UPDATE_AFTER, 20L, 20L)),
          drain(restored, OUTPUT));
      push(restored, allocator, row(RowKind.DELETE, 20L), row(RowKind.DELETE, null));
      assertEquals(
          List.of(
              change(RowKind.UPDATE_BEFORE, 20L, 20L),
              change(RowKind.UPDATE_AFTER, null, null),
              change(RowKind.DELETE, null, null)),
          drain(restored, OUTPUT));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void appendOnlyStateAndItsTtlSurviveRestore(boolean rocks) throws Exception {
    OperatorSubtaskState snapshot;
    try (var allocator = new RootAllocator();
        var before = harness(rocks, 12, 13, 1000)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.setProcessingTime(5000);
      push(before, allocator, row(RowKind.INSERT, 10L), row(RowKind.INSERT, 20L));
      drain(before, OUTPUT);
      snapshot = before.snapshot(1, 1);
    }
    try (var allocator = new RootAllocator();
        var restored = harness(rocks, 12, 13, 1000)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.setProcessingTime(5500);
      push(restored, allocator, row(RowKind.INSERT, 30L));
      assertEquals(
          List.of(change(RowKind.UPDATE_BEFORE, 10L, 20L), change(RowKind.UPDATE_AFTER, 10L, 30L)),
          drain(restored, OUTPUT));
      restored.setProcessingTime(6500);
      push(restored, allocator, row(RowKind.INSERT, 40L));
      assertEquals(List.of(change(RowKind.INSERT, 40L, 40L)), drain(restored, OUTPUT));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void singleValueRestoresNullCardinalityAndCanRetractBeforeReplacement(boolean rocks)
      throws Exception {
    OperatorSubtaskState snapshot;
    RowType output = RowType.of(new BigIntType(), new BigIntType());
    try (var allocator = new RootAllocator();
        var before = harness(rocks, 14, -1, 0)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      push(before, allocator, row(RowKind.INSERT, null));
      drain(before, output);
      snapshot = before.snapshot(1, 1);
    }
    try (var allocator = new RootAllocator();
        var restored = harness(rocks, 14, -1, 0)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      push(restored, allocator, row(RowKind.DELETE, null), row(RowKind.INSERT, 7L));
      assertEquals(
          List.of(Arrays.asList(RowKind.DELETE, 1L, null), Arrays.asList(RowKind.INSERT, 1L, 7L)),
          drain(restored, output));
      assertThrows(
          tech.streamfusion.compat.TableErrors.exceptionType(),
          () -> push(restored, allocator, row(RowKind.INSERT, null)));
    }
    try (var allocator = new RootAllocator();
        var restored = harness(rocks, 14, -1, 0)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      assertThrows(
          tech.streamfusion.compat.TableErrors.exceptionType(),
          () -> push(restored, allocator, row(RowKind.INSERT, null)));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rescalingPreservesOrderedOccurrencesPerKeyGroup(boolean rocks) throws Exception {
    OperatorSubtaskState snapshot;
    try (var allocator = new RootAllocator();
        var before = harness(rocks, 15, 16, 0)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      for (long key = 1; key <= 16; key++) {
        push(
            before,
            allocator,
            GenericRowData.of(key, key * 10),
            GenericRowData.of(key, key * 10 + 1));
      }
      drain(before, OUTPUT);
      snapshot = before.snapshot(1, 1);
    }
    for (int subtask = 0; subtask < 2; subtask++) {
      try (var allocator = new RootAllocator();
          var restored = harness(rocks, 15, 16, 0, 2, subtask)) {
        restored.setup(new ArrowBatchSerializer());
        restored.initializeState(
            org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness
                .repartitionOperatorState(snapshot, 128, 1, 2, subtask));
        restored.open();
        for (long key = 1; key <= 16; key++) {
          int hash =
              new org.apache.flink.table.runtime.typeutils.RowDataSerializer(
                      RowType.of(new BigIntType()))
                  .toBinaryRow(GenericRowData.of(key))
                  .hashCode();
          int group =
              org.apache.flink.runtime.state.KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
                  hash, 128);
          if (org.apache.flink.runtime.state.KeyGroupRangeAssignment
                  .computeOperatorIndexForKeyGroup(128, 2, group)
              != subtask) continue;
          push(restored, allocator, GenericRowData.ofKind(RowKind.DELETE, key, key * 10));
          assertEquals(
              List.of(
                  Arrays.asList(RowKind.UPDATE_BEFORE, key, key * 10, key * 10 + 1),
                  Arrays.asList(RowKind.UPDATE_AFTER, key, key * 10 + 1, key * 10 + 1)),
              drain(restored, OUTPUT));
        }
      }
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      boolean rocks, int first, int second, long ttl) throws Exception {
    return harness(rocks, first, second, ttl, 1, 0);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      boolean rocks, int first, int second, long ttl, int parallelism, int subtask)
      throws Exception {
    int[] kinds = second < 0 ? new int[] {first} : new int[] {first, second};
    int[] values = new int[kinds.length], absent = new int[kinds.length];
    Arrays.fill(values, 1);
    Arrays.fill(absent, -1);
    var operator =
        new NativeColumnarGroupAggregateOperator(
            kinds,
            new int[kinds.length],
            values,
            new int[] {0},
            absent,
            absent,
            absent,
            -1,
            true,
            false,
            -1,
            ttl,
            new int[] {-1},
            128);
    int[] stateKeys =
        tech.streamfusion.planner.FlinkKeyGroupUtils.stateKeysForSubtasks(128, parallelism);
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
                  new Configuration(), NativeGroupedValueStateTest.class.getClassLoader()));
    return harness;
  }

  private static RowData row(RowKind kind, Long value) {
    return GenericRowData.ofKind(kind, 1L, value);
  }

  private static List<Object> change(RowKind kind, Long first, Long last) {
    return Arrays.asList(kind, 1L, first, last);
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      BufferAllocator allocator,
      RowData... rows)
      throws Exception {
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(RowDataArrowConverter.write(List.of(rows), INPUT, allocator, true))));
  }

  private static List<List<Object>> drain(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowType type) {
    List<List<Object>> result = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object item = harness.getOutput().poll();
      if (item instanceof StreamRecord<?>) {
        StreamRecord<?> record = ((StreamRecord<?>) item);

        try (var root = ((ArrowBatch) record.getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, type)) {
            List<Object> fields = new ArrayList<>();
            fields.add(row.getRowKind());
            for (int i = 0; i < row.getArity(); i++)
              fields.add(row.isNullAt(i) ? null : row.getLong(i));
            result.add(fields);
          }
        }
      }
    }
    return result;
  }
}
