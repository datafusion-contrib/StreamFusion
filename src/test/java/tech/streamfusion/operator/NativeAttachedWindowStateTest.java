package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.Native;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class NativeAttachedWindowStateTest {
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new BigIntType(), new TimestampType(3), new TimestampType(3));
  private static final RowType OUTPUT =
      RowType.of(
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new TimestampType(3),
          new TimestampType(3));

  @BeforeAll
  static void initializeMemory() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  @ParameterizedTest
  @CsvSource({
    "false,false,UTC",
    "false,true,UTC",
    "true,false,UTC",
    "true,true,UTC",
    "false,false,GMT+08:00",
    "false,true,GMT+08:00",
    "true,false,GMT+08:00",
    "true,true,GMT+08:00",
    "false,false,America/Los_Angeles",
    "false,true,America/Los_Angeles",
    "true,false,America/Los_Angeles",
    "true,true,America/Los_Angeles"
  })
  void assignedWindowsAndFiringCutoffSurviveRestore(boolean writer, boolean reader, String zone)
      throws Exception {
    OperatorSubtaskState snapshot;
    try (var allocator = new RootAllocator();
        var before = harness(writer, zone)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      push(
          before,
          allocator,
          row(1L, 7L, -4000, 1000),
          row(null, null, -4000, 1000),
          row(1L, 3L, -3000, 2000));
      before.processWatermark(new Watermark(epoch(998, zone)));
      assertTrue(drain(before).isEmpty());
      before.processWatermark(new Watermark(epoch(999, zone)));
      assertEquals(
          List.of(change(1L, 7L, 7L, 1, -4000, 1000), change(null, null, null, 0, -4000, 1000)),
          drain(before));
      before.prepareSnapshotPreBarrier(1);
      snapshot =
          writer == reader
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
        var restored = harness(reader, zone)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      push(restored, allocator, row(1L, 99L, -4000, 1000), row(1L, 5L, -3000, 2000));
      assertEquals(
          1,
          Native.tumblingAggregatorLateDrops(
              ((NativeColumnarWindowAggregateOperator) restored.getOperator()).handle));
      restored.processWatermark(new Watermark(epoch(1998, zone)));
      assertTrue(drain(restored).isEmpty());
      restored.processWatermark(new Watermark(epoch(1999, zone)));
      assertEquals(List.of(change(1L, 8L, 5L, 2, -3000, 2000)), drain(restored));
      push(restored, allocator, row(1L, 1L, -3000, 2000), row(1L, 11L, -2000, 3000));
      restored.processWatermark(new Watermark(Long.MAX_VALUE));
      assertEquals(List.of(change(1L, 11L, 11L, 1, -2000, 3000)), drain(restored));
      assertEquals(
          2,
          Native.tumblingAggregatorLateDrops(
              ((NativeColumnarWindowAggregateOperator) restored.getOperator()).handle));
      long retained =
          ((NativeColumnarWindowAggregateOperator) restored.getOperator()).stateBytesHandle();
      restored.processWatermark(new Watermark(Long.MAX_VALUE));
      assertTrue(drain(restored).isEmpty());
      assertEquals(
          retained,
          ((NativeColumnarWindowAggregateOperator) restored.getOperator()).stateBytesHandle());
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      boolean rocks, String zone) throws Exception {
    var operator =
        new NativeColumnarWindowAggregateOperator(
            false,
            5000,
            5000,
            -1,
            new int[] {1, 1, 1},
            null,
            new int[] {0},
            new int[] {0},
            new int[] {0, 0, 0},
            new int[] {0, 2, 3},
            zone,
            true,
            zone,
            OUTPUT,
            false,
            new int[] {-1},
            128,
            2,
            3);
    int key = FlinkKeyGroupUtils.stateKeysForSubtasks(128, 1)[0];
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, batch -> key, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), NativeAttachedWindowStateTest.class.getClassLoader()));
    return harness;
  }

  private static long epoch(long wallMillis, String zone) {
    return org.apache.flink.table.runtime.util.TimeWindowUtil.toEpochMillsForTimer(
        wallMillis, java.time.ZoneId.of(zone));
  }

  private static GenericRowData row(Long key, Long value, long start, long end) {
    return GenericRowData.of(
        key, value, TimestampData.fromEpochMillis(start), TimestampData.fromEpochMillis(end));
  }

  private static List<Object> change(
      Long key, Long sum, Long max, long count, long start, long end) {
    return Arrays.asList(RowKind.INSERT, key, sum, max, count, start, end);
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RootAllocator allocator,
      RowData... rows)
      throws Exception {
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(RowDataArrowConverter.write(Arrays.asList(rows), INPUT, allocator))));
  }

  private static List<List<Object>> drain(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    var result = new ArrayList<List<Object>>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (!(event instanceof StreamRecord<?>)) continue;
      try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
        for (RowData row : RowDataArrowConverter.read(root, OUTPUT))
          result.add(
              change(
                  row.isNullAt(0) ? null : row.getLong(0),
                  row.isNullAt(1) ? null : row.getLong(1),
                  row.isNullAt(2) ? null : row.getLong(2),
                  row.getLong(3),
                  row.getTimestamp(4, 3).getMillisecond(),
                  row.getTimestamp(5, 3).getMillisecond()));
      }
    }
    result.sort(
        java.util.Comparator.comparing(
            row -> row.get(1) == null ? Long.MAX_VALUE : (Long) row.get(1)));
    return result;
  }
}
