package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
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
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class NativeBooleanTimeWindowDistinctTest {
  private static final RowType INPUT = RowType.of(new BooleanType(), new TimeType(0), new TimestampType(3));
  private static final RowType OUTPUT = RowType.of(new BigIntType(), new BigIntType(),
      new TimestampType(3), new TimestampType(3));

  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  @ParameterizedTest
  @CsvSource({"false,false,true", "false,true,true", "true,false,true", "true,true,true",
      "false,false,false", "false,true,false", "true,false,false", "true,true,false"})
  void membershipSurvivesRawAndCanonicalBackendRestore(boolean writeRocks,
      boolean readRocks, boolean distinct) throws Exception {
    long reservedBefore = TaskOffHeapMemory.reservedBytes();
    OperatorSubtaskState snapshot;
    try (var allocator = new RootAllocator(); var before = harness(writeRocks, distinct)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(new StreamRecord<>(batch(allocator,
          row(true, 123, 100), row(null, null, 100), row(true, 123, 100))));
      snapshot = writeRocks == readRocks ? before.snapshot(1, 1)
          : before.snapshotWithLocalState(1, 1,
              org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                  org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
    }
    try (var allocator = new RootAllocator(); var restored = harness(readRocks, distinct)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(new StreamRecord<>(batch(allocator,
          row(false, 123, 200), row(true, 999, 300), row(null, null, 400))));
      restored.processWatermark(new Watermark(999));
      var rows = drain(restored);
      assertEquals(1, rows.size());
      var result = rows.get(0);
      assertEquals(distinct ? 2 : 4, result.getLong(0));
      assertEquals(distinct ? 2 : 4, result.getLong(1));
      assertEquals(0, result.getTimestamp(2, 3).getMillisecond());
      assertEquals(1000, result.getTimestamp(3, 3).getMillisecond());
      restored.processWatermark(new Watermark(2999));
      assertEquals(0, drain(restored).size(), "Empty windows do not produce a distinct result");
    }
    assertEquals(reservedBefore, TaskOffHeapMemory.reservedBytes());
  }

  private static GenericRowData row(Boolean bool, Integer clock, long eventTime) {
    return GenericRowData.of(bool, clock, TimestampData.fromEpochMillis(eventTime));
  }

  private static ArrowBatch batch(RootAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), INPUT, allocator));
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      boolean rocks, boolean distinct) throws Exception {
    var operator = new NativeColumnarWindowAggregateOperator(false, 1000, 1000, 2,
        new int[] {0, 1}, new int[0], new int[0], new int[] {10, 9},
        new int[] {distinct ? 7 : 3, distinct ? 7 : 3},
        "UTC", OUTPUT, false, new int[0], 128);
    var harness = new KeyedOneInputStreamOperatorTestHarness<>(operator, ignored -> 0,
        Types.INT, 128, 1, 0);
    if (rocks) harness.setStateBackend(new RocksDBNativeStateBackendFactory()
        .createFromConfig(new Configuration(), NativeBooleanTimeWindowDistinctTest.class.getClassLoader()));
    return harness;
  }

  private static List<RowData> drain(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<RowData> rows = new ArrayList<>();
    Object event;
    while ((event = harness.getOutput().poll()) != null) {
      if (event instanceof StreamRecord) {
        try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          rows.addAll(RowDataArrowConverter.read(root, OUTPUT));
        }
      }
    }
    return rows;
  }
}
