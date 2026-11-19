package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class NativeOverStringValuesTest {
  private static final VarCharType STRING = new VarCharType(VarCharType.MAX_LENGTH);
  private static final RowType INPUT = RowType.of(STRING, new BigIntType(), new DecimalType(12, 2));
  private static final RowType OUTPUT =
      RowType.of(
          STRING,
          new BigIntType(),
          new DecimalType(12, 2),
          STRING,
          STRING,
          STRING,
          STRING,
          new DecimalType(38, 2),
          new DecimalType(38, 2),
          new DecimalType(38, 6));

  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> framesAndBackends() {
    List<Arguments> cases = new ArrayList<>();
    for (int frame = 0; frame < 4; frame++) {
      for (boolean proctime : new boolean[] {false, true}) {
        if (proctime && frame == 2) continue;
        for (boolean writer : new boolean[] {false, true}) {
          for (boolean reader : new boolean[] {false, true})
            cases.add(Arguments.of(frame, proctime, writer, reader));
        }
      }
    }
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("framesAndBackends")
  void unicodeNullsEmptyStringsAndVariableWidthsSurviveBackendRecovery(
      int frame, boolean proctime, boolean writer, boolean reader) throws Exception {
    long reserved = TaskOffHeapMemory.reservedBytes();
    OperatorSubtaskState snapshot;
    String wide = "😀".repeat(4096);
    try (var allocator = new RootAllocator();
        var before = harness(frame, proctime, writer)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(
          new StreamRecord<>(
              batch(
                  allocator,
                  row(null, 100),
                  row(wide, 200),
                  row("\ue000", 300),
                  row("\ue000", 300))));
      if (!proctime) before.processWatermark(new Watermark(300));
      var results = drain(before);
      assertEquals(4, results.size());
      for (int field = 3; field < 7; field++) assertTrue(results.get(0).isNullAt(field));
      assertEquals("\ue000", results.get(2).getString(3).toString());
      assertEquals(wide, results.get(2).getString(4).toString());
      // Keep an unfired event-time value in the checkpoint alongside committed string winners.
      if (!proctime) before.processElement(new StreamRecord<>(batch(allocator, row(null, 400))));
      snapshot =
          writer == reader
              ? before.snapshot(1, 1)
              : before
                  .snapshotWithLocalState(
                      1, 1, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
    }
    try (var allocator = new RootAllocator();
        var after = harness(frame, proctime, reader)) {
      after.setup(new ArrowBatchSerializer());
      after.initializeState(snapshot);
      after.open();
      after.processElement(
          new StreamRecord<>(
              proctime
                  ? batch(allocator, row(null, 400), row("", 500))
                  : batch(allocator, row("", 500))));
      if (!proctime) after.processWatermark(new Watermark(500));
      var results = drain(after);
      assertEquals(2, results.size());
      String[] expected =
          frame == 0 || frame == 3
              ? new String[] {"", wide, wide, ""}
              : new String[] {"", "", "", ""};
      for (int field = 0; field < 4; field++)
        assertEquals(expected[field], results.get(1).getString(3 + field).toString());
      int sum = frame == 0 || frame == 3 ? 8 : 3;
      int average = frame == 0 || frame == 3 ? 2 : 3;
      assertEquals(
          BigDecimal.valueOf(sum).setScale(2), results.get(1).getDecimal(7, 38, 2).toBigDecimal());
      assertEquals(
          BigDecimal.valueOf(sum).setScale(2), results.get(1).getDecimal(8, 38, 2).toBigDecimal());
      assertEquals(
          BigDecimal.valueOf(average).setScale(6),
          results.get(1).getDecimal(9, 38, 6).toBigDecimal());
    }
    assertEquals(reserved, TaskOffHeapMemory.reservedBytes());
  }

  private static RowData row(String value, long time) {
    int number = time == 200 ? 1 : time == 300 ? 2 : 3;
    return GenericRowData.of(
        value == null ? null : StringData.fromString(value),
        time,
        value == null ? null : DecimalData.fromBigDecimal(BigDecimal.valueOf(number), 12, 2));
  }

  private static ArrowBatch batch(RootAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), INPUT, allocator));
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      int frame, boolean proctime, boolean rocks) throws Exception {
    var operator =
        new NativeOverAggregateOperator(
            1,
            new int[] {0, 0, 0, 0, 2, 2, 2},
            new int[0],
            new int[] {3, 3, 3, 3, 3202, 3202, 3202},
            new int[] {1, 2, 5, 6, 0, 22, 4},
            frame,
            frame == 1 ? 1 : 100,
            proctime,
            new int[0],
            0,
            INPUT,
            128);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(operator, ignored -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), NativeOverStringValuesTest.class.getClassLoader()));
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
