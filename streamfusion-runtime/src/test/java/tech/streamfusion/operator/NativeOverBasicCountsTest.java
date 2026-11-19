package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class NativeOverBasicCountsTest {
  private static final RowType INPUT =
      RowType.of(
          new DecimalType(38, 38),
          new VarCharType(VarCharType.MAX_LENGTH),
          new TimestampType(9),
          new LocalZonedTimestampType(9),
          new DateType(),
          new TimeType(3),
          new BooleanType(),
          new VarBinaryType(VarBinaryType.MAX_LENGTH),
          new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND),
          new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH),
          new LocalZonedTimestampType(3));
  private static final RowType OUTPUT =
      RowType.of(
          new DecimalType(38, 38),
          new VarCharType(VarCharType.MAX_LENGTH),
          new TimestampType(9),
          new LocalZonedTimestampType(9),
          new DateType(),
          new TimeType(3),
          new BooleanType(),
          new VarBinaryType(VarBinaryType.MAX_LENGTH),
          new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND),
          new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH),
          new LocalZonedTimestampType(3),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType(),
          new BigIntType());

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
          for (boolean reader : new boolean[] {false, true}) {
            cases.add(Arguments.of(frame, proctime, writer, reader));
          }
        }
      }
    }
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("framesAndBackends")
  void validitySurvivesEveryFrameAndBackendTransition(
      int frame, boolean proctime, boolean writer, boolean reader) throws Exception {
    long reservation = TaskOffHeapMemory.reservedBytes();
    OperatorSubtaskState snapshot;
    try (var allocator = new RootAllocator();
        var before = harness(frame, proctime, writer)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(new StreamRecord<>(batch(allocator, row(true, 100), row(false, 100))));
      if (!proctime) before.processWatermark(new Watermark(100));
      assertEquals(List.of(counts(1), counts(1)), drain(before));
      snapshot =
          writer == reader
              ? before.snapshot(1, 1)
              : before
                  .snapshotWithLocalState(
                      1, 1, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
    }
    try (var allocator = new RootAllocator();
        var restored = harness(frame, proctime, reader)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(
          new StreamRecord<>(batch(allocator, row(true, 200), row(false, 300))));
      if (!proctime) restored.processWatermark(new Watermark(300));
      long expected = frame == 0 || frame == 3 ? 2L : 1L;
      long first = frame == 2 ? 2L : expected;
      assertEquals(List.of(counts(first), counts(expected)), drain(restored));
    }
    assertEquals(reservation, TaskOffHeapMemory.reservedBytes());
  }

  private static GenericRowData row(boolean valid, long time) {
    var timestamp = TimestampData.fromEpochMillis(253402300799999L, 999999);
    return GenericRowData.of(
        valid
            ? DecimalData.fromBigDecimal(
                new BigDecimal("0.99999999999999999999999999999999999999"), 38, 38)
            : null,
        valid ? StringData.fromString("😀\ue000".repeat(128)) : null,
        valid ? timestamp : null,
        valid ? timestamp : null,
        valid ? 19723 : null,
        valid ? 45296123 : null,
        valid ? false : null,
        valid ? new byte[] {0, (byte) 0xff} : null,
        valid ? -123456789L : null,
        valid ? -13 : null,
        TimestampData.fromEpochMillis(time));
  }

  private static ArrowBatch batch(RootAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), INPUT, allocator));
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      int frame, boolean proctime, boolean rocks) throws Exception {
    var operator =
        new NativeOverAggregateOperator(
            10,
            new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
            new int[0],
            new int[] {5838, 3, 1009, 1009, 8, 9, 10, 0, 0, 0},
            new int[] {3, 3, 3, 3, 3, 3, 3, 3, 3, 3},
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
                  new Configuration(), NativeOverBasicCountsTest.class.getClassLoader()));
    return harness;
  }

  private static List<List<Long>> drain(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> results = new ArrayList<>();
    Object event;
    while ((event = harness.getOutput().poll()) != null) {
      if (event instanceof StreamRecord) {
        try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (var row : RowDataArrowConverter.read(root, OUTPUT)) {
            for (int column = 7; column < 10; column++) {
              assertEquals(row.isNullAt(0), row.isNullAt(column));
            }
            if (!row.isNullAt(0)) {
              assertArrayEquals(new byte[] {0, (byte) 0xff}, row.getBinary(7));
              assertEquals(-123456789L, row.getLong(8));
              assertEquals(-13, row.getInt(9));
            }
            List<Long> values = new ArrayList<>();
            for (int column = INPUT.getFieldCount(); column < OUTPUT.getFieldCount(); column++) {
              values.add(row.getLong(column));
            }
            results.add(values);
          }
        }
      }
    }
    return results;
  }

  private static List<Long> counts(long count) {
    return java.util.Collections.nCopies(INPUT.getFieldCount() - 1, count);
  }
}
