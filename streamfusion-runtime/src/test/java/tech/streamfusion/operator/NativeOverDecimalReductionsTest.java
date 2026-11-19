package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class NativeOverDecimalReductionsTest {
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new DecimalType(12, 2), new LocalZonedTimestampType(3));
  private static final RowType OUTPUT =
      RowType.of(
          new BigIntType(),
          new DecimalType(12, 2),
          new LocalZonedTimestampType(3),
          new BigIntType(),
          new DecimalType(38, 2),
          new DecimalType(38, 2),
          new BigIntType(),
          new DecimalType(38, 6));

  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  @ParameterizedTest
  @CsvSource({
    "0,false,false,false",
    "0,false,false,true",
    "0,false,true,false",
    "0,false,true,true",
    "3,false,false,false",
    "3,false,false,true",
    "3,false,true,false",
    "3,false,true,true",
    "0,true,false,false",
    "0,true,false,true",
    "0,true,true,false",
    "0,true,true,true",
    "3,true,false,false",
    "3,true,false,true",
    "3,true,true,false",
    "3,true,true,true",
    "1,false,false,false",
    "1,false,false,true",
    "1,false,true,false",
    "1,false,true,true",
    "1,true,false,false",
    "1,true,false,true",
    "1,true,true,false",
    "1,true,true,true",
    "2,false,false,false",
    "2,false,false,true",
    "2,false,true,false",
    "2,false,true,true"
  })
  void mixedArithmeticRestoresRawSumsCountsAndDecimalScales(
      int frame, boolean proctime, boolean writer, boolean reader) throws Exception {
    long reserved = TaskOffHeapMemory.reservedBytes();
    OperatorSubtaskState snapshot;
    try (var allocator = new RootAllocator();
        var before = harness(frame, proctime, writer)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(new StreamRecord<>(batch(allocator, row(null, null, 100))));
      if (!proctime) before.processWatermark(new Watermark(100));
      assertEquals(
          List.of(Arrays.asList(0L, null, new BigDecimal("0.00"), null, null)), drain(before));
      snapshot = snapshot(before, writer != reader);
    }
    try (var allocator = new RootAllocator();
        var restored = harness(frame, proctime, reader)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(
          new StreamRecord<>(
              batch(
                  allocator, row(10L, "1.25", 200), row(20L, "2.50", 300), row(null, null, 400))));
      if (!proctime) restored.processWatermark(new Watermark(400));
      boolean bounded = frame == 1 || frame == 2;
      assertEquals(
          List.of(
              List.of(
                  1L,
                  new BigDecimal("1.25"),
                  new BigDecimal("1.25"),
                  10L,
                  new BigDecimal("1.250000")),
              List.of(
                  2L,
                  new BigDecimal("3.75"),
                  new BigDecimal("3.75"),
                  15L,
                  new BigDecimal("1.875000")),
              bounded
                  ? List.of(
                      1L,
                      new BigDecimal("2.50"),
                      new BigDecimal("2.50"),
                      20L,
                      new BigDecimal("2.500000"))
                  : List.of(
                      2L,
                      new BigDecimal("3.75"),
                      new BigDecimal("3.75"),
                      15L,
                      new BigDecimal("1.875000"))),
          drain(restored));
      snapshot = snapshot(restored, reader != writer);
    }
    try (var allocator = new RootAllocator();
        var restored = harness(frame, proctime, writer)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(new StreamRecord<>(batch(allocator, row(7L, "-0.25", 500))));
      if (!proctime) restored.processWatermark(new Watermark(500));
      boolean bounded = frame == 1 || frame == 2;
      assertEquals(
          List.of(
              bounded
                  ? List.of(
                      1L,
                      new BigDecimal("-0.25"),
                      new BigDecimal("-0.25"),
                      7L,
                      new BigDecimal("-0.250000"))
                  : List.of(
                      3L,
                      new BigDecimal("3.50"),
                      new BigDecimal("3.50"),
                      12L,
                      new BigDecimal("1.166667"))),
          drain(restored));
    }
    assertEquals(reserved, TaskOffHeapMemory.reservedBytes());
  }

  private static OperatorSubtaskState snapshot(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      boolean canonical)
      throws Exception {
    return canonical
        ? harness
            .snapshotWithLocalState(1, 1, SavepointType.savepoint(SavepointFormatType.CANONICAL))
            .getJobManagerOwnedState()
        : harness.snapshot(1, 1);
  }

  private static GenericRowData row(Long integer, String decimal, long timestamp) {
    return GenericRowData.of(
        integer,
        decimal == null ? null : DecimalData.fromBigDecimal(new BigDecimal(decimal), 12, 2),
        TimestampData.fromEpochMillis(timestamp));
  }

  private static ArrowBatch batch(RootAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), INPUT, allocator));
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      int frame, boolean proctime, boolean rocks) throws Exception {
    var operator =
        new NativeOverAggregateOperator(
            2,
            new int[] {0, 1, 1, 0, 1},
            new int[0],
            new int[] {0, 3202, 3202, 0, 3202},
            new int[] {3, 0, 22, 4, 4},
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
                  new Configuration(), NativeOverDecimalReductionsTest.class.getClassLoader()));
    return harness;
  }

  private static List<List<Object>> drain(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    Object event;
    while ((event = harness.getOutput().poll()) != null) {
      if (event instanceof StreamRecord) {
        try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (var row : RowDataArrowConverter.read(root, OUTPUT)) {
            rows.add(
                Arrays.asList(
                    row.getLong(3),
                    decimal(row, 4, 2),
                    decimal(row, 5, 2),
                    row.isNullAt(6) ? null : row.getLong(6),
                    decimal(row, 7, 6)));
          }
        }
      }
    }
    return rows;
  }

  private static BigDecimal decimal(RowData row, int field, int scale) {
    return row.isNullAt(field) ? null : row.getDecimal(field, 38, scale).toBigDecimal();
  }
}
