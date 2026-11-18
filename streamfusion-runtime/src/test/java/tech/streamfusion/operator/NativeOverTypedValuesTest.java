package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
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
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class NativeOverTypedValuesTest {
  private static final TimestampData EARLIEST = TimestampData.fromEpochMillis(-62135596800000L, 1);
  private static final TimestampData MIDDLE = TimestampData.fromEpochMillis(0, 2);
  private static final TimestampData LATEST =
      TimestampData.fromEpochMillis(253402300799999L, 999999);

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
  void typedExtremaAndFirstLastRetainPrecisionNullsPeersAndBackendState(
      int frame, boolean proctime, boolean writer, boolean reader) throws Exception {
    for (int[] decimal : new int[][] {{12, 2}, {38, 0}, {38, 38}}) {
      int precision = decimal[0], scale = decimal[1];
      long reserved = TaskOffHeapMemory.reservedBytes();
      var input =
          RowType.of(
              new DecimalType(precision, scale),
              new TimestampType(9),
              new LocalZonedTimestampType(9),
              new BigIntType());
      var types = new ArrayList<LogicalType>(input.getChildren());
      for (int i = 0; i < 4; i++) types.add(new DecimalType(precision, scale));
      for (int i = 0; i < 4; i++) types.add(new TimestampType(9));
      for (int i = 0; i < 4; i++) types.add(new LocalZonedTimestampType(9));
      var output = RowType.of(types.toArray(new LogicalType[0]));
      OperatorSubtaskState snapshot;
      try (var allocator = new RootAllocator();
          var before = harness(frame, proctime, writer, input, precision, scale)) {
        before.setup(new ArrowBatchSerializer());
        before.open();
        before.processElement(
            new StreamRecord<>(
                batch(
                    allocator,
                    input,
                    row(null, null, 100, precision, scale),
                    row(250, MIDDLE, 200, precision, scale),
                    row(-100, EARLIEST, 300, precision, scale),
                    row(-100, EARLIEST, 300, precision, scale))));
        if (!proctime) before.processWatermark(new Watermark(300));
        var results = drain(before, output);
        assertEquals(4, results.size());
        for (int field = 4; field < 16; field++) assertTrue(results.get(0).isNullAt(field));
        snapshot =
            writer == reader
                ? before.snapshot(1, 1)
                : before
                    .snapshotWithLocalState(
                        1, 1, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                    .getJobManagerOwnedState();
      }
      try (var allocator = new RootAllocator();
          var after = harness(frame, proctime, reader, input, precision, scale)) {
        after.setup(new ArrowBatchSerializer());
        after.initializeState(snapshot);
        after.open();
        after.processElement(
            new StreamRecord<>(
                batch(
                    allocator,
                    input,
                    row(null, null, 400, precision, scale),
                    row(125, LATEST, 500, precision, scale))));
        if (!proctime) after.processWatermark(new Watermark(500));
        var results = drain(after, output);
        assertEquals(2, results.size());
        var last = results.get(1);
        int[] values =
            frame == 0 || frame == 3
                ? new int[] {-100, 250, 250, 125}
                : new int[] {125, 125, 125, 125};
        TimestampData[] timestamps =
            frame == 0 || frame == 3
                ? new TimestampData[] {EARLIEST, LATEST, MIDDLE, LATEST}
                : new TimestampData[] {LATEST, LATEST, LATEST, LATEST};
        for (int i = 0; i < 4; i++) {
          assertEquals(
              BigDecimal.valueOf(values[i], scale),
              last.getDecimal(4 + i, precision, scale).toBigDecimal());
          assertEquals(timestamps[i], last.getTimestamp(8 + i, 9));
          assertEquals(timestamps[i], last.getTimestamp(12 + i, 9));
        }
      }
      assertEquals(reserved, TaskOffHeapMemory.reservedBytes());
    }
  }

  private static GenericRowData row(
      Integer decimal, TimestampData timestamp, long time, int precision, int scale) {
    return GenericRowData.of(
        decimal == null
            ? null
            : DecimalData.fromBigDecimal(
                new BigDecimal(BigInteger.valueOf(decimal), scale), precision, scale),
        timestamp,
        timestamp,
        time);
  }

  private static ArrowBatch batch(RootAllocator allocator, RowType input, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), input, allocator));
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      int frame, boolean proctime, boolean rocks, RowType input, int precision, int scale)
      throws Exception {
    int decimal = 2000 + precision * 100 + scale;
    var operator =
        new NativeOverAggregateOperator(
            3,
            new int[] {0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2},
            new int[0],
            new int[] {
              decimal, decimal, decimal, decimal, 1009, 1009, 1009, 1009, 1009, 1009, 1009, 1009
            },
            new int[] {1, 2, 5, 6, 1, 2, 5, 6, 1, 2, 5, 6},
            frame,
            frame == 1 ? 1 : 100,
            proctime,
            new int[0],
            0,
            input,
            128);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(operator, ignored -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), NativeOverTypedValuesTest.class.getClassLoader()));
    return harness;
  }

  private static List<RowData> drain(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowType output) {
    List<RowData> rows = new ArrayList<>();
    Object event;
    while ((event = harness.getOutput().poll()) != null) {
      if (event instanceof StreamRecord) {
        try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          rows.addAll(RowDataArrowConverter.read(root, output));
        }
      }
    }
    return rows;
  }
}
