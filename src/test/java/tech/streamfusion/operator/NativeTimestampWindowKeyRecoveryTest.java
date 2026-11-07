package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class NativeTimestampWindowKeyRecoveryTest {
  static Stream<Arguments> cases() {
    var result = new ArrayList<Arguments>();
    for (int precision : new int[] {0, 3, 6, 9})
      for (boolean ltz : new boolean[] {false, true})
        for (boolean rocks : new boolean[] {false, true})
          result.add(Arguments.of(precision, ltz, rocks));
    return result.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void componentTimestampGroupingKeysContinueAfterOrdinaryCheckpoints(
      int precision, boolean ltz, boolean rocks) throws Exception {
    LogicalType keyType =
        ltz ? new LocalZonedTimestampType(precision) : new TimestampType(precision);
    RowType input = RowType.of(keyType, new BigIntType(), new LocalZonedTimestampType(3));
    RowType output =
        RowType.of(keyType, new BigIntType(), new TimestampType(3), new TimestampType(3));
    int nanos = precision == 6 ? 123000 : precision == 9 ? 123456 : 0;
    var early = TimestampData.fromEpochMillis(-62135596800000L, nanos);
    var late = TimestampData.fromEpochMillis(253402300799000L, nanos);
    OperatorSubtaskState checkpoint;
    try (var allocator = new RootAllocator();
        var before = harness(precision, output, rocks)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      assertEquals(
          rocks,
          ((NativeColumnarWindowAggregateOperator) before.getOperator()).directRocksDBState());
      send(before, input, allocator, rows(early, late, 10));
      checkpoint = before.snapshot(1, 0);
      assertTrue(before.getOutput().isEmpty());
    }
    try (var allocator = new RootAllocator();
        var restored = harness(precision, output, rocks)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(checkpoint);
      restored.open();
      assertEquals(
          rocks,
          ((NativeColumnarWindowAggregateOperator) restored.getOperator()).directRocksDBState());
      send(restored, input, allocator, rows(early, late, 1));
      restored.processWatermark(new Watermark(999));
      var actual = new ArrayList<String>();
      while (!restored.getOutput().isEmpty()) {
        Object event = restored.getOutput().poll();
        if (event instanceof StreamRecord<?>) {
          try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
            for (RowData row : RowDataArrowConverter.read(root, output)) {
              assertEquals(0, row.getTimestamp(2, 3).getMillisecond());
              assertEquals(1000, row.getTimestamp(3, 3).getMillisecond());
              actual.add(
                  (row.isNullAt(0) ? "null" : row.getTimestamp(0, precision).toString())
                      + ":"
                      + row.getLong(1));
            }
          }
        }
      }
      actual.sort(String::compareTo);
      var expected = new ArrayList<>(List.of("null:11", early + ":22", late + ":33"));
      expected.sort(String::compareTo);
      assertEquals(expected, actual);
    }
  }

  private static List<RowData> rows(TimestampData early, TimestampData late, int multiplier) {
    var time = TimestampData.fromEpochMillis(100);
    return List.of(
        GenericRowData.of(null, (long) multiplier, time),
        GenericRowData.of(early, 2L * multiplier, time),
        GenericRowData.of(late, 3L * multiplier, time));
  }

  private static void send(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowType type,
      RootAllocator allocator,
      List<RowData> rows)
      throws Exception {
    harness.processElement(
        new StreamRecord<>(new ArrowBatch(RowDataArrowConverter.write(rows, type, allocator))));
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      int precision, RowType output, boolean rocks) throws Exception {
    var operator =
        new NativeColumnarWindowAggregateOperator(
            false,
            1000,
            1000,
            2,
            new int[] {1},
            new int[] {0},
            new int[] {1000 + precision},
            new int[] {0},
            new int[] {0},
            "UTC",
            output,
            false,
            new int[] {precision},
            128);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(operator, batch -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(),
                  NativeTimestampWindowKeyRecoveryTest.class.getClassLoader()));
    return harness;
  }
}
