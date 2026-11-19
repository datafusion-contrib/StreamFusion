package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class NativeComplexWindowCountTest {
  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> cases() {
    return java.util.stream.Stream.of(
            "array", "map", "row", "multiset", "binary", "day_time", "year_month")
        .flatMap(
            type ->
                java.util.stream.IntStream.range(0, 3)
                    .boxed()
                    .flatMap(
                        shape ->
                            java.util.stream.Stream.of(false, true)
                                .map(
                                    rocks ->
                                        org.junit.jupiter.params.provider.Arguments.of(
                                            type, shape, rocks))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void parentValidityAndClockTimersSurviveRestore(String type, int shape, boolean rocks)
      throws Exception {
    LogicalType payloadType;
    Object empty, childNull;
    switch (type) {
      case "binary":
        payloadType = new VarBinaryType();
        empty = new byte[0];
        childNull = new byte[] {0, (byte) 0xff, 0};
        break;
      case "day_time":
        payloadType = new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.SECOND);
        empty = 0L;
        childNull = -1234L;
        break;
      case "year_month":
        payloadType = new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.MONTH);
        empty = 0;
        childNull = -13;
        break;
      case "array":
        payloadType = new ArrayType(new BigIntType());
        empty = new GenericArrayData(new Long[0]);
        childNull = new GenericArrayData(new Long[] {null});
        break;
      case "map":
        payloadType = new MapType(new VarCharType(), new BigIntType());
        empty = new GenericMapData(java.util.Map.of());
        var map = new java.util.HashMap<StringData, Long>();
        map.put(StringData.fromString("nil"), null);
        childNull = new GenericMapData(map);
        break;
      case "multiset":
        payloadType = new MultisetType(new VarCharType());
        empty = new GenericMapData(java.util.Map.of());
        var multiset = new java.util.HashMap<StringData, Integer>();
        multiset.put(null, 3);
        childNull = new GenericMapData(multiset);
        break;
      case "row":
        payloadType = RowType.of(new BigIntType());
        empty = GenericRowData.of((Object) null);
        childNull = GenericRowData.of((Object) null);
        break;
      default:
        throw new IllegalArgumentException(type);
    }
    RowType input = RowType.of(payloadType, new LocalZonedTimestampType(3));
    RowType output = RowType.of(new BigIntType(), new TimestampType(3), new TimestampType(3));
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot;
    try (var allocator = new RootAllocator();
        var harness = harness(shape, rocks, output)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(500);
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(
                          GenericRowData.of(null, TimestampData.fromEpochMillis(9999)),
                          GenericRowData.of(empty, TimestampData.fromEpochMillis(9999)),
                          GenericRowData.of(childNull, TimestampData.fromEpochMillis(9999))),
                      input,
                      allocator))));
      snapshot = harness.snapshot(1, 1);
      assertTrue(harness.getOutput().isEmpty());
    }
    try (var allocator = new RootAllocator();
        var harness = harness(shape, rocks, output)) {
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();
      harness.setProcessingTime(800);
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(GenericRowData.of(childNull, TimestampData.fromEpochMillis(-9999))),
                      input,
                      allocator))));
      harness.setProcessingTime(998);
      assertTrue(harness.getOutput().isEmpty());
      harness.setProcessingTime(999);
      var rows = new java.util.ArrayList<List<Long>>();
      while (!harness.getOutput().isEmpty()) {
        Object event = harness.getOutput().poll();
        if (event instanceof StreamRecord<?>) {
          try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
            for (RowData row : RowDataArrowConverter.read(root, output))
              rows.add(
                  List.of(
                      row.getLong(0),
                      row.getTimestamp(1, 3).getMillisecond(),
                      row.getTimestamp(2, 3).getMillisecond()));
          }
        }
      }
      assertEquals(List.of(List.of(3L, shape == 1 ? -1000L : 0L, 1000L)), rows);
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      int shape, boolean rocks, RowType output) throws Exception {
    var operator =
        new NativeColumnarWindowAggregateOperator(
            shape == 2,
            (shape + 1) * 1000L,
            1000,
            1,
            new int[] {0},
            new int[0],
            new int[0],
            new int[] {0},
            new int[] {3},
            "UTC",
            output,
            true,
            new int[0],
            128);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, batch -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new tech.streamfusion.state.RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new org.apache.flink.configuration.Configuration(),
                  NativeComplexWindowCountTest.class.getClassLoader()));
    return harness;
  }
}
