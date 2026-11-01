package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class NativeFinalWindowModeTest {
  private static final RowType PARTIAL =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new BigIntType()},
          new String[] {"key0", "partial0", "slice_end"});
  private static final RowType OUTPUT =
      RowType.of(new BigIntType(), new BigIntType(), new TimestampType(3), new TimestampType(3));

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void finalBoundsAndClosedWindowsSurviveOrdinaryNativeCheckpoints(
      boolean cumulative, boolean rocks) throws Exception {
    OperatorSubtaskState snapshot;
    try (var allocator = new RootAllocator();
        var before = harness(cumulative, rocks)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(
                          GenericRowData.of(1L, 2L, 1000L),
                          GenericRowData.of(1L, 3L, 2000L),
                          GenericRowData.of(1L, 4L, 3000L)),
                      PARTIAL,
                      allocator))));
      before.processWatermark(new Watermark(1499));
      assertEquals(List.of(List.of(2L, cumulative ? 0L : -2000L, 1000L)), collect(before));
      snapshot = before.snapshot(1, 1500);
    }
    try (var allocator = new RootAllocator();
        var restored = harness(cumulative, rocks)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(
                          GenericRowData.of(1L, 99L, 1000L),
                          GenericRowData.of(1L, 5L, 2000L),
                          GenericRowData.of(1L, 7L, 3000L)),
                      PARTIAL,
                      allocator))));
      restored.processWatermark(Watermark.MAX_WATERMARK);
      assertEquals(
          List.of(List.of(8L, cumulative ? 0L : -1000L, 2000L), List.of(11L, 0L, 3000L)),
          collect(restored));
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      boolean cumulative, boolean rocks) throws Exception {
    var operator =
        new NativeColumnarGlobalWindowAggregateOperator(
            3000,
            1000,
            cumulative,
            new int[] {0},
            new int[] {0},
            new int[] {3},
            "UTC",
            OUTPUT,
            new int[] {-1},
            128);
    operator.setFinalWindows();
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(operator, batch -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), NativeFinalWindowModeTest.class.getClassLoader()));
    return harness;
  }

  private static List<List<Long>> collect(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord<?>) {
        try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (var row : RowDataArrowConverter.read(root, OUTPUT)) {
            assertEquals(1L, row.getLong(0));
            rows.add(
                List.of(
                    row.getLong(1),
                    row.getTimestamp(2, 3).getMillisecond(),
                    row.getTimestamp(3, 3).getMillisecond()));
          }
        }
      }
    }
    rows.sort(Comparator.comparingLong(row -> row.get(2)));
    return rows;
  }
}
