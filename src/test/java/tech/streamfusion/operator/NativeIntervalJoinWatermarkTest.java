package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.operators.join.KeyedCoProcessOperatorWithWatermarkDelay;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class NativeIntervalJoinWatermarkTest {
  private static final int MAX_PARALLELISM = 128;
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new BigIntType(), new LocalZonedTimestampType(3));
  private static final RowType WINDOW_OUTPUT =
      RowType.of(new BigIntType(), new TimestampType(3), new TimestampType(3));

  @Test
  void outputWatermarksMatchReleasedFlinkIncludingOverflowAndEndOfInput() throws Exception {
    for (boolean proctime : new boolean[] {false, true}) {
      for (long[] bounds : new long[][] {{0, 0}, {-1000, 2000}, {-3000, 1000}}) {
        long delay = proctime ? 0 : Math.max(-bounds[0], bounds[1]);
        var stockOperator =
            new KeyedCoProcessOperatorWithWatermarkDelay<Integer, Long, Long, Long>(
                new EmptyJoin(), delay);
        try (var nativeHarness = harness(operator(bounds[0], bounds[1], 0, proctime));
            var stockHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                    stockOperator, value -> 0, value -> 0, Types.INT)) {
          nativeHarness.setup(new ArrowBatchSerializer());
          nativeHarness.open();
          stockHarness.open();
          for (long timestamp : new long[] {Long.MIN_VALUE + 1, -2000, 0, 500, 6000, Long.MAX_VALUE}) {
            nativeHarness.processBothWatermarks(new Watermark(timestamp));
            stockHarness.processBothWatermarks(new Watermark(timestamp));
          }
          assertEquals(watermarks(stockHarness.getOutput()), watermarks(nativeHarness.getOutput()));
        }
      }
    }
  }

  @Test
  void cleanupUsesRawInputWatermarkBeforeEmittingDelayedOutput() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        var harness = harness(operator(-1000, 2000, 1, false))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement1(new StreamRecord<>(batch(allocator, 10, 5000)));
      harness.processBothWatermarks(new Watermark(6001));

      Object first = harness.getOutput().poll();
      assertTrue(first instanceof StreamRecord<?>, "raw frontier must emit the expired outer row");
      try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) first).getValue()).root()) {
        assertEquals(1, root.getRowCount());
        assertEquals(10L, root.getVector(1).getObject(0));
        assertTrue(root.getVector(3).isNull(0));
      }
      assertEquals(List.of(4001L), watermarks(harness.getOutput()));
    }
  }

  @Test
  void delayedWatermarkKeepsAJoinedRowAliveForADownstreamWindow() throws Exception {
    var windowOperator =
        new NativeColumnarWindowAggregateOperator(
            false, 1000, 1000, 2, new int[] {1}, new int[0], new int[0],
            new int[] {0}, new int[] {0}, "UTC", WINDOW_OUTPUT, false,
            new int[0], MAX_PARALLELISM);
    try (BufferAllocator allocator = new RootAllocator();
        var join = harness(operator(-1000, 2000, 0, false));
        var window =
            new KeyedOneInputStreamOperatorTestHarness<>(
                windowOperator, (ArrowBatch ignored) -> 0, Types.INT, MAX_PARALLELISM, 1, 0)) {
      join.setup(new ArrowBatchSerializer());
      window.setup(new ArrowBatchSerializer());
      join.open();
      window.open();
      join.processElement1(new StreamRecord<>(batch(allocator, 10, 5000)));
      join.processBothWatermarks(new Watermark(6000));
      forward(join, window);
      join.processElement2(new StreamRecord<>(batch(allocator, 20, 5500)));
      forward(join, window);
      join.processBothWatermarks(Watermark.MAX_WATERMARK);
      forward(join, window);

      List<Long> totals = new ArrayList<>();
      for (Object event : window.getOutput()) {
        if (event instanceof StreamRecord<?>) {
          StreamRecord<?> record = ((StreamRecord<?>) event);

          try (VectorSchemaRoot root = ((ArrowBatch) record.getValue()).root()) {
            var rows = RowDataArrowConverter.read(root, WINDOW_OUTPUT);
            for (var row : rows) {
              totals.add(row.getLong(0));
              assertEquals(5000, row.getTimestamp(1, 3).getMillisecond());
              assertEquals(6000, row.getTimestamp(2, 3).getMillisecond());
            }
          }
        }
      }
      assertEquals(
          List.of(10L), totals, "the join's output watermark must not make its next pair late");
    }
  }

  private static NativeIntervalJoinOperator operator(long lower, long upper, int kind, boolean proctime) {
    return new NativeIntervalJoinOperator(
        new int[] {0}, new int[] {0}, 2, 2, lower, upper, kind, INPUT, INPUT,
        EncodedPredicate.NONE, proctime, new int[] {-1}, MAX_PARALLELISM);
  }

  private static KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      harness(NativeIntervalJoinOperator operator) throws Exception {
    return new KeyedTwoInputStreamOperatorTestHarness<>(
        operator, ignored -> 0, ignored -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static ArrowBatch batch(BufferAllocator allocator, long value, long rowtime) {
    return new ArrowBatch(
        RowDataArrowConverter.write(
            List.of(GenericRowData.of(1L, value, TimestampData.fromEpochMillis(rowtime))),
            INPUT,
            allocator));
  }

  private static List<Long> watermarks(Iterable<?> events) {
    List<Long> result = new ArrayList<>();
    for (Object event : events) {
      if (event instanceof Watermark) {
        Watermark watermark = ((Watermark) event);

        result.add(watermark.getTimestamp());
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private static void forward(
      KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> join,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> window)
      throws Exception {
    while (!join.getOutput().isEmpty()) {
      Object event = join.getOutput().poll();
      if (event instanceof StreamRecord<?>) {
        StreamRecord<?> record = ((StreamRecord<?>) event);

        window.processElement((StreamRecord<ArrowBatch>) record);
      } else if (event instanceof Watermark) {
        Watermark watermark = ((Watermark) event);

        window.processWatermark(watermark);
      }
    }
  }

  private static final class EmptyJoin extends KeyedCoProcessFunction<Integer, Long, Long, Long> {
    @Override
    public void processElement1(Long value, Context context, Collector<Long> out) {}

    @Override
    public void processElement2(Long value, Context context, Collector<Long> out) {}
  }
}
