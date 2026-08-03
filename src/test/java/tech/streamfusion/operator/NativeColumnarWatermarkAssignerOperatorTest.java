package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

/**
 * The columnar watermark assigner forwards batches untouched and emits the same watermark sequence
 * the host's {@code WatermarkAssignerOperator} (bounded out-of-orderness) would: candidate =
 * {@code max(rowtime) - delay}, floored at 0, emitted eagerly on a large jump and otherwise on the
 * periodic processing-time timer, with MAX forwarded at end of input.
 */
class NativeColumnarWatermarkAssignerOperatorTest {

  // Input schema [value BIGINT, rt TIMESTAMP_LTZ(3)] — the rt column (index 1) carries event time.
  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"value", "rt"});

  @Test
  void forwardsBatchesAndEmitsWatermarkEagerly() throws Exception {
    // Delay 100ms; each batch jumps the watermark by more than the 200ms interval, so it emits eagerly.
    NativeColumnarWatermarkAssignerOperator operator =
        new NativeColumnarWatermarkAssignerOperator(1, 100);
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(new StreamRecord<>(batch(allocator, event(1, 0), event(2, 500), event(3, 1000))));
      harness.processElement(new StreamRecord<>(batch(allocator, event(4, 1500), event(5, 2500))));

      // Both batches forwarded intact, and a watermark of max(rt)-delay after each.
      assertEquals(List.of(3, 2), forwardedRowCounts(harness));
      assertEquals(List.of(900L, 2400L), watermarks(harness));
      closeForwarded(harness);
    }
  }

  @Test
  void floorsCandidateAtZeroAndForwardsMaxAtEndOfInput() throws Exception {
    // Delay 100ms with rowtimes below it: candidate is negative, so the watermark stays at 0 (no emit).
    NativeColumnarWatermarkAssignerOperator operator =
        new NativeColumnarWatermarkAssignerOperator(1, 100);
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(new StreamRecord<>(batch(allocator, event(1, 0), event(2, 50))));
      assertEquals(List.of(2), forwardedRowCounts(harness));
      assertEquals(List.of(), watermarks(harness));

      // End of input forwards MAX_WATERMARK (what finish() delegates to), flushing downstream windows.
      harness.processWatermark(new Watermark(Long.MAX_VALUE));
      assertEquals(List.of(Long.MAX_VALUE), watermarks(harness));
      closeForwarded(harness);
    }
  }

  @Test
  void emitsOnPeriodicTimerWhenJumpIsSmall() throws Exception {
    // Delay 0, a small candidate (<= interval) so the eager path does not fire; the periodic timer does.
    NativeColumnarWatermarkAssignerOperator operator =
        new NativeColumnarWatermarkAssignerOperator(1, 0);
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(new StreamRecord<>(batch(allocator, event(1, 100))));
      assertEquals(List.of(), watermarks(harness)); // 100 - 0 <= 200, no eager emit

      harness.setProcessingTime(1000); // fire the periodic timer
      assertEquals(List.of(100L), watermarks(harness));
      closeForwarded(harness);
    }
  }

  @Test
  void slicesOutOfOrderBatchToEmitWatermarksBetweenRows() throws Exception {
    // An out-of-order batch: a high rowtime mid-batch jumps the watermark past the interval, then a
    // low rowtime follows. The host emits the watermark between them (per row); we must too, so the
    // batch is sliced into [row0,row1] + watermark + [row2] rather than forwarded whole.
    NativeColumnarWatermarkAssignerOperator operator =
        new NativeColumnarWatermarkAssignerOperator(1, 0);
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(
          new StreamRecord<>(batch(allocator, event(1, 0), event(2, 5000), event(3, 500))));

      // Sliced after the row that jumped the watermark (rows 0,1), then the trailing late row (row 2).
      assertEquals(List.of(2, 1), forwardedRowCounts(harness));
      assertEquals(List.of(5000L), watermarks(harness));
      closeForwarded(harness);
    }
  }

  private static RowData event(long value, long eventTimeMillis) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, value);
    row.setField(1, TimestampData.fromEpochMillis(eventTimeMillis));
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    VectorSchemaRoot root = RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator);
    return new ArrowBatch(root);
  }

  /** Row counts of the forwarded {@link ArrowBatch} records, in order. */
  private static List<Integer> forwardedRowCounts(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<Integer> counts = new ArrayList<>();
    for (Object event : harness.getOutput()) {
      if (event instanceof StreamRecord) {
        counts.add(((ArrowBatch) ((StreamRecord<?>) event).getValue()).rowCount());
      }
    }
    return counts;
  }

  /** Timestamps of the emitted {@link Watermark} events, in order. */
  private static List<Long> watermarks(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<Long> marks = new ArrayList<>();
    for (Object event : harness.getOutput()) {
      if (event instanceof Watermark) {
        marks.add(((Watermark) event).getTimestamp());
      }
    }
    return marks;
  }

  // The operator forwards batches without closing them (the downstream consumer owns them), so the
  // test plays that consumer and releases the forwarded roots before the allocator is checked.
  private static void closeForwarded(OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    for (Object event : harness.getOutput()) {
      if (event instanceof StreamRecord) {
        ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root().close();
      }
    }
  }
}
