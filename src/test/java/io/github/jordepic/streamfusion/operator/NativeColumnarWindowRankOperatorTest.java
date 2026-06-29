package io.github.jordepic.streamfusion.operator;

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
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;

/**
 * The window-rank operator keeps the top-N rows by the order key per window and emits them — with
 * the 1-based rank number — when a watermark closes the window, then evicts it. A row for an
 * already-closed window is dropped as late. Uses UTC so the rendered window bounds are deterministic.
 */
class NativeColumnarWindowRankOperatorTest {

  // [v BIGINT, window_start TIMESTAMP_LTZ(3), window_end TIMESTAMP_LTZ(3)]; window cols at 1 and 2.
  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new LocalZonedTimestampType(3), new LocalZonedTimestampType(3)
          },
          new String[] {"v", "window_start", "window_end"});

  // Output appends the rank number (w0$o0).
  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new TimestampType(3), new TimestampType(3), new BigIntType()
          },
          new String[] {"v", "window_start", "window_end", "w0$o0"});

  @Test
  void emitsTopNWithRankNumberOnWindowClose() throws Exception {
    NativeColumnarWindowRankOperator operator =
        new NativeColumnarWindowRankOperator(
            1, 2, new int[0], new int[] {0}, new int[] {0}, new int[] {0}, 2, true, "UTC",
            false, 0, 0, false);
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      // Window [0,1000): v 10/30/20 -> top-2 by v DESC = 30 (rank 1), 20 (rank 2).
      // Window [1000,2000): v 50/40 -> top-2 = 50 (rank 1), 40 (rank 2).
      harness.processElement(
          new StreamRecord<>(
              batch(
                  allocator,
                  row(10, 0, 1000),
                  row(30, 0, 1000),
                  row(20, 0, 1000),
                  row(50, 1000, 2000),
                  row(40, 1000, 2000))));

      // Watermark 1000 closes only window [0,1000).
      harness.processWatermark(new Watermark(1000));
      assertEquals(List.of(ranked(30, 0, 1), ranked(20, 0, 2)), collect(harness));

      // A late row for the now-closed window is dropped; the next watermark closes [1000,2000).
      harness.processElement(new StreamRecord<>(batch(allocator, row(99, 0, 1000))));
      harness.processWatermark(new Watermark(2000));
      assertEquals(List.of(ranked(50, 1000, 1), ranked(40, 1000, 2)), collect(harness));
    }
  }

  /**
   * A proctime window rank (TUMBLE 1s) closes each window on a processing-time timer, not a
   * watermark. The upstream proctime TVF stamps window_start/window_end by the clock; here the rows
   * already carry those columns, and driving {@code setProcessingTime} past a window end fires the
   * timer that emits its top-N. Deterministic via the controlled clock (proctime is non-deterministic
   * in a real run — see the CLAUDE.md note).
   */
  @Test
  void emitsTopNOnProcessingTimeTimer() throws Exception {
    NativeColumnarWindowRankOperator operator =
        new NativeColumnarWindowRankOperator(
            1, 2, new int[0], new int[] {0}, new int[] {0}, new int[] {0}, 2, true, "UTC",
            true, 1000, 1000, false);
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.setProcessingTime(500);
      harness.processElement(
          new StreamRecord<>(
              batch(allocator, row(10, 0, 1000), row(30, 0, 1000), row(20, 0, 1000))));
      assertEquals(List.of(), collect(harness)); // window [0,1000) still open at proctime 500
      harness.setProcessingTime(1000); // fires the window-end timer
      assertEquals(List.of(ranked(30, 0, 1), ranked(20, 0, 2)), collect(harness));
    }
  }

  private static RowData row(long v, long startMillis, long endMillis) {
    GenericRowData row = new GenericRowData(3);
    row.setField(0, v);
    row.setField(1, TimestampData.fromEpochMillis(startMillis));
    row.setField(2, TimestampData.fromEpochMillis(endMillis));
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator));
  }

  private static List<Long> ranked(long v, long windowStartMillis, long rank) {
    return List.of(v, windowStartMillis, rank);
  }

  private static List<List<Long>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData r : RowDataArrowConverter.read(root, OUTPUT)) {
            rows.add(List.of(r.getLong(0), r.getTimestamp(1, 3).getMillisecond(), r.getLong(3)));
          }
        }
      }
    }
    return rows;
  }
}
