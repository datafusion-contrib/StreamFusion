package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
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
 * The columnar window operator (Arrow-batch input) produces the same window aggregates the row-fed
 * operator did — proving the columnar input extraction (time nanos→millis, value, keys) and the
 * Arrow-batch emit (the window-result reshape, with the bounds rendered as session-local timestamps)
 * are correct. Output is now an Arrow batch shaped to the output row type; read it back with the
 * boundary converter, as the dedicated transpose operator does.
 */
class NativeColumnarWindowAggregateOperatorTest {

  // Input schema [value BIGINT, rt TIMESTAMP_LTZ(3)]; output [total BIGINT, window_start, window_end].
  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"value", "rt"});

  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new TimestampType(3), new TimestampType(3)},
          new String[] {"total", "window_start", "window_end"});

  @Test
  void emitsWindowAggregatesFromArrowBatches() throws Exception {
    NativeColumnarWindowAggregateOperator operator =
        new NativeColumnarWindowAggregateOperator(
            false, 1000, 1000, 1, new int[] {0}, new int[0], new int[0], new int[] {0}, new int[] {0},
            "UTC", OUTPUT, false);
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(new StreamRecord<>(batch(allocator, event(1, 0), event(2, 500), event(3, 1000))));
      harness.processWatermark(new Watermark(1000));
      assertEquals(List.of(row(3, 0, 1000)), collect(harness));

      harness.processElement(new StreamRecord<>(batch(allocator, event(4, 1500), event(5, 2500))));
      harness.processWatermark(new Watermark(3000));
      assertEquals(List.of(row(7, 1000, 2000), row(5, 2000, 3000)), collect(harness));
    }
  }

  /**
   * Proctime windows ignore the row time column and assign by the operator's processing-time clock,
   * firing on a processing-time timer. Driving the clock with {@code setProcessingTime} makes this
   * deterministic (proctime is non-deterministic in a real run — see the CLAUDE.md note). The event
   * times below are deliberately scattered to show they are ignored: assignment is by the clock.
   */
  @Test
  void emitsProctimeWindowsOnTimer() throws Exception {
    NativeColumnarWindowAggregateOperator operator =
        new NativeColumnarWindowAggregateOperator(
            false, 1000, 1000, 1, new int[] {0}, new int[0], new int[0], new int[] {0}, new int[] {0},
            "UTC", OUTPUT, true);
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.setProcessingTime(500);
      harness.processElement(new StreamRecord<>(batch(allocator, event(1, 7000), event(2, 0), event(3, 9000))));
      assertEquals(List.of(), collect(harness)); // window [0,1000) still open at proctime 500
      harness.setProcessingTime(1000); // fires the window-end timer
      assertEquals(List.of(row(6, 0, 1000)), collect(harness));

      harness.setProcessingTime(1500);
      harness.processElement(new StreamRecord<>(batch(allocator, event(4, 100), event(5, 8000))));
      harness.setProcessingTime(2000);
      assertEquals(List.of(row(9, 1000, 2000)), collect(harness));
    }
  }

  /**
   * A proctime hopping window leaves several windows open at once, so the timer chains: each firing
   * closes the earliest-ending open window and schedules the next slide boundary. A single row at
   * proctime 1300 (slide 500, size 1000) lands in windows [500,1500) and [1000,2000), which close in
   * order as the clock crosses 1500 then 2000.
   */
  @Test
  void emitsProctimeHoppingWindowsOnChainedTimers() throws Exception {
    NativeColumnarWindowAggregateOperator operator =
        new NativeColumnarWindowAggregateOperator(
            false, 1000, 500, 1, new int[] {0}, new int[0], new int[0], new int[] {0}, new int[] {0},
            "UTC", OUTPUT, true);
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.setProcessingTime(1300);
      harness.processElement(new StreamRecord<>(batch(allocator, event(10, 42))));
      assertEquals(List.of(), collect(harness)); // both windows still open at proctime 1300
      harness.setProcessingTime(1500);
      assertEquals(List.of(row(10, 500, 1500)), collect(harness));
      harness.setProcessingTime(2000);
      assertEquals(List.of(row(10, 1000, 2000)), collect(harness));
    }
  }

  /**
   * A proctime cumulative window nests several windows sharing a start, all open at once. A single row
   * at proctime 500 (step 1000, max size 3000) lands in [0,1000), [0,2000) and [0,3000); the chained
   * timer emits each as the clock crosses its end.
   */
  @Test
  void emitsProctimeCumulativeWindowsOnChainedTimers() throws Exception {
    NativeColumnarWindowAggregateOperator operator =
        new NativeColumnarWindowAggregateOperator(
            true, 3000, 1000, 1, new int[] {0}, new int[0], new int[0], new int[] {0}, new int[] {0},
            "UTC", OUTPUT, true);
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.setProcessingTime(500);
      harness.processElement(new StreamRecord<>(batch(allocator, event(10, 42))));
      assertEquals(List.of(), collect(harness));
      harness.setProcessingTime(1000);
      assertEquals(List.of(row(10, 0, 1000)), collect(harness));
      harness.setProcessingTime(2000);
      assertEquals(List.of(row(10, 0, 2000)), collect(harness));
      harness.setProcessingTime(3000);
      assertEquals(List.of(row(10, 0, 3000)), collect(harness));
    }
  }

  /**
   * A checkpoint barrier landing mid-stream — after rows for an event-time window have arrived but
   * before the watermark closes it — must lose nothing. {@code snapshotState} flushes the buffered
   * input into native state before serializing, so restoring into a fresh operator resumes the
   * still-open window and combines the pre- and post-restore rows. Pins the synchronous-mailbox
   * guarantee (ticket 01): buffered work survives a checkpoint even though native compute runs
   * synchronously on the task thread. (Re-added after the row-fed operator carrying the original test
   * was deleted in the fully-columnar migration.)
   */
  @Test
  void bufferedInputSurvivesCheckpointMidStream() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(eventTimeOperator(), new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      // Rows land in window [0,1000); the watermark stays below 1000, so the window is still open and
      // its two rows are buffered — not yet emitted — when the checkpoint barrier arrives.
      harness.processElement(new StreamRecord<>(batch(allocator, event(1, 0), event(2, 500))));
      assertEquals(List.of(), collect(harness));
      snapshot = harness.snapshot(1L, 1L);
    }

    // A fresh operator restored from that snapshot must resume the open window: a third row joins the
    // two that survived the checkpoint, and closing the window with a watermark emits their sum.
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(eventTimeOperator(), new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, event(3, 700))));
      harness.processWatermark(new Watermark(1000));
      assertEquals(List.of(row(6, 0, 1000)), collect(harness)); // 1 + 2 (pre-checkpoint) + 3 (post)
    }
  }

  private static NativeColumnarWindowAggregateOperator eventTimeOperator() {
    return new NativeColumnarWindowAggregateOperator(
        false, 1000, 1000, 1, new int[] {0}, new int[0], new int[0], new int[] {0}, new int[] {0},
        "UTC", OUTPUT, false);
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

  private static List<Long> row(long total, long start, long end) {
    return List.of(total, start, end);
  }

  private static List<List<Long>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData r : RowDataArrowConverter.read(root, OUTPUT)) {
            rows.add(
                List.of(
                    r.getLong(0),
                    r.getTimestamp(1, 3).getMillisecond(),
                    r.getTimestamp(2, 3).getMillisecond()));
          }
        }
      }
    }
    return rows;
  }
}
