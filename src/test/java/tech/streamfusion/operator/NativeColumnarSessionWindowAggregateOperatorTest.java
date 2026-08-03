package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
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
 * A proctime session window times the gap on the operator's processing-time clock and closes a
 * gap-separated session on a processing-time timer rather than a watermark. Driving the clock with
 * {@code setProcessingTime} makes this deterministic (proctime is non-deterministic in a real run —
 * see the CLAUDE.md note); the event times in the rows are scattered to show they are ignored, the
 * session being timed by the clock at which each batch arrives.
 */
class NativeColumnarSessionWindowAggregateOperatorTest {

  private static final int MAX_PARALLELISM = 128;

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
  void mergesWithinGapAndClosesOnTimer() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            rawKeyedHarness(true)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.setProcessingTime(100);
      harness.processElement(new StreamRecord<>(batch(allocator, event(10, 7000))));
      harness.setProcessingTime(300); // within the 500ms gap of the first element — same session
      harness.processElement(new StreamRecord<>(batch(allocator, event(20, 0))));
      assertEquals(List.of(), collect(harness)); // session still open: last element 300 + gap = 800

      harness.setProcessingTime(600); // the first element's 600 timer fires, but the session extended
      assertEquals(List.of(), collect(harness));
      harness.setProcessingTime(800); // last element's 800 timer: now the merged session closes
      assertEquals(List.of(row(30, 100, 800)), collect(harness));

      // A later element beyond the gap opens a fresh session.
      harness.setProcessingTime(2000);
      harness.processElement(new StreamRecord<>(batch(allocator, event(99, 1234))));
      harness.setProcessingTime(2500);
      assertEquals(List.of(row(99, 2000, 2500)), collect(harness));
    }
  }

  @Test
  void rawKeyedSessionStateSurvivesCheckpointRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before =
            rawKeyedHarness()) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(new StreamRecord<>(batch(allocator, event(10, 0))));
      snapshot = before.snapshot(1L, 1L);
      collect(before);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            rawKeyedHarness()) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(new StreamRecord<>(batch(allocator, event(20, 200))));
      restored.processWatermark(new Watermark(700));
      assertEquals(List.of(row(30, 0, 700)), collect(restored));
    }
  }

  @Test
  void rawKeyedProctimeSessionRearmsTimerAfterRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before =
            rawKeyedHarness(true)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.setProcessingTime(100);
      before.processElement(new StreamRecord<>(batch(allocator, event(10, 0))));
      snapshot = before.snapshot(1L, 1L);
      collect(before);
    }

    try (KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
        rawKeyedHarness(true)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.setProcessingTime(600);
      assertEquals(List.of(row(10, 100, 600)), collect(restored));
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      rawKeyedHarness() throws Exception {
    return rawKeyedHarness(false);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      rawKeyedHarness(boolean proctime) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new NativeColumnarSessionWindowAggregateOperator(
            500,
            1,
            new int[] {0},
            new int[0],
            new int[0],
            new int[] {0},
            new int[] {0},
            "UTC",
            OUTPUT,
            proctime,
            new int[0],
            MAX_PARALLELISM),
        batch -> 0,
        Types.INT,
        MAX_PARALLELISM,
        1,
        0);
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
