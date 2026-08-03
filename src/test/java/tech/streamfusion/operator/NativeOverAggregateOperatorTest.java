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
import org.junit.jupiter.api.Test;

/**
 * The columnar OVER operator passes the input columns through and appends the running aggregate,
 * emitting each row (as an Arrow batch) once the watermark passes its rowtime.
 */
class NativeOverAggregateOperatorTest {

  private static final int MAX_PARALLELISM = 128;

  // Input schema [v BIGINT, rt TIMESTAMP_LTZ(3)]; output appends the running SUM (BIGINT).
  private static final RowType INPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"v", "rt"});

  private static NativeOverAggregateOperator operator() {
    return operator(0);
  }

  private static NativeOverAggregateOperator operator(long stateTtlMillis) {
    // Unpartitioned running SUM over value column 0, ordered by the rowtime in column 1.
    return new NativeOverAggregateOperator(
        1,
        new int[] {0},
        new int[0],
        new int[] {0},
        new int[] {0},
        0,
        0,
        false,
        new int[0],
        stateTtlMillis,
        INPUT,
        MAX_PARALLELISM);
  }

  @Test
  void emitsRunningSumWithPassthrough() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            keyedHarness(operator())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(new StreamRecord<>(batch(allocator, event(10, 0), event(20, 500))));
      harness.processElement(new StreamRecord<>(batch(allocator, event(30, 1000))));
      assertEquals(List.of(), collect(harness)); // nothing before the watermark

      harness.processWatermark(new Watermark(1000));
      // Each input row [v, rt] is passed through with the running SUM appended.
      assertEquals(
          List.of(List.of(10L, 0L, 10L), List.of(20L, 500L, 30L), List.of(30L, 1000L, 60L)),
          collect(harness));
      closeForwarded(harness);
    }
  }

  @Test
  void retentionRestartsAnIdleKeysRunningSum() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            keyedHarness(operator(2000))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      // The element at 5000 registers the key's cleanup deadline at 5000 + 1.5x2000 = 8000
      // (Flink's per-key processing-time cleanup timer at the planner's max idle retention).
      harness.setProcessingTime(5000);
      harness.processElement(new StreamRecord<>(batch(allocator, event(10, 0))));
      harness.processWatermark(new Watermark(0));
      // At the deadline the accumulator is gone, with nothing emitted for the expiry: the next
      // row folds fresh, so the running sum restarts from it alone.
      harness.setProcessingTime(8000);
      harness.processElement(new StreamRecord<>(batch(allocator, event(5, 1000))));
      harness.processWatermark(new Watermark(1000));
      assertEquals(
          List.of(List.of(10L, 0L, 10L), List.of(5L, 1000L, 5L)), collect(harness));
      closeForwarded(harness);
    }
  }

  @Test
  void retentionDefersCleanupWhileARowIsPending() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            keyedHarness(operator(2000))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.setProcessingTime(5000);
      harness.processElement(new StreamRecord<>(batch(allocator, event(10, 0))));
      harness.processWatermark(new Watermark(0)); // folds: acc 10, deadline 8000
      // A row above the watermark stays pending; Flink's fired timer then DEFERS the cleanup
      // (re-registering) rather than clearing state a watermark still needs.
      harness.processElement(new StreamRecord<>(batch(allocator, event(1, 10_000))));
      harness.setProcessingTime(9000);
      harness.processElement(new StreamRecord<>(batch(allocator, event(2, 10_001))));
      harness.processWatermark(new Watermark(20_000));
      // Both rows fold into the SURVIVING accumulator: 10 + 1, then + 2 — not a fresh fold.
      assertEquals(
          List.of(
              List.of(10L, 0L, 10L),
              List.of(1L, 10_000L, 11L),
              List.of(2L, 10_001L, 13L)),
          collect(harness));
      closeForwarded(harness);
    }
  }

  @Test
  void retentionDeadlineSurvivesSnapshotAndRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            keyedHarness(operator(2000))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(5000); // deadline 8000 rides the snapshot
      harness.processElement(new StreamRecord<>(batch(allocator, event(10, 0))));
      harness.processWatermark(new Watermark(0)); // fold, so the acc row carries the deadline
      snapshot = harness.snapshot(1L, 1L);
      closeForwarded(harness);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            keyedHarness(operator(2000))) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      // The restored deadline is the writer's absolute 8000 — not re-stamped from the restore
      // clock (that would be 0 + 3000, already past by 6000): the running sum still continues...
      restored.setProcessingTime(6000);
      restored.processElement(new StreamRecord<>(batch(allocator, event(1, 1000))));
      restored.processWatermark(new Watermark(1000));
      assertEquals(List.of(List.of(1L, 1000L, 11L)), collect(restored));
      // ...and restarts once the clock reaches 8000 (a re-stamp would keep it to 9000).
      restored.setProcessingTime(8000);
      restored.processElement(new StreamRecord<>(batch(allocator, event(2, 2000))));
      restored.processWatermark(new Watermark(2000));
      assertEquals(
          List.of(List.of(1L, 1000L, 11L), List.of(2L, 2000L, 2L)), collect(restored));
      closeForwarded(restored);
    }
  }

  private static RowData event(long v, long rtMillis) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, v);
    row.setField(1, TimestampData.fromEpochMillis(rtMillis));
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), INPUT, allocator));
  }

  /** Drains the output as [v, rt-millis, sum] triples. */
  private static List<List<Long>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    for (Object event : harness.getOutput()) {
      if (event instanceof StreamRecord) {
        VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root();
        var v = (org.apache.arrow.vector.BigIntVector) root.getVector(0);
        var rt = (org.apache.arrow.vector.TimeStampNanoVector) root.getVector(1);
        var sum = (org.apache.arrow.vector.BigIntVector) root.getVector(2);
        for (int i = 0; i < root.getRowCount(); i++) {
          rows.add(List.of(v.get(i), rt.get(i) / 1_000_000L, sum.get(i)));
        }
      }
    }
    return rows;
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> keyedHarness(
      NativeOverAggregateOperator operator) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static void closeForwarded(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    for (Object event : harness.getOutput()) {
      if (event instanceof StreamRecord) {
        ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root().close();
      }
    }
  }
}
