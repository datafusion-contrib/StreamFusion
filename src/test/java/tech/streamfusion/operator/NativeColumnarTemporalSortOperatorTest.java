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
 * The event-time sort operator releases buffered rows in ascending rowtime order as the watermark
 * advances — the ordering the (order-independent) SQL parity harness cannot check. Rows whose
 * rowtime is past the watermark stay buffered until a later watermark reaches them.
 */
class NativeColumnarTemporalSortOperatorTest {

  // Input/output schema [v BIGINT, rt TIMESTAMP_LTZ(3)]; rt is the order key (column 1).
  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"v", "rt"});

  @Test
  void releasesRowsInRowtimeOrderAsWatermarkAdvances() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness()) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      // Out-of-order arrival across two batches.
      harness.processElement(new StreamRecord<>(batch(allocator, event(30, 2000), event(10, 500))));
      harness.processElement(new StreamRecord<>(batch(allocator, event(50, 1500), event(20, 0))));

      // Watermark 1500 releases rt <= 1500 in ascending order; rt=2000 stays buffered.
      harness.processWatermark(new Watermark(1500));
      assertEquals(List.of(row(20, 0), row(10, 500), row(50, 1500)), collect(harness));

      // A later watermark releases the rest.
      harness.processWatermark(new Watermark(3000));
      assertEquals(List.of(row(30, 2000)), collect(harness));
    }
  }

  @Test
  void rawEmptyKeyStateSurvivesCheckpointRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before =
            harness()) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(
          new StreamRecord<>(batch(allocator, event(30, 2000), event(10, 500))));
      snapshot = before.snapshot(1L, 1L);
      collect(before);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            harness()) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processWatermark(new Watermark(1500));
      assertEquals(List.of(row(10, 500)), collect(restored));
      restored.processWatermark(new Watermark(3000));
      assertEquals(List.of(row(30, 2000)), collect(restored));
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness()
      throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new NativeColumnarTemporalSortOperator(1, SCHEMA), batch -> 0, Types.INT, 1, 1, 0);
  }

  private static RowData event(long v, long rtMillis) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, v);
    row.setField(1, TimestampData.fromEpochMillis(rtMillis));
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator));
  }

  private static List<Long> row(long v, long rtMillis) {
    return List.of(v, rtMillis);
  }

  private static List<List<Long>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData r : RowDataArrowConverter.read(root, SCHEMA)) {
            rows.add(List.of(r.getLong(0), r.getTimestamp(1, 3).getMillisecond()));
          }
        }
      }
    }
    return rows;
  }
}
