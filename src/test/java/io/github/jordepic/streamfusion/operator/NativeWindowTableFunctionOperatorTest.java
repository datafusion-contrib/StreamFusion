package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
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
 * The windowing TVF operator assigns each Arrow row to its window(s) and appends
 * window_start/window_end/window_time, fanning a row out into one output row per window for hopping
 * and cumulative windows. Tumbling (one window per row) is covered end-to-end by the window-join SQL
 * harness; this exercises the fan-out the join tests do not.
 */
class NativeWindowTableFunctionOperatorTest {

  // Input [k BIGINT, rt TIMESTAMP_LTZ(3)]; output appends window_start/end/time (read as TIMESTAMP).
  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"k", "rt"});

  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(),
            new TimestampType(3),
            new TimestampType(3),
            new TimestampType(3),
            new TimestampType(3)
          },
          new String[] {"k", "rt", "window_start", "window_end", "window_time"});

  @Test
  void hoppingFansEachRowIntoEveryOverlappingWindow() throws Exception {
    // HOP size 2s, slide 1s: a row at t=1500 falls in [0,2000) and [1000,3000).
    assertEquals(
        List.of(window(0, 2000), window(1000, 3000)),
        run(new NativeWindowTableFunctionOperator(1, 2000, 1000, false, false), 7L, 1500L));
  }

  @Test
  void cumulativeFansEachRowIntoEveryNestedWindow() throws Exception {
    // CUMULATE max 2s, step 1s: a row at t=500 falls in the nested [0,1000) and [0,2000).
    assertEquals(
        List.of(window(0, 1000), window(0, 2000)),
        run(new NativeWindowTableFunctionOperator(1, 2000, 1000, true, false), 7L, 500L));
  }

  @Test
  void proctimeAssignsByTheClockIgnoringTheTimeColumn() throws Exception {
    // A proctime HOP (size 2s, slide 1s): the row's time column (t=9999) is ignored — assignment is
    // by the operator's processing-time clock, set to 1500 here, so the row falls in [0,2000) and
    // [1000,3000) exactly as the event-time row at 1500 above. Deterministic via setProcessingTime.
    NativeWindowTableFunctionOperator operator =
        new NativeWindowTableFunctionOperator(1, 2000, 1000, false, true);
    List<List<Long>> rows = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(1500);

      GenericRowData row = new GenericRowData(2);
      row.setField(0, 7L);
      row.setField(1, TimestampData.fromEpochMillis(9999L));
      harness.processElement(
          new StreamRecord<>(new ArrowBatch(RowDataArrowConverter.write(List.of(row), SCHEMA, allocator))));

      while (!harness.getOutput().isEmpty()) {
        Object event = harness.getOutput().poll();
        if (event instanceof StreamRecord) {
          try (VectorSchemaRoot out = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
            for (RowData r : RowDataArrowConverter.read(out, OUTPUT)) {
              rows.add(
                  List.of(
                      r.getLong(0),
                      r.getTimestamp(2, 3).getMillisecond(),
                      r.getTimestamp(3, 3).getMillisecond(),
                      r.getTimestamp(4, 3).getMillisecond()));
            }
          }
        }
      }
    }
    rows.sort(Comparator.<List<Long>>comparingLong(r -> r.get(2)).thenComparingLong(r -> r.get(1)));
    assertEquals(List.of(window(0, 2000), window(1000, 3000)), rows);
  }

  /** window_time is window_end - 1ms (Flink's windowing TVF semantics). */
  private static List<Long> window(long start, long end) {
    return List.of(7L, start, end, end - 1);
  }

  private static List<List<Long>> run(NativeWindowTableFunctionOperator operator, long k, long ts)
      throws Exception {
    List<List<Long>> rows = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      GenericRowData row = new GenericRowData(2);
      row.setField(0, k);
      row.setField(1, TimestampData.fromEpochMillis(ts));
      harness.processElement(
          new StreamRecord<>(new ArrowBatch(RowDataArrowConverter.write(List.of(row), SCHEMA, allocator))));

      while (!harness.getOutput().isEmpty()) {
        Object event = harness.getOutput().poll();
        if (event instanceof StreamRecord) {
          try (VectorSchemaRoot out = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
            for (RowData r : RowDataArrowConverter.read(out, OUTPUT)) {
              rows.add(
                  List.of(
                      r.getLong(0),
                      r.getTimestamp(2, 3).getMillisecond(),
                      r.getTimestamp(3, 3).getMillisecond(),
                      r.getTimestamp(4, 3).getMillisecond()));
            }
          }
        }
      }
    }
    rows.sort(Comparator.<List<Long>>comparingLong(r -> r.get(2)).thenComparingLong(r -> r.get(1)));
    return rows;
  }
}
