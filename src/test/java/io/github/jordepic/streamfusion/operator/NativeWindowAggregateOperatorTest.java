package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

class NativeWindowAggregateOperatorTest {

  // Input schema [value, rt]; output schema [total, window_start, window_end].
  @Test
  void emitsWindowAggregateRowsOnWatermarks() throws Exception {
    // No grouping key (-1); UTC so the emitted window bounds stay on the epoch millis asserted here.
    NativeWindowAggregateOperator operator =
        new NativeWindowAggregateOperator(
            false, 1000, 1000, 1, 0, new int[0], 0, new int[] {0}, "UTC", 8);
    try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(operator)) {
      harness.open();

      harness.processElement(new StreamRecord<>(event(1, 0)));
      harness.processElement(new StreamRecord<>(event(2, 500)));
      harness.processElement(new StreamRecord<>(event(3, 1000)));
      harness.processWatermark(new Watermark(1000));

      assertEquals(List.of(row(3, 0, 1000)), collect(harness));

      harness.processElement(new StreamRecord<>(event(4, 1500)));
      harness.processElement(new StreamRecord<>(event(5, 2500)));
      harness.processWatermark(new Watermark(3000));

      assertEquals(List.of(row(7, 1000, 2000), row(5, 2000, 3000)), collect(harness));
    }
  }

  private static RowData event(long value, long eventTimeMillis) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, value);
    row.setField(1, TimestampData.fromEpochMillis(eventTimeMillis));
    return row;
  }

  /** (total, window_start_millis, window_end_millis) for comparison. */
  private static List<Long> row(long total, long start, long end) {
    return List.of(total, start, end);
  }

  private static List<List<Long>> collect(
      OneInputStreamOperatorTestHarness<RowData, RowData> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        RowData r = (RowData) ((StreamRecord<?>) event).getValue();
        rows.add(
            List.of(
                r.getLong(0),
                r.getTimestamp(1, 3).getMillisecond(),
                r.getTimestamp(2, 3).getMillisecond()));
      }
    }
    return rows;
  }
}
