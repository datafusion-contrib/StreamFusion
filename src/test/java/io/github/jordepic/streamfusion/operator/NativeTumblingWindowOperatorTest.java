package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

class NativeTumblingWindowOperatorTest {

  @Test
  void emitsWindowsAsWatermarksCloseThem() throws Exception {
    NativeTumblingWindowOperator operator = new NativeTumblingWindowOperator(1000, 8);
    try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(operator)) {
      harness.open();

      harness.processElement(new StreamRecord<>(event(0, 1)));
      harness.processElement(new StreamRecord<>(event(500, 2)));
      harness.processElement(new StreamRecord<>(event(1000, 3)));
      harness.processWatermark(new Watermark(1000));

      // Only window [0, 1000) is closed at watermark 1000.
      assertEquals(List.of(window(0, 3)), collectWindows(harness));

      harness.processElement(new StreamRecord<>(event(1500, 4)));
      harness.processElement(new StreamRecord<>(event(2500, 5)));
      harness.processWatermark(new Watermark(3000));

      // Window [1000, 2000) accumulated 3 + 4 across two batches; [2000, 3000) holds 5.
      assertEquals(List.of(window(1000, 7), window(2000, 5)), collectWindows(harness));
    }
  }

  @Test
  void restoresOpenWindowStateFromCheckpoint() throws Exception {
    OperatorSubtaskState snapshot;
    try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(new NativeTumblingWindowOperator(1000, 8))) {
      harness.open();
      // Lands in the still-open window [1000, 2000); not yet flushed to native (buffered).
      harness.processElement(new StreamRecord<>(event(1500, 4)));
      snapshot = harness.snapshot(1L, 1L);
    }

    try (OneInputStreamOperatorTestHarness<RowData, RowData> restored =
        new OneInputStreamOperatorTestHarness<>(new NativeTumblingWindowOperator(1000, 8))) {
      restored.initializeState(snapshot);
      restored.open();
      // Same window gets more data after restore.
      restored.processElement(new StreamRecord<>(event(1700, 6)));
      restored.processWatermark(new Watermark(3000));

      // The restored partial total (4) combines with post-restore data (6).
      assertEquals(List.of(window(1000, 10)), collectWindows(restored));
    }
  }

  private static RowData event(long ts, long value) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, ts);
    row.setField(1, value);
    return row;
  }

  private static List<Long> window(long start, long total) {
    return List.of(start, total);
  }

  private static List<List<Long>> collectWindows(
      OneInputStreamOperatorTestHarness<RowData, RowData> harness) {
    List<List<Long>> windows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        RowData row = (RowData) ((StreamRecord<?>) event).getValue();
        windows.add(window(row.getLong(0), row.getLong(1)));
      }
    }
    return windows;
  }
}
