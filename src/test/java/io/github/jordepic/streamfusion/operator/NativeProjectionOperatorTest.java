package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

class NativeProjectionOperatorTest {

  @Test
  void doublesRowsBatchedThroughNative() throws Exception {
    // Batch size 2 against 3 rows forces both a mid-stream flush and an end-of-input flush.
    NativeProjectionOperator operator = new NativeProjectionOperator(2);
    try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(operator)) {
      harness.open();
      harness.processElement(new StreamRecord<>(rowOf(3)));
      harness.processElement(new StreamRecord<>(rowOf(4)));
      harness.processElement(new StreamRecord<>(rowOf(5)));
      harness.endInput();

      List<Integer> doubled = new ArrayList<>();
      for (Object event : harness.getOutput()) {
        if (event instanceof StreamRecord) {
          doubled.add(((RowData) ((StreamRecord<?>) event).getValue()).getInt(0));
        }
      }

      assertEquals(List.of(6, 8, 10), doubled);
    }
  }

  private static RowData rowOf(int value) {
    GenericRowData row = new GenericRowData(1);
    row.setField(0, value);
    return row;
  }
}
