package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class NativeTopNOperatorTest {

  // [p (partition), s (sort key)] — top-2 per p by s ascending.
  private static final RowType SCHEMA =
      RowType.of(new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"p", "s"});

  private static NativeTopNOperator operator() {
    return new NativeTopNOperator(
        new int[] {0}, new int[] {1}, new int[] {1}, new int[] {0}, 2L, SCHEMA, 8);
  }

  @Test
  void emitsTopNChangelog() throws Exception {
    try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(operator())) {
      harness.open();
      harness.processElement(new StreamRecord<>(row(1, 5)));
      harness.processElement(new StreamRecord<>(row(1, 3)));
      harness.processElement(new StreamRecord<>(row(1, 8))); // rank 3 — dropped
      harness.processElement(new StreamRecord<>(row(1, 1))); // enters, displaces 5
      harness.endInput();

      assertEquals(
          List.of(
              change(RowKind.INSERT, 1, 5),
              change(RowKind.INSERT, 1, 3),
              change(RowKind.DELETE, 1, 5),
              change(RowKind.INSERT, 1, 1)),
          collect(harness));
    }
  }

  @Test
  void topNStateSurvivesCheckpoint() throws Exception {
    OperatorSubtaskState snapshot;
    try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(operator())) {
      harness.open();
      harness.processElement(new StreamRecord<>(row(1, 5)));
      harness.processElement(new StreamRecord<>(row(1, 3))); // top-2 = {3, 5}
      harness.prepareSnapshotPreBarrier(1L);
      snapshot = harness.snapshot(1L, 1L);
      collect(harness); // drain the two inserts
    }

    try (OneInputStreamOperatorTestHarness<RowData, RowData> restored =
        new OneInputStreamOperatorTestHarness<>(operator())) {
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(new StreamRecord<>(row(1, 1))); // enters, displaces the restored 5
      restored.endInput();
      assertEquals(
          List.of(change(RowKind.DELETE, 1, 5), change(RowKind.INSERT, 1, 1)), collect(restored));
    }
  }

  private static RowData row(long partition, long sort) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, partition);
    row.setField(1, sort);
    return row;
  }

  private static List<Object> change(RowKind kind, long partition, long sort) {
    return List.of(kind, partition, sort);
  }

  private static List<List<Object>> collect(
      OneInputStreamOperatorTestHarness<RowData, RowData> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        RowData r = (RowData) ((StreamRecord<?>) event).getValue();
        rows.add(List.of(r.getRowKind(), r.getLong(0), r.getLong(1)));
      }
    }
    return rows;
  }
}
