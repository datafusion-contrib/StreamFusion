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

class NativeGroupAggregateOperatorTest {

  // Input [key, value]; output [key, sum] — SUM(value) GROUP BY key.
  private static final RowType INPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"key", "value"});
  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"key", "sum"});

  private static NativeGroupAggregateOperator operator() {
    return new NativeGroupAggregateOperator(
        new int[] {0}, // SUM
        new int[] {0}, // bigint value
        new int[] {1}, // value column
        new int[] {0}, // group by key column
        true, // emit UPDATE_BEFORE
        INPUT,
        OUTPUT,
        8);
  }

  @Test
  void emitsInsertThenUpdateChangelog() throws Exception {
    try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(operator())) {
      harness.open();
      harness.processElement(new StreamRecord<>(row(1, 10)));
      harness.processElement(new StreamRecord<>(row(1, 20)));
      harness.processElement(new StreamRecord<>(row(2, 5)));
      harness.endInput();

      assertEquals(
          List.of(
              change(RowKind.INSERT, 1, 10),
              change(RowKind.UPDATE_BEFORE, 1, 10),
              change(RowKind.UPDATE_AFTER, 1, 30),
              change(RowKind.INSERT, 2, 5)),
          collect(harness));
    }
  }

  // A key's running sum survives a checkpoint: after restore, a new row for that key updates
  // (UPDATE_BEFORE/UPDATE_AFTER from the restored total) rather than re-inserting.
  @Test
  void runningStateSurvivesCheckpoint() throws Exception {
    OperatorSubtaskState snapshot;
    try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(operator())) {
      harness.open();
      harness.processElement(new StreamRecord<>(row(1, 10)));
      // Drains the buffer (emitting the INSERT) before the barrier, then snapshots native state.
      harness.prepareSnapshotPreBarrier(1L);
      snapshot = harness.snapshot(1L, 1L);
      assertEquals(List.of(change(RowKind.INSERT, 1, 10)), collect(harness));
    }

    try (OneInputStreamOperatorTestHarness<RowData, RowData> restored =
        new OneInputStreamOperatorTestHarness<>(operator())) {
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(new StreamRecord<>(row(1, 5)));
      restored.endInput();
      assertEquals(
          List.of(change(RowKind.UPDATE_BEFORE, 1, 10), change(RowKind.UPDATE_AFTER, 1, 15)),
          collect(restored));
    }
  }

  private static RowData row(long key, long value) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, key);
    row.setField(1, value);
    return row;
  }

  /** (kind, key, sum) for comparison. */
  private static List<Object> change(RowKind kind, long key, long sum) {
    return List.of(kind, key, sum);
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
