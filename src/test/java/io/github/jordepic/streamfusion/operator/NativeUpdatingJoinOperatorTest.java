package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class NativeUpdatingJoinOperatorTest {

  private static final RowType LEFT =
      RowType.of(new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"k", "lv"});
  private static final RowType RIGHT =
      RowType.of(new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"k", "rv"});
  // Join output is left columns then right columns: [k, lv, k, rv].
  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType()},
          new String[] {"k", "lv", "k0", "rv"});

  private static NativeUpdatingJoinOperator operator() {
    return new NativeUpdatingJoinOperator(new int[] {0}, new int[] {0}, LEFT, RIGHT, OUTPUT, 8);
  }

  @Test
  void emitsMatchesAndRetractsOnChangelog() throws Exception {
    NativeUpdatingJoinOperator operator = operator();
    try (TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
        new TwoInputStreamOperatorTestHarness<>(operator)) {
      harness.open();

      harness.processElement2(new StreamRecord<>(row(RowKind.INSERT, 1, 100))); // right (k=1, rv=100)
      harness.processElement1(new StreamRecord<>(row(RowKind.INSERT, 1, 10))); // left (k=1, lv=10)
      operator.prepareSnapshotPreBarrier(1L); // drain buffers
      assertEquals(List.of(change(RowKind.INSERT, 1, 10, 1, 100)), collect(harness));

      // A right row on a different key never matches.
      harness.processElement2(new StreamRecord<>(row(RowKind.INSERT, 2, 200)));
      operator.prepareSnapshotPreBarrier(2L);
      assertEquals(List.of(), collect(harness));

      // Retracting the left row retracts the matched pair.
      harness.processElement1(new StreamRecord<>(row(RowKind.DELETE, 1, 10)));
      operator.prepareSnapshotPreBarrier(3L);
      assertEquals(List.of(change(RowKind.DELETE, 1, 10, 1, 100)), collect(harness));
    }
  }

  @Test
  void stateSurvivesCheckpoint() throws Exception {
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot;
    NativeUpdatingJoinOperator operator = operator();
    try (TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
        new TwoInputStreamOperatorTestHarness<>(operator)) {
      harness.open();
      harness.processElement2(new StreamRecord<>(row(RowKind.INSERT, 1, 100))); // buffer right
      operator.prepareSnapshotPreBarrier(1L);
      snapshot = harness.snapshot(1L, 1L);
    }

    NativeUpdatingJoinOperator restored = operator();
    try (TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
        new TwoInputStreamOperatorTestHarness<>(restored)) {
      harness.initializeState(snapshot);
      harness.open();
      // The right row survived; a matching left arrival joins it.
      harness.processElement1(new StreamRecord<>(row(RowKind.INSERT, 1, 10)));
      restored.prepareSnapshotPreBarrier(2L);
      assertEquals(List.of(change(RowKind.INSERT, 1, 10, 1, 100)), collect(harness));
    }
  }

  private static RowData row(RowKind kind, long key, long value) {
    GenericRowData row = new GenericRowData(2);
    row.setRowKind(kind);
    row.setField(0, key);
    row.setField(1, value);
    return row;
  }

  private static List<Object> change(RowKind kind, long lk, long lv, long rk, long rv) {
    return List.of(kind, lk, lv, rk, rv);
  }

  private static List<List<Object>> collect(
      TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        RowData r = (RowData) ((StreamRecord<?>) event).getValue();
        rows.add(List.of(r.getRowKind(), r.getLong(0), r.getLong(1), r.getLong(2), r.getLong(3)));
      }
    }
    return rows;
  }
}
