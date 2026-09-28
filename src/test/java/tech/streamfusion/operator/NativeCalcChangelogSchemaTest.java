package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class NativeCalcChangelogSchemaTest {
  private static final RowType TYPE = RowType.of(new IntType());

  @Test
  void unionInputsMayAlternatePresenceOfRowKind() throws Exception {
    var calc =
        new NativeCalcOperator(
            new int[] {0},
            new int[] {0},
            new int[] {0},
            new long[0],
            new double[0],
            new String[0],
            new int[] {0},
            -1,
            new String[] {"f0"},
            NativeUdf.Binding.EMPTY);
    try (var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(calc)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      List<RowKind> expected =
          List.of(RowKind.INSERT, RowKind.DELETE, RowKind.INSERT, RowKind.UPDATE_AFTER);
      for (int i = 0; i < expected.size(); i++) {
        var row = GenericRowData.of(i);
        row.setRowKind(expected.get(i));
        harness.processElement(
            new StreamRecord<>(
                new ArrowBatch(
                    RowDataArrowConverter.write(
                        List.of(row), TYPE, NativeAllocator.SHARED, i % 2 == 1))));
      }
      List<RowKind> actual = new ArrayList<>();
      for (Object record : harness.getOutput()) {
        if (record instanceof StreamRecord<?>) {
          try (var root = ((ArrowBatch) ((StreamRecord<?>) record).getValue()).root()) {
            var rows = RowDataArrowConverter.read(root, TYPE);
            assertEquals(actual.size(), rows.get(0).getInt(0));
            actual.add(rows.get(0).getRowKind());
          }
        }
      }
      assertEquals(expected, actual);
    }
  }

  @Test
  void transposeRetainsUnexpectedRetractionsAcrossBatchBoundaries() throws Exception {
    try (var harness =
        new OneInputStreamOperatorTestHarness<RowData, ArrowBatch>(
            new RowDataToArrowOperator(TYPE, 1, false, null))) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      List<RowKind> expected =
          List.of(
              RowKind.INSERT,
              RowKind.DELETE,
              RowKind.UPDATE_BEFORE,
              RowKind.UPDATE_AFTER,
              RowKind.INSERT);
      for (RowKind kind : expected) {
        var row = GenericRowData.of(5);
        row.setRowKind(kind);
        harness.processElement(new StreamRecord<>(row));
      }
      List<RowKind> actual = new ArrayList<>();
      for (Object record : harness.getOutput()) {
        if (record instanceof StreamRecord<?>) {
          try (var root = ((ArrowBatch) ((StreamRecord<?>) record).getValue()).root()) {
            actual.add(RowDataArrowConverter.read(root, TYPE).get(0).getRowKind());
          }
        }
      }
      assertEquals(expected, actual);
    }
  }
}
