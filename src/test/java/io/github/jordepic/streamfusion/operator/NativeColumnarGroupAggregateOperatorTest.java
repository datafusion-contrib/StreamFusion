package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
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

/** The columnar GROUP BY operator: Arrow batches in, a changelog of Arrow batches out. */
class NativeColumnarGroupAggregateOperatorTest {

  private static final RowType INPUT =
      RowType.of(new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"key", "value"});
  private static final RowType OUTPUT =
      RowType.of(new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"key", "sum"});

  private static NativeColumnarGroupAggregateOperator operator() {
    return new NativeColumnarGroupAggregateOperator(
        new int[] {0},
        new int[] {0},
        new int[] {1},
        new int[] {0},
        new int[] {-1},
        new int[] {-1},
        new int[] {-1},
        true);
  }

  @Test
  void emitsChangelogFromArrowBatches() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator(), new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10), row(1, 20), row(2, 5))));
      assertEquals(
          List.of(
              change(RowKind.INSERT, 1, 10),
              change(RowKind.UPDATE_BEFORE, 1, 10),
              change(RowKind.UPDATE_AFTER, 1, 30),
              change(RowKind.INSERT, 2, 5)),
          collect(harness));
    }
  }

  @Test
  void runningStateSurvivesCheckpoint() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator(), new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10))));
      snapshot = harness.snapshot(1L, 1L);
      assertEquals(List.of(change(RowKind.INSERT, 1, 10)), collect(harness));
    }
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> restored =
            new OneInputStreamOperatorTestHarness<>(operator(), new ArrowBatchSerializer())) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(new StreamRecord<>(batch(allocator, row(1, 5))));
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

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    // Insert-only input: no $row_kind$ column, so the native side reads every row as an INSERT.
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), INPUT, allocator, false));
  }

  private static List<Object> change(RowKind kind, long key, long sum) {
    return List.of(kind, key, sum);
  }

  private static List<List<Object>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData r : RowDataArrowConverter.read(root, OUTPUT)) {
            rows.add(List.of(r.getRowKind(), r.getLong(0), r.getLong(1)));
          }
        }
      }
    }
    return rows;
  }
}
