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

/** The columnar Top-N operator: Arrow batches in, a changelog of Arrow batches out. */
class NativeColumnarTopNOperatorTest {

  // [p (partition), s (sort key)]; output is the same row (no rank column).
  private static final RowType SCHEMA =
      RowType.of(new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"p", "s"});

  private static NativeColumnarTopNOperator operator() {
    return new NativeColumnarTopNOperator(
        new int[] {0}, new int[] {1}, new int[] {1}, new int[] {0}, 2L, false);
  }

  @Test
  void emitsTopNChangelogFromArrowBatches() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator(), new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      // s = 5, 3, 8 (dropped, rank 3), 1 (enters, displaces 5).
      harness.processElement(
          new StreamRecord<>(batch(allocator, row(1, 5), row(1, 3), row(1, 8), row(1, 1))));
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
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator(), new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 5), row(1, 3)))); // top2 {3,5}
      snapshot = harness.snapshot(1L, 1L);
      collect(harness);
    }
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> restored =
            new OneInputStreamOperatorTestHarness<>(operator(), new ArrowBatchSerializer())) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(new StreamRecord<>(batch(allocator, row(1, 1)))); // displaces 5
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

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator, false));
  }

  private static List<Object> change(RowKind kind, long partition, long sort) {
    return List.of(kind, partition, sort);
  }

  private static List<List<Object>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData r : RowDataArrowConverter.read(root, SCHEMA)) {
            rows.add(List.of(r.getRowKind(), r.getLong(0), r.getLong(1)));
          }
        }
      }
    }
    return rows;
  }
}
