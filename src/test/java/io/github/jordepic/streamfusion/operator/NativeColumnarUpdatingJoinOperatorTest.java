package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

/** The columnar updating INNER join: Arrow batches in on both sides, a changelog of batches out. */
class NativeColumnarUpdatingJoinOperatorTest {

  private static final int MAX_PARALLELISM = 128;

  private static final RowType LEFT =
      RowType.of(new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"k", "lv"});
  private static final RowType RIGHT =
      RowType.of(new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"k", "rv"});
  // Join output: left columns then right columns.
  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType()},
          new String[] {"k", "lv", "k0", "rv"});

  private static NativeColumnarUpdatingJoinOperator operator() {
    return rawKeyedOperator();
  }

  private static NativeColumnarUpdatingJoinOperator rawKeyedOperator() {
    return rawKeyedOperator(false, -1);
  }

  private static NativeColumnarUpdatingJoinOperator rawKeyedOperator(
      boolean miniBatch, long miniBatchSize) {
    return new NativeColumnarUpdatingJoinOperator(
        new int[] {0},
        new int[] {0},
        0,
        LEFT,
        RIGHT,
        new int[0],
        new int[0],
        new int[0],
        new long[0],
        new double[0],
        new String[0],
        NativeUdf.Binding.EMPTY,
        new int[] {-1},
        miniBatch,
        miniBatchSize,
        MAX_PARALLELISM);
  }

  @Test
  void emitsMatchesAndRetractsFromArrowBatches() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> harness =
            rawKeyedHarness()) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      // Right (k=1, rv=100), then left (k=1, lv=10) → +I match; then retract the left → -D match.
      harness.processElement2(new StreamRecord<>(batch(allocator, RIGHT, row(RowKind.INSERT, 1, 100))));
      harness.processElement1(new StreamRecord<>(batch(allocator, LEFT, row(RowKind.INSERT, 1, 10))));
      harness.processElement1(new StreamRecord<>(batch(allocator, LEFT, row(RowKind.DELETE, 1, 10))));
      assertEquals(
          List.of(change(RowKind.INSERT, 1, 10, 1, 100), change(RowKind.DELETE, 1, 10, 1, 100)),
          collect(harness));
    }
  }

  @Test
  void uniqueInputsShareOneLogicalCountBoundaryAcrossBothSides() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> harness =
            new KeyedTwoInputStreamOperatorTestHarness<>(
                rawKeyedOperator(true, 4),
                batch -> 0,
                batch -> 0,
                Types.INT,
                MAX_PARALLELISM,
                1,
                0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement2(
          new StreamRecord<>(batch(allocator, RIGHT, row(RowKind.INSERT, 1, 100))));
      assertEquals(List.of(), collect(harness));
      harness.processElement1(
          new StreamRecord<>(
              batch(
                  allocator,
                  LEFT,
                  row(RowKind.INSERT, 1, 10),
                  row(RowKind.DELETE, 1, 10),
                  row(RowKind.INSERT, 1, 20))));
      assertEquals(List.of(change(RowKind.INSERT, 1, 20, 1, 100)), collect(harness));
    }
  }

  @Test
  void uniqueInputsFlushBeforeTheCombinedWatermark() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> harness =
            new KeyedTwoInputStreamOperatorTestHarness<>(
                rawKeyedOperator(true, 10),
                batch -> 0,
                batch -> 0,
                Types.INT,
                MAX_PARALLELISM,
                1,
                0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement2(
          new StreamRecord<>(batch(allocator, RIGHT, row(RowKind.INSERT, 1, 100))));
      harness.processElement1(
          new StreamRecord<>(batch(allocator, LEFT, row(RowKind.INSERT, 1, 20))));
      harness.processWatermark1(new Watermark(5));
      assertEquals(List.of(), collect(harness));
      harness.processWatermark2(new Watermark(5));
      assertEquals(List.of(change(RowKind.INSERT, 1, 20, 1, 100)), collect(harness));
    }
  }

  @Test
  void rawKeyedStateRestoresAllNativeJoinKeyGroups() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> before =
            rawKeyedHarness()) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement2(
          new StreamRecord<>(
              batch(allocator, RIGHT, row(RowKind.INSERT, 1, 100), row(RowKind.INSERT, 2, 200))));
      snapshot = before.snapshot(1L, 1L);
      collect(before);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> restored =
            rawKeyedHarness()) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement1(
          new StreamRecord<>(
              batch(allocator, LEFT, row(RowKind.INSERT, 1, 10), row(RowKind.INSERT, 2, 20))));
      assertEquals(
          List.of(change(RowKind.INSERT, 1, 10, 1, 100), change(RowKind.INSERT, 2, 20, 2, 200)),
          collect(restored));
    }
  }

  private static KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      rawKeyedHarness() throws Exception {
    return new KeyedTwoInputStreamOperatorTestHarness<>(
        rawKeyedOperator(), batch -> 0, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static RowData row(RowKind kind, long key, long value) {
    GenericRowData row = new GenericRowData(2);
    row.setRowKind(kind);
    row.setField(0, key);
    row.setField(1, value);
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowType schema, RowData... rows) {
    // Carry the kind: the inputs are changelogs (the join consumes -D/-U).
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), schema, allocator, true));
  }

  private static List<Object> change(RowKind kind, long lk, long lv, long rk, long rv) {
    return List.of(kind, lk, lv, rk, rv);
  }

  private static List<List<Object>> collect(
      TwoInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData r : RowDataArrowConverter.read(root, OUTPUT)) {
            rows.add(List.of(r.getRowKind(), r.getLong(0), r.getLong(1), r.getLong(2), r.getLong(3)));
          }
        }
      }
    }
    return rows;
  }
}
