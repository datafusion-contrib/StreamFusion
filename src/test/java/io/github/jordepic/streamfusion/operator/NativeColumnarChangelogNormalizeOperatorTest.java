package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

/**
 * The changelog normalizer keeps the last row per key and emits the normalized changelog: the first
 * row inserts, a changed row retracts the previous then appends the new (when generateUpdateBefore),
 * an unchanged row is suppressed, and a delete retracts the stored full row. This verifies the exact
 * emitted change sequence, which the SQL harness's collapsed comparison cannot see.
 */
class NativeColumnarChangelogNormalizeOperatorTest {

  private static final int MAX_PARALLELISM = 128;

  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"f0", "f1"});

  @Test
  void emitsNormalizedChangelog() throws Exception {
    List<List<Object>> out =
        run(
            operator(true),
            row(RowKind.INSERT, 1L, 10L), // first row for key 1 → +I
            row(RowKind.UPDATE_AFTER, 1L, 20L), // changed → -U(prev) +U(new)
            row(RowKind.INSERT, 2L, 5L), // first row for key 2 → +I
            row(RowKind.UPDATE_AFTER, 2L, 5L), // unchanged → suppressed
            row(RowKind.DELETE, 1L, 20L)); // delete key 1 → -D(stored row)

    assertEquals(
        List.of(
            List.of("+I", 1L, 10L),
            List.of("-U", 1L, 10L),
            List.of("+U", 1L, 20L),
            List.of("+I", 2L, 5L),
            List.of("-D", 1L, 20L)),
        out);
  }

  @Test
  void withoutUpdateBeforeOmitsRetraction() throws Exception {
    List<List<Object>> out =
        run(
            operator(false),
            row(RowKind.INSERT, 1L, 10L),
            row(RowKind.UPDATE_AFTER, 1L, 20L)); // changed → +U only (no -U)

    assertEquals(List.of(List.of("+I", 1L, 10L), List.of("+U", 1L, 20L)), out);
  }

  @Test
  void coalescesAcrossPhysicalBatchesAtTheExactLogicalCount() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(operator(true, true, 4), 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(
              batch(
                  allocator,
                  row(RowKind.INSERT, 1L, 10L),
                  row(RowKind.UPDATE_AFTER, 1L, 20L))));
      assertEquals(List.of(), collect(harness));
      harness.processElement(
          new StreamRecord<>(
              batch(
                  allocator,
                  row(RowKind.INSERT, 2L, 5L),
                  row(RowKind.UPDATE_AFTER, 1L, 30L))));
      assertEquals(
          List.of(List.of("+I", 1L, 30L), List.of("+I", 2L, 5L)), collect(harness));
    }
  }

  @Test
  void watermarkFlushesPendingChangesBeforeItIsForwarded() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(operator(true, true, 100), 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(
              batch(
                  allocator,
                  row(RowKind.INSERT, 1L, 10L),
                  row(RowKind.UPDATE_AFTER, 1L, 20L))));
      assertEquals(List.of(), collect(harness));
      harness.processWatermark(new org.apache.flink.streaming.api.watermark.Watermark(42L));
      assertEquals(List.of(List.of("+I", 1L, 20L)), collect(harness));
    }
  }

  @Test
  void checkpointFlushesBeforeSnapshotAndRestoreContinuesFromDurableState() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before =
            harness(operator(true, true, 100), 1, 0)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(
          new StreamRecord<>(
              batch(
                  allocator,
                  row(RowKind.INSERT, 1L, 10L),
                  row(RowKind.UPDATE_AFTER, 1L, 20L))));
      assertEquals(List.of(), collect(before));
      before.prepareSnapshotPreBarrier(1L);
      assertEquals(List.of(List.of("+I", 1L, 20L)), collect(before));
      snapshot = before.snapshot(1L, 1L);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            harness(operator(true, true, 100), 1, 0)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(
          new StreamRecord<>(batch(allocator, row(RowKind.UPDATE_AFTER, 1L, 30L))));
      assertEquals(List.of(), collect(restored));
      restored.processWatermark(new org.apache.flink.streaming.api.watermark.Watermark(42L));
      assertEquals(
          List.of(List.of("-U", 1L, 20L), List.of("+U", 1L, 30L)), collect(restored));
    }
  }

  @Test
  void rawKeyedStateRescalesByFlinkKeyGroup() throws Exception {
    long[] keys = keysForBothSubtasks();
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before =
            harness(true, 1, 0)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(
          new StreamRecord<>(batch(allocator, row(RowKind.INSERT, keys[0], 10L), row(RowKind.INSERT, keys[1], 20L))));
      snapshot = before.snapshot(1L, 1L);
      assertEquals(List.of(List.of("+I", keys[0], 10L), List.of("+I", keys[1], 20L)), collect(before));
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored0 =
            harness(true, 2, 0);
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored1 =
            harness(true, 2, 1)) {
      restored0.setup(new ArrowBatchSerializer());
      restored1.setup(new ArrowBatchSerializer());
      restored0.initializeState(
          AbstractStreamOperatorTestHarness.repartitionOperatorState(
              snapshot, MAX_PARALLELISM, 1, 2, 0));
      restored1.initializeState(
          AbstractStreamOperatorTestHarness.repartitionOperatorState(
              snapshot, MAX_PARALLELISM, 1, 2, 1));
      restored0.open();
      restored1.open();

      for (long key : keys) {
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> destination =
            destinationForKey(key) == 0 ? restored0 : restored1;
        destination.processElement(
            new StreamRecord<>(
                batch(
                    allocator,
                    destinationForKey(key),
                    row(RowKind.UPDATE_AFTER, key, key == keys[0] ? 11L : 21L))));
      }
      List<List<Object>> actual = new ArrayList<>();
      actual.addAll(collect(restored0));
      actual.addAll(collect(restored1));
      actual.sort(java.util.Comparator.comparing(row -> (Long) row.get(1)));
      List<List<Object>> expected =
          new ArrayList<>(
              List.of(
                  List.of("-U", keys[0], 10L),
                  List.of("+U", keys[0], 11L),
                  List.of("-U", keys[1], 20L),
                  List.of("+U", keys[1], 21L)));
      expected.sort(java.util.Comparator.comparing(row -> (Long) row.get(1)));
      assertEquals(expected, actual);
    }
  }

  private static GenericRowData row(RowKind kind, long f0, long f1) {
    GenericRowData row = new GenericRowData(2);
    row.setRowKind(kind);
    row.setField(0, f0);
    row.setField(1, f1);
    return row;
  }

  private static List<List<Object>> run(
      NativeColumnarChangelogNormalizeOperator operator, GenericRowData... rows) throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(operator, 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      for (GenericRowData row : rows) {
        harness.processElement(
            new StreamRecord<>(
                new ArrowBatch(
                    RowDataArrowConverter.write(List.of(row), SCHEMA, allocator, true))));
      }

      return collect(harness);
    }
  }

  private static NativeColumnarChangelogNormalizeOperator operator(boolean generateUpdateBefore) {
    return operator(generateUpdateBefore, false, 0);
  }

  private static NativeColumnarChangelogNormalizeOperator operator(
      boolean generateUpdateBefore, boolean miniBatch, long miniBatchSize) {
    return new NativeColumnarChangelogNormalizeOperator(
        new int[] {0},
        new int[] {-1},
        generateUpdateBefore,
        miniBatch,
        miniBatchSize,
        MAX_PARALLELISM);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      boolean generateUpdateBefore, int parallelism, int subtask) throws Exception {
    return harness(operator(generateUpdateBefore), parallelism, subtask);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      NativeColumnarChangelogNormalizeOperator operator, int parallelism, int subtask)
      throws Exception {
    int[] stateKeys = stateKeysForSubtasks(parallelism);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator,
        batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0],
        Types.INT,
        MAX_PARALLELISM,
        parallelism,
        subtask);
  }

  private static ArrowBatch batch(BufferAllocator allocator, GenericRowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator, true));
  }

  private static ArrowBatch batch(BufferAllocator allocator, int destination, GenericRowData... rows) {
    return new ArrowBatch(
        RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator, true), destination);
  }

  private static List<List<Object>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> emitted = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
        Object event = harness.getOutput().poll();
        if (event instanceof StreamRecord) {
          try (VectorSchemaRoot vsr = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
            for (RowData r : RowDataArrowConverter.read(vsr, SCHEMA)) {
              emitted.add(List.of(r.getRowKind().shortString(), r.getLong(0), r.getLong(1)));
            }
          }
        }
      }
    return emitted;
  }

  private static long[] keysForBothSubtasks() {
    long[] keys = new long[] {Long.MIN_VALUE, Long.MIN_VALUE};
    for (long candidate = 0;
        candidate < 10_000 && (keys[0] == Long.MIN_VALUE || keys[1] == Long.MIN_VALUE);
        candidate++) {
      int subtask = destinationForKey(candidate);
      if (keys[subtask] == Long.MIN_VALUE) {
        keys[subtask] = candidate;
      }
    }
    if (keys[0] == Long.MIN_VALUE || keys[1] == Long.MIN_VALUE) {
      throw new AssertionError("did not find one key for each rescaled subtask");
    }
    return keys;
  }

  private static int destinationForKey(long key) {
    GenericRowData keyRow = GenericRowData.of(key);
    int keyGroup =
        KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
            new org.apache.flink.table.runtime.typeutils.RowDataSerializer(
                    RowType.of(new BigIntType()))
                .toBinaryRow(keyRow)
                .hashCode(),
            MAX_PARALLELISM);
    return KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(MAX_PARALLELISM, 2, keyGroup);
  }

  private static int[] stateKeysForSubtasks(int parallelism) {
    int[] keys = new int[parallelism];
    boolean[] found = new boolean[parallelism];
    int remaining = parallelism;
    for (int candidate = 0; remaining > 0; candidate++) {
      int keyGroup =
          KeyGroupRangeAssignment.computeKeyGroupForKeyHash(candidate, MAX_PARALLELISM);
      int subtask =
          KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
              MAX_PARALLELISM, parallelism, keyGroup);
      if (!found[subtask]) {
        keys[subtask] = candidate;
        found[subtask] = true;
        remaining--;
      }
    }
    return keys;
  }
}
