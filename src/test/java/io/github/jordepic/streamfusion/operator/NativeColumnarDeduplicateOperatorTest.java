package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

/**
 * The keep-first deduplicate operator emits, per partition key, the minimum-rowtime row once the
 * watermark reaches that rowtime — not the first to arrive. After a key emits, later rows for it are
 * ignored, and a row arriving with a rowtime already below the watermark is dropped as late.
 */
class NativeColumnarDeduplicateOperatorTest {

  private static final int MAX_PARALLELISM = 128;

  // [k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3)]; partition key column 0, rowtime column 2.
  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"k", "v", "rt"});

  @Test
  void emitsMinimumRowtimeRowPerKeyOnWatermark() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            keepFirstHarness(1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      // key 1: rows at rt 2000, 0, 800 -> min-rowtime row is (v=20, rt=0). key 2: single (v=40, rt=1000).
      harness.processElement(
          new StreamRecord<>(
              batch(allocator, row(1, 30, 2000), row(2, 40, 1000), row(1, 20, 0), row(1, 25, 800))));

      // Watermark 1000 releases both keys' first rows (rt 0 and 1000 are <= 1000).
      harness.processWatermark(new Watermark(1000));
      assertEquals(List.of(emitted(1, 20), emitted(2, 40)), collect(harness));

      // A later row for the already-emitted key 1 is ignored; a row for key 3 below the watermark is
      // dropped as late; key 3's in-time row becomes its candidate.
      harness.processElement(
          new StreamRecord<>(
              batch(allocator, row(1, 99, 1500), row(3, 7, 300), row(3, 8, 1200))));
      harness.processWatermark(new Watermark(3000));
      assertEquals(List.of(emitted(3, 8)), collect(harness));
    }
  }

  @Test
  void eagerDeduplicationRawKeyedStateRescalesByFlinkKeyGroup() throws Exception {
    long[] keys = keysForBothSubtasks();
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before =
            eagerHarness(1, 0)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(
          new StreamRecord<>(batch(allocator, row(keys[0], 10, 1000), row(keys[1], 20, 1000))));
      snapshot = before.snapshot(1L, 1L);
      assertEquals(
          List.of(List.of("+I", keys[0], 10L), List.of("+I", keys[1], 20L)),
          collectChanges(before));
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored0 =
            eagerHarness(2, 0);
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored1 =
            eagerHarness(2, 1)) {
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
                    row(key, key == keys[0] ? 11 : 21, 2000))));
      }
      List<List<Object>> actual = new ArrayList<>();
      actual.addAll(collectChanges(restored0));
      actual.addAll(collectChanges(restored1));
      actual.sort(Comparator.comparing(row -> (Long) row.get(1)));
      List<List<Object>> expected =
          new ArrayList<>(
              List.of(
                  List.of("-U", keys[0], 10L),
                  List.of("+U", keys[0], 11L),
                  List.of("-U", keys[1], 20L),
                  List.of("+U", keys[1], 21L)));
      expected.sort(Comparator.comparing(row -> (Long) row.get(1)));
      assertEquals(expected, actual);
    }
  }

  @Test
  void keepLastCoalescesPhysicalBatchesAtTheLogicalBoundary() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            eagerHarness(1, 0, true, 4)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(batch(allocator, row(1, 10, 0), row(1, 20, 1))));
      assertEquals(List.of(), collectChanges(harness));
      harness.processElement(
          new StreamRecord<>(batch(allocator, row(2, 5, 0), row(1, 30, 2))));
      assertEquals(
          List.of(List.of("+I", 1L, 30L), List.of("+I", 2L, 5L)),
          collectChanges(harness));
    }
  }

  @Test
  void keepLastFlushesAnIncompleteLogicalBatchBeforeWatermark() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            eagerHarness(1, 0, true, 4)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(batch(allocator, row(1, 10, 0), row(1, 20, 1))));
      assertEquals(List.of(), collectChanges(harness));

      harness.processWatermark(new Watermark(1));
      assertEquals(List.of(List.of("+I", 1L, 20L)), collectChanges(harness));
    }
  }

  @Test
  void bufferedDeduplicationRawKeyedStateRescalesByFlinkKeyGroup() throws Exception {
    long[] keys = keysForBothSubtasks();
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before =
            keepFirstHarness(1, 0)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(
          new StreamRecord<>(batch(allocator, row(keys[0], 10, 1000), row(keys[1], 20, 2000))));
      snapshot = before.snapshot(1L, 1L);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored0 =
            keepFirstHarness(2, 0);
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored1 =
            keepFirstHarness(2, 1)) {
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
      restored0.processWatermark(new Watermark(3000));
      restored1.processWatermark(new Watermark(3000));
      List<List<Long>> actual = new ArrayList<>();
      actual.addAll(collect(restored0));
      actual.addAll(collect(restored1));
      actual.sort(Comparator.comparingLong(row -> row.get(0)));
      List<List<Long>> expected = new ArrayList<>(List.of(emitted(keys[0], 10), emitted(keys[1], 20)));
      expected.sort(Comparator.comparingLong(row -> row.get(0)));
      assertEquals(expected, actual);
    }
  }

  private static RowData row(long k, long v, long rtMillis) {
    GenericRowData row = new GenericRowData(3);
    row.setField(0, k);
    row.setField(1, v);
    row.setField(2, TimestampData.fromEpochMillis(rtMillis));
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator));
  }

  private static ArrowBatch batch(BufferAllocator allocator, int destination, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator), destination);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      eagerHarness(int parallelism, int subtask) throws Exception {
    return eagerHarness(parallelism, subtask, false, 0);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      eagerHarness(int parallelism, int subtask, boolean miniBatch, long miniBatchSize)
          throws Exception {
    int[] stateKeys = stateKeysForSubtasks(parallelism);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new NativeColumnarKeepLastDeduplicateOperator(
            new int[] {0},
            new int[] {-1},
            2,
            true,
            true,
            false,
            miniBatch,
            miniBatchSize,
            MAX_PARALLELISM),
        batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0],
        Types.INT,
        MAX_PARALLELISM,
        parallelism,
        subtask);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      keepFirstHarness(int parallelism, int subtask) throws Exception {
    int[] stateKeys = stateKeysForSubtasks(parallelism);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new NativeColumnarDeduplicateOperator(
            new int[] {0}, new int[] {-1}, 2, MAX_PARALLELISM),
        batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0],
        Types.INT,
        MAX_PARALLELISM,
        parallelism,
        subtask);
  }

  private static List<Long> emitted(long k, long v) {
    return List.of(k, v);
  }

  private static List<List<Long>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData r : RowDataArrowConverter.read(root, SCHEMA)) {
            rows.add(List.of(r.getLong(0), r.getLong(1)));
          }
        }
      }
    }
    rows.sort(Comparator.comparingLong(r -> r.get(0)));
    return rows;
  }

  private static List<List<Object>> collectChanges(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, SCHEMA)) {
            rows.add(List.of(row.getRowKind().shortString(), row.getLong(0), row.getLong(1)));
          }
        }
      }
    }
    return rows;
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
    int keyGroup =
        KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
            new org.apache.flink.table.runtime.typeutils.RowDataSerializer(
                    RowType.of(new BigIntType()))
                .toBinaryRow(GenericRowData.of(key))
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
