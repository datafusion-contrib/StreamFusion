package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import tech.streamfusion.planner.FlinkKeyGroupUtils;
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
import org.junit.jupiter.api.extension.ExtendWith;

/** The columnar Top-N operator: Arrow batches in, a changelog of Arrow batches out. */
@ExtendWith(CoalescingOff.class)
class NativeColumnarTopNOperatorTest {

  private static final int MAX_PARALLELISM = 128;

  // [p (partition), s (sort key)]; output is the same row (no rank column).
  private static final RowType SCHEMA =
      RowType.of(new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"p", "s"});

  private static NativeColumnarTopNOperator operator() {
    return operator(0);
  }

  private static NativeColumnarTopNOperator operator(long stateTtlMillis) {
    return new NativeColumnarTopNOperator(
        new int[] {0},
        new int[] {-1},
        SCHEMA,
        new int[] {1},
        new int[] {1},
        new int[] {0},
        0L,
        2L,
        false,
        false,
        null,
        null,
        false,
        -1,
        stateTtlMillis,
        MAX_PARALLELISM);
  }

  private static NativeColumnarTopNOperator netDiffOperator(long miniBatchSize) {
    return new NativeColumnarTopNOperator(
        new int[] {0},
        new int[] {-1},
        SCHEMA,
        new int[] {1},
        new int[] {1},
        new int[] {0},
        0L,
        2L,
        false,
        false,
        null,
        null,
        true,
        miniBatchSize,
        0,
        MAX_PARALLELISM);
  }

  private static NativeColumnarTopNOperator retractingNetDiffOperator(long miniBatchSize) {
    return new NativeColumnarTopNOperator(
        new int[] {0},
        new int[] {-1},
        SCHEMA,
        new int[] {1},
        new int[] {1},
        new int[] {0},
        0L,
        2L,
        false,
        true,
        null,
        null,
        true,
        miniBatchSize,
        0,
        MAX_PARALLELISM);
  }

  @Test
  void emitsTopNChangelogFromArrowBatches() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness = harness()) {
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
  void coalescesAcrossPhysicalBatchesUntilTheLogicalCountBoundary() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(netDiffOperator(4), 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 5), row(1, 3))));
      assertEquals(List.of(), collect(harness));

      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 8), row(1, 1))));
      assertEquals(
          List.of(change(RowKind.INSERT, 1, 1), change(RowKind.INSERT, 1, 3)),
          collect(harness));
    }
  }

  @Test
  void splitsOnePhysicalBatchAcrossLogicalTopNBoundaries() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(netDiffOperator(2), 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(
          new StreamRecord<>(
              batch(allocator, row(1, 5), row(1, 3), row(1, 8), row(1, 1), row(1, 2))));

      assertEquals(
          List.of(
              change(RowKind.INSERT, 1, 3),
              change(RowKind.INSERT, 1, 5),
              change(RowKind.DELETE, 1, 5),
              change(RowKind.INSERT, 1, 1)),
          collect(harness));

      ((NativeColumnarTopNOperator) harness.getOneInputOperator()).finish();
      assertEquals(
          List.of(change(RowKind.DELETE, 1, 3), change(RowKind.INSERT, 1, 2)),
          collect(harness));
    }
  }

  @Test
  void retractingTopNCoalescesRankChurnAcrossPhysicalBatches() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(retractingNetDiffOperator(4), 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(batch(allocator, row(1, 10), row(1, 20), row(1, 30), row(1, 40))));
      assertEquals(
          List.of(change(RowKind.INSERT, 1, 10), change(RowKind.INSERT, 1, 20)),
          collect(harness));

      harness.processElement(
          new StreamRecord<>(
              changelogBatch(allocator, row(RowKind.DELETE, 1, 10), row(RowKind.INSERT, 1, 5))));
      assertEquals(List.of(), collect(harness));
      harness.processElement(
          new StreamRecord<>(
              changelogBatch(
                  allocator, row(RowKind.INSERT, 1, 100), row(RowKind.DELETE, 1, 100))));
      assertEquals(
          List.of(change(RowKind.INSERT, 1, 5), change(RowKind.DELETE, 1, 10)),
          collect(harness));
    }
  }

  @Test
  void topNStateSurvivesCheckpoint() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness = harness()) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 5), row(1, 3)))); // top2 {3,5}
      snapshot = harness.snapshot(1L, 1L);
      collect(harness);
    }
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored = harness()) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(new StreamRecord<>(batch(allocator, row(1, 1)))); // displaces 5
      assertEquals(
          List.of(change(RowKind.DELETE, 1, 5), change(RowKind.INSERT, 1, 1)), collect(restored));
    }
  }

  @Test
  void ttlExpiresIdleRankStateSilently() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(operator(1000), 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(5000);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 5), row(1, 3))));
      assertEquals(
          List.of(change(RowKind.INSERT, 1, 5), change(RowKind.INSERT, 1, 3)), collect(harness));
      // ts 5000 + ttl 1000 <= 6000: expired exactly at the boundary — no DELETE is emitted, and
      // the worse 8 enters a fresh top-2 instead of being dropped at rank 3.
      harness.setProcessingTime(6000);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 8))));
      assertEquals(List.of(change(RowKind.INSERT, 1, 8)), collect(harness));
    }
  }

  @Test
  void ttlRefreshesOnEveryWrite() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(operator(1000), 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(5000);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 5))));
      collect(harness);
      // A byte-equal sort key joins the first 5's list, refreshing both (Flink rewrites the
      // whole sort-key list on insert).
      harness.setProcessingTime(5900);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 5))));
      collect(harness);
      // At 6800 the first write is long past its ttl but alive through the refresh: the top-2 is
      // still {5, 5}, so the 9 ranks third and emits nothing.
      harness.setProcessingTime(6800);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 9))));
      assertEquals(List.of(), collect(harness));
    }
  }

  @Test
  void ttlTimestampsSurviveSnapshotRestore() throws Exception {
    // Timestamps are absolute: expiry after a restore is timed from the original write.
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(operator(1000), 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(5000);
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 5), row(1, 3))));
      snapshot = harness.snapshot(1L, 1L);
      collect(harness);
    }
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            harness(operator(1000), 1, 0)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.setProcessingTime(5999);
      restored.processElement(new StreamRecord<>(batch(allocator, row(1, 4))));
      assertEquals(
          List.of(change(RowKind.DELETE, 1, 5), change(RowKind.INSERT, 1, 4)), collect(restored));
    }
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            harness(operator(1000), 1, 0)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.setProcessingTime(6000);
      restored.processElement(new StreamRecord<>(batch(allocator, row(1, 4))));
      assertEquals(List.of(change(RowKind.INSERT, 1, 4)), collect(restored));
    }
  }

  @Test
  void rawKeyedStateRescalesByFlinkKeyGroup() throws Exception {
    long[] keys = keysForBothSubtasks();
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before = harness()) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(
          new StreamRecord<>(batch(allocator, row(keys[0], 5), row(keys[1], 5))));
      snapshot = before.snapshot(1L, 1L);
      collect(before);
    }
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored0 =
            harness(2, 0);
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored1 =
            harness(2, 1)) {
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
            new StreamRecord<>(batch(allocator, destinationForKey(key), row(key, 1), row(key, 2))));
      }
      List<List<Object>> actual = new ArrayList<>();
      actual.addAll(collect(restored0));
      actual.addAll(collect(restored1));
      actual.sort(java.util.Comparator.comparing(row -> (Long) row.get(1)));
      List<List<Object>> expected =
          new ArrayList<>(
              List.of(
                  change(RowKind.INSERT, keys[0], 1),
                  change(RowKind.DELETE, keys[0], 5),
                  change(RowKind.INSERT, keys[0], 2),
                  change(RowKind.INSERT, keys[1], 1),
                  change(RowKind.DELETE, keys[1], 5),
                  change(RowKind.INSERT, keys[1], 2)));
      expected.sort(java.util.Comparator.comparing(row -> (Long) row.get(1)));
      assertEquals(expected, actual);
    }
  }

  /** A parallelism-one restore receives several raw key-group streams, not just one per task. */
  @Test
  void rawKeyedStateRestoresMultipleKeyGroupsInOneTask() throws Exception {
    long[] keys = keysForBothSubtasks();
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before = harness()) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(
          new StreamRecord<>(batch(allocator, row(keys[0], 5), row(keys[1], 5))));
      snapshot = before.snapshot(1L, 1L);
      collect(before);
    }
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored = harness()) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(
          new StreamRecord<>(
              batch(
                  allocator,
                  row(keys[0], 1),
                  row(keys[0], 2),
                  row(keys[1], 1),
                  row(keys[1], 2))));
      List<List<Object>> actual = collect(restored);
      actual.sort(java.util.Comparator.comparing(row -> (Long) row.get(1)));
      List<List<Object>> expected =
          new ArrayList<>(
              List.of(
                  change(RowKind.INSERT, keys[0], 1),
                  change(RowKind.DELETE, keys[0], 5),
                  change(RowKind.INSERT, keys[0], 2),
                  change(RowKind.INSERT, keys[1], 1),
                  change(RowKind.DELETE, keys[1], 5),
                  change(RowKind.INSERT, keys[1], 2)));
      expected.sort(java.util.Comparator.comparing(row -> (Long) row.get(1)));
      assertEquals(expected, actual);
    }
  }

  // Update-fast: [p (partition), k (unique key), s (sort key)]; the bounded state keeps only the
  // top-N and a record replaces its unique key's previous version.
  private static final RowType UPDATE_FAST_SCHEMA =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new BigIntType()},
          new String[] {"p", "k", "s"});

  private static NativeColumnarTopNOperator updateFastOperator() {
    return updateFastOperator(0);
  }

  private static NativeColumnarTopNOperator updateFastOperator(long stateTtlMillis) {
    return new NativeColumnarTopNOperator(
        new int[] {0},
        new int[] {-1},
        UPDATE_FAST_SCHEMA,
        new int[] {2},
        new int[] {1},
        new int[] {0},
        0L,
        2L,
        false,
        false,
        new int[] {0, 1},
        new int[] {-1, -1},
        false,
        -1,
        stateTtlMillis,
        MAX_PARALLELISM);
  }

  /**
   * The update-fast ranker's raw snapshot must carry the unique-key bytes: after restore, a new
   * version of a buffered row must replace it (found by row key). Were the row key lost, the
   * record would enter as a second row and evict the true rank-2 occupant instead.
   */
  @Test
  void updateFastStateSurvivesCheckpoint() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(updateFastOperator(), 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(updateFastBatch(allocator, row3(1, 1, 5), row3(1, 2, 3))));
      snapshot = harness.snapshot(1L, 1L);
      collect3(harness);
    }
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            harness(updateFastOperator(), 1, 0)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(new StreamRecord<>(updateFastBatch(allocator, row3(1, 2, 2))));
      assertEquals(
          List.of(change3(RowKind.INSERT, 1, 2, 2), change3(RowKind.DELETE, 1, 2, 3)),
          collect3(restored));
    }
  }

  @Test
  void ttlExpiredUpdateFastEntryReinsertsFresh() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(updateFastOperator(1000), 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(5000);
      harness.processElement(new StreamRecord<>(updateFastBatch(allocator, row3(1, 1, 5))));
      assertEquals(List.of(change3(RowKind.INSERT, 1, 1, 5)), collect3(harness));
      // Key 1's entry expired: its new version is a fresh insert — un-expired, this update would
      // also retract the version-5 payload.
      harness.setProcessingTime(6000);
      harness.processElement(new StreamRecord<>(updateFastBatch(allocator, row3(1, 1, 9))));
      assertEquals(List.of(change3(RowKind.INSERT, 1, 1, 9)), collect3(harness));
    }
  }

  private static RowData row3(long partition, long key, long sort) {
    GenericRowData row = new GenericRowData(3);
    row.setField(0, partition);
    row.setField(1, key);
    row.setField(2, sort);
    return row;
  }

  private static ArrowBatch updateFastBatch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(
        RowDataArrowConverter.write(List.of(rows), UPDATE_FAST_SCHEMA, allocator, true));
  }

  private static List<Object> change3(RowKind kind, long partition, long key, long sort) {
    return List.of(kind, partition, key, sort);
  }

  private static List<List<Object>> collect3(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData r : RowDataArrowConverter.read(root, UPDATE_FAST_SCHEMA)) {
            rows.add(List.of(r.getRowKind(), r.getLong(0), r.getLong(1), r.getLong(2)));
          }
        }
      }
    }
    return rows;
  }

  private static RowData row(long partition, long sort) {
    return row(RowKind.INSERT, partition, sort);
  }

  private static RowData row(RowKind kind, long partition, long sort) {
    GenericRowData row = new GenericRowData(2);
    row.setRowKind(kind);
    row.setField(0, partition);
    row.setField(1, sort);
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator, false));
  }

  private static ArrowBatch changelogBatch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator, true));
  }

  private static ArrowBatch batch(BufferAllocator allocator, int destination, RowData... rows) {
    return new ArrowBatch(
        RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator, false), destination);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness()
      throws Exception {
    return harness(1, 0);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      int parallelism, int subtask) throws Exception {
    return harness(operator(), parallelism, subtask);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      NativeColumnarTopNOperator operator, int parallelism, int subtask) throws Exception {
    int[] stateKeys = FlinkKeyGroupUtils.stateKeysForSubtasks(MAX_PARALLELISM, parallelism);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator,
        batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0],
        Types.INT,
        MAX_PARALLELISM,
        parallelism,
        subtask);
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
}
