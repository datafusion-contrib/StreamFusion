package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

/** The columnar GROUP BY operator: Arrow batches in, a changelog of Arrow batches out. */
class NativeColumnarGroupAggregateOperatorTest {

  private static final int MAX_PARALLELISM = 128;

  private static final RowType INPUT =
      RowType.of(new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"key", "value"});
  private static final RowType OUTPUT =
      RowType.of(new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"key", "sum"});
  private static final RowType COMPLEX_INPUT =
      RowType.of(
          new LogicalType[] {
            new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType()), new BigIntType()
          },
          new String[] {"key", "value"});
  private static final RowType COMPLEX_OUTPUT =
      RowType.of(
          new LogicalType[] {
            new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType()), new BigIntType()
          },
          new String[] {"key", "sum"});

  private static NativeColumnarGroupAggregateOperator operator() {
    return operator(false, -1);
  }

  private static NativeColumnarGroupAggregateOperator operator(
      boolean miniBatch, long miniBatchSize) {
    return new NativeColumnarGroupAggregateOperator(
        new int[] {0},
        new int[] {0},
        new int[] {1},
        new int[] {0},
        new int[] {-1},
        new int[] {-1},
        new int[] {-1},
        -1,
        true,
        miniBatch,
        miniBatchSize,
        new int[] {-1},
        MAX_PARALLELISM);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness()
      throws Exception {
    return harness(1, 0);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      int parallelism, int subtask) throws Exception {
    int[] stateKeys = stateKeysForSubtasks(parallelism);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator(),
        batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0],
        Types.INT,
        MAX_PARALLELISM,
        parallelism,
        subtask);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      complexHarness() throws Exception {
    NativeColumnarGroupAggregateOperator operator =
        new NativeColumnarGroupAggregateOperator(
            new int[] {0},
            new int[] {0},
            new int[] {1},
            new int[] {0},
            new int[] {-1},
            new int[] {-1},
            new int[] {-1},
            -1,
            true,
            false,
            -1,
            new int[] {-1, -1, -1},
            MAX_PARALLELISM);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  @Test
  void emitsChangelogFromArrowBatches() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness = harness()) {
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
  void coalescesGroupsAcrossPhysicalBatchesAtTheLogicalBoundary() throws Exception {
    int[] stateKeys = stateKeysForSubtasks(1);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                operator(true, 4),
                batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0],
                Types.INT,
                MAX_PARALLELISM,
                1,
                0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10), row(2, 7))));
      assertEquals(List.of(), collect(harness));
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 5), row(1, 2))));
      assertEquals(
          List.of(change(RowKind.INSERT, 1, 17), change(RowKind.INSERT, 2, 7)),
          collect(harness));
    }
  }

  @Test
  void splitsPhysicalBatchesAndFlushesTheRemainderOnWatermark() throws Exception {
    int[] stateKeys = stateKeysForSubtasks(1);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                operator(true, 2),
                batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0],
                Types.INT,
                MAX_PARALLELISM,
                1,
                0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(
              batch(allocator, row(1, 10), row(1, 5), row(1, 2), row(2, 7), row(1, 3))));
      assertEquals(
          List.of(
              change(RowKind.INSERT, 1, 15),
              change(RowKind.UPDATE_BEFORE, 1, 15),
              change(RowKind.UPDATE_AFTER, 1, 17),
              change(RowKind.INSERT, 2, 7)),
          collect(harness));

      harness.processWatermark(new Watermark(100));
      assertEquals(
          List.of(
              change(RowKind.UPDATE_BEFORE, 1, 17),
              change(RowKind.UPDATE_AFTER, 1, 20)),
          collect(harness));
    }
  }

  @Test
  void runningStateSurvivesCheckpoint() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness = harness()) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10))));
      snapshot = harness.snapshot(1L, 1L);
      assertEquals(List.of(change(RowKind.INSERT, 1, 10)), collect(harness));
    }
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored = harness()) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(new StreamRecord<>(batch(allocator, row(1, 5))));
      assertEquals(
          List.of(change(RowKind.UPDATE_BEFORE, 1, 10), change(RowKind.UPDATE_AFTER, 1, 15)),
          collect(restored));
    }
  }

  @Test
  void miniBatchFlushesBeforeCheckpointAndRestoresOnlyDurableState() throws Exception {
    OperatorSubtaskState snapshot;
    int[] stateKeys = stateKeysForSubtasks(1);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                operator(true, 100),
                batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0],
                Types.INT,
                MAX_PARALLELISM,
                1,
                0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10))));
      harness.getOneInputOperator().prepareSnapshotPreBarrier(1L);
      snapshot = harness.snapshot(1L, 1L);
      assertEquals(List.of(change(RowKind.INSERT, 1, 10)), collect(harness));
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            new KeyedOneInputStreamOperatorTestHarness<>(
                operator(true, 100),
                batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0],
                Types.INT,
                MAX_PARALLELISM,
                1,
                0)) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(new StreamRecord<>(batch(allocator, row(1, 5))));
      restored.getOneInputOperator().finish();
      assertEquals(
          List.of(change(RowKind.UPDATE_BEFORE, 1, 10), change(RowKind.UPDATE_AFTER, 1, 15)),
          collect(restored));
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
          new StreamRecord<>(batch(allocator, row(keys[0], 10), row(keys[1], 20))));
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
            new StreamRecord<>(batch(allocator, destinationForKey(key), row(key, 1))));
      }
      List<List<Object>> changes = new ArrayList<>();
      changes.addAll(collect(restored0));
      changes.addAll(collect(restored1));
      java.util.Comparator<List<Object>> byKey = java.util.Comparator.comparing(row -> (Long) row.get(1));
      List<List<Object>> expected =
          new ArrayList<>(
              List.of(
                  change(RowKind.UPDATE_BEFORE, keys[0], 10),
                  change(RowKind.UPDATE_AFTER, keys[0], 11),
                  change(RowKind.UPDATE_BEFORE, keys[1], 20),
                  change(RowKind.UPDATE_AFTER, keys[1], 21)));
      expected.sort(byKey);
      assertEquals(expected, changes.stream().sorted(byKey).toList());
    }
  }

  @Test
  void mapKeySurvivesRawKeyedStateCheckpoint() throws Exception {
    OperatorSubtaskState snapshot;
    GenericMapData key = mapKey();
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            complexHarness()) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(complexBatch(allocator, complexRow(key, 10))));
      snapshot = harness.snapshot(1L, 1L);
      assertComplexChanges(
          List.of(complexChange(RowKind.INSERT, key, 10)), collectComplex(harness));
    }
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            complexHarness()) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(new StreamRecord<>(complexBatch(allocator, complexRow(mapKey(), 5))));
      assertComplexChanges(
          List.of(
              complexChange(RowKind.UPDATE_BEFORE, key, 10),
              complexChange(RowKind.UPDATE_AFTER, key, 15)),
          collectComplex(restored));
    }
  }

  private static long[] keysForBothSubtasks() {
    long[] keys = new long[] {Long.MIN_VALUE, Long.MIN_VALUE};
    for (long candidate = 0; candidate < 10_000 && (keys[0] == Long.MIN_VALUE || keys[1] == Long.MIN_VALUE); candidate++) {
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
    GenericRowData keyRow = new GenericRowData(1);
    keyRow.setField(0, key);
    int keyGroup =
        KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
            new org.apache.flink.table.runtime.typeutils.RowDataSerializer(RowType.of(new BigIntType()))
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

  private static RowData row(long key, long value) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, key);
    row.setField(1, value);
    return row;
  }

  private static GenericMapData mapKey() {
    LinkedHashMap<StringData, Integer> map = new LinkedHashMap<>();
    map.put(StringData.fromString("short"), 1);
    map.put(StringData.fromString("a-long-map-key"), 2);
    return new GenericMapData(map);
  }

  private static RowData complexRow(GenericMapData key, long value) {
    return GenericRowData.of(key, value);
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    // Insert-only input: no $row_kind$ column, so the native side reads every row as an INSERT.
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), INPUT, allocator, false));
  }

  private static ArrowBatch batch(BufferAllocator allocator, int destination, RowData... rows) {
    return new ArrowBatch(
        RowDataArrowConverter.write(List.of(rows), INPUT, allocator, false), destination);
  }

  private static ArrowBatch complexBatch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), COMPLEX_INPUT, allocator, false));
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

  private static RowData complexChange(RowKind kind, GenericMapData key, long sum) {
    GenericRowData row = GenericRowData.of(key, sum);
    row.setRowKind(kind);
    return row;
  }

  private static List<RowData> collectComplex(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<RowData> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          rows.addAll(RowDataArrowConverter.read(root, COMPLEX_OUTPUT));
        }
      }
    }
    return rows;
  }

  private static void assertComplexChanges(List<RowData> expected, List<RowData> actual) {
    assertEquals(expected.size(), actual.size());
    RowDataSerializer serializer = new RowDataSerializer(COMPLEX_OUTPUT);
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i).getRowKind(), actual.get(i).getRowKind());
      assertEquals(
          serializer.toBinaryRow(expected.get(i)), serializer.toBinaryRow(actual.get(i)), "change " + i);
    }
  }
}
