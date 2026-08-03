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
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;

/**
 * The window-rank operator keeps the top-N rows by the order key per window and emits them — with
 * the 1-based rank number — when a watermark closes the window, then evicts it. A row for an
 * already-closed window is dropped as late. Uses UTC so the rendered window bounds are deterministic.
 */
class NativeColumnarWindowRankOperatorTest {

  private static final int MAX_PARALLELISM = 128;

  private static final org.apache.flink.table.types.logical.RowType TEST_ROW_TYPE =
      org.apache.flink.table.types.logical.RowType.of(
          new org.apache.flink.table.types.logical.BigIntType(),
          new org.apache.flink.table.types.logical.LocalZonedTimestampType(3),
          new org.apache.flink.table.types.logical.LocalZonedTimestampType(3));

  private static final org.apache.flink.table.types.logical.RowType KEYED_ROW_TYPE =
      org.apache.flink.table.types.logical.RowType.of(
          new org.apache.flink.table.types.logical.BigIntType(),
          new org.apache.flink.table.types.logical.BigIntType(),
          new org.apache.flink.table.types.logical.LocalZonedTimestampType(3),
          new org.apache.flink.table.types.logical.LocalZonedTimestampType(3));

  // [v BIGINT, window_start TIMESTAMP_LTZ(3), window_end TIMESTAMP_LTZ(3)]; window cols at 1 and 2.
  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new LocalZonedTimestampType(3), new LocalZonedTimestampType(3)
          },
          new String[] {"v", "window_start", "window_end"});

  // Output appends the rank number (w0$o0).
  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new TimestampType(3), new TimestampType(3), new BigIntType()
          },
          new String[] {"v", "window_start", "window_end", "w0$o0"});

  private static final RowType KEYED_SCHEMA =
      RowType.of(
          new LogicalType[] {
            new BigIntType(),
            new BigIntType(),
            new LocalZonedTimestampType(3),
            new LocalZonedTimestampType(3)
          },
          new String[] {"p", "v", "window_start", "window_end"});

  private static final RowType KEYED_OUTPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(),
            new BigIntType(),
            new TimestampType(3),
            new TimestampType(3),
            new BigIntType()
          },
          new String[] {"p", "v", "window_start", "window_end", "w0$o0"});

  @Test
  void emitsTopNWithRankNumberOnWindowClose() throws Exception {
    NativeColumnarWindowRankOperator operator =
        new NativeColumnarWindowRankOperator(
            1, 2, new int[0], new int[0], new int[] {0}, new int[] {0}, new int[] {0}, 2, true,
            "UTC", false, 0, 0, false, TEST_ROW_TYPE, MAX_PARALLELISM);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(operator)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      // Window [0,1000): v 10/30/20 -> top-2 by v DESC = 30 (rank 1), 20 (rank 2).
      // Window [1000,2000): v 50/40 -> top-2 = 50 (rank 1), 40 (rank 2).
      harness.processElement(
          new StreamRecord<>(
              batch(
                  allocator,
                  row(10, 0, 1000),
                  row(30, 0, 1000),
                  row(20, 0, 1000),
                  row(50, 1000, 2000),
                  row(40, 1000, 2000))));

      // Watermark 1000 closes only window [0,1000).
      harness.processWatermark(new Watermark(1000));
      assertEquals(List.of(ranked(30, 0, 1), ranked(20, 0, 2)), collect(harness));

      // A late row for the now-closed window is dropped; the next watermark closes [1000,2000).
      harness.processElement(new StreamRecord<>(batch(allocator, row(99, 0, 1000))));
      harness.processWatermark(new Watermark(2000));
      assertEquals(List.of(ranked(50, 1000, 1), ranked(40, 1000, 2)), collect(harness));
    }
  }

  /**
   * A proctime window rank (TUMBLE 1s) closes each window on a processing-time timer, not a
   * watermark. The upstream proctime TVF stamps window_start/window_end by the clock; here the rows
   * already carry those columns, and driving {@code setProcessingTime} past a window end fires the
   * timer that emits its top-N. Deterministic via the controlled clock (proctime is non-deterministic
   * in a real run — see the CLAUDE.md note).
   */
  @Test
  void emitsTopNOnProcessingTimeTimer() throws Exception {
    NativeColumnarWindowRankOperator operator =
        new NativeColumnarWindowRankOperator(
            1, 2, new int[0], new int[0], new int[] {0}, new int[] {0}, new int[] {0}, 2, true,
            "UTC", true, 1000, 1000, false, TEST_ROW_TYPE, MAX_PARALLELISM);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            harness(operator)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.setProcessingTime(500);
      harness.processElement(
          new StreamRecord<>(
              batch(allocator, row(10, 0, 1000), row(30, 0, 1000), row(20, 0, 1000))));
      assertEquals(List.of(), collect(harness)); // window [0,1000) still open at proctime 500
      harness.setProcessingTime(1000); // fires the window-end timer
      assertEquals(List.of(ranked(30, 0, 1), ranked(20, 0, 2)), collect(harness));
    }
  }

  @Test
  void rawKeyedProctimeRankRearmsTimerAfterRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before =
            harness(proctimeOperator())) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.setProcessingTime(500);
      before.processElement(
          new StreamRecord<>(batch(allocator, row(10, 0, 1000), row(30, 0, 1000))));
      snapshot = before.snapshot(1L, 1L);
      collect(before);
    }

    try (KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
        harness(proctimeOperator())) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      restored.setProcessingTime(1000);
      assertEquals(List.of(ranked(30, 0, 1), ranked(10, 0, 2)), collect(restored));
    }
  }

  /** Window Rank state follows its partition's exact Flink key group through a 1 -> 2 rescale. */
  @Test
  void rawKeyedStateRescalesByPartition() throws Exception {
    long[] keys = keysForBothSubtasks();
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before =
            keyedHarness(1, 0)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(
          new StreamRecord<>(
              keyedBatch(
                  allocator,
                  keyedRow(keys[0], 5, 0, 1000),
                  keyedRow(keys[0], 3, 0, 1000),
                  keyedRow(keys[1], 5, 0, 1000),
                  keyedRow(keys[1], 3, 0, 1000))));
      snapshot = before.snapshot(1L, 1L);
      keyedCollect(before);
    }

    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored0 =
            keyedHarness(2, 0);
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored1 =
            keyedHarness(2, 1)) {
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
        int destination = destinationForKey(key);
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            destination == 0 ? restored0 : restored1;
        restored.processElement(
            new StreamRecord<>(keyedBatch(allocator, destination, keyedRow(key, 10, 0, 1000))));
      }
      restored0.processWatermark(new Watermark(1000));
      restored1.processWatermark(new Watermark(1000));
      List<List<Long>> actual = new ArrayList<>();
      actual.addAll(keyedCollect(restored0));
      actual.addAll(keyedCollect(restored1));
      actual.sort(
          java.util.Comparator.comparing((List<Long> row) -> row.get(0)).thenComparing(row -> row.get(3)));
      List<List<Long>> expected =
          new ArrayList<>(
              List.of(
                  keyedRanked(keys[0], 10, 0, 1),
                  keyedRanked(keys[0], 5, 0, 2),
                  keyedRanked(keys[1], 10, 0, 1),
                  keyedRanked(keys[1], 5, 0, 2)));
      expected.sort(
          java.util.Comparator.comparing((List<Long> row) -> row.get(0)).thenComparing(row -> row.get(3)));
      assertEquals(expected, actual);
    }
  }

  private static RowData row(long v, long startMillis, long endMillis) {
    GenericRowData row = new GenericRowData(3);
    row.setField(0, v);
    row.setField(1, TimestampData.fromEpochMillis(startMillis));
    row.setField(2, TimestampData.fromEpochMillis(endMillis));
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator));
  }

  private static RowData keyedRow(long partition, long value, long startMillis, long endMillis) {
    GenericRowData row = new GenericRowData(4);
    row.setField(0, partition);
    row.setField(1, value);
    row.setField(2, TimestampData.fromEpochMillis(startMillis));
    row.setField(3, TimestampData.fromEpochMillis(endMillis));
    return row;
  }

  private static ArrowBatch keyedBatch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), KEYED_SCHEMA, allocator));
  }

  private static ArrowBatch keyedBatch(BufferAllocator allocator, int destination, RowData... rows) {
    return new ArrowBatch(
        RowDataArrowConverter.write(List.of(rows), KEYED_SCHEMA, allocator), destination);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness(
      NativeColumnarWindowRankOperator operator) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static NativeColumnarWindowRankOperator proctimeOperator() {
    return new NativeColumnarWindowRankOperator(
        1,
        2,
        new int[0],
        new int[0],
        new int[] {0},
        new int[] {0},
        new int[] {0},
        2,
        true,
        "UTC",
        true,
        1000,
        1000,
        false,
        TEST_ROW_TYPE,
        MAX_PARALLELISM);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> keyedHarness(
      int parallelism, int subtask) throws Exception {
    int[] stateKeys = FlinkKeyGroupUtils.stateKeysForSubtasks(MAX_PARALLELISM, parallelism);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new NativeColumnarWindowRankOperator(
            2,
            3,
            new int[] {0},
            new int[] {-1},
            new int[] {1},
            new int[] {0},
            new int[] {0},
            2,
            true,
            "UTC",
            false,
            0,
            0,
            false,
            KEYED_ROW_TYPE,
            MAX_PARALLELISM),
        batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0],
        Types.INT,
        MAX_PARALLELISM,
        parallelism,
        subtask);
  }

  private static List<Long> ranked(long v, long windowStartMillis, long rank) {
    return List.of(v, windowStartMillis, rank);
  }

  private static List<List<Long>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData r : RowDataArrowConverter.read(root, OUTPUT)) {
            rows.add(List.of(r.getLong(0), r.getTimestamp(1, 3).getMillisecond(), r.getLong(3)));
          }
        }
      }
    }
    return rows;
  }

  private static List<Long> keyedRanked(long partition, long value, long windowStartMillis, long rank) {
    return List.of(partition, value, windowStartMillis, rank);
  }

  private static List<List<Long>> keyedCollect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData r : RowDataArrowConverter.read(root, KEYED_OUTPUT)) {
            rows.add(
                List.of(
                    r.getLong(0),
                    r.getLong(1),
                    r.getTimestamp(2, 3).getMillisecond(),
                    r.getLong(4)));
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
