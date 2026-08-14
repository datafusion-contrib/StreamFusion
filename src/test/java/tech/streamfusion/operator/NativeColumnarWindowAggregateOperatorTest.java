package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * The columnar window operator (Arrow-batch input) produces the same window aggregates the row-fed
 * operator did — proving the columnar input extraction (time nanos→millis, value, keys) and the
 * Arrow-batch emit (the window-result reshape, with the bounds rendered as session-local timestamps)
 * are correct. Output is now an Arrow batch shaped to the output row type; read it back with the
 * boundary converter, as the dedicated transpose operator does.
 */
class NativeColumnarWindowAggregateOperatorTest {

  private static final int MAX_PARALLELISM = 128;

  // Input schema [value BIGINT, rt TIMESTAMP_LTZ(3)]; output [total BIGINT, window_start, window_end].
  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"value", "rt"});

  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new TimestampType(3), new TimestampType(3)},
          new String[] {"total", "window_start", "window_end"});

  private static final RowType LEGACY_EVENT_OUTPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(),
            new TimestampType(3),
            new TimestampType(3),
            new LocalZonedTimestampType(3),
            new LocalZonedTimestampType(3)
          },
          new String[] {"total", "window_start", "window_end", "rowtime", "proctime"});

  private static final RowType LEGACY_PROCTIME_OUTPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(),
            new TimestampType(3),
            new TimestampType(3),
            new LocalZonedTimestampType(3)
          },
          new String[] {"total", "window_start", "window_end", "proctime"});

  private static final RowType LEGACY_NO_PROPERTIES_OUTPUT =
      RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"total"});

  // [key BIGINT, value BIGINT, rt TIMESTAMP_LTZ(3)] / [key, total, window_start, window_end].
  private static final RowType KEYED_SCHEMA =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new BigIntType(), new LocalZonedTimestampType(3)
          },
          new String[] {"key", "value", "rt"});

  private static final RowType KEYED_OUTPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new BigIntType(), new TimestampType(3), new TimestampType(3)
          },
          new String[] {"key", "total", "window_start", "window_end"});

  @Test
  void emitsWindowAggregatesFromArrowBatches() throws Exception {
    NativeColumnarWindowAggregateOperator operator =
        new NativeColumnarWindowAggregateOperator(
            false, 1000, 1000, 1, new int[] {0}, new int[0], new int[0], new int[] {0}, new int[] {0},
            "UTC", OUTPUT, false, new int[0], MAX_PARALLELISM);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            rawHarness(operator)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.processElement(new StreamRecord<>(batch(allocator, event(1, 0), event(2, 500), event(3, 1000))));
      harness.processWatermark(new Watermark(1000));
      assertEquals(List.of(row(3, 0, 1000)), collect(harness));

      harness.processElement(new StreamRecord<>(batch(allocator, event(4, 1500), event(5, 2500))));
      harness.processWatermark(new Watermark(3000));
      assertEquals(List.of(row(7, 1000, 2000), row(5, 2000, 3000)), collect(harness));
    }
  }

  @Test
  void emitsLegacyEventTimeProperties() throws Exception {
    NativeColumnarWindowAggregateOperator operator =
        operator(LEGACY_EVENT_OUTPUT, false);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            rawHarness(operator)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, event(1, 0), event(2, 500))));
      harness.processWatermark(new Watermark(1000));

      try (VectorSchemaRoot root = outputRoot(harness)) {
        RowData result = RowDataArrowConverter.read(root, LEGACY_EVENT_OUTPUT).get(0);
        assertEquals(3L, result.getLong(0));
        assertEquals(0L, result.getTimestamp(1, 3).getMillisecond());
        assertEquals(1000L, result.getTimestamp(2, 3).getMillisecond());
        assertEquals(999L, result.getTimestamp(3, 3).getMillisecond());
        assertTrue(result.isNullAt(4));
      }
    }
  }

  @Test
  void emitsLegacyProctimePropertyAsNull() throws Exception {
    NativeColumnarWindowAggregateOperator operator =
        new NativeColumnarWindowAggregateOperator(
            false,
            1000,
            1000,
            1,
            new int[] {0},
            new int[0],
            new int[0],
            new int[] {0},
            new int[] {0},
            "Asia/Kolkata",
            true,
            "Asia/Kolkata",
            LEGACY_PROCTIME_OUTPUT,
            true,
            new int[0],
            MAX_PARALLELISM);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            rawHarness(operator)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.setProcessingTime(500);
      harness.processElement(new StreamRecord<>(batch(allocator, event(3, 9000))));
      harness.setProcessingTime(1000);

      try (VectorSchemaRoot root = outputRoot(harness)) {
        RowData result = RowDataArrowConverter.read(root, LEGACY_PROCTIME_OUTPUT).get(0);
        assertEquals(3L, result.getLong(0));
        assertEquals(19_800_000L, result.getTimestamp(1, 3).getMillisecond());
        assertEquals(19_801_000L, result.getTimestamp(2, 3).getMillisecond());
        assertTrue(result.isNullAt(3));
      }
    }
  }

  @Test
  void emitsLegacyWindowWithoutProperties() throws Exception {
    NativeColumnarWindowAggregateOperator operator =
        operator(LEGACY_NO_PROPERTIES_OUTPUT, false);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            rawHarness(operator)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, event(4, 0))));
      harness.processWatermark(new Watermark(1000));

      try (VectorSchemaRoot root = outputRoot(harness)) {
        RowData result = RowDataArrowConverter.read(root, LEGACY_NO_PROPERTIES_OUTPUT).get(0);
        assertEquals(4L, result.getLong(0));
      }
    }
  }

  /**
   * Proctime windows ignore the row time column and assign by the operator's processing-time clock,
   * firing on a processing-time timer. Driving the clock with {@code setProcessingTime} makes this
   * deterministic (proctime is non-deterministic in a real run — see the CLAUDE.md note). The event
   * times below are deliberately scattered to show they are ignored: assignment is by the clock.
   */
  @Test
  void emitsProctimeWindowsOnTimer() throws Exception {
    NativeColumnarWindowAggregateOperator operator =
        new NativeColumnarWindowAggregateOperator(
            false, 1000, 1000, 1, new int[] {0}, new int[0], new int[0], new int[] {0}, new int[] {0},
            "UTC", OUTPUT, true, new int[0], MAX_PARALLELISM);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            rawHarness(operator)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.setProcessingTime(500);
      harness.processElement(new StreamRecord<>(batch(allocator, event(1, 7000), event(2, 0), event(3, 9000))));
      assertEquals(List.of(), collect(harness)); // window [0,1000) still open at proctime 500
      harness.setProcessingTime(1000); // fires the window-end timer
      assertEquals(List.of(row(6, 0, 1000)), collect(harness));

      harness.setProcessingTime(1500);
      harness.processElement(new StreamRecord<>(batch(allocator, event(4, 100), event(5, 8000))));
      harness.setProcessingTime(2000);
      assertEquals(List.of(row(9, 1000, 2000)), collect(harness));
    }
  }

  /**
   * A proctime hopping window leaves several windows open at once, so the timer chains: each firing
   * closes the earliest-ending open window and schedules the next slide boundary. A single row at
   * proctime 1300 (slide 500, size 1000) lands in windows [500,1500) and [1000,2000), which close in
   * order as the clock crosses 1500 then 2000.
   */
  @Test
  void emitsProctimeHoppingWindowsOnChainedTimers() throws Exception {
    NativeColumnarWindowAggregateOperator operator =
        new NativeColumnarWindowAggregateOperator(
            false, 1000, 500, 1, new int[] {0}, new int[0], new int[0], new int[] {0}, new int[] {0},
            "UTC", OUTPUT, true, new int[0], MAX_PARALLELISM);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            rawHarness(operator)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.setProcessingTime(1300);
      harness.processElement(new StreamRecord<>(batch(allocator, event(10, 42))));
      assertEquals(List.of(), collect(harness)); // both windows still open at proctime 1300
      harness.setProcessingTime(1500);
      assertEquals(List.of(row(10, 500, 1500)), collect(harness));
      harness.setProcessingTime(2000);
      assertEquals(List.of(row(10, 1000, 2000)), collect(harness));
    }
  }

  /**
   * A proctime cumulative window nests several windows sharing a start, all open at once. A single row
   * at proctime 500 (step 1000, max size 3000) lands in [0,1000), [0,2000) and [0,3000); the chained
   * timer emits each as the clock crosses its end.
   */
  @Test
  void emitsProctimeCumulativeWindowsOnChainedTimers() throws Exception {
    NativeColumnarWindowAggregateOperator operator =
        new NativeColumnarWindowAggregateOperator(
            true, 3000, 1000, 1, new int[] {0}, new int[0], new int[0], new int[] {0}, new int[] {0},
            "UTC", OUTPUT, true, new int[0], MAX_PARALLELISM);
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            rawHarness(operator)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      harness.setProcessingTime(500);
      harness.processElement(new StreamRecord<>(batch(allocator, event(10, 42))));
      assertEquals(List.of(), collect(harness));
      harness.setProcessingTime(1000);
      assertEquals(List.of(row(10, 0, 1000)), collect(harness));
      harness.setProcessingTime(2000);
      assertEquals(List.of(row(10, 0, 2000)), collect(harness));
      harness.setProcessingTime(3000);
      assertEquals(List.of(row(10, 0, 3000)), collect(harness));
    }
  }

  @Test
  void rawKeyedProctimeWindowRearmsTimerAfterRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before =
            proctimeRawHarness()) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.setProcessingTime(500);
      before.processElement(new StreamRecord<>(batch(allocator, event(6, 9999))));
      snapshot = before.snapshot(1L, 1L);
      collect(before);
    }

    try (KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
        proctimeRawHarness()) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      // No input after recovery: the restored timer is solely responsible for closing the window.
      restored.setProcessingTime(1000);
      assertEquals(List.of(row(6, 0, 1000)), collect(restored));
    }
  }

  /**
   * A checkpoint barrier landing mid-stream — after rows for an event-time window have arrived but
   * before the watermark closes it — must lose nothing. {@code snapshotState} flushes the buffered
   * input into native state before serializing, so restoring into a fresh operator resumes the
   * still-open window and combines the pre- and post-restore rows. Pins the synchronous-mailbox
   * guarantee (divergences/04): buffered work survives a checkpoint even though native compute runs
   * synchronously on the task thread. (Re-added after the row-fed operator carrying the original test
   * was deleted in the fully-columnar migration.)
   */
  @Test
  void bufferedInputSurvivesCheckpointMidStream() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            rawHarness(eventTimeOperator())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      // Rows land in window [0,1000); the watermark stays below 1000, so the window is still open and
      // its two rows are buffered — not yet emitted — when the checkpoint barrier arrives.
      harness.processElement(new StreamRecord<>(batch(allocator, event(1, 0), event(2, 500))));
      assertEquals(List.of(), collect(harness));
      snapshot = harness.snapshot(1L, 1L);
    }

    // A fresh operator restored from that snapshot must resume the open window: a third row joins the
    // two that survived the checkpoint, and closing the window with a watermark emits their sum.
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            rawHarness(eventTimeOperator())) {
      harness.setup(new ArrowBatchSerializer());
      harness.initializeState(snapshot);
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, event(3, 700))));
      harness.processWatermark(new Watermark(1000));
      assertEquals(List.of(row(6, 0, 1000)), collect(harness)); // 1 + 2 (pre-checkpoint) + 3 (post)
    }
  }

  @Test
  void consecutiveCheckpointsPreserveUnsetNonzeroKeyGroupContext() throws Exception {
    NativeColumnarWindowAggregateOperator operator = eventTimeOperator();
    try (KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            operator, batch -> 0, Types.INT, MAX_PARALLELISM, 4, 1)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.snapshot(1L, 1L);
      harness.snapshot(2L, 2L);
    }
  }

  /** Raw keyed window state must redistribute by BinaryRow/Flink key group across a rescale. */
  @Test
  void rawKeyedWindowStateRescalesAndContinuesAggregation() throws Exception {
    long[] keys = keysForBothSubtasks();
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> before =
            keyedHarness(1, 0)) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.processElement(
          new StreamRecord<>(keyedBatch(allocator, keyedEvent(keys[0], 1, 0), keyedEvent(keys[1], 2, 0))));
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
      for (int index = 0; index < keys.length; index++) {
        long key = keys[index];
        int destination = destinationForKey(key);
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> restored =
            destination == 0 ? restored0 : restored1;
        restored.processElement(
            new StreamRecord<>(
                keyedBatch(allocator, destination, keyedEvent(key, (index + 1L) * 10, 500))));
      }
      restored0.processWatermark(new Watermark(1000));
      restored1.processWatermark(new Watermark(1000));
      List<List<Long>> actual = new ArrayList<>();
      actual.addAll(keyedCollect(restored0));
      actual.addAll(keyedCollect(restored1));
      actual.sort(java.util.Comparator.comparing(row -> row.get(0)));
      List<List<Long>> expected =
          new ArrayList<>(
              List.of(
                  keyedRow(keys[0], 11, 0, 1000), keyedRow(keys[1], 22, 0, 1000)));
      expected.sort(java.util.Comparator.comparing(row -> row.get(0)));
      assertEquals(expected, actual);
    }
  }

  private static NativeColumnarWindowAggregateOperator eventTimeOperator() {
    return new NativeColumnarWindowAggregateOperator(
        false, 1000, 1000, 1, new int[] {0}, new int[0], new int[0], new int[] {0}, new int[] {0},
        "UTC", OUTPUT, false, new int[0], MAX_PARALLELISM);
  }

  private static NativeColumnarWindowAggregateOperator operator(
      RowType outputType, boolean proctime) {
    return new NativeColumnarWindowAggregateOperator(
        false, 1000, 1000, 1, new int[] {0}, new int[0], new int[0], new int[] {0}, new int[] {0},
        "UTC", outputType, proctime, new int[0], MAX_PARALLELISM);
  }

  private static RowData event(long value, long eventTimeMillis) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, value);
    row.setField(1, TimestampData.fromEpochMillis(eventTimeMillis));
    return row;
  }

  private static RowData keyedEvent(long key, long value, long eventTimeMillis) {
    GenericRowData row = new GenericRowData(3);
    row.setField(0, key);
    row.setField(1, value);
    row.setField(2, TimestampData.fromEpochMillis(eventTimeMillis));
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    VectorSchemaRoot root = RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator);
    return new ArrowBatch(root);
  }

  private static ArrowBatch keyedBatch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), KEYED_SCHEMA, allocator));
  }

  private static ArrowBatch keyedBatch(BufferAllocator allocator, int destination, RowData... rows) {
    return new ArrowBatch(
        RowDataArrowConverter.write(List.of(rows), KEYED_SCHEMA, allocator), destination);
  }

  private static List<Long> row(long total, long start, long end) {
    return List.of(total, start, end);
  }

  private static List<Long> keyedRow(long key, long total, long start, long end) {
    return List.of(key, total, start, end);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> keyedHarness(
      int parallelism, int subtask) throws Exception {
    int[] stateKeys = FlinkKeyGroupUtils.stateKeysForSubtasks(MAX_PARALLELISM, parallelism);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new NativeColumnarWindowAggregateOperator(
            false,
            1000,
            1000,
            2,
            new int[] {1},
            new int[] {0},
            new int[] {0},
            new int[] {0},
            new int[] {0},
            "UTC",
            KEYED_OUTPUT,
            false,
            new int[] {-1},
            MAX_PARALLELISM),
        batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0],
        Types.INT,
        MAX_PARALLELISM,
        parallelism,
        subtask);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      proctimeRawHarness() throws Exception {
    return rawHarness(
        new NativeColumnarWindowAggregateOperator(
            false,
            1000,
            1000,
            1,
            new int[] {0},
            new int[0],
            new int[0],
            new int[] {0},
            new int[] {0},
            "UTC",
            OUTPUT,
            true,
            new int[0],
            MAX_PARALLELISM));
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> rawHarness(
      NativeColumnarWindowAggregateOperator operator) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0);
  }

  private static List<List<Long>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData r : RowDataArrowConverter.read(root, OUTPUT)) {
            rows.add(
                List.of(
                    r.getLong(0),
                    r.getTimestamp(1, 3).getMillisecond(),
                    r.getTimestamp(2, 3).getMillisecond()));
          }
        }
      }
    }
    return rows;
  }

  private static VectorSchemaRoot outputRoot(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    Object event = harness.getOutput().poll();
    StreamRecord<?> record = assertInstanceOf(StreamRecord.class, event);
    return assertInstanceOf(ArrowBatch.class, record.getValue()).root();
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
                    r.getTimestamp(3, 3).getMillisecond()));
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
