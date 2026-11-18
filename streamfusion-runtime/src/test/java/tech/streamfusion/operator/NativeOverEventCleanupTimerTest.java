package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

class NativeOverEventCleanupTimerTest {
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new BigIntType(), new TimestampType(3));
  private static final RowType OUTPUT =
      RowType.of(new BigIntType(), new BigIntType(), new TimestampType(3), new BigIntType());

  static Stream<Arguments> modes() {
    var cases = new ArrayList<Arguments>();
    for (boolean rocks : new boolean[] {false, true})
      for (boolean rows : new boolean[] {false, true})
        for (boolean keyless : new boolean[] {false, true})
          cases.add(Arguments.of(rocks, rows, keyless));
    return cases.stream();
  }

  static Stream<Arguments> restores() {
    return modes()
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .map(
                        overdue -> {
                          Object[] args = mode.get();
                          return Arguments.of(args[0], args[1], args[2], overdue);
                        }));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void idleCallbackClearsFoldAndLateOnlyKeysWithoutInput(
      boolean rocks, boolean rows, boolean keyless) throws Exception {
    try (var fixture = fixture(rocks, rows, keyless, false)) {
      fixture.harness.open();
      fixture.harness.setProcessingTime(100);
      fixture.push(1, 3, 1);
      fixture.harness.processWatermark(new Watermark(1));
      assertEquals(List.of(3L), fixture.drain());
      fixture.push(9, 99, 1);
      assertArrayEquals(new long[] {1600}, Native.nextOverEventCleanup(fixture.operator.handle));
      fixture.harness.setProcessingTime(1600);
      assertArrayEquals(new long[0], Native.nextOverEventCleanup(fixture.operator.handle));
      assertEquals(0, fixture.foldRows());
      assertTrue(fixture.drain().isEmpty());
      fixture.push(1, 5, 2);
      fixture.harness.processWatermark(new Watermark(2));
      assertEquals(List.of(5L), fixture.drain());
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void delayedCallbacksRetainPendingRowsAndRearmFromActualClock(
      boolean rocks, boolean rows, boolean keyless) throws Exception {
    try (var fixture = fixture(rocks, rows, keyless, false)) {
      fixture.harness.open();
      fixture.harness.setProcessingTime(100);
      fixture.push(1, 3, 1);
      fixture.harness.processWatermark(new Watermark(1));
      fixture.drain();
      fixture.push(1, 7, 10);
      fixture.harness.setProcessingTime(1800);
      assertArrayEquals(new long[] {3300}, Native.nextOverEventCleanup(fixture.operator.handle));
      assertEquals(1, fixture.foldRows());
      assertTrue(fixture.drain().isEmpty());
      fixture.harness.processWatermark(new Watermark(10));
      assertEquals(List.of(10L), fixture.drain());
      assertArrayEquals(new long[] {3300}, Native.nextOverEventCleanup(fixture.operator.handle));
      fixture.harness.setProcessingTime(3300);
      assertEquals(0, fixture.foldRows());
      assertArrayEquals(new long[0], Native.nextOverEventCleanup(fixture.operator.handle));
    }
  }

  @ParameterizedTest
  @MethodSource("restores")
  void nativeCheckpointRearmsIdleTimersBeforeOrAfterTheirDeadline(
      boolean rocks, boolean rows, boolean keyless, boolean overdue) throws Exception {
    OperatorSubtaskState snapshot;
    try (var fixture = fixture(rocks, rows, keyless, false)) {
      fixture.harness.open();
      fixture.harness.setProcessingTime(100);
      fixture.push(1, 3, 1);
      fixture.harness.processWatermark(new Watermark(1));
      fixture.drain();
      snapshot = fixture.harness.snapshot(1, 100);
    }
    try (var fixture = fixture(rocks, rows, keyless, false)) {
      fixture.harness.setProcessingTime(overdue ? 1800 : 1000);
      fixture.harness.initializeState(snapshot);
      fixture.harness.open();
      fixture.harness.setProcessingTime(overdue ? 1800 : 1000);
      if (overdue) {
        assertEquals(0, fixture.foldRows());
        assertArrayEquals(new long[0], Native.nextOverEventCleanup(fixture.operator.handle));
      } else {
        assertEquals(1, fixture.foldRows());
        assertArrayEquals(new long[] {1600}, Native.nextOverEventCleanup(fixture.operator.handle));
        fixture.harness.setProcessingTime(1600);
        assertEquals(0, fixture.foldRows());
      }
      fixture.push(1, 5, 2);
      fixture.harness.processWatermark(new Watermark(2));
      assertEquals(List.of(5L), fixture.drain());
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void overdueRestoreDefersPendingRowsWithoutDiscardingTheFold(
      boolean rocks, boolean rows, boolean keyless) throws Exception {
    OperatorSubtaskState snapshot;
    try (var fixture = fixture(rocks, rows, keyless, false)) {
      fixture.harness.open();
      fixture.harness.setProcessingTime(100);
      fixture.push(1, 3, 1);
      fixture.harness.processWatermark(new Watermark(1));
      fixture.drain();
      fixture.push(1, 7, 10);
      snapshot = fixture.harness.snapshot(1, 100);
    }
    try (var fixture = fixture(rocks, rows, keyless, false)) {
      fixture.harness.setProcessingTime(1800);
      fixture.harness.initializeState(snapshot);
      fixture.harness.open();
      fixture.harness.setProcessingTime(1800);
      assertEquals(1, fixture.foldRows());
      assertArrayEquals(new long[] {3300}, Native.nextOverEventCleanup(fixture.operator.handle));
      fixture.harness.processWatermark(new Watermark(10));
      assertEquals(List.of(10L), fixture.drain());
      fixture.harness.setProcessingTime(3300);
      assertEquals(0, fixture.foldRows());
      assertArrayEquals(new long[0], Native.nextOverEventCleanup(fixture.operator.handle));
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void firedUnarmedCleanupValueSurvivesNativeRecovery(boolean rocks, boolean rows, boolean keyless)
      throws Exception {
    OperatorSubtaskState snapshot;
    try (var fixture = fixture(rocks, rows, keyless, false)) {
      fixture.harness.open();
      fixture.harness.setProcessingTime(Long.MAX_VALUE - 500);
      fixture.push(1, 3, 10);
      assertArrayEquals(
          new long[] {Long.MIN_VALUE + 999}, Native.nextOverEventCleanup(fixture.operator.handle));
      fixture.harness.setProcessingTime(Long.MAX_VALUE - 500);
      assertArrayEquals(new long[0], Native.nextOverEventCleanup(fixture.operator.handle));
      snapshot = fixture.harness.snapshot(1, 100);
    }
    try (var fixture = fixture(rocks, rows, keyless, false)) {
      fixture.harness.setProcessingTime(100);
      fixture.harness.initializeState(snapshot);
      fixture.harness.open();
      assertArrayEquals(new long[0], Native.nextOverEventCleanup(fixture.operator.handle));
      fixture.push(1, 4, 11);
      assertArrayEquals(new long[] {1600}, Native.nextOverEventCleanup(fixture.operator.handle));
      fixture.harness.processWatermark(new Watermark(11));
      assertEquals(List.of(3L, 7L), fixture.drain());
      fixture.harness.setProcessingTime(1600);
      assertEquals(0, fixture.foldRows());
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void replacingDeadlineCancelsTheOldCallback(boolean rocks, boolean rows, boolean keyless)
      throws Exception {
    try (var fixture = fixture(rocks, rows, keyless, false)) {
      fixture.harness.open();
      fixture.harness.setProcessingTime(100);
      fixture.push(1, 3, 1);
      fixture.harness.processWatermark(new Watermark(1));
      fixture.drain();
      fixture.harness.setProcessingTime(900);
      fixture.push(1, 7, 2);
      fixture.harness.processWatermark(new Watermark(2));
      assertEquals(List.of(10L), fixture.drain());
      assertArrayEquals(new long[] {2400}, Native.nextOverEventCleanup(fixture.operator.handle));
      fixture.harness.setProcessingTime(1600);
      assertEquals(1, fixture.foldRows());
      assertArrayEquals(new long[] {2400}, Native.nextOverEventCleanup(fixture.operator.handle));
      fixture.harness.setProcessingTime(2400);
      assertEquals(0, fixture.foldRows());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void overdueProcessingRowsCheckpointStillClearsWithoutInput(boolean rocks) throws Exception {
    OperatorSubtaskState snapshot;
    try (var fixture = fixture(rocks, true, false, true)) {
      fixture.harness.open();
      fixture.harness.setProcessingTime(100);
      fixture.push(1, 3, 1);
      assertEquals(List.of(3L), fixture.drain());
      snapshot = fixture.harness.snapshot(1, 100);
    }
    try (var fixture = fixture(rocks, true, false, true)) {
      fixture.harness.setProcessingTime(1800);
      fixture.harness.initializeState(snapshot);
      fixture.harness.open();
      fixture.harness.setProcessingTime(1800);
      assertArrayEquals(new long[0], Native.nextOverProcessingRowsCleanup(fixture.operator.handle));
      fixture.push(1, 5, 2);
      assertEquals(List.of(5L), fixture.drain());
    }
  }

  @Test
  void incompatibleModesRejectBeforeNativeAllocation() {
    for (long ttl : new long[] {0, 1})
      assertThrows(
          IllegalArgumentException.class,
          () -> operator(false, false, false, ttl).withEventTimeCleanupTimers());
    assertThrows(
        IllegalArgumentException.class,
        () -> operator(false, false, true, 1000).withEventTimeCleanupTimers());
  }

  private static NativeOverAggregateOperator operator(
      boolean rows, boolean keyless, boolean proctime, long ttl) {
    return new NativeOverAggregateOperator(
        2,
        new int[] {1},
        keyless ? new int[0] : new int[] {0},
        new int[] {0},
        new int[] {0},
        proctime ? 1 : rows ? 3 : 0,
        proctime ? 2 : 0,
        proctime,
        keyless
            ? new int[] {Native.STOCK_KEY_PROJECTION}
            : new int[] {Native.STOCK_KEY_PROJECTION, -1},
        ttl,
        INPUT,
        128);
  }

  static Fixture fixture(boolean rocks, boolean rows, boolean keyless, boolean proctime)
      throws Exception {
    var operator = operator(rows, keyless, proctime, 1000);
    if (!proctime) operator.withEventTimeCleanupTimers();
    operator = org.apache.flink.util.InstantiationUtil.clone(operator);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, ignored -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), NativeOverEventCleanupTimerTest.class.getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return new Fixture(operator, harness);
  }

  static final class Fixture implements AutoCloseable {
    final NativeOverAggregateOperator operator;
    final KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness;

    Fixture(
        NativeOverAggregateOperator operator,
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
      this.operator = operator;
      this.harness = harness;
    }

    void push(long key, long value, long timestamp) throws Exception {
      var row = GenericRowData.of(key, value, TimestampData.fromEpochMillis(timestamp));
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(List.of(row), INPUT, NativeAllocator.SHARED))));
    }

    List<Long> drain() {
      var values = new ArrayList<Long>();
      for (var batch : harness.extractOutputValues())
        try (var root = batch.root()) {
          for (var row : RowDataArrowConverter.read(root, OUTPUT)) values.add(row.getLong(3));
        }
      harness.getOutput().clear();
      return values;
    }

    int foldRows() {
      int[] count = {0};
      Native.exportOverFoldCanonical(
          operator.handle,
          (array, schema) -> {
            try (ArrowArray a = ArrowArray.wrap(array);
                ArrowSchema s = ArrowSchema.wrap(schema);
                VectorSchemaRoot root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, a, s, NativeAllocator.DICTIONARIES)) {
              count[0] += root.getRowCount();
            }
          });
      return count[0];
    }

    @Override
    public void close() throws Exception {
      drain();
      harness.close();
    }
  }
}
