package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;

class CanonicalEventRowsCleanupStateTest {
  static Stream<Arguments> corruptions() {
    return Stream.of("missing-value", "wrong-deadline", "extra-timer")
        .flatMap(mode -> Stream.of(false, true).map(rocks -> Arguments.of(mode, rocks)));
  }

  @ParameterizedTest
  @MethodSource("corruptions")
  void inconsistentCleanupOwnersFailBeforeStateInstallation(String mode, boolean rocks)
      throws Exception {
    var plan = new CanonicalBoundedRangeInteropTest.Plan("BIGINT", false, false, true, 2, 1000);
    OperatorSubtaskState saved = null;
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var stock = plan.stock(rocks); var target = plan.nativeHarness(rocks)) {
      stock.open();
      stock.setProcessingTime(100);
      var row = plan.row(1L, 7, 1000);
      plan.pushStock(stock, row);
      var backend = stock.getOperator().<org.apache.flink.table.data.RowData>getKeyedStateBackend();
      var key = plan.selector.getKey(new org.apache.flink.table.runtime.typeutils.RowDataSerializer(
          plan.input).toBinaryRow(row).copy());
      backend.setCurrentKey(key);
      var cleanup = backend.getPartitionedState(
          org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
          org.apache.flink.runtime.state.VoidNamespaceSerializer.INSTANCE,
          new org.apache.flink.api.common.state.ValueStateDescriptor<>(
              "RowTimeBoundedRowsOverCleanupTime",
              org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE));
      if (mode.equals("missing-value")) cleanup.clear();
      else if (mode.equals("wrong-deadline")) cleanup.update(1601L);
      else {
        var timers = ((org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<org.apache.flink.table.data.RowData>) backend)
            .create("_timer_state/processing_user-timers",
                new org.apache.flink.streaming.api.operators.TimerSerializer<>(
                    new org.apache.flink.table.runtime.typeutils.RowDataSerializer(plan.keyType),
                    org.apache.flink.runtime.state.VoidNamespaceSerializer.INSTANCE));
        timers.add(new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
            1601, key, org.apache.flink.runtime.state.VoidNamespace.INSTANCE));
      }
      saved = CanonicalBoundedRangeInteropTest.snapshot(stock);
      target.initializeState(saved);
      assertThrows(Exception.class, target::open);
    } finally {
      if (saved != null) saved.discardState();
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String type : List.of("BIGINT", "DOUBLE", "DECIMAL(38,2)"))
      for (boolean keyless : new boolean[] {false, true})
        for (boolean stockStore : new boolean[] {false, true})
          for (boolean nativeStore : new boolean[] {false, true})
            for (int phase = 0; phase < 4; phase++)
              for (boolean checkpoint : new boolean[] {false, true})
                cases.add(Arguments.of(type, keyless, stockStore, nativeStore, phase, checkpoint));
    return tech.streamfusion.CanonicalTestMatrix.select(cases.stream());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void pendingDeferralsLateOnlyOwnersAndExpiredCutoffsSurviveBothDirections(
      String type, boolean keyless, boolean stockStore, boolean nativeStore, int phase,
      boolean checkpoint)
      throws Exception {
    var plan = new CanonicalBoundedRangeInteropTest.Plan(type, keyless, false, true, 2, 1000);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    OperatorSubtaskState fromStock = null, fromNative = null;
    try (var reference = plan.stock(stockStore)) {
      reference.open();
      reference.setProcessingTime(100);
      plan.pushStock(reference, plan.row(1L, 3, 10));
      reference.processWatermark(new Watermark(10));
      assertFalse(plan.stockOutput(reference).isEmpty());
      plan.pushStock(reference, plan.row(1L, 7, 40));
      plan.pushStock(reference, plan.row(9L, 99, 0));
      try (var source = plan.nativeHarness(nativeStore)) {
        source.open();
        source.setProcessingTime(100);
        plan.push(source, plan.row(1L, 3, 10));
        source.processWatermark(new Watermark(10));
        assertFalse(plan.nativeOutput(source).isEmpty());
        plan.push(source, plan.row(1L, 7, 40));
        plan.push(source, plan.row(9L, 99, 0));
        if (phase > 0) {
          reference.setProcessingTime(1800);
          source.setProcessingTime(1800);
          assertArrayEquals(new long[] {3300}, Native.nextOverEventCleanup(
              ((NativeOverAggregateOperator) source.getOperator()).handle));
        }
        if (phase > 1) {
          reference.processWatermark(new Watermark(40));
          source.processWatermark(new Watermark(40));
          assertEquals(plan.stockOutput(reference), plan.nativeOutput(source));
        }
        if (phase > 2) {
          reference.setProcessingTime(3300);
          source.setProcessingTime(3300);
          assertArrayEquals(new long[0], Native.nextOverEventCleanup(
              ((NativeOverAggregateOperator) source.getOperator()).handle));
        }
        fromStock = CanonicalBoundedRangeInteropTest.snapshot(reference);
        if (checkpoint) {
          source.prepareSnapshotPreBarrier(2);
          var saved = source.snapshot(2, 0);
          try (var recovered = plan.nativeHarness(nativeStore)) {
            recovered.setProcessingTime(phase == 0 ? 100 : phase == 3 ? 3300 : 1800);
            recovered.initializeState(saved);
            recovered.open();
            fromNative = CanonicalBoundedRangeInteropTest.snapshot(recovered);
          } finally {
            saved.discardState();
          }
        } else fromNative = CanonicalBoundedRangeInteropTest.snapshot(source);
      }
      try (var returned = plan.stock(!stockStore);
          var imported = plan.nativeHarness(!nativeStore)) {
        long restoreTime = phase == 3 ? 3300 : 1800;
        returned.setProcessingTime(restoreTime);
        imported.setProcessingTime(restoreTime);
        returned.initializeState(fromNative);
        imported.initializeState(fromStock);
        returned.open();
        imported.open();
        reference.setProcessingTime(restoreTime);
        returned.setProcessingTime(restoreTime);
        imported.setProcessingTime(restoreTime);
        reference.processWatermark(new Watermark(40));
        returned.processWatermark(new Watermark(40));
        imported.processWatermark(new Watermark(40));
        var expected = plan.stockOutput(reference);
        assertEquals(expected, plan.stockOutput(returned));
        assertEquals(expected, plan.nativeOutput(imported));
        reference.setProcessingTime(3300);
        returned.setProcessingTime(3300);
        imported.setProcessingTime(3300);
        plan.pushStock(reference, plan.row(1L, 5, 20));
        plan.pushStock(returned, plan.row(1L, 5, 20));
        plan.push(imported, plan.row(1L, 5, 20));
        reference.processWatermark(new Watermark(50));
        returned.processWatermark(new Watermark(50));
        imported.processWatermark(new Watermark(50));
        expected = plan.stockOutput(reference);
        assertFalse(expected.isEmpty(), "Expired cutoff must admit a later arrival at an earlier time");
        assertEquals(expected, plan.stockOutput(returned));
        assertEquals(expected, plan.nativeOutput(imported));
      }
    } finally {
      if (fromStock != null) fromStock.discardState();
      if (fromNative != null) fromNative.discardState();
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }
}
