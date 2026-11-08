package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;

/** Released SQL operators supply the state layout and the controlled-clock output oracle. */
class CanonicalProcessingTimeWindowInteropTest {
  private static final long BASE = CanonicalSharedWindowInteropTest.BASE;
  @TempDir Path directory;

  static Stream<Arguments> corruptions() {
    return Stream.of("TUMBLE", "HOP", "CUMULATE")
        .flatMap(
            shape ->
                Stream.of("missing", "timestamp", "eventQueue")
                    .flatMap(
                        mode ->
                            Stream.of(
                                Arguments.of(shape, mode, false),
                                Arguments.of(shape, mode, true))));
  }

  @ParameterizedTest
  @MethodSource("corruptions")
  void invalidClockTimersFailBeforeAnyNativeAccumulatorIsImported(
      String shape, String mode, boolean rocks) throws Exception {
    var plan =
        new CanonicalSharedWindowInteropTest.StockPlan(shape, "UTC", false, false, false, 0, true);
    long handle = plan.create(rocks, directory.resolve("native"));
    try (var source = plan.stock(rocks);
        var view = plan.harness(new Projection(), !rocks)) {
      source.open();
      source.setProcessingTime(BASE + 100);
      plan.send(source, input(-1L, 5L, 100));
      source.prepareSnapshotPreBarrier(1);
      view.initializeState(
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState());
      view.open();
      var serializer =
          new org.apache.flink.streaming.api.operators.TimerSerializer<>(
              plan.selector.getProducedType().toSerializer(),
              org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE);
      var timers = backend(view).create("_timer_state/processing_window-timers", serializer);
      var timer = timers.poll();
      assertNotNull(timer);
      if (mode.equals("timestamp"))
        timers.add(
            new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                timer.getTimestamp() + 1, timer.getKey(), timer.getNamespace()));
      else if (mode.equals("eventQueue")) {
        timers.add(timer);
        backend(view).create("_timer_state/event_window-timers", serializer).add(timer);
      }
      var damaged =
          view.snapshotWithLocalState(2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = plan.harness(new Projection(), rocks)) {
        restored.initializeState(damaged);
        restored.open();
        var contract = plan.contract();
        contract.initializeOperatorState(((Projection) restored.getOperator()).store, true);
        assertThrows(
            IllegalStateException.class,
            () -> contract.importState(backend(restored), handle, rocks));
      }
      var imported = new java.util.concurrent.atomic.AtomicInteger();
      Native.BatchReceiver receiver =
          (arrayAddress, schemaAddress) -> {
            try (var array = org.apache.arrow.c.ArrowArray.wrap(arrayAddress);
                var schema = org.apache.arrow.c.ArrowSchema.wrap(schemaAddress);
                var root =
                    org.apache.arrow.c.Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              imported.addAndGet(root.getRowCount());
            }
          };
      if (plan.tumbling) Native.exportTumblingAggregatorCanonical(handle, receiver);
      else Native.exportSlicingWindowCanonical(handle, false, receiver);
      assertEquals(0, imported.get());
    } finally {
      Native.closeTumblingAggregator(handle);
    }
  }

  @org.junit.jupiter.api.Test
  void releasedProcessingTimeSessionStillChecksTheEventWatermarkWhileMerging() throws Exception {
    var plan =
        new CanonicalSharedWindowInteropTest.StockPlan(
            "SESSION", "UTC", false, false, false, 0, true);
    try (var stock = plan.stock(false)) {
      stock.open();
      stock.setProcessingTime(BASE + 100);
      plan.send(stock, input(-1L, 5L, 100));
      stock.processWatermark(new Watermark(CanonicalSharedWindowInteropTest.processingWatermark()));
      stock.setProcessingTime(BASE + 200);
      var failure =
          assertThrows(
              UnsupportedOperationException.class, () -> plan.send(stock, input(-1L, 5L, 200)));
      assertTrue(failure.getMessage().contains("event-time window"));
    }
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String shape : List.of("TUMBLE", "HOP", "CUMULATE", "SESSION"))
      for (String zone : List.of("UTC", "+05:45", "Europe/Berlin", "America/New_York"))
        for (boolean rocks : new boolean[] {false, true})
          for (boolean keyless : new boolean[] {false, true})
            for (boolean distinct : new boolean[] {false, true})
              cases.add(Arguments.of(shape, zone, rocks, keyless, distinct));
    return cases.stream();
  }

  @ParameterizedTest(name = "{0} {1} rocks={2} keyless={3} distinct={4}")
  @MethodSource("cases")
  void releasedClockStateContinuesAcrossRepeatedCanonicalHandoffs(
      String shape, String zone, boolean rocks, boolean keyless, boolean distinct)
      throws Exception {
    var plan =
        new CanonicalSharedWindowInteropTest.StockPlan(
            shape, zone, keyless, false, distinct, 0, true);
    long handle = plan.create(rocks, directory.resolve("native"));
    try (var source = plan.stock(rocks);
        var reference = plan.stock(!rocks);
        var view = plan.harness(new Projection(), !rocks)) {
      source.open();
      for (var row :
          List.of(
              input(-1L, 5L, 100),
              input(-1L, null, 200),
              input(null, 7L, 700),
              input(-1L, 5L, 800))) {
        source.setProcessingTime(BASE + row.timestamp);
        plan.send(source, row);
      }
      // Watermarks ahead of every deadline must leave processing-time state and timers alive.
      long watermark =
          plan.session ? Long.MIN_VALUE : CanonicalSharedWindowInteropTest.processingWatermark();
      source.processWatermark(new Watermark(watermark));
      source.prepareSnapshotPreBarrier(1);
      var snapshot =
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      reference.initializeState(snapshot);
      reference.setProcessingTime(BASE + 800);
      reference.open();
      view.initializeState(snapshot);
      view.open();
      var contract = plan.contract();
      contract.initializeOperatorState(((Projection) view.getOperator()).store, true);
      assertEquals(watermark, contract.restoredWatermark());
      contract.importState(backend(view), handle, rocks);
      contract.snapshotOperatorState(watermark);
      contract.exportState(backend(view), handle, rocks);
      var immediate =
          view.snapshotWithLocalState(2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = plan.stock(rocks)) {
        restored.initializeState(immediate);
        restored.setProcessingTime(BASE + 800);
        restored.open();
        if (!plan.session) {
          reference.processWatermark(
              new Watermark(CanonicalSharedWindowInteropTest.processingWatermark()));
          restored.processWatermark(
              new Watermark(CanonicalSharedWindowInteropTest.processingWatermark()));
        }
        assertEquals(List.of(), plan.output(reference));
        assertEquals(List.of(), plan.output(restored));
        for (long time : new long[] {900, 999, 1200, 1999, 2100, 2999, 4999, 7999}) {
          reference.setProcessingTime(BASE + time);
          restored.setProcessingTime(BASE + time);
          var expected = plan.output(reference);
          assertEquals(expected, plan.output(restored), "stock re-export at " + time);
          assertEquals(expected, plan.flush(handle, BASE + time), "native at " + time);
          if (time == 900 || time == 1200 || time == 2100) {
            var rows = List.of(input(-1L, 5L, time), input(2L, null, time), input(null, -2L, time));
            for (var row : rows) {
              plan.send(reference, row);
              plan.send(restored, row);
            }
            plan.update(handle, rows);
          }
          if (time == 1999) {
            // Re-export after firing as well as immediately after import.
            contract.exportState(backend(view), handle, rocks);
            var afterFiring =
                view.snapshotWithLocalState(
                        3, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                    .getJobManagerOwnedState();
            try (var resumed = plan.stock(!rocks)) {
              resumed.initializeState(afterFiring);
              resumed.setProcessingTime(BASE + time);
              resumed.open();
              resumed.setProcessingTime(BASE + 1999);
              assertEquals(List.of(), plan.output(resumed));
              resumed.setProcessingTime(BASE + 2000);
              assertEquals(plan.flush(handle, BASE + 2000), plan.output(resumed));
            }
          }
        }
      }
    } finally {
      if (plan.session) Native.closeSessionAggregator(handle);
      else Native.closeTumblingAggregator(handle);
    }
  }

  private static CanonicalSharedWindowInteropTest.Input input(Long key, Long value, long time) {
    return new CanonicalSharedWindowInteropTest.Input(key, value, time);
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> backend(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
    return (CheckpointableKeyedStateBackend<RowData>)
        harness.getOperator().<RowData>getKeyedStateBackend();
  }

  private static final class Projection extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    transient OperatorStateStore store;

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
      super.initializeState(context);
      store = context.getOperatorStateStore();
    }

    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}
