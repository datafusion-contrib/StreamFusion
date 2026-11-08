package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Released processing ROWS canonical state, including minimum sentinels and clock buckets. */
class CanonicalProcessingRowsInteropTest {
  @BeforeAll
  static void memory() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> modes() {
    return tech.streamfusion.CanonicalTestMatrix.select(allModes());
  }

  private static Stream<Arguments> allModes() {
    var cases = new ArrayList<Arguments>();
    for (String type : List.of("BIGINT", "FLOAT", "DOUBLE", "DECIMAL(38,2)"))
      for (boolean keyless : new boolean[] {false, true})
        for (boolean stock : new boolean[] {false, true})
          for (boolean nativeStore : new boolean[] {false, true})
            for (int offset : new int[] {0, 2})
              for (int clock = 0; clock < 4; clock++)
                cases.add(Arguments.of(type, keyless, stock, nativeStore, offset, clock));
    return cases.stream();
  }

  static long[] clocks(int mode) {
    switch (mode) {
      case 0:
        return new long[] {100, 200, 300, 400, 500, 600};
      case 1:
        return new long[] {300, 200, 100, 400, 200, 500};
      case 2:
        return new long[] {100, 100, 100, 100, 100, 100};
      default:
        return new long[] {
          Long.MAX_VALUE,
          Long.MAX_VALUE,
          Long.MAX_VALUE,
          Long.MAX_VALUE,
          Long.MAX_VALUE,
          Long.MAX_VALUE
        };
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void canonicalRoundTripsRetainActualClockBucketsAndCounterMinimum(
      String type, boolean keyless, boolean stockStore, boolean nativeStore, int offset, int clock)
      throws Exception {
    check(type, keyless, stockStore, nativeStore, offset, clocks(clock), 0);
  }

  static Stream<Arguments> retentionModes() {
    var cases = new ArrayList<Arguments>();
    for (boolean keyless : new boolean[] {false, true})
      for (boolean stock : new boolean[] {false, true})
        for (boolean nativeStore : new boolean[] {false, true})
          for (int offset : new int[] {0, 2})
            for (int clock = 0; clock < 3; clock++)
              cases.add(Arguments.of(keyless, stock, nativeStore, offset, clock));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("retentionModes")
  void processingCleanupOwnershipContinuesAcrossCanonicalHandoffs(
      boolean keyless, boolean stockStore, boolean nativeStore, int offset, int clock)
      throws Exception {
    check(
        "BIGINT",
        keyless,
        stockStore,
        nativeStore,
        offset,
        clock == 0
            ? new long[] {100, 200, 300, 400, 5000, 6000}
            : clock == 1
                ? new long[] {Long.MAX_VALUE - 200, Long.MAX_VALUE - 100, 0, 100, 2000, 4000}
                : new long[] {Long.MAX_VALUE - 1499, Long.MAX_VALUE - 1498, 0, 100, 2000, 4000},
        1000);
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void idleCanonicalExportReflectsFiredProcessingCleanup(boolean nativeStore) throws Exception {
    var plan = new CanonicalBoundedRangeInteropTest.Plan("BIGINT", false, true, 2, 1000);
    OperatorSubtaskState exported;
    try (var nativeOp = plan.nativeHarness(nativeStore)) {
      nativeOp.open();
      nativeSteps(plan, nativeOp, new long[] {100, 200}, 0, 2);
      plan.nativeOutput(nativeOp);
      nativeOp.setProcessingTime(5000);
      exported = snapshot(nativeOp);
    }
    try (var view = plan.view(false)) {
      view.initializeState(exported);
      view.open();
      var backend = view.getOperator().getKeyedStateBackend();
      for (var state : plan.contract().stateDescriptors().keySet())
        try (var keys =
            backend.getKeys(state, org.apache.flink.runtime.state.VoidNamespace.INSTANCE)) {
          assertEquals(0, keys.count(), "Expired processing state remained in " + state);
        }
    }
  }

  static Stream<Arguments> corruptions() {
    var cases = new ArrayList<Arguments>();
    for (String mode :
        List.of(
            "missingCount",
            "zeroCount",
            "largeCount",
            "missingMinimum",
            "badMinimum",
            "missingAccumulator",
            "hiddenCount",
            "missingCleanup",
            "missingTimer",
            "extraTimer",
            "eventTimer",
            "emptyBucket",
            "wrongKey",
            "retraction",
            "unknownState"))
      for (boolean stock : new boolean[] {false, true})
        for (boolean nativeStore : new boolean[] {false, true})
          cases.add(Arguments.of(mode, stock, nativeStore));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("corruptions")
  @SuppressWarnings("unchecked")
  void malformedStockCountersInputsAndTimersRejectBeforeNativeInstallation(
      String mode, boolean stockStore, boolean nativeStore) throws Exception {
    var plan = new CanonicalBoundedRangeInteropTest.Plan("BIGINT", false, true, 2, 1000);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var stock = plan.stock(stockStore);
        var view = plan.view(stockStore);
        var target = plan.nativeHarness(nativeStore)) {
      stock.open();
      stockSteps(plan, stock, new long[] {100, 200}, 0, 2);
      view.initializeState(snapshot(stock));
      view.open();
      var backend =
          (org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<RowData>)
              view.getOperator().<RowData>getKeyedStateBackend();
      var ns = org.apache.flink.runtime.state.VoidNamespace.INSTANCE;
      var namespaces = org.apache.flink.runtime.state.VoidNamespaceSerializer.INSTANCE;
      var longs = org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE;
      backend.setCurrentKey(plan.selector.getKey(plan.row(null, 1, 9000)));
      var key = backend.getCurrentKey();
      var counter =
          backend.getPartitionedState(
              ns,
              namespaces,
              new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                  "processedCountState", longs));
      var minimum =
          backend.getPartitionedState(
              ns,
              namespaces,
              new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                  "smallestTSState", longs));
      var cleanup =
          backend.getPartitionedState(
              ns,
              namespaces,
              new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                  "ProcTimeBoundedRowsOverCleanupTime", longs));
      var acc =
          backend.getPartitionedState(
              ns,
              namespaces,
              new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                  "accState",
                  new org.apache.flink.table.runtime.typeutils.RowDataSerializer(
                      plan.accumulator)));
      var inputs =
          backend.getPartitionedState(
              ns,
              namespaces,
              new org.apache.flink.api.common.state.MapStateDescriptor<Long, List<RowData>>(
                  "inputState",
                  longs,
                  new org.apache.flink.api.common.typeutils.base.ListSerializer<>(
                      new org.apache.flink.table.runtime.typeutils.RowDataSerializer(plan.input))));
      var serializer =
          new org.apache.flink.streaming.api.operators.TimerSerializer<>(
              plan.selector.getProducedType().toSerializer(), namespaces);
      var timers = backend.create("_timer_state/processing_user-timers", serializer);
      String expected;
      switch (mode) {
        case "missingCount":
          counter.clear();
          expected = "counter/minimum";
          break;
        case "zeroCount":
          counter.update(0L);
          expected = "counter/minimum";
          break;
        case "largeCount":
          counter.update(4L);
          expected = "counter/minimum";
          break;
        case "missingMinimum":
          minimum.clear();
          expected = "counter/minimum";
          break;
        case "badMinimum":
          minimum.update(99L);
          expected = "counter/minimum";
          break;
        case "missingAccumulator":
          acc.clear();
          expected = "counter/minimum";
          break;
        case "hiddenCount":
          assertTrue(plan.hiddenCount >= 0);
          var saved = acc.value();
          var changed =
              new org.apache.flink.table.data.GenericRowData(plan.accumulator.getFieldCount());
          for (int i = 0; i < changed.getArity(); i++)
            changed.setField(
                i,
                RowData.createFieldGetter(plan.accumulator.getTypeAt(i), i).getFieldOrNull(saved));
          changed.setField(plan.hiddenCount, 99L);
          acc.update(changed);
          expected = "hidden input count";
          break;
        case "missingCleanup":
          cleanup.clear();
          expected = "cleanup/timer";
          break;
        case "missingTimer":
          assertNotNull(timers.poll());
          expected = "cleanup/timer";
          break;
        case "extraTimer":
          timers.add(
              new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(1700, key, ns));
          expected = "cleanup/timer";
          break;
        case "eventTimer":
          backend
              .create("_timer_state/event_user-timers", serializer)
              .add(
                  new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                      1700, key, ns));
          expected = "timer";
          break;
        case "emptyBucket":
          inputs.put(100L, List.of());
          expected = "Empty";
          break;
        case "wrongKey":
          inputs.put(100L, List.of(plan.row(2L, 1, 9000)));
          expected = "key/time/kind";
          break;
        case "retraction":
          var bad = plan.row(null, 1, 9000);
          bad.setRowKind(org.apache.flink.types.RowKind.DELETE);
          inputs.put(100L, List.of(bad));
          expected = "key/time/kind";
          break;
        case "unknownState":
          backend
              .getPartitionedState(
                  ns,
                  namespaces,
                  new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                      "unowned-proctime-state", longs))
              .update(1L);
          expected = "not owned";
          break;
        default:
          throw new AssertionError(mode);
      }
      var malformed = snapshot(view);
      var failure = assertThrows(Exception.class, () -> target.initializeState(malformed));
      var messages = new StringBuilder();
      for (Throwable cause = failure; cause != null; cause = cause.getCause())
        messages.append(cause.getMessage()).append('\n');
      assertTrue(messages.toString().contains(expected), messages.toString());
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static void check(
      String type,
      boolean keyless,
      boolean stockStore,
      boolean nativeStore,
      int offset,
      long[] clocks,
      long ttl)
      throws Exception {
    var plan = new CanonicalBoundedRangeInteropTest.Plan(type, keyless, true, offset, ttl);
    int split = offset == 0 ? 2 : 4;
    OperatorSubtaskState original;
    List<String> expected;
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var stock = plan.stock(stockStore)) {
      stock.open();
      stockSteps(plan, stock, clocks, 0, split);
      original = snapshot(stock);
      stockSteps(plan, stock, clocks, split, clocks.length);
      expected = plan.stockOutput(stock);
      assertFalse(expected.isEmpty());
    }
    OperatorSubtaskState projected;
    try (var nativeOp = plan.nativeHarness(nativeStore)) {
      nativeOp.initializeState(original);
      nativeOp.open();
      projected = snapshot(nativeOp);
      nativeSteps(plan, nativeOp, clocks, split, clocks.length);
      assertEquals(expected, plan.nativeOutput(nativeOp));
    }
    try (var stock = plan.stock(stockStore)) {
      stock.initializeState(projected);
      stock.open();
      stockSteps(plan, stock, clocks, split, clocks.length);
      assertEquals(expected, plan.stockOutput(stock));
    }
    OperatorSubtaskState fromNative;
    try (var nativeOp = plan.nativeHarness(nativeStore)) {
      nativeOp.open();
      nativeSteps(plan, nativeOp, clocks, 0, split);
      fromNative = snapshot(nativeOp);
      nativeSteps(plan, nativeOp, clocks, split, clocks.length);
      assertEquals(expected, plan.nativeOutput(nativeOp));
    }
    try (var stock = plan.stock(stockStore)) {
      stock.initializeState(fromNative);
      stock.open();
      stockSteps(plan, stock, clocks, split, clocks.length);
      assertEquals(expected, plan.stockOutput(stock));
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static RowData input(CanonicalBoundedRangeInteropTest.Plan plan, int index) {
    // Neither materialized timestamp is the processing clock; NULL keys/values must survive.
    return index == 4
        ? plan.nullRow(null, 9000 + index)
        : plan.row(null, index == 5 ? 2 : index + 1, 9000 + index);
  }

  static void stockSteps(
      CanonicalBoundedRangeInteropTest.Plan plan,
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
      long[] clocks,
      int start,
      int end)
      throws Exception {
    harness.getOutput().clear();
    for (int i = start; i < end; i++) {
      harness.setProcessingTime(clocks[i]);
      plan.pushStock(harness, input(plan, i));
    }
  }

  static void nativeSteps(
      CanonicalBoundedRangeInteropTest.Plan plan,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      long[] clocks,
      int start,
      int end)
      throws Exception {
    // Close each prefix's owned Arrow output before replacing it with the suffix.
    plan.nativeOutput(harness);
    for (int i = start; i < end; i++) {
      harness.setProcessingTime(clocks[i]);
      plan.push(harness, input(plan, i));
    }
  }

  static OperatorSubtaskState snapshot(AbstractStreamOperatorTestHarness<?> harness)
      throws Exception {
    return harness
        .snapshotWithLocalState(1, 100, SavepointType.savepoint(SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }
}
