package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;

class CanonicalWindowJoinImportFailureTest {
  @org.junit.jupiter.api.Test
  void releasedEventTimeContractCannotBeBoundToANativeProcessingTimeJob() {
    var plan = new CanonicalWindowJoinInteropTest.Plan("INNER", false, "UTC");
    var contract =
        new tech.streamfusion.state.StockWindowJoinStateContract(
            new RowDataSerializer(plan.keyType),
            plan.keyType,
            CanonicalWindowJoinInteropTest.INPUT,
            CanonicalWindowJoinInteropTest.INPUT,
            new int[] {0},
            new int[] {0},
            2,
            3,
            2,
            3,
            "UTC",
            tech.streamfusion.state.StockWindowJoinStateContract.WindowSpec.fixed(1000));
    var operator =
        new NativeWindowJoinOperator(
            new int[] {0},
            new int[] {0},
            2,
            3,
            2,
            3,
            0,
            CanonicalWindowJoinInteropTest.INPUT,
            CanonicalWindowJoinInteropTest.INPUT,
            EncodedPredicate.NONE,
            true,
            1000,
            1000,
            false,
            new int[] {-1},
            128);
    assertThrows(IllegalArgumentException.class, () -> operator.setStockStateContract(contract));
  }

  static Stream<Arguments> cases() {
    var result = new ArrayList<Arguments>();
    for (String mode :
        List.of(
            "missingTimer",
            "timerOnly",
            "wrongDeadline",
            "processingTimer",
            "wrongKey",
            "wrongNamespace",
            "wrongStart",
            "retraction",
            "unknownState"))
      for (boolean stockStore : new boolean[] {false, true})
        for (boolean nativeStore : new boolean[] {false, true})
          result.add(Arguments.of(mode, stockStore, nativeStore));
    return result.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  @SuppressWarnings("unchecked")
  void malformedStockStateFailsBeforeLeavingNativeOrArrowOwnership(
      String mode, boolean stockStore, boolean nativeStore) throws Exception {
    var plan = new CanonicalWindowJoinInteropTest.Plan("INNER", false, "UTC");
    OperatorSubtaskState saved;
    try (var source = plan.stock(stockStore)) {
      source.open();
      plan.prefix(source);
      var backend =
          (CheckpointableKeyedStateBackend<RowData>)
              source.getOperator().<RowData>getKeyedStateBackend();
      var keyRows = new RowDataSerializer(plan.keyType);
      var key = keyRows.toBinaryRow(GenericRowData.of(1L)).copy();
      var other = keyRows.toBinaryRow(GenericRowData.of(99L)).copy();
      long end = plan.local + 2000;
      var operator =
          (org.apache.flink.table.runtime.operators.join.window.WindowJoinOperator)
              source.getOperator();
      var timers =
          operator.getInternalTimerService("window-timers", LongSerializer.INSTANCE, operator);
      if (mode.equals("missingTimer")) {
        var pending = new ArrayList<TimerHeapInternalTimer<RowData, Long>>();
        timers.forEachEventTimeTimer(
            (namespace, timestamp) ->
                pending.add(
                    new TimerHeapInternalTimer<>(
                        timestamp, keyRows.copy(backend.getCurrentKey()), namespace)));
        for (var timer : pending) {
          backend.setCurrentKey(timer.getKey());
          timers.deleteEventTimeTimer(timer.getNamespace(), timer.getTimestamp());
        }
      } else if (mode.equals("timerOnly") || mode.equals("wrongDeadline")) {
        backend.setCurrentKey(other);
        timers.registerEventTimeTimer(
            end, plan.deadline(end) + (mode.equals("wrongDeadline") ? 1 : 0));
      } else if (mode.equals("processingTimer")) {
        backend.setCurrentKey(key);
        timers.registerProcessingTimeTimer(end, plan.deadline(end));
      } else if (mode.equals("unknownState")) {
        backend.setCurrentKey(key);
        backend
            .getPartitionedState(
                end,
                LongSerializer.INSTANCE,
                new ValueStateDescriptor<>("unowned-window-state", LongSerializer.INSTANCE))
            .update(42L);
      } else {
        long namespace = mode.equals("wrongNamespace") ? end + 1000 : end;
        var storedKey = mode.equals("wrongKey") ? other : key;
        backend.setCurrentKey(storedKey);
        var row =
            GenericRowData.ofKind(
                mode.equals("retraction") ? RowKind.DELETE : RowKind.INSERT,
                1L,
                999L,
                end - 1000 + (mode.equals("wrongStart") ? 1 : 0),
                end);
        backend
            .getPartitionedState(
                namespace,
                LongSerializer.INSTANCE,
                new ListStateDescriptor<>(
                    "left-records", new RowDataSerializer(CanonicalWindowJoinInteropTest.INPUT)))
            .add(row);
        timers.registerEventTimeTimer(namespace, plan.deadline(namespace));
      }
      saved = CanonicalWindowJoinInteropTest.snapshot(source);
    }
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    String handles = Native.liveNativeHandles();
    var error =
        assertThrows(
            Exception.class,
            () -> {
              try (var target = plan.nativeHarness(nativeStore)) {
                target.initializeState(saved);
                target.open();
              }
            });
    StringBuilder message = new StringBuilder();
    for (Throwable cause = error; cause != null; cause = cause.getCause())
      message.append(cause).append('\n');
    String expected =
        Map.of(
                "missingTimer",
                "no firing timer",
                "timerOnly",
                "timer has no state",
                "wrongDeadline",
                "Unexpected stock window-join timer",
                "processingTimer",
                "does not consume these pending timers",
                "wrongKey",
                "row disagrees with its key",
                "wrongNamespace",
                "row disagrees with its namespace",
                "wrongStart",
                "row disagrees with its namespace",
                "retraction",
                "row disagrees with its namespace",
                "unknownState",
                "descriptor is not owned")
            .get(mode);
    assertTrue(message.toString().contains(expected), message.toString());
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
    assertEquals(handles, Native.liveNativeHandles());
  }
}
