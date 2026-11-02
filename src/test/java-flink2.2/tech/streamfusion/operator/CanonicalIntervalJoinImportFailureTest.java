package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;

class CanonicalIntervalJoinImportFailureTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String mode :
        List.of(
            "missingTimer",
            "missingDeadline",
            "wrongDeadline",
            "orphanTimer",
            "processingTimer",
            "wrongKey",
            "wrongBucket",
            "retraction",
            "emptyBucket",
            "unknownState",
            "unknownTimer"))
      for (boolean stockDisk : new boolean[] {false, true})
        for (boolean nativeDisk : new boolean[] {false, true})
          cases.add(Arguments.of(mode, stockDisk, nativeDisk));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void corruptStateFailsBeforeAnyNativeImportAndReleasesArrow(
      String mode, boolean stockDisk, boolean nativeDisk) throws Exception {
    var plan = new CanonicalIntervalJoinInteropTest.Plan(3, false);
    OperatorSubtaskState saved;
    try (var source = plan.stock(stockDisk)) {
      source.open();
      plan.prefix(source);
      plan.stockOutput(source);
      var backend = CanonicalIntervalJoinInteropTest.backend(source);
      var operator =
          (KeyedCoProcessOperator<RowData, RowData, RowData, RowData>) source.getOperator();
      var timers =
          operator.getInternalTimerService(
              "user-timers", VoidNamespaceSerializer.INSTANCE, operator);
      var key = plan.selector.getKey(plan.row(1L, 0L, 100));
      backend.setCurrentKey(key);
      var deadline =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("IntervalJoinRightTimerState", LongSerializer.INSTANCE));
      var cache = cache(backend, plan);
      if (mode.equals("missingTimer")) timers.deleteEventTimeTimer(VoidNamespace.INSTANCE, 201);
      else if (mode.equals("missingDeadline")) deadline.clear();
      else if (mode.equals("wrongDeadline")) deadline.update(999L);
      else if (mode.equals("orphanTimer"))
        timers.registerEventTimeTimer(VoidNamespace.INSTANCE, 999);
      else if (mode.equals("processingTimer"))
        timers.registerProcessingTimeTimer(VoidNamespace.INSTANCE, 999);
      else if (mode.equals("wrongKey"))
        cache.put(100L, List.of(Tuple2.of(plan.row(99L, 1L, 100), false)));
      else if (mode.equals("wrongBucket")) {
        var rows = cache.get(100L);
        cache.remove(100L);
        cache.put(101L, rows);
      } else if (mode.equals("retraction")) {
        var row = plan.row(1L, 1L, 100);
        row.setRowKind(RowKind.DELETE);
        cache.put(100L, List.of(Tuple2.of(row, true)));
      } else if (mode.equals("emptyBucket")) cache.put(100L, new ArrayList<>());
      else if (mode.equals("unknownState"))
        backend
            .getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>("interval-foreign", LongSerializer.INSTANCE))
            .update(1L);
      else if (mode.equals("unknownTimer"))
        operator
            .getInternalTimerService(
                "interval-foreign",
                LongSerializer.INSTANCE,
                new org.apache.flink.streaming.api.operators.Triggerable<RowData, Long>() {
                  @Override
                  public void onEventTime(
                      org.apache.flink.streaming.api.operators.InternalTimer<RowData, Long>
                          timer) {}

                  @Override
                  public void onProcessingTime(
                      org.apache.flink.streaming.api.operators.InternalTimer<RowData, Long>
                          timer) {}
                })
            .registerEventTimeTimer(1L, 999);
      saved = CanonicalIntervalJoinInteropTest.snapshot(source);
    }
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var target = plan.nativeHarness(nativeDisk)) {
      var failure =
          assertThrows(
              Exception.class,
              () -> {
                target.initializeState(saved);
                target.open();
              },
              mode);
      var reasons = new StringBuilder();
      for (Throwable cause = failure; cause != null; cause = cause.getCause())
        reasons.append(cause.getMessage()).append('\n');
      assertTrue(
          reasons.toString().toLowerCase(java.util.Locale.ROOT).contains("stock interval")
              || reasons.toString().contains("Cannot import canonical state"),
          reasons.toString());
      var operator = (NativeIntervalJoinOperator) target.getOperator();
      if (operator.handle != 0) {
        var imported = new AtomicInteger();
        Native.BatchReceiver count =
            (a, s) -> {
              try (var array = ArrowArray.wrap(a);
                  var schema = ArrowSchema.wrap(s);
                  var root =
                      Data.importVectorSchemaRoot(
                          NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
                imported.addAndGet(root.getRowCount());
              }
            };
        Native.exportIntervalJoinCanonicalRows(operator.handle, true, count);
        Native.exportIntervalJoinCanonicalRows(operator.handle, false, count);
        assertEquals(
            0, imported.get(), "preflight imported a valid prefix before rejecting later state");
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static MapState<Long, List<Tuple2<RowData, Boolean>>> cache(
      org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<RowData> backend,
      CanonicalIntervalJoinInteropTest.Plan plan)
      throws Exception {
    var tuple =
        new TupleSerializer<Tuple2<RowData, Boolean>>(
            (Class) Tuple2.class,
            new TypeSerializer<?>[] {
              new RowDataSerializer(plan.input), BooleanSerializer.INSTANCE
            });
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new MapStateDescriptor<>(
            "IntervalJoinLeftCache", LongSerializer.INSTANCE, new ListSerializer<>(tuple)));
  }

  @Test
  void eventTimeStateCannotBindToAProcessingTimeOperator() throws Exception {
    var plan = new CanonicalIntervalJoinInteropTest.Plan(0, false);
    var operator =
        new NativeIntervalJoinOperator(
            new int[] {0},
            new int[] {0},
            2,
            2,
            -100,
            100,
            0,
            plan.input,
            plan.input,
            EncodedPredicate.NONE,
            true,
            new int[] {-1},
            128);
    assertThrows(
        IllegalArgumentException.class, () -> operator.setStockStateContract(plan.contract()));
  }
}
