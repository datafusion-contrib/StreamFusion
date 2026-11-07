package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;

class CanonicalIntervalProctimeImportFailureTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String mode :
        List.of("eventTimer", "missingDeadline", "wrongKey", "orphanProcessingTimer"))
      for (boolean stockDisk : new boolean[] {false, true})
        for (boolean nativeDisk : new boolean[] {false, true})
          cases.add(Arguments.of(mode, stockDisk, nativeDisk));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void incompatibleProcessingStateFailsBeforeAnyNativeRows(
      String mode, boolean stockDisk, boolean nativeDisk) throws Exception {
    var plan = new CanonicalIntervalJoinInteropTest.Plan(3, true, "long", true);
    OperatorSubtaskState saved;
    try (var source = plan.stock(stockDisk)) {
      source.open();
      source.setProcessingTime(500);
      plan.prefix(source);
      plan.stockOutput(source);
      var backend = CanonicalIntervalJoinInteropTest.backend(source);
      backend.setCurrentKey(plan.selector.getKey(plan.row(1L, 10L, 123)));
      var operator =
          (KeyedCoProcessOperator<
                  org.apache.flink.table.data.RowData,
                  org.apache.flink.table.data.RowData,
                  org.apache.flink.table.data.RowData,
                  org.apache.flink.table.data.RowData>)
              source.getOperator();
      var timers =
          operator.getInternalTimerService(
              "user-timers", VoidNamespaceSerializer.INSTANCE, operator);
      if (mode.equals("eventTimer")) timers.registerEventTimeTimer(VoidNamespace.INSTANCE, 999);
      else if (mode.equals("orphanProcessingTimer"))
        timers.registerProcessingTimeTimer(VoidNamespace.INSTANCE, 999);
      else if (mode.equals("missingDeadline"))
        backend
            .getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>("IntervalJoinRightTimerState", LongSerializer.INSTANCE))
            .clear();
      else
        CanonicalIntervalJoinImportFailureTest.cache(backend, plan)
            .put(500L, List.of(Tuple2.of(plan.row(99L, 1L, 123), false)));
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
              });
      var reasons = new StringBuilder();
      for (Throwable cause = failure; cause != null; cause = cause.getCause())
        reasons.append(cause.getMessage());
      assertTrue(
          reasons.toString().toLowerCase(Locale.ROOT).contains("stock interval")
              || reasons.toString().contains("Cannot import canonical state"),
          reasons.toString());
      var operator = (NativeIntervalJoinOperator) target.getOperator();
      if (operator.handle != 0) {
        var count = new AtomicInteger();
        Native.BatchReceiver receive =
            (a, s) -> {
              try (var array = ArrowArray.wrap(a);
                  var schema = ArrowSchema.wrap(s);
                  var root =
                      Data.importVectorSchemaRoot(
                          NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
                count.addAndGet(root.getRowCount());
              }
            };
        Native.exportIntervalJoinCanonicalRows(operator.handle, true, receive);
        Native.exportIntervalJoinCanonicalRows(operator.handle, false, receive);
        assertEquals(0, count.get());
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @Test
  void processingContractCannotBindToAnEventTimeOperator() throws Exception {
    var plan = new CanonicalIntervalJoinInteropTest.Plan(0, true, "long", true);
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
            false,
            new int[] {-1},
            128);
    assertThrows(
        IllegalArgumentException.class, () -> operator.setStockStateContract(plan.contract()));
  }
}
