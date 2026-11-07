package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalIntervalProctimeInteropTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (int kind = 0; kind < 4; kind++)
      for (boolean stockDisk : new boolean[] {false, true})
        for (boolean nativeDisk : new boolean[] {false, true})
          for (boolean nativeFirst : new boolean[] {false, true})
            cases.add(Arguments.of(kind, stockDisk, nativeDisk, nativeFirst));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void independentClockAndOriginalPayloadSurviveReexportNativeRecoveryAndIdleTimers(
      int kind, boolean stockDisk, boolean nativeDisk, boolean nativeFirst) throws Exception {
    var plan = new CanonicalIntervalJoinInteropTest.Plan(kind, true, "long", true);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    OperatorSubtaskState saved;
    try (var reference = plan.stock(stockDisk)) {
      reference.open();
      prefix(plan, reference);
      assertEquals(1, plan.stockOutput(reference).size());
      if (nativeFirst) {
        try (var source = plan.nativeHarness(nativeDisk)) {
          source.open();
          prefixNative(plan, source);
          assertEquals(1, plan.nativeOutput(source).size());
          saved = CanonicalIntervalJoinInteropTest.snapshot(source);
        }
      } else saved = CanonicalIntervalJoinInteropTest.snapshot(reference);
      try (var stock = plan.stock(!stockDisk);
          var target = plan.nativeHarness(nativeDisk)) {
        stock.initializeState(saved);
        stock.open();
        stock.setProcessingTime(550);
        target.initializeState(saved);
        target.open();
        target.setProcessingTime(550);
        assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
        assertTrue(plan.stockOutput(stock).isEmpty());
        assertTrue(plan.nativeOutput(target).isEmpty());
        plan.verifyMetadata(stock);
        var canonical = CanonicalIntervalJoinInteropTest.snapshot(target);
        try (var again = plan.stock(stockDisk)) {
          again.initializeState(canonical);
          again.open();
          again.setProcessingTime(550);
          target.prepareSnapshotPreBarrier(7);
          var nativeCheckpoint = target.snapshot(7, 0);
          try (var recovered = plan.nativeHarness(nativeDisk)) {
            recovered.initializeState(nativeCheckpoint);
            recovered.open();
            recovered.setProcessingTime(550);
            // Arrival clock is independent of the row's NULL or arbitrary original time field.
            for (var h : List.of(reference, stock, again)) {
              h.setProcessingTime(590);
              h.processElement2(new StreamRecord<>(payload(plan, 2L, 40L, 999L)));
            }
            for (var h : List.of(target, recovered)) {
              h.setProcessingTime(590);
              plan.push(h, false, payload(plan, 2L, 40L, 999L));
            }
            var expected = plan.stockOutput(reference);
            assertEquals(1, expected.size());
            assertTrue(expected.get(0).contains("null"));
            assertEquals(expected, plan.stockOutput(stock));
            assertEquals(expected, plan.stockOutput(again));
            assertEquals(expected, plan.nativeOutput(target));
            assertEquals(expected, plan.nativeOutput(recovered));
            for (var h : List.of(reference, stock, again)) {
              h.setProcessingTime(600);
              h.processElement1(new StreamRecord<>(payload(plan, 3L, 50L, 77L)));
            }
            for (var h : List.of(target, recovered)) {
              h.setProcessingTime(600);
              plan.push(h, true, payload(plan, 3L, 50L, 77L));
            }
            for (long time : new long[] {601, 611, 651, 691, 701, 1000}) {
              for (var h : List.of(reference, stock, again)) h.setProcessingTime(time);
              target.setProcessingTime(time);
              recovered.setProcessingTime(time);
              expected = plan.stockOutput(reference);
              assertEquals(expected, plan.stockOutput(stock));
              assertEquals(expected, plan.stockOutput(again));
              assertEquals(expected, plan.nativeOutput(target));
              assertEquals(expected, plan.nativeOutput(recovered));
            }
          }
        }
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @MethodSource("timerOnlyCases")
  void timerOnlyProcessingKeysRearmWithoutInput(int kind, boolean stockDisk, boolean nativeDisk)
      throws Exception {
    var plan = new CanonicalIntervalJoinInteropTest.Plan(kind, true, "long", true);
    OperatorSubtaskState saved;
    try (var source = plan.stock(stockDisk)) {
      source.open();
      source.setProcessingTime(500);
      var backend = CanonicalIntervalJoinInteropTest.backend(source);
      backend.setCurrentKey(plan.selector.getKey(payload(plan, 99L, 0L, null)));
      backend
          .getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("IntervalJoinRightTimerState", LongSerializer.INSTANCE))
          .update(650L);
      var operator =
          (KeyedCoProcessOperator<RowData, RowData, RowData, RowData>) source.getOperator();
      operator
          .getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, operator)
          .registerProcessingTimeTimer(VoidNamespace.INSTANCE, 650);
      saved = CanonicalIntervalJoinInteropTest.snapshot(source);
    }
    try (var target = plan.nativeHarness(nativeDisk)) {
      target.initializeState(saved);
      target.open();
      target.setProcessingTime(650);
      assertTrue(plan.nativeOutput(target).isEmpty());
      try (var stock = plan.stock(!stockDisk)) {
        stock.initializeState(CanonicalIntervalJoinInteropTest.snapshot(target));
        stock.open();
        var backend = CanonicalIntervalJoinInteropTest.backend(stock);
        backend.setCurrentKey(plan.selector.getKey(payload(plan, 99L, 0L, null)));
        assertNull(
            backend
                .getPartitionedState(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    new ValueStateDescriptor<>(
                        "IntervalJoinRightTimerState", LongSerializer.INSTANCE))
                .value());
      }
    }
  }

  static Stream<Arguments> timerOnlyCases() {
    var cases = new ArrayList<Arguments>();
    for (int kind = 0; kind < 4; kind++)
      for (boolean stockDisk : new boolean[] {false, true})
        for (boolean nativeDisk : new boolean[] {false, true})
          cases.add(Arguments.of(kind, stockDisk, nativeDisk));
    return cases.stream();
  }

  static Stream<Arguments> nativeCases() {
    var cases = new ArrayList<Arguments>();
    for (int kind = 0; kind < 4; kind++)
      for (boolean nativeDisk : new boolean[] {false, true})
        cases.add(Arguments.of(kind, nativeDisk));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("nativeCases")
  void endOfInputDoesNotFireFutureStockProcessingCallbacks(int kind, boolean nativeDisk)
      throws Exception {
    var plan = new CanonicalIntervalJoinInteropTest.Plan(kind, true, "long", true);
    try (var nativeOp = plan.nativeHarness(nativeDisk)) {
      nativeOp.open();
      nativeOp.setProcessingTime(500);
      plan.push(nativeOp, true, payload(plan, 1L, 10L, null));
      nativeOp.getOperator().finish();
      assertTrue(plan.nativeOutput(nativeOp).isEmpty());
      try (var stock = plan.stock(false)) {
        stock.initializeState(CanonicalIntervalJoinInteropTest.snapshot(nativeOp));
        stock.open();
        var backend = CanonicalIntervalJoinInteropTest.backend(stock);
        try (var keys = backend.getKeys("IntervalJoinLeftCache", VoidNamespace.INSTANCE)) {
          assertEquals(1, keys.count());
        }
        stock.setProcessingTime(601);
        assertEquals(kind == 1 || kind == 3 ? 1 : 0, plan.stockOutput(stock).size());
      }
    }
  }

  static Stream<Arguments> typedKeys() {
    return CanonicalIntervalJoinInteropTest.typedKeys();
  }

  @ParameterizedTest
  @MethodSource("typedKeys")
  void typedActualKeysKeepIndependentClocksThroughBothCanonicalDirectionsAndNativeRecovery(
      String key, int kind, boolean stockDisk, boolean nativeDisk) throws Exception {
    var plan = new CanonicalIntervalJoinInteropTest.Plan(kind, true, key, true);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var reference = plan.stock(stockDisk);
        var original = plan.nativeHarness(nativeDisk)) {
      reference.open();
      original.open();
      reference.setProcessingTime(500);
      original.setProcessingTime(500);
      plan.prefix(reference);
      plan.prefixNative(original);
      var expected = plan.stockOutput(reference);
      assertFalse(expected.isEmpty());
      assertEquals(expected, plan.nativeOutput(original));
      try (var stock = plan.stock(!stockDisk)) {
        stock.initializeState(CanonicalIntervalJoinInteropTest.snapshot(original));
        stock.open();
        stock.setProcessingTime(500);
        plan.verifyMetadata(stock);
        try (var restored = plan.nativeHarness(nativeDisk)) {
          restored.initializeState(CanonicalIntervalJoinInteropTest.snapshot(stock));
          restored.open();
          restored.setProcessingTime(500);
          restored.prepareSnapshotPreBarrier(9);
          var checkpoint = restored.snapshot(9, 0);
          try (var recovered = plan.nativeHarness(nativeDisk)) {
            recovered.initializeState(checkpoint);
            recovered.open();
            for (var h : List.of(reference, stock)) {
              h.setProcessingTime(590);
              plan.extra(h);
            }
            for (var h : List.of(original, restored, recovered)) {
              h.setProcessingTime(590);
              plan.extraNative(h);
            }
            expected = plan.stockOutput(reference);
            assertEquals(expected, plan.stockOutput(stock));
            assertEquals(expected, plan.nativeOutput(original));
            assertEquals(expected, plan.nativeOutput(restored));
            assertEquals(expected, plan.nativeOutput(recovered));
            for (long time : new long[] {601, 691, 1000}) {
              for (var h : List.of(reference, stock)) h.setProcessingTime(time);
              for (var h : List.of(original, restored, recovered)) h.setProcessingTime(time);
              expected = plan.stockOutput(reference);
              assertEquals(expected, plan.stockOutput(stock));
              assertEquals(expected, plan.nativeOutput(original));
              assertEquals(expected, plan.nativeOutput(restored));
              assertEquals(expected, plan.nativeOutput(recovered));
            }
          }
        }
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @MethodSource("nativeCases")
  void overdueCallbacksFireOnOpenWithoutWaitingForNewInput(int kind, boolean nativeDisk)
      throws Exception {
    var plan = new CanonicalIntervalJoinInteropTest.Plan(kind, true, "long", true);
    OperatorSubtaskState state;
    java.util.List<String> expected;
    try (var stock = plan.stock(false)) {
      stock.open();
      stock.setProcessingTime(500);
      stock.processElement1(new StreamRecord<>(payload(plan, 1L, 10L, null)));
      state = CanonicalIntervalJoinInteropTest.snapshot(stock);
      stock.setProcessingTime(1000);
      expected = plan.stockOutput(stock);
    }
    try (var target = plan.nativeHarness(nativeDisk)) {
      target.initializeState(state);
      target.setProcessingTime(1000);
      target.open();
      assertEquals(expected, plan.nativeOutput(target));
      try (var stock = plan.stock(true)) {
        stock.initializeState(CanonicalIntervalJoinInteropTest.snapshot(target));
        stock.open();
        stock.setProcessingTime(1000);
        assertTrue(plan.stockOutput(stock).isEmpty());
      }
    }
  }

  private static RowData payload(
      CanonicalIntervalJoinInteropTest.Plan plan, Long key, Long value, Long original) {
    return new RowDataSerializer(plan.input)
        .toBinaryRow(
            GenericRowData.of(
                key, value, original == null ? null : TimestampData.fromEpochMillis(original)))
        .copy();
  }

  private static void prefix(
      CanonicalIntervalJoinInteropTest.Plan plan,
      KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h)
      throws Exception {
    h.setProcessingTime(500);
    h.processElement1(new StreamRecord<>(payload(plan, 1L, 10L, 123L)));
    h.processElement1(new StreamRecord<>(payload(plan, 2L, 20L, null)));
    h.setProcessingTime(510);
    h.processElement1(new StreamRecord<>(payload(plan, 4L, 21L, null)));
    h.setProcessingTime(550);
    h.processElement2(new StreamRecord<>(payload(plan, 1L, 30L, 456L)));
  }

  private static void prefixNative(
      CanonicalIntervalJoinInteropTest.Plan plan,
      KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> h)
      throws Exception {
    h.setProcessingTime(500);
    plan.push(h, true, payload(plan, 1L, 10L, 123L));
    plan.push(h, true, payload(plan, 2L, 20L, null));
    h.setProcessingTime(510);
    plan.push(h, true, payload(plan, 4L, 21L, null));
    h.setProcessingTime(550);
    plan.push(h, false, payload(plan, 1L, 30L, 456L));
  }
}
