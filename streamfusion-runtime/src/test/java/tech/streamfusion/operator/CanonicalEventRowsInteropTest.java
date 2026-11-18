package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.configuration.*;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.*;
import org.apache.flink.runtime.state.*;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

/** Actual released ROWS state, including retained MAX peers and untouched re-export. */
class CanonicalEventRowsInteropTest {
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
              for (int layout = 0; layout < 5; layout++)
                cases.add(Arguments.of(type, keyless, stock, nativeStore, offset, layout));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void actualStockCountersRowsRawAccumulatorsAndTimersRoundTripWithoutInputReplay(
      String type, boolean keyless, boolean stockStore, boolean nativeStore, int offset, int layout)
      throws Exception {
    var plan = new CanonicalBoundedRangeInteropTest.Plan(type, keyless, false, true, offset, 0);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    OperatorSubtaskState original;
    List<String> expected, expectedFinal;
    try (var stock = plan.stock(stockStore)) {
      stock.open();
      prefixStock(plan, stock, layout);
      original = snapshot(stock);
      suffixStock(plan, stock, layout);
      expected = plan.stockOutput(stock);
      if (layout != 4) assertFalse(expected.isEmpty());
      expectedFinal = digest(plan, snapshot(stock));
    }
    OperatorSubtaskState projected;
    try (var nativeOp = plan.nativeHarness(nativeStore)) {
      nativeOp.initializeState(original);
      nativeOp.open();
      projected = snapshot(nativeOp);
      assertEquals(
          digest(plan, original), digest(plan, projected), "Untouched stock import/export");
      suffixNative(plan, nativeOp, layout);
      assertEquals(expected, plan.nativeOutput(nativeOp));
      assertEquals(
          expectedFinal, digest(plan, snapshot(nativeOp)), "State after continued callbacks");
    }
    try (var stock = plan.stock(stockStore)) {
      stock.initializeState(projected);
      stock.open();
      suffixStock(plan, stock, layout);
      assertEquals(expected, plan.stockOutput(stock));
    }
    OperatorSubtaskState fromNative;
    try (var nativeOp = plan.nativeHarness(nativeStore)) {
      nativeOp.open();
      prefixNative(plan, nativeOp, layout);
      fromNative = snapshot(nativeOp);
      assertEquals(digest(plan, original), digest(plan, fromNative), "Direct native projection");
      suffixNative(plan, nativeOp, layout);
      assertEquals(expected, plan.nativeOutput(nativeOp));
      assertEquals(
          expectedFinal, digest(plan, snapshot(nativeOp)), "State after continued callbacks");
    }
    try (var stock = plan.stock(stockStore)) {
      stock.initializeState(fromNative);
      stock.open();
      suffixStock(plan, stock, layout);
      assertEquals(expected, plan.stockOutput(stock));
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static Stream<Arguments> corruptions() {
    var cases = new ArrayList<Arguments>();
    for (String corruption :
        List.of(
            "missingCount",
            "zeroCount",
            "largeCount",
            "missingLast",
            "aheadLast",
            "missingAccumulator",
            "hiddenCount",
            "missingTimer",
            "extraTimer",
            "processingTimer",
            "emptyBucket",
            "wrongKey",
            "wrongTime",
            "retraction",
            "unknownState"))
      for (boolean stock : new boolean[] {false, true})
        for (boolean nativeStore : new boolean[] {false, true})
          cases.add(Arguments.of(corruption, stock, nativeStore));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("corruptions")
  @SuppressWarnings("unchecked")
  void corruptStateRejectsBeforeInstallingNativeRows(
      String corruption, boolean stockStore, boolean nativeStore) throws Exception {
    var p = new CanonicalBoundedRangeInteropTest.Plan("BIGINT", false, false, true, 2, 0);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var stock = p.stock(stockStore);
        var view = p.view(stockStore);
        var target = p.nativeHarness(nativeStore)) {
      stock.open();
      prefixStock(p, stock, 0);
      view.initializeState(snapshot(stock));
      view.open();
      var backend =
          (CheckpointableKeyedStateBackend<RowData>)
              view.getOperator().<RowData>getKeyedStateBackend();
      backend.setCurrentKey(p.selector.getKey(p.row(null, 1, 100)));
      var ns = VoidNamespace.INSTANCE;
      var namespaces = VoidNamespaceSerializer.INSTANCE;
      var longs = org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE;
      var counter =
          backend.getPartitionedState(
              ns,
              namespaces,
              new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                  "processedCountState", longs));
      var last =
          backend.getPartitionedState(
              ns,
              namespaces,
              new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                  "lastTriggeringTsState", longs));
      var acc =
          backend.getPartitionedState(
              ns,
              namespaces,
              new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                  "accState", new RowDataSerializer(p.accumulator)));
      var inputs =
          backend.getPartitionedState(
              ns,
              namespaces,
              new org.apache.flink.api.common.state.MapStateDescriptor<>(
                  "inputState",
                  longs,
                  new org.apache.flink.api.common.typeutils.base.ListSerializer<>(
                      new RowDataSerializer(p.input))));
      var timerSerializer =
          new TimerSerializer<>(p.selector.getProducedType().toSerializer(), namespaces);
      var events =
          backend.<TimerHeapInternalTimer<RowData, VoidNamespace>>create(
              "_timer_state/event_user-timers", timerSerializer);
      String expected = "Event ROWS";
      switch (corruption) {
        case "missingCount":
          counter.clear();
          break;
        case "zeroCount":
          counter.update(0L);
          break;
        case "largeCount":
          counter.update(4L);
          break;
        case "missingLast":
          last.clear();
          expected = "Pending bounded OVER";
          break;
        case "aheadLast":
          last.update(101L);
          break;
        case "missingAccumulator":
          acc.clear();
          break;
        case "hiddenCount":
          {
            assertTrue(p.hiddenCount >= 0);
            var saved =
                new org.apache.flink.table.data.GenericRowData(p.accumulator.getFieldCount());
            for (int i = 0; i < saved.getArity(); i++)
              saved.setField(
                  i,
                  RowData.createFieldGetter(p.accumulator.getTypeAt(i), i)
                      .getFieldOrNull(acc.value()));
            saved.setField(p.hiddenCount, 999L);
            acc.update(saved);
            break;
          }
        case "missingTimer":
          events.remove(new TimerHeapInternalTimer<>(300L, backend.getCurrentKey(), ns));
          expected = "Pending bounded OVER";
          break;
        case "extraTimer":
          events.add(new TimerHeapInternalTimer<>(400L, backend.getCurrentKey(), ns));
          break;
        case "processingTimer":
          backend
              .<TimerHeapInternalTimer<RowData, VoidNamespace>>create(
                  "_timer_state/processing_user-timers", timerSerializer)
              .add(new TimerHeapInternalTimer<>(400L, backend.getCurrentKey(), ns));
          expected = "pending timers";
          break;
        case "emptyBucket":
          inputs.put(300L, new ArrayList<>());
          expected = "Empty";
          break;
        case "wrongKey":
          inputs.put(300L, List.of(p.row(9L, 1, 300)));
          expected = "key/time/kind";
          break;
        case "wrongTime":
          inputs.put(300L, List.of(p.row(null, 1, 301)));
          expected = "key/time/kind";
          break;
        case "retraction":
          {
            var row = p.row(null, 1, 300);
            row.setRowKind(org.apache.flink.types.RowKind.DELETE);
            inputs.put(300L, List.of(row));
            expected = "key/time/kind";
            break;
          }
        case "unknownState":
          backend
              .getPartitionedState(
                  ns,
                  namespaces,
                  new org.apache.flink.api.common.state.ValueStateDescriptor<>("unowned", longs))
              .update(1L);
          expected = "not owned";
          break;
        default:
          throw new AssertionError(corruption);
      }
      var malformed = snapshot(view);
      var error = assertThrows(Exception.class, () -> target.initializeState(malformed));
      var messages = new StringBuilder();
      for (Throwable cause = error; cause != null; cause = cause.getCause())
        messages.append(cause.getMessage()).append('\n');
      assertTrue(messages.toString().contains(expected), messages.toString());
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static OperatorSubtaskState snapshot(AbstractStreamOperatorTestHarness<?> harness)
      throws Exception {
    return harness
        .snapshotWithLocalState(1, 100, SavepointType.savepoint(SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  private static long time(int layout, boolean pending) {
    return layout >= 2 ? (pending ? Long.MAX_VALUE : Long.MAX_VALUE - 1) : (pending ? 300 : 100);
  }

  static void prefixStock(
      CanonicalBoundedRangeInteropTest.Plan p,
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h,
      int layout)
      throws Exception {
    if (layout != 1 && layout != 3) {
      for (int i = 0; i < 5; i++) p.pushStock(h, p.row(null, i + 1, time(layout, layout == 4)));
      h.processWatermark(new Watermark(time(layout, layout == 4)));
      p.stockOutput(h);
    }
    if (layout != 4) {
      p.pushStock(h, p.row(null, 7, time(layout, true)));
      p.pushStock(h, p.nullRow(null, time(layout, true)));
      p.pushStock(h, p.row(2L, 11, time(layout, true)));
    }
  }

  static void prefixNative(
      CanonicalBoundedRangeInteropTest.Plan p,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h,
      int layout)
      throws Exception {
    if (layout != 1 && layout != 3) {
      for (int i = 0; i < 5; i++) p.push(h, p.row(null, i + 1, time(layout, layout == 4)));
      h.processWatermark(new Watermark(time(layout, layout == 4)));
      p.nativeOutput(h);
    }
    if (layout != 4) {
      p.push(h, p.row(null, 7, time(layout, true)));
      p.push(h, p.nullRow(null, time(layout, true)));
      p.push(h, p.row(2L, 11, time(layout, true)));
    }
  }

  static void suffixStock(
      CanonicalBoundedRangeInteropTest.Plan p,
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h,
      int layout)
      throws Exception {
    p.pushStock(h, p.row(null, 2, time(layout, true)));
    p.pushStock(h, p.row(null, 999, 0));
    p.pushStock(h, p.row(null, 999, -1));
    h.processWatermark(new Watermark(time(layout, true)));
  }

  static void suffixNative(
      CanonicalBoundedRangeInteropTest.Plan p,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h,
      int layout)
      throws Exception {
    p.push(h, p.row(null, 2, time(layout, true)));
    p.push(h, p.row(null, 999, 0));
    p.push(h, p.row(null, 999, -1));
    h.processWatermark(new Watermark(time(layout, true)));
  }

  @SuppressWarnings("unchecked")
  static List<String> digest(CanonicalBoundedRangeInteropTest.Plan p, OperatorSubtaskState state)
      throws Exception {
    var result = new ArrayList<String>();
    try (var view = p.view(false)) {
      view.initializeState(state);
      view.open();
      var backend =
          (CheckpointableKeyedStateBackend<RowData>)
              view.getOperator().<RowData>getKeyedStateBackend();
      var ns = VoidNamespace.INSTANCE;
      var namespaces = VoidNamespaceSerializer.INSTANCE;
      var longs = org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE;
      var inputSerializer = new RowDataSerializer(p.input);
      for (var name : p.contract().stateDescriptors().keySet()) {
        try (var keys = backend.getKeys(name, ns)) {
          for (var key : keys.collect(java.util.stream.Collectors.toList())) {
            backend.setCurrentKey(key);
            String owner = encoded(new RowDataSerializer(p.keyType), key);
            if (name.equals("inputState")) {
              var rows =
                  backend.getPartitionedState(
                      ns,
                      namespaces,
                      new org.apache.flink.api.common.state.MapStateDescriptor<>(
                          name,
                          longs,
                          new org.apache.flink.api.common.typeutils.base.ListSerializer<>(
                              inputSerializer)));
              for (var entry : rows.entries()) {
                int index = 0;
                for (var row : entry.getValue())
                  result.add(
                      owner
                          + ":"
                          + name
                          + ":"
                          + entry.getKey()
                          + ":"
                          + index++
                          + ":"
                          + encoded(inputSerializer, row));
              }
            } else if (name.equals("accState")) {
              var serializer = new RowDataSerializer(p.accumulator);
              var acc =
                  backend.getPartitionedState(
                      ns,
                      namespaces,
                      new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                          name, serializer));
              result.add(owner + ":" + name + ":" + encoded(serializer, acc.value()));
            } else {
              var scalar =
                  backend.getPartitionedState(
                      ns,
                      namespaces,
                      new org.apache.flink.api.common.state.ValueStateDescriptor<>(name, longs));
              result.add(owner + ":" + name + ":" + scalar.value());
            }
          }
        }
      }
      for (String domain : List.of("event", "processing")) {
        var timers =
            backend.<TimerHeapInternalTimer<RowData, VoidNamespace>>create(
                "_timer_state/" + domain + "_user-timers",
                new TimerSerializer<>(p.selector.getProducedType().toSerializer(), namespaces));
        try (var iterator = timers.iterator()) {
          while (iterator.hasNext()) {
            var timer = iterator.next();
            result.add(
                encoded(new RowDataSerializer(p.keyType), timer.getKey())
                    + ":"
                    + domain
                    + ":"
                    + timer.getTimestamp());
          }
        }
      }
    }
    Collections.sort(result);
    return result;
  }

  private static String encoded(RowDataSerializer serializer, RowData row) throws Exception {
    var bytes = new org.apache.flink.core.memory.DataOutputSerializer(128);
    serializer.serialize(row, bytes);
    return Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer());
  }
}
