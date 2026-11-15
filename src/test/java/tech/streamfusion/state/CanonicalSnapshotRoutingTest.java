package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.FlinkStateBackendCompat;
import tech.streamfusion.compat.KeyedBackendContext;

class CanonicalSnapshotRoutingTest {
  private static final RowType KEY = RowType.of(new BigIntType());
  private static final ValueStateDescriptor<Long> VALUE =
      new ValueStateDescriptor<>("stock-value", LongSerializer.INSTANCE);

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void clearingVisitsVariableNamespacesAndLeavesUnownedStateIntact(boolean disk) throws Exception {
    var storage = disk ? new Backend() : new HashMapStateBackend();
    try (var source = harness(storage, new AtomicInteger(), new AtomicInteger())) {
      source.open();
      var raw = source.getOperator().<RowData>getKeyedStateBackend();
      CheckpointableKeyedStateBackend<RowData> backend =
          raw instanceof CanonicalKeyedStateBackend
              ? ((CanonicalKeyedStateBackend<RowData>) raw).projection().backend()
              : (CheckpointableKeyedStateBackend<RowData>) raw;
      var namespace = org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE;
      var value = new ValueStateDescriptor<>("windows", LongSerializer.INSTANCE);
      var lists =
          new org.apache.flink.api.common.state.ListStateDescriptor<>(
              "lists", LongSerializer.INSTANCE);
      var maps =
          new org.apache.flink.api.common.state.MapStateDescriptor<>(
              "maps", LongSerializer.INSTANCE, LongSerializer.INSTANCE);
      for (long i = 0; i < 1031; i++) {
        backend.setCurrentKey(new RowDataSerializer(KEY).toBinaryRow(GenericRowData.of(i)).copy());
        for (String name : List.of("short", "a longer variable namespace")) {
          backend.getPartitionedState(name, namespace, value).update(i);
          backend.getPartitionedState(name, namespace, lists).addAll(List.of(i, i + 1));
          backend.getPartitionedState(name, namespace, maps).put(i, i + 1);
        }
      }
      backend.setCurrentKey(key());
      backend
          .getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, VALUE)
          .update(71L);
      StockStateCleanup.clear(
          backend,
          java.util.Map.of(
              "windows", org.apache.flink.api.common.state.StateDescriptor.Type.VALUE,
              "lists", org.apache.flink.api.common.state.StateDescriptor.Type.LIST,
              "maps", org.apache.flink.api.common.state.StateDescriptor.Type.MAP));
      assertEquals(key(), backend.getCurrentKey());
      for (String name : List.of("windows", "lists", "maps")) {
        try (var entries = backend.getKeysAndNamespaces(name)) {
          assertEquals(0, entries.count(), name);
        }
      }
      assertEquals(
          71L,
          backend
              .getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, VALUE)
              .value());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void immutableTimerInspectionSeesRemovalReinsertionAndNewNamespaces(boolean disk)
      throws Exception {
    var storage = disk ? new Backend() : new HashMapStateBackend();
    try (var source = harness(storage, new AtomicInteger(), new AtomicInteger())) {
      source.open();
      var raw = source.getOperator().<RowData>getKeyedStateBackend();
      CheckpointableKeyedStateBackend<RowData> backend =
          raw instanceof CanonicalKeyedStateBackend
              ? ((CanonicalKeyedStateBackend<RowData>) raw).projection().backend()
              : (CheckpointableKeyedStateBackend<RowData>) raw;
      String name = "_timer_state/event_inspection";
      org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue<
              org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<RowData, Long>>
          timers =
              backend.create(
                  name,
                  new org.apache.flink.streaming.api.operators.TimerSerializer<>(
                      new RowDataSerializer(KEY), LongSerializer.INSTANCE));
      var first =
          new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<RowData, Long>(
              100, key(), 7L);
      var removed =
          new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<RowData, Long>(
              200, key(), 8L);
      timers.add(first);
      timers.add(removed);
      backend.setCurrentKey(key());
      assertEquals(List.of("100:7", "200:8"), inspectTimers(backend, name));
      assertEquals(first, timers.poll());
      timers.add(first);
      timers.remove(removed);
      timers.add(
          new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(300, key(), 9L));
      assertEquals(List.of("100:7", "300:9"), inspectTimers(backend, name));
      assertEquals(key(), backend.getCurrentKey());
    }
  }

  private static List<String> inspectTimers(
      CheckpointableKeyedStateBackend<RowData> backend, String name) throws Exception {
    var values = new java.util.ArrayList<String>();
    try (var entries = StockTimerSnapshot.<RowData, Long>iterator(backend, name)) {
      while (entries.hasNext()) {
        var timer = entries.next();
        assertEquals(key(), timer.getKey());
        values.add(timer.getTimestamp() + ":" + timer.getNamespace());
      }
    }
    java.util.Collections.sort(values);
    return values;
  }

  @Test
  void canonicalExportIsExplicitAndPendingCapturesStayImmutable() throws Exception {
    var captures = new AtomicInteger();
    var nativeCaptures = new AtomicInteger();
    var stateBackend = new Backend();
    try (var source = harness(stateBackend, captures, nativeCaptures)) {
      source.open();
      source.processElement(new StreamRecord<>(GenericRowData.of(11L)));
      assertEquals(0, captures.get());
      var checkpoint = capture(source, CheckpointType.CHECKPOINT, 1);
      assertEquals(0, captures.get());
      assertEquals(1, nativeCaptures.get());
      checkpoint.cancel();

      var first = capture(source, SavepointType.savepoint(SavepointFormatType.CANONICAL), 2);
      source.processElement(new StreamRecord<>(GenericRowData.of(29L)));
      var second = capture(source, SavepointType.savepoint(SavepointFormatType.CANONICAL), 3);
      source.processElement(new StreamRecord<>(GenericRowData.of(41L)));
      assertEquals(2, captures.get());
      assertEquals(1, nativeCaptures.get());
      assertTrue(stateBackend.stock.getClass().getName().contains("RocksDB"));
      assertStockValue(finish(second), 29L);
      assertStockValue(finish(first), 11L);
    }
  }

  @Test
  void cancelledCanonicalCaptureDoesNotPreventTheNextOne() throws Exception {
    var captures = new AtomicInteger();
    try (var source = harness(new Backend(), captures, new AtomicInteger())) {
      source.open();
      source.processElement(new StreamRecord<>(GenericRowData.of(5L)));
      var cancelled = capture(source, SavepointType.savepoint(SavepointFormatType.CANONICAL), 1);
      cancelled.cancel();
      source.processElement(new StreamRecord<>(GenericRowData.of(9L)));
      assertStockValue(
          finish(capture(source, SavepointType.savepoint(SavepointFormatType.CANONICAL), 2)), 9L);
      assertEquals(2, captures.get());
    }
  }

  private static OperatorSnapshotFutures capture(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> source,
      org.apache.flink.runtime.checkpoint.SnapshotType type,
      long id)
      throws Exception {
    return source
        .getOperator()
        .snapshotState(
            id,
            0,
            new CheckpointOptions(type, CheckpointStorageLocationReference.getDefault()),
            new MemCheckpointStreamFactory(1 << 20));
  }

  static OperatorSubtaskState finish(OperatorSnapshotFutures futures) throws Exception {
    futures.getKeyedStateManagedFuture().run();
    futures.getOperatorStateManagedFuture().run();
    var keyed = futures.getKeyedStateManagedFuture().get().getJobManagerOwnedSnapshot();
    var operator = futures.getOperatorStateManagedFuture().get().getJobManagerOwnedSnapshot();
    var result = OperatorSubtaskState.builder();
    if (keyed != null) result.setManagedKeyedState(new StateObjectCollection<>(List.of(keyed)));
    if (operator != null)
      result.setManagedOperatorState(new StateObjectCollection<>(List.of(operator)));
    return result.build();
  }

  static void assertStockValue(OperatorSubtaskState saved, long expected) throws Exception {
    try (var restored =
        harness(new HashMapStateBackend(), new AtomicInteger(), new AtomicInteger())) {
      restored.initializeState(saved);
      restored.open();
      var backend = restored.getOperator().<RowData>getKeyedStateBackend();
      backend.setCurrentKey(key());
      assertEquals(
          expected,
          backend
              .getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, VALUE)
              .value());
    }
  }

  static RowData key() {
    return new RowDataSerializer(KEY).toBinaryRow(GenericRowData.of(1L)).copy();
  }

  static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      org.apache.flink.runtime.state.StateBackend backend,
      AtomicInteger canonicalCaptures,
      AtomicInteger nativeCaptures)
      throws Exception {
    return harness(backend, canonicalCaptures, nativeCaptures, 128);
  }

  static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      org.apache.flink.runtime.state.StateBackend backend,
      AtomicInteger canonicalCaptures,
      AtomicInteger nativeCaptures,
      int maximum)
      throws Exception {
    var selector =
        KeySelectorUtil.getRowDataSelector(
            CanonicalSnapshotRoutingTest.class.getClassLoader(),
            new int[] {0},
            InternalTypeInfo.of(KEY));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            new Fixture(canonicalCaptures, nativeCaptures),
            selector,
            selector.getProducedType(),
            maximum,
            1,
            0);
    harness.setStateBackend(backend);
    harness.setup(new RowDataSerializer(KEY));
    return harness;
  }

  private static final class Backend extends FlinkStateBackendCompat {
    transient CheckpointableKeyedStateBackend<RowData> stock;

    Backend() {
      super(new HashMapStateBackend());
    }

    @Override
    protected <K> CheckpointableKeyedStateBackend<K> createNativeKeyedBackend(
        KeyedBackendContext<K> context) throws Exception {
      var projection =
          new CanonicalProjection(
              () ->
                  stock =
                      context.stockBackendFactory().create(new RowDataSerializer(KEY), List.of()),
              false);
      return new CanonicalKeyedStateBackend<>(
          context.delegateFactory().apply(context.stateHandles()), projection);
    }
  }

  private static final class Fixture extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    private final AtomicInteger canonicalCaptures;
    private final AtomicInteger nativeCaptures;
    private long value;

    Fixture(AtomicInteger canonicalCaptures, AtomicInteger nativeCaptures) {
      this.canonicalCaptures = canonicalCaptures;
      this.nativeCaptures = nativeCaptures;
    }

    @Override
    public void open() throws Exception {
      super.open();
      var backend = getKeyedStateBackend();
      if (backend instanceof CanonicalKeyedStateBackend) {
        var canonical = (CanonicalKeyedStateBackend<?>) backend;
        canonical.configureSnapshots(
            () -> {
              canonicalCaptures.incrementAndGet();
              var stock = canonical.projection().backend();
              stock.setCurrentKey(key());
              stock
                  .getPartitionedState(
                      VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, VALUE)
                  .update(value);
              return stock.savepoint();
            },
            nativeCaptures::incrementAndGet);
      }
    }

    @Override
    public void processElement(StreamRecord<RowData> row) {
      value = row.getValue().getLong(0);
    }
  }
}
