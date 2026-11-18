package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class StockNamespacedStateInventoryTest {
  private static final RowType KEY = RowType.of(new BigIntType());
  private static final String EVENTS = "_timer_state/event_window-timers";
  private static final String PROCESSING = "_timer_state/processing_window-timers";

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void mixedStateAndTimersRetainTheirNamespacesAcrossStockBackendSwitches(
      boolean rocks, boolean session) throws Exception {
    if (session)
      roundTrip(
          rocks, new TimeWindow.Serializer(), new TimeWindow(0, 1000), new TimeWindow(2000, 3000));
    else roundTrip(rocks, LongSerializer.INSTANCE, 1000L, 3000L);
  }

  private static <N> void roundTrip(boolean rocks, TypeSerializer<N> namespace, N first, N second)
      throws Exception {
    var descriptor = new ValueStateDescriptor<>("window-aggs", new RowDataSerializer(KEY));
    try (var source = harness(rocks)) {
      source.open();
      var backend = backend(source);
      backend.setCurrentKey(key());
      backend.getPartitionedState(first, namespace, descriptor).update(GenericRowData.of(7L));
      backend.getPartitionedState(second, namespace, descriptor).update(GenericRowData.of(11L));
      var merge =
          new MapStateDescriptor<>(
              "merging-windows", IntSerializer.INSTANCE, LongSerializer.INSTANCE);
      backend
          .getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, merge)
          .put(1, 23L);
      var events =
          backend.create(EVENTS, new TimerSerializer<>(new RowDataSerializer(KEY), namespace));
      events.add(new TimerHeapInternalTimer<>(999, key(), first));
      events.add(new TimerHeapInternalTimer<>(2999, key(), second));
      backend.create(PROCESSING, new TimerSerializer<>(new RowDataSerializer(KEY), namespace));
      var snapshot =
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = harness(!rocks)) {
        restored.initializeState(snapshot);
        restored.open();
        var saved = backend(restored);
        var states =
            Map.of(
                "window-aggs",
                state(namespace),
                "merging-windows",
                new StockStateInventory.State(
                    StateDescriptor.Type.MAP, VoidNamespaceSerializer.INSTANCE));
        var timers =
            Map.of(
                EVENTS,
                StockStateInventory.Timer.consumed(namespace),
                PROCESSING,
                StockStateInventory.Timer.empty(namespace));
        for (int attempt = 0; attempt < 2; attempt++)
          StockStateInventory.validate(saved, states, timers, new RowDataSerializer(KEY));
        saved.setCurrentKey(key());
        assertEquals(7, saved.getPartitionedState(first, namespace, descriptor).value().getLong(0));
        assertEquals(
            11, saved.getPartitionedState(second, namespace, descriptor).value().getLong(0));
        assertEquals(
            23L,
            saved
                .getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, merge)
                .get(1));
        try (var entries = saved.<N>getKeysAndNamespaces("window-aggs")) {
          var namespaces =
              entries.map(entry -> entry.f1).collect(java.util.stream.Collectors.toSet());
          assertEquals(java.util.Set.of(first, second), namespaces);
        }
        var pending =
            saved.create(EVENTS, new TimerSerializer<>(new RowDataSerializer(KEY), namespace));
        assertEquals(2, pending.size());
        var earliest = pending.poll();
        assertEquals(999, earliest.getTimestamp());
        assertEquals(first, earliest.getNamespace());
        assertEquals(second, pending.poll().getNamespace());
        restored.snapshotWithLocalState(
            2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aKnownStateCannotUseAnUndeclaredNamespaceEvenWhenEmpty(boolean rocks) throws Exception {
    for (boolean populated : new boolean[] {false, true}) {
      try (var view = harness(rocks)) {
        view.open();
        var backend = backend(view);
        backend.setCurrentKey(key());
        var state =
            backend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>("window-aggs", LongSerializer.INSTANCE));
        if (populated) state.update(23L);
        var error =
            assertThrows(
                IllegalStateException.class,
                () ->
                    validate(
                        backend, Map.of("window-aggs", state(LongSerializer.INSTANCE)), Map.of()));
        assertTrue(error.getMessage().contains("LongSerializer"), error.getMessage());
        if (populated) assertEquals(23L, state.value());
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void consumedAndEmptyOnlyQueuesBothRequireTheDeclaredNamespace(boolean rocks, boolean emptyOnly)
      throws Exception {
    try (var view = harness(rocks)) {
      view.open();
      var backend = backend(view);
      backend.create(
          EVENTS,
          new TimerSerializer<>(new RowDataSerializer(KEY), VoidNamespaceSerializer.INSTANCE));
      var error =
          assertThrows(
              IllegalStateException.class,
              () ->
                  validate(
                      backend,
                      Map.of(),
                      Map.of(
                          EVENTS,
                          emptyOnly
                              ? StockStateInventory.Timer.empty(LongSerializer.INSTANCE)
                              : StockStateInventory.Timer.consumed(LongSerializer.INSTANCE))));
      assertTrue(error.getMessage().contains("LongSerializer"), error.getMessage());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void unusedWindowTimerServiceMustBeEmptyWithoutOpeningOrConsumingItsQueue(boolean rocks)
      throws Exception {
    try (var view = harness(rocks)) {
      view.open();
      var backend = backend(view);
      var queue =
          backend.create(
              PROCESSING,
              new TimerSerializer<>(new RowDataSerializer(KEY), LongSerializer.INSTANCE));
      var timers = Map.of(PROCESSING, StockStateInventory.Timer.empty(LongSerializer.INSTANCE));
      validate(backend, Map.of(), timers);
      var timer = new TimerHeapInternalTimer<>(999, key(), 1000L);
      queue.add(timer);
      var error =
          assertThrows(IllegalStateException.class, () -> validate(backend, Map.of(), timers));
      assertTrue(error.getMessage().contains("pending timers"), error.getMessage());
      assertEquals(1, queue.size());
      assertEquals(timer, queue.poll());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void exactTimerKeySerializerIsRequiredAlongsideTheWindowNamespace(boolean rocks)
      throws Exception {
    try (var view = harness(rocks)) {
      view.open();
      var backend = backend(view);
      backend.create(
          EVENTS,
          new TimerSerializer<>(
              new RowDataSerializer(RowType.of(new IntType())), LongSerializer.INSTANCE));
      var error =
          assertThrows(
              IllegalStateException.class,
              () ->
                  validate(
                      backend,
                      Map.of(),
                      Map.of(EVENTS, StockStateInventory.Timer.consumed(LongSerializer.INSTANCE))));
      assertTrue(error.getMessage().contains("timer key serializer"), error.getMessage());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void unknownWindowTimerNamesAreRejectedInsteadOfMatchingAServicePrefix(boolean rocks)
      throws Exception {
    try (var view = harness(rocks)) {
      view.open();
      var backend = backend(view);
      backend.create(
          EVENTS + "-other",
          new TimerSerializer<>(new RowDataSerializer(KEY), LongSerializer.INSTANCE));
      var error =
          assertThrows(
              IllegalStateException.class,
              () ->
                  validate(
                      backend,
                      Map.of(),
                      Map.of(EVENTS, StockStateInventory.Timer.consumed(LongSerializer.INSTANCE))));
      assertTrue(error.getMessage().contains("not owned"), error.getMessage());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void namespaceSerializerConfigurationMustMatchEvenWhenItsClassMatches(boolean rocks)
      throws Exception {
    try (var view = harness(rocks)) {
      view.open();
      var backend = backend(view);
      backend.setCurrentKey(key());
      backend.getPartitionedState(
          GenericRowData.of(1),
          new RowDataSerializer(RowType.of(new IntType())),
          new ValueStateDescriptor<>("window-aggs", LongSerializer.INSTANCE));
      var error =
          assertThrows(
              IllegalStateException.class,
              () ->
                  validate(
                      backend, Map.of("window-aggs", state(new RowDataSerializer(KEY))), Map.of()));
      assertTrue(error.getMessage().contains("RowDataSerializer"), error.getMessage());
    }
  }

  @Test
  void missingNamespaceMetadataIsRejectedAndTheCaptureIsReleased() throws Exception {
    var probe =
        new StockSnapshotProbe(
            List.of(
                new StateMetaInfoSnapshot(
                    "window-aggs",
                    StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                    Map.of(
                        StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString(),
                        "VALUE"),
                    Map.of())));
    assertThrows(
        IllegalStateException.class,
        () ->
            validate(
                probe.backend, Map.of("window-aggs", state(LongSerializer.INSTANCE)), Map.of()));
    assertEquals(1, probe.releases);
  }

  private static StockStateInventory.State state(TypeSerializer<?> namespace) {
    return new StockStateInventory.State(StateDescriptor.Type.VALUE, namespace);
  }

  private static void validate(
      CheckpointableKeyedStateBackend<RowData> backend,
      Map<String, StockStateInventory.State> states,
      Map<String, StockStateInventory.Timer> timers)
      throws Exception {
    StockStateInventory.validate(backend, states, timers, new RowDataSerializer(KEY));
  }

  private static RowData key() {
    return new RowDataSerializer(KEY).toBinaryRow(GenericRowData.of(1L)).copy();
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      boolean rocks) throws Exception {
    var selector =
        KeySelectorUtil.getRowDataSelector(
            StockNamespacedStateInventoryTest.class.getClassLoader(),
            new int[] {0},
            InternalTypeInfo.of(KEY));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            new Projection(), selector, selector.getProducedType(), 128, 1, 0);
    var config = new Configuration();
    config.set(StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
    harness.setStateBackend(
        StateBackendLoader.loadStateBackendFromConfig(
            config, StockNamespacedStateInventoryTest.class.getClassLoader(), null));
    harness.setup(new RowDataSerializer(KEY));
    return harness;
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> backend(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
    return (CheckpointableKeyedStateBackend<RowData>)
        harness.getOperator().<RowData>getKeyedStateBackend();
  }

  private static final class Projection extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> record) {}
  }
}
