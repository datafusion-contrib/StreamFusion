package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StockStateInventoryTest {
  private static final RowType KEY = RowType.of(new BigIntType());
  private static final RowType BUFFERED = RowType.of(new BigIntType(), new TimestampType(3));
  private static final String EVENTS = "_timer_state/event_user-timers";
  private static final String PROCESSING = "_timer_state/processing_user-timers";

  @Test
  void emptyAndExpectedRestoredStateRetainTheirValuesAfterValidation() throws Exception {
    try (var source = harness()) {
      source.open();
      var backend = backend(source);
      validate(backend, false);
      backend.setCurrentKey(key());
      backend
          .getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("deduplicate-state", new RowDataSerializer(KEY)))
          .update(GenericRowData.of(7L));
      var snapshot =
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = harness()) {
        restored.initializeState(snapshot);
        restored.open();
        var saved = backend(restored);
        validate(saved, false);
        saved.setCurrentKey(key());
        assertEquals(
            7L,
            saved
                .getPartitionedState(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    new ValueStateDescriptor<>("deduplicate-state", new RowDataSerializer(KEY)))
                .value()
                .getLong(0));
        restored.snapshotWithLocalState(
            2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void unknownDescriptorsAreRejectedEvenWhenEmpty(boolean populated) throws Exception {
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      backend.setCurrentKey(key());
      var state =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("unexpected-state", LongSerializer.INSTANCE));
      if (populated) state.update(23L);
      var error = assertThrows(IllegalStateException.class, () -> validate(backend, false));
      assertTrue(error.getMessage().contains("unexpected-state"));
      if (populated) assertEquals(23L, state.value());
    }
  }

  @Test
  void knownNameWithTheWrongStateKindIsRejected() throws Exception {
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      backend.setCurrentKey(key());
      backend
          .getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new MapStateDescriptor<>(
                  "deduplicate-state", IntSerializer.INSTANCE, LongSerializer.INSTANCE))
          .put(1, 23L);
      var error = assertThrows(IllegalStateException.class, () -> validate(backend, false));
      assertTrue(error.getMessage().contains("expected VALUE"), error.getMessage());
    }
  }

  @Test
  void knownStateWithAnUnconsumedNamespaceIsRejected() throws Exception {
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      backend.setCurrentKey(key());
      backend
          .getPartitionedState(
              3,
              IntSerializer.INSTANCE,
              new ValueStateDescriptor<>("deduplicate-state", new RowDataSerializer(KEY)))
          .update(GenericRowData.of(7L));
      var error = assertThrows(IllegalStateException.class, () -> validate(backend, false));
      assertTrue(error.getMessage().contains("void namespace"), error.getMessage());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {EVENTS, PROCESSING})
  void knownUnusedUserTimersAreAllowedOnlyWhenEmpty(String name) throws Exception {
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      var queue =
          backend.create(
              name,
              new TimerSerializer<>(new RowDataSerializer(KEY), VoidNamespaceSerializer.INSTANCE));
      validate(backend, false);
      queue.add(new TimerHeapInternalTimer<>(1234, key(), VoidNamespace.INSTANCE));
      var error = assertThrows(IllegalStateException.class, () -> validate(backend, false));
      assertTrue(error.getMessage().contains("pending timers"), error.getMessage());
      assertEquals(1, queue.size());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void anUnrecognizedTimerServiceIsRejectedEvenWhenEmpty(boolean populated) throws Exception {
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      var queue =
          backend.create(
              "_timer_state/event_other-service",
              new TimerSerializer<>(new RowDataSerializer(KEY), VoidNamespaceSerializer.INSTANCE));
      if (populated) queue.add(new TimerHeapInternalTimer<>(1234, key(), VoidNamespace.INSTANCE));
      var error = assertThrows(IllegalStateException.class, () -> validate(backend, false));
      assertTrue(error.getMessage().contains("other-service"), error.getMessage());
      assertEquals(populated ? 1 : 0, queue.size());
    }
  }

  @Test
  void bufferedDedupOwnsPendingEventsButDoesNotConsumeProcessingTimers() throws Exception {
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      backend.setCurrentKey(key());
      backend
          .getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>(
                  "waiting-to-emit-on-timer", new RowDataSerializer(BUFFERED)))
          .update(GenericRowData.of(1L, TimestampData.fromEpochMillis(1234)));
      var events =
          backend.create(
              EVENTS,
              new TimerSerializer<>(new RowDataSerializer(KEY), VoidNamespaceSerializer.INSTANCE));
      events.add(new TimerHeapInternalTimer<>(1234, key(), VoidNamespace.INSTANCE));
      validate(backend, true);
      assertEquals(1, events.size());
      var processing =
          backend.create(
              PROCESSING,
              new TimerSerializer<>(new RowDataSerializer(KEY), VoidNamespaceSerializer.INSTANCE));
      processing.add(new TimerHeapInternalTimer<>(2345, key(), VoidNamespace.INSTANCE));
      var error = assertThrows(IllegalStateException.class, () -> validate(backend, true));
      assertTrue(error.getMessage().contains("pending timers"), error.getMessage());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void evenKnownTimerQueuesMustHaveTheReleasedNamespace(boolean owned) throws Exception {
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      var queue =
          backend.create(
              EVENTS, new TimerSerializer<>(new RowDataSerializer(KEY), IntSerializer.INSTANCE));
      queue.add(new TimerHeapInternalTimer<>(1234, key(), 3));
      var error = assertThrows(IllegalStateException.class, () -> validate(backend, owned));
      assertTrue(error.getMessage().contains("void namespace"), error.getMessage());
    }
  }

  @Test
  void inspectionDoesNotExpireOrRefreshRawTtlValuesWhileCheckingEmptyTimers() throws Exception {
    var serializer =
        new org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer<>(
            LongSerializer.INSTANCE, new RowDataSerializer(KEY));
    var descriptor = new ValueStateDescriptor<>("deduplicate-state", serializer);
    try (var source = harness()) {
      source.open();
      var backend = backend(source);
      backend.setCurrentKey(key());
      backend
          .getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor)
          .update(
              new org.apache.flink.runtime.state.ttl.TtlValue<RowData>(
                  GenericRowData.of(7L), 1500));
      backend.create(
          EVENTS,
          new TimerSerializer<>(new RowDataSerializer(KEY), VoidNamespaceSerializer.INSTANCE));
      var snapshot =
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = harness()) {
        restored.initializeState(snapshot);
        restored.open();
        restored.setStateTtlProcessingTime(10000);
        var saved = backend(restored);
        validate(saved, false);
        saved.setCurrentKey(key());
        var value =
            saved
                .getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor)
                .value();
        assertNotNull(value);
        assertEquals(1500, value.getLastAccessTimestamp());
        assertEquals(7, value.getUserValue().getLong(0));
      }
    }
  }

  @Test
  void timerKeysMustUseTheSameSerializerAsTheOperatorKeys() throws Exception {
    try (var view = harness()) {
      view.open();
      var backend = backend(view);
      backend.create(
          EVENTS,
          new TimerSerializer<>(
              new RowDataSerializer(RowType.of(new org.apache.flink.table.types.logical.IntType())),
              VoidNamespaceSerializer.INSTANCE));
      var error = assertThrows(IllegalStateException.class, () -> validate(backend, false));
      assertTrue(error.getMessage().contains("timer key serializer"), error.getMessage());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void validationAlwaysReleasesCapturedStockResources(boolean rejected) throws Exception {
    var probe =
        new StockSnapshotProbe(
            rejected
                ? List.of(
                    new StateMetaInfoSnapshot(
                        "unknown",
                        StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                        Map.of(),
                        Map.of()))
                : List.of());
    if (rejected) assertThrows(IllegalStateException.class, () -> validate(probe.backend, false));
    else validate(probe.backend, false);
    assertEquals(1, probe.releases);
  }

  private static void validate(CheckpointableKeyedStateBackend<RowData> backend, boolean buffered)
      throws Exception {
    var expected =
        buffered
            ? Map.of(
                "waiting-to-emit-on-timer",
                org.apache.flink.api.common.state.StateDescriptor.Type.VALUE,
                "already-emitted-state-boolean",
                org.apache.flink.api.common.state.StateDescriptor.Type.VALUE)
            : Map.of(
                "deduplicate-state", org.apache.flink.api.common.state.StateDescriptor.Type.VALUE);
    StockStateInventory.validate(
        backend,
        expected,
        buffered ? java.util.Set.of(EVENTS) : java.util.Set.of(),
        new RowDataSerializer(KEY));
  }

  private static RowData key() {
    return new RowDataSerializer(KEY).toBinaryRow(GenericRowData.of(1L)).copy();
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness()
      throws Exception {
    var selector =
        KeySelectorUtil.getRowDataSelector(
            StockStateInventoryTest.class.getClassLoader(),
            new int[] {0},
            InternalTypeInfo.of(KEY));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            new Projection(), selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(new HashMapStateBackend());
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
