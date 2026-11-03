package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.deduplicate.RowTimeDeduplicateKeepFirstRowFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.FlinkRocksDBOptions;
import tech.streamfusion.state.StockBufferedDedupStateContract;

class CanonicalBufferedDedupInteropTest {
  private static final RowType ROW =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
  private static final RowType KEY = RowType.of(new BigIntType());
  private static final int[] DESCRIPTORS = {Native.STOCK_KEY_PROJECTION, -1};
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void emittedMarkersDoNotRequireNonKeyPayloadValues(boolean persistent) throws Exception {
    RowType nonNull =
        RowType.of(new BigIntType(false), new BigIntType(false), new BigIntType(false));
    long handle = create(persistent, nonNull);
    try (var view = harness(false)) {
      view.open();
      var backend = backend(view);
      var markers =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>(
                  "already-emitted-state-boolean",
                  new TtlSerializer<>(LongSerializer.INSTANCE, BooleanSerializer.INSTANCE)));
      backend.setCurrentKey(selector().getKey(row(1, 0, 0)));
      markers.update(new TtlValue<>(true, 1000));
      var contract =
          new StockBufferedDedupStateContract(
              new RowDataSerializer(KEY), selector(), KEY, nonNull, new int[] {0}, 2, 1000);
      contract.importState(backend, handle, persistent);
      contract.exportState(backend, handle, persistent);
      backend.setCurrentKey(selector().getKey(row(1, 0, 0)));
      assertEquals(Boolean.TRUE, markers.value().getUserValue());
      assertEquals(1000, markers.value().getLastAccessTimestamp());
      push(handle, persistent, 1500, row(1, 11, 400));
      assertEquals(List.of(), flush(handle, persistent, 400, 1500));
      push(handle, persistent, 2200, row(1, 12, 500));
      assertEquals(List.of("1:12:500"), flush(handle, persistent, 500, 2200));
    } finally {
      if (persistent) Native.closeRocksDBKeepFirstDeduplicator(handle);
      else Native.closeKeepFirstDeduplicator(handle);
    }
  }

  @Test
  void orphanedEventTimersAreRejectedInsteadOfDropped() throws Exception {
    OperatorSubtaskState corrupt;
    try (var view = harness(false)) {
      view.open();
      var backend = backend(view);
      var queue =
          backend.create(
              "_timer_state/event_user-timers",
              new org.apache.flink.streaming.api.operators.TimerSerializer<>(
                  new RowDataSerializer(KEY), VoidNamespaceSerializer.INSTANCE));
      queue.add(
          new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
              300,
              new RowDataSerializer(KEY).copy(selector().getKey(row(1, 0, 0))),
              VoidNamespace.INSTANCE));
      corrupt = canonical(view, 10);
    }
    long handle = create(false);
    try (var view = harness(false)) {
      view.initializeState(corrupt);
      view.open();
      var backend = backend(view);
      var contract =
          new StockBufferedDedupStateContract(
              new RowDataSerializer(KEY), selector(), KEY, ROW, new int[] {0}, 2, 1000);
      var failure =
          assertThrows(
              IllegalStateException.class, () -> contract.importState(backend, handle, false));
      assertTrue(failure.getMessage().contains("timer has no pending row"), failure.getMessage());
      var failedSnapshot = canonical(view, 11);
      try (var retry = harness(false)) {
        retry.initializeState(failedSnapshot);
        retry.open();
        var retryContract =
            new StockBufferedDedupStateContract(
                new RowDataSerializer(KEY), selector(), KEY, ROW, new int[] {0}, 2, 1000);
        var repeated =
            assertThrows(
                IllegalStateException.class,
                () -> retryContract.importState(backend(retry), handle, false));
        assertTrue(
            repeated.getMessage().contains("timer has no pending row"), repeated.getMessage());
      }
    } finally {
      Native.closeKeepFirstDeduplicator(handle);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void pendingTimersAndOriginalMarkerAgesRoundTripThroughNativeState(boolean persistent)
      throws Exception {
    OperatorSubtaskState original;
    try (var stock = harness(true)) {
      stock.open();
      time(stock, 1000);
      stock.processElement(new StreamRecord<>(row(1, 10, 100)));
      stock.processElement(new StreamRecord<>(row(2, 20, 200)));
      stock.processWatermark(new Watermark(150));
      time(stock, 1500);
      stock.processElement(new StreamRecord<>(row(3, 30, 300)));
      stock.processWatermark(new Watermark(250));
      assertEquals(List.of("1:10:100", "2:20:200"), values(stock.extractOutputValues()));
      original = canonical(stock, 1);
    }
    long handle = create(persistent);
    try {
      OperatorSubtaskState exported;
      try (var view = harness(false)) {
        view.initializeState(original);
        view.open();
        time(view, 10_000);
        var backend = backend(view);
        var contract =
            new StockBufferedDedupStateContract(
                new RowDataSerializer(KEY), selector(), KEY, ROW, new int[] {0}, 2, 1000);
        contract.importState(backend, handle, persistent);
        contract.exportState(backend, handle, persistent);
        var markers =
            backend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>(
                    "already-emitted-state-boolean",
                    new TtlSerializer<>(LongSerializer.INSTANCE, BooleanSerializer.INSTANCE)));
        backend.setCurrentKey(selector().getKey(row(1, 0, 0)));
        assertEquals(1000, markers.value().getLastAccessTimestamp());
        backend.setCurrentKey(selector().getKey(row(2, 0, 0)));
        assertEquals(1500, markers.value().getLastAccessTimestamp());
        exported = canonical(view, 2);
      }
      try (var restored = harness(true)) {
        restored.initializeState(exported);
        restored.open();
        time(restored, 2200);
        RowData[] suffix = {row(1, 11, 400), row(2, 21, 400), row(3, 35, 240)};
        for (RowData row : suffix) restored.processElement(new StreamRecord<>(row));
        restored.processWatermark(new Watermark(400));
        push(handle, persistent, 2200, suffix);
        assertEquals(values(restored.extractOutputValues()), flush(handle, persistent, 400, 2200));
        assertEquals(List.of("1:11:400", "3:35:240"), values(restored.extractOutputValues()));
        time(restored, 2500);
        restored.getOutput().clear();
        restored.processElement(new StreamRecord<>(row(2, 22, 500)));
        restored.processWatermark(new Watermark(500));
        push(handle, persistent, 2500, row(2, 22, 500));
        assertEquals(List.of("2:22:500"), values(restored.extractOutputValues()));
        assertEquals(values(restored.extractOutputValues()), flush(handle, persistent, 500, 2500));
      }
    } finally {
      if (persistent) Native.closeRocksDBKeepFirstDeduplicator(handle);
      else Native.closeKeepFirstDeduplicator(handle);
    }
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> malformedViews() {
    return java.util.stream.Stream.of(
            "orphan",
            "timer-time",
            "missing-timer",
            "multiple-timers",
            "overlap",
            "false-marker",
            "payload-key",
            "row-kind",
            "processing-timer",
            "marker-key-kind")
        .flatMap(
            mode ->
                java.util.stream.Stream.of(false, true)
                    .map(
                        persistent ->
                            org.junit.jupiter.params.provider.Arguments.of(mode, persistent)));
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("malformedViews")
  void aMalformedCompleteViewCannotPartiallyImportOrClearStockState(String mode, boolean persistent)
      throws Exception {
    long handle = create(persistent);
    try (var view = harness(false);
        var observed = harness(false)) {
      view.open();
      observed.open();
      var backend = backend(view);
      var waiting =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("waiting-to-emit-on-timer", new RowDataSerializer(ROW)));
      var emitted =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>(
                  "already-emitted-state-boolean",
                  new TtlSerializer<>(LongSerializer.INSTANCE, BooleanSerializer.INSTANCE)));
      var timers =
          backend.create(
              "_timer_state/event_user-timers",
              new org.apache.flink.streaming.api.operators.TimerSerializer<>(
                  new RowDataSerializer(KEY), VoidNamespaceSerializer.INSTANCE));
      var keyRows = new RowDataSerializer(KEY);
      var payloadRows = new RowDataSerializer(ROW);
      for (int index = 0; index < 1026; index++) {
        var key = keyRows.copy(selector().getKey(row(index, 10, 3000 + index)));
        backend.setCurrentKey(key);
        RowData value =
            payloadRows.copy(
                row(index == 1025 && mode.equals("payload-key") ? -1 : index, 10, 3000 + index));
        if (index == 1025 && mode.equals("row-kind"))
          value.setRowKind(org.apache.flink.types.RowKind.DELETE);
        waiting.update(value);
        if (!(index == 1025 && mode.equals("missing-timer"))) {
          timers.add(
              new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                  3000L + index + (index == 1025 && mode.equals("timer-time") ? 1 : 0),
                  key,
                  VoidNamespace.INSTANCE));
        }
      }
      var last = keyRows.copy(selector().getKey(row(1025, 0, 0)));
      if (mode.equals("multiple-timers"))
        timers.add(
            new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                9999, last, VoidNamespace.INSTANCE));
      if (mode.equals("orphan"))
        timers.add(
            new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                9999, keyRows.copy(selector().getKey(row(5000, 0, 0))), VoidNamespace.INSTANCE));
      if (java.util.Set.of("overlap", "false-marker", "marker-key-kind").contains(mode)) {
        var markerKey =
            mode.equals("overlap") ? last : keyRows.copy(selector().getKey(row(2000, 0, 0)));
        if (mode.equals("marker-key-kind"))
          markerKey.setRowKind(org.apache.flink.types.RowKind.DELETE);
        backend.setCurrentKey(markerKey);
        emitted.update(new TtlValue<>(!mode.equals("false-marker"), 1500));
      }
      if (mode.equals("processing-timer")) {
        var processing =
            backend.create(
                "_timer_state/processing_user-timers",
                new org.apache.flink.streaming.api.operators.TimerSerializer<>(
                    new RowDataSerializer(KEY), VoidNamespaceSerializer.INSTANCE));
        processing.add(
            new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                5000, last, VoidNamespace.INSTANCE));
      }
      int timerCount = timers.size();
      var saved = canonical(view, 10);
      try (var restoredView = harness(false)) {
        restoredView.initializeState(saved);
        restoredView.open();
        var restoredBackend = backend(restoredView);
        var contract =
            new StockBufferedDedupStateContract(
                new RowDataSerializer(KEY), selector(), KEY, ROW, new int[] {0}, 2, 1000);
        assertThrows(
            IllegalStateException.class,
            () -> contract.importState(ordered(restoredBackend), handle, persistent));
        try (var keys =
            restoredBackend.getKeys("waiting-to-emit-on-timer", VoidNamespace.INSTANCE)) {
          assertEquals(1026, keys.count(), "A rejected import must retain the original stock rows");
        }
        assertEquals(
            timerCount,
            savedEventTimers(restoredBackend),
            "A rejected import must retain original timers");
        contract.exportState(backend(observed), handle, persistent);
        try (var keys =
            backend(observed).getKeys("waiting-to-emit-on-timer", VoidNamespace.INSTANCE)) {
          assertEquals(0, keys.count(), "Earlier valid chunks must not enter the native target");
        }
        try (var keys =
            backend(observed).getKeys("already-emitted-state-boolean", VoidNamespace.INSTANCE)) {
          assertEquals(0, keys.count());
        }
      }
    } finally {
      if (persistent) Native.closeRocksDBKeepFirstDeduplicator(handle);
      else Native.closeKeepFirstDeduplicator(handle);
    }
  }

  private static int savedEventTimers(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    var resources = backend.savepoint().getSnapshotResources();
    try (AutoCloseable release = resources::release) {
      var metadata = resources.getMetaInfoSnapshots();
      int timerId = -1;
      for (int index = 0; index < metadata.size(); index++)
        if (metadata.get(index).getName().equals("_timer_state/event_user-timers")) timerId = index;
      assertTrue(timerId >= 0);
      int count = 0;
      try (var iterator = resources.createKVStateIterator()) {
        while (iterator.isValid()) {
          if (iterator.kvStateId() == timerId) count++;
          iterator.next();
        }
      }
      return count;
    }
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> ordered(
      CheckpointableKeyedStateBackend<RowData> backend) {
    return (CheckpointableKeyedStateBackend<RowData>)
        java.lang.reflect.Proxy.newProxyInstance(
            CanonicalBufferedDedupInteropTest.class.getClassLoader(),
            new Class<?>[] {CheckpointableKeyedStateBackend.class},
            (proxy, method, args) -> {
              try {
                Object result = method.invoke(backend, args);
                if (method.getName().equals("getKeys"))
                  return ((java.util.stream.Stream<RowData>) result)
                      .map(backend.getKeySerializer()::copy)
                      .sorted(java.util.Comparator.comparingLong(key -> key.getLong(0)));
                return result;
              } catch (java.lang.reflect.InvocationTargetException failure) {
                throw failure.getCause();
              }
            });
  }

  private long create(boolean persistent) {
    return create(persistent, ROW);
  }

  private long create(boolean persistent, RowType rowType) {
    if (!persistent)
      return Native.createKeepFirstDeduplicator(new int[] {0}, DESCRIPTORS, 2, 1000, 1 << 24);
    assertTrue(Native.rocksdbStateAvailable());
    try (ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED,
          ArrowConversion.toArrowSchema(rowType),
          NativeAllocator.DICTIONARIES,
          schema);
      return Native.createRocksDBKeepFirstDeduplicator(
          new int[] {0},
          DESCRIPTORS,
          2,
          1000,
          0,
          1 << 24,
          schema.memoryAddress(),
          directory.resolve("native").toString(),
          128,
          FlinkRocksDBOptions.from(new org.apache.flink.configuration.Configuration()).json(),
          0,
          new String[0],
          new String[0],
          0,
          127,
          false,
          new byte[0][]);
    }
  }

  private static void push(long handle, boolean persistent, long time, RowData... rows) {
    try (VectorSchemaRoot root =
            RowDataArrowConverter.write(List.of(rows), ROW, NativeAllocator.SHARED);
        ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (persistent)
        Native.pushRocksDBKeepFirstDeduplicator(
            handle, array.memoryAddress(), schema.memoryAddress(), time);
      else
        Native.pushKeepFirstDeduplicator(
            handle, array.memoryAddress(), schema.memoryAddress(), time);
    }
  }

  private static List<String> flush(long handle, boolean persistent, long watermark, long time) {
    try (ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      if (persistent)
        Native.flushRocksDBKeepFirstDeduplicator(
            handle, watermark, time, array.memoryAddress(), schema.memoryAddress());
      else
        Native.flushKeepFirstDeduplicator(
            handle, watermark, time, array.memoryAddress(), schema.memoryAddress());
      try (VectorSchemaRoot root =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
        var reader = ArrowConversion.createArrowReader(root, ROW);
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < root.getRowCount(); i++) rows.add(value(reader.read(i)));
        return rows.stream().sorted().collect(java.util.stream.Collectors.toList());
      }
    }
  }

  private static List<String> values(List<RowData> rows) {
    return rows.stream()
        .map(CanonicalBufferedDedupInteropTest::value)
        .sorted()
        .collect(java.util.stream.Collectors.toList());
  }

  private static String value(RowData row) {
    return row.getLong(0) + ":" + row.getLong(1) + ":" + row.getLong(2);
  }

  private static GenericRowData row(long key, long value, long time) {
    return GenericRowData.of(key, value, time);
  }

  private static void time(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness, long time)
      throws Exception {
    harness.setProcessingTime(time);
    harness.setStateTtlProcessingTime(time);
  }

  private static OperatorSubtaskState canonical(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness, long id)
      throws Exception {
    return harness
        .snapshotWithLocalState(id, 1600, SavepointType.savepoint(SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  private static RowDataKeySelector selector() {
    return KeySelectorUtil.getRowDataSelector(
        CanonicalBufferedDedupInteropTest.class.getClassLoader(),
        new int[] {0},
        InternalTypeInfo.of(ROW));
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> backend(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
    return (CheckpointableKeyedStateBackend<RowData>)
        harness.getOperator().<RowData>getKeyedStateBackend();
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      boolean stock) throws Exception {
    org.apache.flink.streaming.api.operators.OneInputStreamOperator<RowData, RowData> operator =
        stock
            ? new KeyedProcessOperator<>(
                new RowTimeDeduplicateKeepFirstRowFunction(InternalTypeInfo.of(ROW), 1000, 2))
            : new ProjectionOperator();
    var selector = selector();
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(new HashMapStateBackend());
    harness.setup(new RowDataSerializer(ROW));
    return harness;
  }

  private static final class ProjectionOperator
      extends org.apache.flink.streaming.api.operators.AbstractStreamOperator<RowData>
      implements org.apache.flink.streaming.api.operators.OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}
