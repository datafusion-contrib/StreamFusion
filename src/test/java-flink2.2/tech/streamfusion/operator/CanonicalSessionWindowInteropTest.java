package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.operators.aggregate.window.StreamFusionCanonicalWindowFixture;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.FlinkRocksDBOptions;
import tech.streamfusion.state.StockSessionWindowStateContract;

class CanonicalSessionWindowInteropTest {
  private static final RowType INPUT =
      RowType.of(new VarCharType(), new IntType(), new TimestampType(3));
  private static final RowType ACC = RowType.of(new BigIntType(), new BigIntType());
  private static final long BASE = Instant.parse("2024-03-31T00:59:58Z").toEpochMilli();
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    var result = new ArrayList<Arguments>();
    for (String zone : List.of("UTC", "+05:45", "Europe/Berlin", "America/New_York"))
      for (boolean persistent : new boolean[] {false, true})
        for (boolean stockRocks : new boolean[] {false, true})
          for (boolean keyless : new boolean[] {false, true})
            result.add(Arguments.of(zone, persistent, stockRocks, keyless));
    return result.stream();
  }

  static Stream<Arguments> corruptions() {
    return Stream.of(
            "missingMapping",
            "missingAccumulator",
            "outsideAlias",
            "missingTimer",
            "wrongTimer",
            "processingTimer",
            "orphanAccumulator",
            "unknownState")
        .flatMap(mode -> Stream.of(Arguments.of(mode, false), Arguments.of(mode, true)));
  }

  @ParameterizedTest
  @MethodSource("corruptions")
  void corruptSessionMetadataFailsBeforeImportingNativeRows(String mode, boolean persistent)
      throws Exception {
    long handle = create(persistent, false, ZoneId.of("UTC"));
    try (var source = stock(ZoneId.of("UTC"), false, false);
        var view = harness(new Projection(), false, false)) {
      source.open();
      source.processElement(new StreamRecord<>(row("a", 10, 0)));
      source.processElement(new StreamRecord<>(row("a", 20, 1000)));
      source.processWatermark(new Watermark(BASE + 500));
      source.prepareSnapshotPreBarrier(1);
      view.initializeState(
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState());
      view.open();
      var backend = backend(view);
      backend.setCurrentKey(selector(false).getKey(row("a", 1, 0)));
      var mapping =
          backend.getPartitionedState(
              org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
              org.apache.flink.runtime.state.VoidNamespaceSerializer.INSTANCE,
              new org.apache.flink.api.common.state.MapStateDescriptor<>(
                  "session-window-mapping",
                  new TimeWindow.Serializer(),
                  new TimeWindow.Serializer()));
      var entry = mapping.entries().iterator().next();
      var actual = entry.getKey();
      var alias = entry.getValue();
      if (mode.equals("missingMapping")) mapping.clear();
      else if (mode.equals("missingAccumulator"))
        backend
            .getPartitionedState(
                alias,
                new TimeWindow.Serializer(),
                new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                    "window-aggs", new RowDataSerializer(ACC)))
            .clear();
      else if (mode.equals("outsideAlias"))
        mapping.put(actual, new TimeWindow(actual.getStart() - 1, actual.getEnd()));
      else if (mode.equals("orphanAccumulator"))
        backend
            .getPartitionedState(
                new TimeWindow(BASE + 9000, BASE + 10000),
                new TimeWindow.Serializer(),
                new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                    "window-aggs", new RowDataSerializer(ACC)))
            .update(GenericRowData.of(99L, 1L));
      else if (mode.equals("unknownState"))
        backend
            .getPartitionedState(
                org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                org.apache.flink.runtime.state.VoidNamespaceSerializer.INSTANCE,
                new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                    "unowned-session-state",
                    org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE))
            .update(1L);
      else {
        var timers =
            backend.create(
                "_timer_state/event_window-timers",
                new org.apache.flink.streaming.api.operators.TimerSerializer<>(
                    selector(false).getProducedType().toSerializer(), new TimeWindow.Serializer()));
        var timer = timers.poll();
        assertNotNull(timer);
        if (mode.equals("wrongTimer"))
          timers.add(
              new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                  timer.getTimestamp() + 1, timer.getKey(), timer.getNamespace()));
        else if (mode.equals("processingTimer")) {
          timers.add(timer);
          backend
              .create(
                  "_timer_state/processing_window-timers",
                  new org.apache.flink.streaming.api.operators.TimerSerializer<>(
                      selector(false).getProducedType().toSerializer(),
                      new TimeWindow.Serializer()))
              .add(timer);
        }
      }
      var damaged =
          view.snapshotWithLocalState(2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = harness(new Projection(), false, false)) {
        restored.initializeState(damaged);
        restored.open();
        var contract = contract(false, ZoneId.of("UTC"));
        contract.initializeOperatorState(((Projection) restored.getOperator()).store, true);
        assertThrows(
            IllegalStateException.class,
            () -> contract.importState(backend(restored), handle, persistent));
      }
      var rows = new java.util.concurrent.atomic.AtomicInteger();
      Native.exportSessionAggregatorCanonical(
          handle,
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var schema = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              rows.addAndGet(root.getRowCount());
            }
          });
      assertEquals(0, rows.get());
    } finally {
      if (persistent) Native.closeRocksDBSessionAggregator(handle);
      else Native.closeSessionAggregator(handle);
    }
  }

  @ParameterizedTest
  @MethodSource("cases")
  void historicalMergeAliasesContinueAcrossRepeatedCanonicalHandoffs(
      String zoneId, boolean persistent, boolean stockRocks, boolean keyless) throws Exception {
    var zone = ZoneId.of(zoneId);
    var prefix =
        List.of(
            row("a", 10, 0),
            row("a", 30, 2000),
            row("a", 20, 1000),
            row(null, null, 10000),
            row("expired", 999, -4000));
    var middle =
        List.of(
            row("a", 5, 100),
            row("late-missing", 999, 100),
            row("a", null, 3000),
            row("a", 50, 5000),
            row("a", 40, 4000));
    long handle = create(persistent, keyless, zone);
    try (var source = stock(zone, stockRocks, keyless);
        var reference = stock(zone, stockRocks, keyless);
        var view = harness(new Projection(), !stockRocks, keyless)) {
      source.open();
      for (var row : prefix) source.processElement(new StreamRecord<>(row));
      source.processWatermark(new Watermark(BASE + 1500));
      source.prepareSnapshotPreBarrier(1);
      var snapshot =
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      reference.initializeState(snapshot);
      reference.open();
      view.initializeState(snapshot);
      view.open();
      var contract = contract(keyless, zone);
      contract.initializeOperatorState(((Projection) view.getOperator()).store, true);
      assertEquals(BASE + 1500, contract.restoredWatermark());
      assertHistoricalAlias(backend(view), keyless);
      contract.importState(backend(view), handle, persistent);
      // Re-export before new input: stock must not need a StreamFusion state namespace.
      contract.snapshotOperatorState(BASE + 1500);
      contract.exportState(backend(view), handle, persistent);
      var immediate =
          view.snapshotWithLocalState(2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = stock(zone, stockRocks, keyless)) {
        restored.initializeState(immediate);
        restored.open();
        for (var row : middle) {
          restored.processElement(new StreamRecord<>(row));
          reference.processElement(new StreamRecord<>(row));
        }
        update(handle, persistent, keyless, zone, middle);
        for (long watermark : new long[] {BASE + 3999, BASE + 5999, Long.MAX_VALUE}) {
          reference.processWatermark(new Watermark(watermark));
          restored.processWatermark(new Watermark(watermark));
          var expected = output(reference, keyless);
          assertEquals(expected, output(restored, keyless));
          assertEquals(expected, flush(handle, persistent, watermark, keyless, zone));
        }
      }
    } finally {
      if (persistent) Native.closeRocksDBSessionAggregator(handle);
      else Native.closeSessionAggregator(handle);
    }
  }

  private static RowData row(String key, Integer value, long time) {
    return GenericRowData.of(
        key == null ? null : StringData.fromString(key),
        value,
        TimestampData.fromEpochMillis(BASE + time));
  }

  private static org.apache.flink.table.runtime.keyselector.RowDataKeySelector selector(
      boolean keyless) {
    return KeySelectorUtil.getRowDataSelector(
        CanonicalSessionWindowInteropTest.class.getClassLoader(),
        keyless ? new int[0] : new int[] {0},
        InternalTypeInfo.of(INPUT));
  }

  private static StockSessionWindowStateContract contract(boolean keyless, ZoneId zone) {
    var selector = selector(keyless);
    return new StockSessionWindowStateContract(
        selector.getProducedType().toSerializer(),
        selector.getProducedType().toRowType(),
        ACC,
        1000,
        zone.getId(),
        new int[0]);
  }

  private static void assertHistoricalAlias(
      CheckpointableKeyedStateBackend<RowData> backend, boolean keyless) throws Exception {
    boolean alias = false;
    try (var keys =
        backend.getKeys(
            "session-window-mapping", org.apache.flink.runtime.state.VoidNamespace.INSTANCE)) {
      var iterator = keys.iterator();
      while (iterator.hasNext()) {
        backend.setCurrentKey(iterator.next());
        var mapping =
            backend.getPartitionedState(
                org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                org.apache.flink.runtime.state.VoidNamespaceSerializer.INSTANCE,
                new org.apache.flink.api.common.state.MapStateDescriptor<>(
                    "session-window-mapping",
                    new TimeWindow.Serializer(),
                    new TimeWindow.Serializer()));
        for (var entry : mapping.entries()) alias |= !entry.getKey().equals(entry.getValue());
      }
    }
    assertTrue(alias, "Fixture must contain a historical state-window alias");
  }

  private long create(boolean persistent, boolean keyless, ZoneId zone) {
    long handle =
        persistent
            ? Native.createRocksDBSessionAggregator(
                1000,
                new int[] {0, 0},
                new int[] {0, 3},
                keyless ? new int[0] : new int[] {3},
                keyless
                    ? new int[] {Native.STOCK_KEY_PROJECTION}
                    : new int[] {Native.STOCK_KEY_PROJECTION, -1},
                1 << 26,
                directory.resolve("native").toString(),
                128,
                FlinkRocksDBOptions.from(new Configuration()).json(),
                0,
                new String[0],
                new String[0],
                0,
                127,
                false,
                new byte[0][],
                Long.MIN_VALUE)
            : Native.createSessionAggregator(1000, new int[] {0, 0}, new int[] {0, 3}, 1 << 26);
    if (!zone.getRules().isFixedOffset())
      Native.setSessionWindowCalendar(handle, new WindowCalendar(zone.getId()));
    return handle;
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      ZoneId zone, boolean rocks, boolean keyless) throws Exception {
    return harness(
        StreamFusionCanonicalWindowFixture.session(
            INPUT,
            (PagedTypeSerializer<RowData>) selector(keyless).getProducedType().toSerializer(),
            1000,
            zone),
        rocks,
        keyless);
  }

  private static void update(
      long handle, boolean persistent, boolean keyless, ZoneId zone, List<RowData> input) {
    var fields = new ArrayList<RowType.RowField>();
    if (!keyless) fields.add(new RowType.RowField("key0", new VarCharType()));
    fields.add(new RowType.RowField("value0", new BigIntType()));
    fields.add(new RowType.RowField("value1", new BigIntType()));
    fields.add(new RowType.RowField("ts", new BigIntType(false)));
    var rows = new ArrayList<RowData>();
    for (var row : input) {
      var output = new GenericRowData(fields.size());
      int column = 0;
      if (!keyless) output.setField(column++, row.isNullAt(0) ? null : row.getString(0));
      Object value = row.isNullAt(1) ? null : (long) row.getInt(1);
      output.setField(column++, value);
      output.setField(column++, value);
      long timestamp = row.getTimestamp(2, 3).getMillisecond();
      output.setField(
          column,
          zone.getRules().isFixedOffset()
              ? timestamp
              : org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills(
                  timestamp, zone));
      rows.add(output);
    }
    try (var root = RowDataArrowConverter.write(rows, new RowType(fields), NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (persistent)
        Native.pushRocksDBSessionAggregator(handle, array.memoryAddress(), schema.memoryAddress());
      else Native.updateSessionAggregator(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }

  private static List<String> flush(
      long handle, boolean persistent, long watermark, boolean keyless, ZoneId zone) {
    var result = new ArrayList<String>();
    try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      if (persistent)
        Native.flushRocksDBSessionAggregator(
            handle,
            WindowTimeDomain.closeThreshold(watermark, 0),
            array.memoryAddress(),
            schema.memoryAddress());
      else
        Native.flushSessionAggregator(
            handle,
            WindowTimeDomain.closeThreshold(watermark, 0),
            array.memoryAddress(),
            schema.memoryAddress());
      try (var root =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
        var types = new ArrayList<RowType.RowField>();
        if (!keyless) types.add(new RowType.RowField("key0", new VarCharType()));
        for (String name : List.of("window_start", "window_end", "result0", "result1"))
          types.add(new RowType.RowField(name, new BigIntType()));
        var reader = ArrowConversion.createArrowReader(root, new RowType(types));
        int shift = keyless ? 0 : 1;
        for (int i = 0; i < root.getRowCount(); i++) {
          var row = reader.read(i);
          long start = row.getLong(shift), end = row.getLong(shift + 1);
          if (zone.getRules().isFixedOffset()) {
            start =
                org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills(start, zone);
            end = org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills(end, zone);
          }
          result.add(
              (keyless ? "global" : row.isNullAt(0) ? "null" : row.getString(0).toString())
                  + ":"
                  + (row.isNullAt(shift + 2) ? "null" : row.getLong(shift + 2))
                  + ":"
                  + row.getLong(shift + 3)
                  + ":"
                  + start
                  + ":"
                  + end);
        }
      }
    }
    result.sort(String::compareTo);
    return result;
  }

  private static List<String> output(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness, boolean keyless) {
    var result = new ArrayList<String>();
    int shift = keyless ? 0 : 1;
    while (!harness.getOutput().isEmpty()) {
      var event = harness.getOutput().poll();
      if (event instanceof StreamRecord<?>) {
        var row = (RowData) ((StreamRecord<?>) event).getValue();
        result.add(
            (keyless ? "global" : row.isNullAt(0) ? "null" : row.getString(0).toString())
                + ":"
                + (row.isNullAt(shift) ? "null" : row.getLong(shift))
                + ":"
                + row.getLong(shift + 1)
                + ":"
                + row.getLong(shift + 2)
                + ":"
                + row.getLong(shift + 3));
      }
    }
    result.sort(String::compareTo);
    return result;
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      OneInputStreamOperator<RowData, RowData> operator, boolean rocks, boolean keyless)
      throws Exception {
    var selector = selector(keyless);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            operator, selector, selector.getProducedType(), 128, 1, 0);
    var config = new Configuration();
    config.set(StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
    harness.setStateBackend(
        StateBackendLoader.loadStateBackendFromConfig(
            config, CanonicalSessionWindowInteropTest.class.getClassLoader(), null));
    var outputTypes = new ArrayList<org.apache.flink.table.types.logical.LogicalType>();
    if (!keyless) outputTypes.add(new VarCharType());
    for (int i = 0; i < 4; i++) outputTypes.add(new BigIntType());
    harness.setup(
        new RowDataSerializer(
            outputTypes.toArray(new org.apache.flink.table.types.logical.LogicalType[0])));
    return harness;
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> backend(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> view) {
    return (CheckpointableKeyedStateBackend<RowData>)
        view.getOperator().<RowData>getKeyedStateBackend();
  }

  private static final class Projection extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    transient org.apache.flink.api.common.state.OperatorStateStore store;

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
      super.initializeState(context);
      store = context.getOperatorStateStore();
    }

    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}
