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
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.window.StreamFusionCanonicalWindowFixture;
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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.FlinkRocksDBOptions;
import tech.streamfusion.state.StockSlicingWindowStateContract;

class CanonicalTumblingWindowInteropTest {
  private static final RowType INPUT =
      RowType.of(new VarCharType(), new IntType(), new TimestampType(3));
  private static final RowType ACC = RowType.of(new BigIntType(), new BigIntType());
  private static final long BASE = Instant.parse("2024-03-31T00:59:58Z").toEpochMilli();
  @TempDir Path directory;

  @ParameterizedTest
  @CsvSource({
    "missing,false",
    "missing,true",
    "timestamp,false",
    "timestamp,true",
    "orphan,false",
    "orphan,true",
    "processing,false",
    "processing,true"
  })
  void incompatibleWindowTimersFailBeforeAnyNativeRowsAreImported(String mode, boolean persistent)
      throws Exception {
    var zone = ZoneId.of("UTC");
    var selector = selector(false);
    long handle = create(persistent, false, zone);
    try (var source = stock(zone, false, false);
        var view = view(false, false)) {
      source.open();
      for (var row : prefix()) send(source, row);
      source.processWatermark(new Watermark(BASE + 499));
      source.prepareSnapshotPreBarrier(1);
      view.initializeState(
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState());
      view.open();
      var backend = backend(view);
      var timers =
          backend.create(
              "_timer_state/event_window-timers",
              new org.apache.flink.streaming.api.operators.TimerSerializer<>(
                  selector.getProducedType().toSerializer(),
                  org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE));
      var timer = timers.peek();
      assertNotNull(timer);
      if (mode.equals("missing")) timers.poll();
      else if (mode.equals("timestamp")) {
        timers.poll();
        timers.add(
            new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                timer.getTimestamp() + 1, timer.getKey(), timer.getNamespace()));
      } else if (mode.equals("orphan")) {
        timers.add(
            new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                BASE + 9999, timer.getKey(), BASE + 10000));
      } else {
        backend
            .create(
                "_timer_state/processing_window-timers",
                new org.apache.flink.streaming.api.operators.TimerSerializer<>(
                    selector.getProducedType().toSerializer(),
                    org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE))
            .add(
                new org.apache.flink.streaming.api.operators.TimerHeapInternalTimer<>(
                    timer.getTimestamp(), timer.getKey(), timer.getNamespace()));
      }
      var damaged =
          view.snapshotWithLocalState(2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = view(false, false)) {
        restored.initializeState(damaged);
        restored.open();
        var contract = contract(selector, zone);
        contract.initializeOperatorState(((Projection) restored.getOperator()).store, true);
        var rejected =
            assertThrows(
                IllegalStateException.class,
                () -> contract.importState(backend(restored), handle, persistent));
        String message =
            mode.equals("missing")
                ? "no matching timer"
                : mode.equals("timestamp")
                    ? "Unexpected stock window timer"
                    : mode.equals("orphan")
                        ? "timer has no accumulator"
                        : "does not consume these pending timers";
        assertTrue(rejected.getMessage().contains(message), rejected.getMessage());
      }
      var rows = new java.util.concurrent.atomic.AtomicInteger();
      Native.exportTumblingAggregatorCanonical(
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
      Native.closeTumblingAggregator(handle);
    }
  }

  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String zone : List.of("UTC", "+05:45", "Europe/Berlin", "America/New_York"))
      for (boolean persistent : new boolean[] {false, true})
        for (boolean stockRocks : new boolean[] {false, true})
          for (boolean keyless : new boolean[] {false, true})
            cases.add(Arguments.of(zone, persistent, stockRocks, keyless));
    return cases.stream();
  }

  @ParameterizedTest(name = "zone={0}, native rocks={1}, stock rocks={2}, keyless={3}")
  @MethodSource("cases")
  void releasedStockWindowsContinueNativelyAndRestoreBackToStock(
      String zoneId, boolean persistent, boolean stockRocks, boolean keyless) throws Exception {
    var zone = ZoneId.of(zoneId);
    var selector = selector(keyless);
    long handle = create(persistent, keyless, zone);
    try (var source = stock(zone, stockRocks, keyless);
        var reference = stock(zone, stockRocks, keyless);
        var view = view(!stockRocks, keyless)) {
      source.open();
      for (RowData row : prefix()) send(source, row);
      long cut = BASE + (keyless ? 999 : 499);
      source.processWatermark(new Watermark(cut));
      source.prepareSnapshotPreBarrier(1);
      var snapshot =
          source
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      reference.initializeState(snapshot);
      reference.open();
      view.initializeState(snapshot);
      view.open();
      // Global combination may retain already-fired accumulator bytes without another timer.
      var projected = backend(view);
      projected.setCurrentKey(selector.getKey(row("expired", 999, -100)));
      long expiredEnd =
          org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills(BASE, zone);
      projected
          .getPartitionedState(
              expiredEnd,
              org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE,
              new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                  "window-aggs", new RowDataSerializer(ACC)))
          .update(GenericRowData.of(999L, 1L));
      var contract = contract(selector, zone);
      contract.initializeOperatorState(((Projection) view.getOperator()).store, true);
      assertEquals(cut, contract.restoredWatermark());
      contract.importState(backend(view), handle, persistent);
      for (RowData row : middle()) send(reference, row);
      update(handle, keyless, zone, middle());
      reference.processWatermark(new Watermark(BASE + 999));
      assertEquals(output(reference, keyless), flush(handle, BASE + 999, keyless, zone));
      // No input arrives between export and the next stock restore. Pending later windows and
      // their timer namespaces must survive, while the first closed window must stay closed.
      contract.snapshotOperatorState(BASE + 999);
      contract.exportState(backend(view), handle, persistent);
      var exported =
          view.snapshotWithLocalState(2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = stock(zone, stockRocks, keyless)) {
        restored.initializeState(exported);
        restored.open();
        var suffix = suffix();
        for (RowData row : suffix) {
          send(reference, row);
          send(restored, row);
        }
        reference.processWatermark(new Watermark(BASE + 2999));
        restored.processWatermark(new Watermark(BASE + 2999));
        assertEquals(output(reference, keyless), output(restored, keyless));
        reference.processWatermark(new Watermark(Long.MAX_VALUE));
        restored.processWatermark(new Watermark(Long.MAX_VALUE));
        assertEquals(output(reference, keyless), output(restored, keyless));
      }
    } finally {
      Native.closeTumblingAggregator(handle);
    }
  }

  private static List<RowData> prefix() {
    return List.of(
        row("a", 10, -100),
        row("a", 5, 100),
        row("a", null, 300),
        row("b", null, 900),
        row(null, 7, 800),
        row("a", 11, 1100),
        row("b", null, 2100));
  }

  private static List<RowData> middle() {
    return List.of(
        row("a", 99, -200),
        row("a", 3, 700),
        row("b", null, 800),
        row(null, -2, 900),
        row("a", 9, 1300),
        row("b", 20, 2300));
  }

  private static List<RowData> suffix() {
    return List.of(
        row("a", 1000, 900),
        row("a", -4, 1500),
        row("b", 2, 2500),
        row(null, 5, 1800),
        row("a", 3, 3500));
  }

  private static RowData row(String key, Integer value, long timestamp) {
    return GenericRowData.of(
        key == null ? null : StringData.fromString(key),
        value,
        TimestampData.fromEpochMillis(BASE + timestamp));
  }

  private static void send(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness, RowData row)
      throws Exception {
    harness.processElement(
        new StreamRecord<>(new RowDataSerializer(INPUT).toBinaryRow(row).copy()));
  }

  private static RowDataKeySelector selector(boolean keyless) {
    return KeySelectorUtil.getRowDataSelector(
        CanonicalTumblingWindowInteropTest.class.getClassLoader(),
        keyless ? new int[0] : new int[] {0},
        InternalTypeInfo.of(INPUT));
  }

  private static StockSlicingWindowStateContract contract(
      RowDataKeySelector selector, ZoneId zone) {
    return new StockSlicingWindowStateContract(
        selector.getProducedType().toSerializer(),
        selector.getProducedType().toRowType(),
        ACC,
        1000,
        zone.getId(),
        new int[0]);
  }

  private long create(boolean persistent, boolean keyless, ZoneId zone) throws Exception {
    int[] keyTypes = keyless ? new int[0] : new int[] {3};
    int[] descriptors =
        keyless
            ? new int[] {Native.STOCK_KEY_PROJECTION}
            : new int[] {Native.STOCK_KEY_PROJECTION, -1};
    long handle =
        persistent
            ? Native.createRocksDBWindowAggregator(
                1000,
                1000,
                false,
                new int[] {0, 0},
                new int[] {0, 3},
                keyTypes,
                descriptors,
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
            : Native.createTumblingAggregator(
                1000, 1000, new int[] {0, 0}, new int[] {0, 3}, 1 << 26);
    if (zone.getRules().isFixedOffset())
      Native.setFixedWindowAssignmentOffset(
          handle, zone.getRules().getOffset(Instant.EPOCH).getTotalSeconds() * 1000L);
    else Native.setFixedWindowCalendar(handle, new WindowCalendar(zone.getId()));
    return handle;
  }

  private static void update(long handle, boolean keyless, ZoneId zone, List<RowData> input) {
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
      Native.updateTumblingAggregator(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }

  private static List<String> flush(long handle, long watermark, boolean keyless, ZoneId zone) {
    var result = new ArrayList<String>();
    try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Native.flushTumblingAggregator(
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

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      ZoneId zone, boolean rocks, boolean keyless) throws Exception {
    var selector = selector(keyless);
    return harness(
        StreamFusionCanonicalWindowFixture.tumbling(
            INPUT,
            (PagedTypeSerializer<RowData>) selector.getProducedType().toSerializer(),
            1000,
            zone),
        rocks,
        keyless);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> view(
      boolean rocks, boolean keyless) throws Exception {
    return harness(new Projection(), rocks, keyless);
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
            config, CanonicalTumblingWindowInteropTest.class.getClassLoader(), null));
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
