package tech.streamfusion.state;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released SESSION accumulators, DISTINCT views, merge aliases and timers for legacy SQL and TVFs. */
public final class StockSessionWindowStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final String STATE = "window-aggs";
  private static final String MAPPING = "session-window-mapping";
  private static final String EVENTS = "_timer_state/event_window-timers";
  private static final String PROCESSING = "_timer_state/processing_window-timers";
  private static final int CHUNK_ROWS = 1024, CHUNK_BYTES = 4 * 1024 * 1024;
  private static final int TIMERS = 1, MATCHED_TIMERS = 2, ALIASES = 3, WINDOWS = 4, OWNED = 5;
  private final TypeSerializer<RowData> keys;
  private final RowType keyType;
  private final RowType accumulators;
  private final RowType bridgeType;
  private final ZoneId zone;
  private final long gap;
  private final int[] averageOffsets;
  private final List<StockWindowDistinctView> distinctViews;
  private final int scalarFields;
  private final boolean processingTime, legacy;
  private transient StockWindowWatermark watermark;
  private transient CheckpointableKeyedStateBackend<RowData> timerBackend;
  private transient KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, TimeWindow>>
      events;
  private transient KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, TimeWindow>>
      processing;

  public StockSessionWindowStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulators,
      long gap,
      String zone,
      int[] averageOffsets) {
    this(
        keys,
        keyType,
        accumulators,
        gap,
        zone,
        averageOffsets,
        List.of(),
        accumulators.getFieldCount());
  }

  public StockSessionWindowStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulators,
      long gap,
      String zone,
      int[] averageOffsets,
      List<StockWindowDistinctView> distinctViews,
      int scalarFields) {
    this(
        keys, keyType, accumulators, gap, zone, averageOffsets, distinctViews, scalarFields, false);
  }

  public StockSessionWindowStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulators,
      long gap,
      String zone,
      int[] averageOffsets,
      List<StockWindowDistinctView> distinctViews,
      int scalarFields,
      boolean processingTime) {
    this(
        keys,
        keyType,
        accumulators,
        gap,
        zone,
        averageOffsets,
        distinctViews,
        scalarFields,
        processingTime,
        false);
  }

  public static StockSessionWindowStateContract legacy(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulators,
      long gap,
      String zone,
      int[] averageOffsets,
      List<StockWindowDistinctView> distinctViews,
      int scalarFields) {
    return new StockSessionWindowStateContract(
        keys,
        keyType,
        accumulators,
        gap,
        zone,
        averageOffsets,
        distinctViews,
        scalarFields,
        false,
        true);
  }

  private StockSessionWindowStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulators,
      long gap,
      String zone,
      int[] averageOffsets,
      List<StockWindowDistinctView> distinctViews,
      int scalarFields,
      boolean processingTime,
      boolean legacy) {
    this.legacy = legacy;
    this.processingTime = processingTime;
    if (gap <= 0
        || scalarFields < 0
        || scalarFields + distinctViews.size() != accumulators.getFieldCount())
      throw new IllegalArgumentException("Invalid SESSION accumulator/view layout");
    this.distinctViews = List.copyOf(distinctViews);
    this.scalarFields = scalarFields;
    for (int i = scalarFields; i < accumulators.getFieldCount(); i++) {
      var type = accumulators.getTypeAt(i);
      if (!(type instanceof org.apache.flink.table.types.logical.RawType)
          || !(((org.apache.flink.table.types.logical.RawType<?>) type).getTypeSerializer()
              instanceof org.apache.flink.table.dataview.NullSerializer))
        throw new IllegalArgumentException(
            "SESSION view placeholder must use the stock NULL serializer");
    }
    if (!RowDataArrowConverter.supports(
        RowType.of(
            accumulators
                .getChildren()
                .subList(0, scalarFields)
                .toArray(new org.apache.flink.table.types.logical.LogicalType[0]))))
      throw new IllegalArgumentException("Unsupported SESSION scalar fields");
    this.keys = keys.duplicate();
    this.keyType = keyType;
    this.accumulators = accumulators;
    this.gap = gap;
    this.zone = ZoneId.of(zone);
    this.averageOffsets = averageOffsets.clone();
    for (int offset : averageOffsets)
      if (offset < 0
          || offset + 1 >= scalarFields
          || !accumulators
              .getTypeAt(offset + 1)
              .is(org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT))
        throw new IllegalArgumentException("Invalid SESSION AVG accumulator offset");
    var fields = new ArrayList<RowType.RowField>();
    fields.add(new RowType.RowField("window_start", new BigIntType(false)));
    fields.add(new RowType.RowField("window_end", new BigIntType(false)));
    for (int i = 0; i < keyType.getFieldCount(); i++)
      fields.add(new RowType.RowField("key" + i, keyType.getTypeAt(i)));
    for (int i = 0; i < scalarFields; i++)
      fields.add(new RowType.RowField("acc" + i, accumulators.getTypeAt(i)));
    bridgeType = new RowType(fields);
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    var states = new HashMap<String, StateDescriptor.Type>();
    states.put(STATE, StateDescriptor.Type.VALUE);
    states.put(MAPPING, StateDescriptor.Type.MAP);
    for (var view : distinctViews)
      for (var entry : view.stateDescriptors().entrySet())
        if (states.put(entry.getKey(), entry.getValue()) != null)
          throw new IllegalStateException("Duplicate SESSION descriptor: " + entry.getKey());
    return Map.copyOf(states);
  }

  @Override
  public TypeSerializer<?> namespaceSerializer(String state) {
    if (!state.equals(MAPPING) && stateDescriptors().containsKey(state))
      return new TimeWindow.Serializer();
    if (state.equals(MAPPING)) return VoidNamespaceSerializer.INSTANCE;
    throw new IllegalArgumentException("Unknown SESSION state: " + state);
  }

  @Override
  public Map<String, StockStateInventory.Timer> timerSchema() {
    return Map.of(
        EVENTS,
        processingTime
            ? StockStateInventory.Timer.empty(new TimeWindow.Serializer())
            : StockStateInventory.Timer.consumed(new TimeWindow.Serializer()),
        PROCESSING,
        processingTime
            ? StockStateInventory.Timer.consumed(new TimeWindow.Serializer())
            : StockStateInventory.Timer.empty(new TimeWindow.Serializer()));
  }

  @Override
  public void initializeOperatorState(OperatorStateStore store, boolean restored) throws Exception {
    if (!legacy) watermark = new StockWindowWatermark(store, restored);
    else if (!store.getRegisteredStateNames().isEmpty()
        || !store.getRegisteredBroadcastStateNames().isEmpty())
      throw new IllegalStateException("Legacy SESSION has no operator-state contract");
  }

  @Override
  public long restoredWatermark() {
    if (legacy) return Long.MIN_VALUE;
    if (watermark == null) throw new IllegalStateException("SESSION watermark is not initialized");
    return watermark.restoredWatermark();
  }

  @Override
  public void snapshotOperatorState(long value) throws Exception {
    if (legacy) return;
    if (watermark == null) throw new IllegalStateException("SESSION watermark is not initialized");
    watermark.snapshot(value);
  }

  @Override
  public void configureNativeState(long handle, boolean persistent) {
    for (var view : distinctViews) view.configure(handle, true);
    if (!processingTime) Native.restoreSessionCanonicalWatermark(handle, restoredWatermark());
  }

  private ValueStateDescriptor<RowData> descriptor() {
    return new ValueStateDescriptor<>(STATE, new RowDataSerializer(accumulators));
  }

  private MapStateDescriptor<TimeWindow, TimeWindow> mappingDescriptor() {
    return new MapStateDescriptor<>(
        MAPPING, new TimeWindow.Serializer(), new TimeWindow.Serializer());
  }

  private KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, TimeWindow>> timers(
      CheckpointableKeyedStateBackend<RowData> backend, String name) {
    if (timerBackend != backend) {
      events =
          backend.create(
              EVENTS, new TimerSerializer<>(keys.duplicate(), new TimeWindow.Serializer()));
      processing =
          backend.create(
              PROCESSING, new TimerSerializer<>(keys.duplicate(), new TimeWindow.Serializer()));
      timerBackend = backend;
    }
    return name.equals(EVENTS) ? events : processing;
  }

  @Override
  public void clearProjectionTimers(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    timers(backend, EVENTS);
    timers(backend, PROCESSING);
    for (String name : List.of(EVENTS, PROCESSING)) {
      var queue = timers(backend, name);
      while (queue.poll() != null) {}
    }
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    clear(backend);
    var keyRows = new StockKeyProjection(keyType);
    var accRows = new RowDataSerializer(accumulators);
    var getters = getters(bridgeType);
    var events = timers(backend, processingTime ? PROCESSING : EVENTS);
    Native.exportSessionAggregatorCanonical(
        handle,
        (arrayAddress, schemaAddress) -> {
          try (var array = ArrowArray.wrap(arrayAddress);
              var schema = ArrowSchema.wrap(schemaAddress);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            var reader = ArrowConversion.createArrowReader(root, bridgeType);
            for (int index = 0; index < root.getRowCount(); index++) {
              var row = reader.read(index);
              var window = new TimeWindow(local(row.getLong(0)), local(row.getLong(1)));
              validateBounds(window);
              var key = new GenericRowData(keyType.getFieldCount());
              for (int i = 0; i < key.getArity(); i++)
                key.setField(i, getters[2 + i].getFieldOrNull(row));
              var binary = keyRows.project(key);
              backend.setCurrentKey(binary);
              var value = new GenericRowData(accumulators.getFieldCount());
              for (int i = 0; i < scalarFields; i++)
                value.setField(i, getters[2 + key.getArity() + i].getFieldOrNull(row));
              for (int offset : averageOffsets)
                if (value.isNullAt(offset) && value.getLong(offset + 1) == 0)
                  value.setField(
                      offset, StockWindowAccumulators.zero(accumulators.getTypeAt(offset)));
              backend
                  .getPartitionedState(window, new TimeWindow.Serializer(), descriptor())
                  .update(accRows.copy(value));
              backend
                  .getPartitionedState(
                      VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, mappingDescriptor())
                  .put(window, window);
              events.add(new TimerHeapInternalTimer<>(deadline(window), binary, window));
            }
          } catch (Exception failure) {
            throw new IllegalStateException("Cannot export stock SESSION state", failure);
          }
        });
    var namespaces = namespaces(backend, null);
    for (var view : distinctViews) view.exportState(backend, keyType, handle, namespaces, true);
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    configureNativeState(handle, persistent);
    var schemas = new HashMap<String, StockStateInventory.State>();
    stateDescriptors()
        .forEach(
            (name, kind) ->
                schemas.put(name, new StockStateInventory.State(kind, namespaceSerializer(name))));
    StockStateInventory.validate(backend, schemas, timerSchema(), keys);
    var keyRows = new RowDataSerializer(keyType);
    try (var aliases = new StockStateSpool(keys)) {
      try (var iterator =
          StockTimerSnapshot.<RowData, TimeWindow>iterator(
              backend, processingTime ? PROCESSING : EVENTS)) {
        while (iterator.hasNext()) {
          var timer = iterator.next();
          var window = timer.getNamespace();
          validateBounds(window);
          var key = keyRows.toBinaryRow(timer.getKey());
          if (timer.getTimestamp() != deadline(window)
              || (!processingTime && timer.getTimestamp() <= restoredWatermark())
              || aliases.contains(TIMERS, key, window.getStart()))
            throw new IllegalStateException("Unexpected stock SESSION timer");
          aliases.addOwner(key);
          aliases.put(TIMERS, key, window.getStart(), longBytes(window.getEnd()));
        }
      }
      try (var keyStream = backend.getKeys(MAPPING, VoidNamespace.INSTANCE)) {
        var iterator = keyStream.iterator();
        while (iterator.hasNext()) {
          var key = keyRows.toBinaryRow(iterator.next()).copy();
          aliases.addOwner(key);
          backend.setCurrentKey(key);
          var mapping =
              backend.getPartitionedState(
                  VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, mappingDescriptor());
          for (var entry : mapping.entries()) {
            var actual = entry.getKey();
            var state = entry.getValue();
            validateBounds(actual);
            validateBounds(state);
            // Disjoint actual windows give each state-window start one owner; retain its end too.
            if (state.getStart() < actual.getStart()
                || state.getEnd() > actual.getEnd()
                || aliases.contains(ALIASES, key, state.getStart()))
              throw new IllegalStateException("Invalid stock SESSION merge alias");
            byte[] scheduled = aliases.get(TIMERS, key, actual.getStart());
            if (scheduled == null
                || ByteBuffer.wrap(scheduled).getLong() != actual.getEnd()
                || !aliases.add(MATCHED_TIMERS, key, actual.getStart()))
              throw new IllegalStateException("Stock SESSION mapping has no matching timer");
            if (aliases.contains(WINDOWS, key, actual.getStart()))
              throw new IllegalStateException("Duplicate stock SESSION start");
            aliases.put(WINDOWS, key, actual.getStart(), longBytes(actual.getEnd()));
            aliases.put(
                ALIASES,
                key,
                state.getStart(),
                ByteBuffer.allocate(24)
                    .putLong(actual.getStart())
                    .putLong(actual.getEnd())
                    .putLong(state.getEnd())
                    .array());
          }
          Long previous = null;
          try (var starts = aliases.times(WINDOWS, key)) {
            while (starts.hasNext()) {
              long start = starts.next();
              if (previous != null && previous >= start)
                throw new IllegalStateException("Overlapping stock SESSION mappings");
              previous = ByteBuffer.wrap(aliases.get(WINDOWS, key, start)).getLong();
            }
          }
        }
      }
      try (var owners = aliases.owners()) {
        while (owners.hasNext()) {
          var owner = owners.next();
          try (var starts = aliases.times(TIMERS, owner)) {
            while (starts.hasNext())
              if (!aliases.contains(MATCHED_TIMERS, owner, starts.next()))
                throw new IllegalStateException("Stock SESSION timer has no mapping");
          }
        }
      }
      try (var entries = backend.<TimeWindow>getKeysAndNamespaces(STATE)) {
        var iterator = entries.iterator();
        while (iterator.hasNext()) {
          var entry = iterator.next();
          var owner = keyRows.toBinaryRow(entry.f0);
          alias(aliases, owner, entry.f1);
          if (!aliases.add(OWNED, owner, entry.f1.getStart()))
            throw new IllegalStateException("Stock SESSION accumulator has no merge alias");
        }
      }
      try (var owners = aliases.owners()) {
        while (owners.hasNext()) {
          var owner = owners.next();
          try (var starts = aliases.times(ALIASES, owner)) {
            while (starts.hasNext())
              if (!aliases.contains(OWNED, owner, starts.next()))
                throw new IllegalStateException("Stock SESSION merge alias has no accumulator");
          }
        }
      }
      var namespaces = namespaces(backend, aliases);
      for (var view : distinctViews) view.validateState(backend, descriptor(), namespaces);
      var keyGetters = getters(keyType);
      var accGetters =
          getters(
              RowType.of(
                  accumulators
                      .getChildren()
                      .subList(0, scalarFields)
                      .toArray(new org.apache.flink.table.types.logical.LogicalType[0])));
      var batch = new ArrayList<RowData>(CHUNK_ROWS);
      var serializer = new RowDataSerializer(bridgeType);
      long bytes = 0;
      try (var owners = aliases.owners()) {
        while (owners.hasNext()) {
          var owner = owners.next();
          backend.setCurrentKey(owner);
          try (var starts = aliases.times(ALIASES, owner)) {
            while (starts.hasNext()) {
              long start = starts.next();
              var encoded = ByteBuffer.wrap(aliases.get(ALIASES, owner, start));
              var actual = new TimeWindow(encoded.getLong(), encoded.getLong());
              var state = new TimeWindow(start, encoded.getLong());
              var value =
                  backend
                      .getPartitionedState(state, new TimeWindow.Serializer(), descriptor())
                      .value();
              if (value == null)
                throw new IllegalStateException("Stock SESSION has no accumulator");
              var row = new GenericRowData(bridgeType.getFieldCount());
              row.setField(0, nativeBound(actual.getStart()));
              row.setField(1, nativeBound(actual.getEnd()));
              for (int i = 0; i < keyGetters.length; i++)
                row.setField(2 + i, keyGetters[i].getFieldOrNull(owner));
              for (int i = 0; i < accGetters.length; i++)
                row.setField(2 + keyGetters.length + i, accGetters[i].getFieldOrNull(value));
              long size = serializer.toBinaryRow(row).getSizeInBytes();
              if (!batch.isEmpty() && (batch.size() == CHUNK_ROWS || bytes + size > CHUNK_BYTES)) {
                importBatch(batch, handle);
                batch.clear();
                bytes = 0;
              }
              batch.add(row);
              bytes += size;
            }
          }
        }
      }
      if (!batch.isEmpty()) importBatch(batch, handle);
      for (var view : distinctViews)
        view.importState(backend, keyType, handle, descriptor(), namespaces, true);
      if (!distinctViews.isEmpty()) Native.finishSessionCanonicalDistinct(handle);
    }
    clear(backend);
  }

  private StockWindowDistinctView.Namespaces<TimeWindow> namespaces(
      CheckpointableKeyedStateBackend<RowData> backend, StockStateSpool aliases) {
    return new StockWindowDistinctView.Namespaces<TimeWindow>() {
      public TypeSerializer<TimeWindow> serializer() {
        return new TimeWindow.Serializer();
      }

      public TimeWindow stock(long end, RowData key) throws Exception {
        long localEnd = local(end);
        var mapping =
            backend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, mappingDescriptor());
        for (var window : mapping.keys()) if (window.getEnd() == localEnd) return window;
        throw new IllegalStateException("SESSION DISTINCT value has no exported window");
      }

      public long nativeEnd(TimeWindow namespace, RowData key) throws Exception {
        if (aliases == null) throw new IllegalStateException("SESSION alias inventory is missing");
        return nativeBound(alias(aliases, key, namespace).getEnd());
      }

      public boolean live(TimeWindow namespace) {
        return true;
      }
    };
  }

  private void importBatch(List<RowData> rows, long handle) {
    try (var root = RowDataArrowConverter.write(rows, bridgeType, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      Native.importSessionAggregatorCanonical(
          handle, keyType.getFieldCount(), array.memoryAddress(), schema.memoryAddress());
    }
  }

  private void validateBounds(TimeWindow window) {
    if (window.getEnd() <= window.getStart()
        || java.math.BigInteger.valueOf(window.getEnd())
                .subtract(java.math.BigInteger.valueOf(window.getStart()))
                .compareTo(java.math.BigInteger.valueOf(gap))
            < 0) throw new IllegalStateException("SESSION namespace is shorter than its gap");
  }

  private long deadline(TimeWindow window) {
    return TimeWindowUtil.toEpochMillsForTimer(Math.subtractExact(window.getEnd(), 1), zone);
  }

  private long local(long value) {
    return zone.getRules().isFixedOffset()
        ? TimeWindowUtil.toUtcTimestampMills(value, zone)
        : value;
  }

  private long nativeBound(long value) {
    return zone.getRules().isFixedOffset() ? TimeWindowUtil.toEpochMills(value, zone) : value;
  }

  private static RowData.FieldGetter[] getters(RowType type) {
    var result = new RowData.FieldGetter[type.getFieldCount()];
    for (int i = 0; i < result.length; i++)
      result[i] = RowData.createFieldGetter(type.getTypeAt(i), i);
    return result;
  }

  private static byte[] longBytes(long value) {
    return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
  }

  private static TimeWindow alias(StockStateSpool aliases, RowData key, TimeWindow namespace)
      throws Exception {
    byte[] bytes = aliases.get(ALIASES, key, namespace.getStart());
    if (bytes == null || ByteBuffer.wrap(bytes).getLong(16) != namespace.getEnd())
      throw new IllegalStateException("Stock SESSION accumulator has no merge alias");
    var encoded = ByteBuffer.wrap(bytes);
    return new TimeWindow(encoded.getLong(), encoded.getLong());
  }
}
