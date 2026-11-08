package tech.streamfusion.state;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released event-time SESSION accumulators, DISTINCT views, merge aliases and timers. */
public final class StockSessionWindowStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final String STATE = "window-aggs";
  private static final String MAPPING = "session-window-mapping";
  private static final String EVENTS = "_timer_state/event_window-timers";
  private static final String PROCESSING = "_timer_state/processing_window-timers";
  private static final int CHUNK_ROWS = 1024;
  private final TypeSerializer<RowData> keys;
  private final RowType keyType;
  private final RowType accumulators;
  private final RowType bridgeType;
  private final ZoneId zone;
  private final long gap;
  private final int[] averageOffsets;
  private final List<StockWindowDistinctView> distinctViews;
  private final int scalarFields;
  private final boolean processingTime;
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
    watermark = new StockWindowWatermark(store, restored);
  }

  @Override
  public long restoredWatermark() {
    if (watermark == null) throw new IllegalStateException("SESSION watermark is not initialized");
    return watermark.restoredWatermark();
  }

  @Override
  public void snapshotOperatorState(long value) throws Exception {
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
    var namespaces = namespaces(backend, Map.of());
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
    Set<WindowKey> scheduled = new HashSet<>();
    try (var iterator =
        StockTimerSnapshot.<RowData, TimeWindow>iterator(
            backend, processingTime ? PROCESSING : EVENTS)) {
      while (iterator.hasNext()) {
        var timer = iterator.next();
        validateBounds(timer.getNamespace());
        if (timer.getTimestamp() != deadline(timer.getNamespace())
            || (!processingTime && timer.getTimestamp() <= restoredWatermark())
            || !scheduled.add(
                new WindowKey(keyRows.toBinaryRow(timer.getKey()).copy(), timer.getNamespace())))
          throw new IllegalStateException("Unexpected stock SESSION timer");
      }
    }
    Map<WindowKey, TimeWindow> aliases = new HashMap<>();
    try (var keyStream = backend.getKeys(MAPPING, VoidNamespace.INSTANCE)) {
      var iterator = keyStream.iterator();
      while (iterator.hasNext()) {
        var key = keyRows.toBinaryRow(iterator.next()).copy();
        backend.setCurrentKey(key);
        var mapping =
            backend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, mappingDescriptor());
        var ordered = new java.util.TreeMap<Long, Long>();
        for (var entry : mapping.entries()) {
          var actual = entry.getKey();
          var state = entry.getValue();
          validateBounds(actual);
          validateBounds(state);
          if (state.getStart() < actual.getStart()
              || state.getEnd() > actual.getEnd()
              || aliases.put(new WindowKey(key, state), actual) != null)
            throw new IllegalStateException("Invalid stock SESSION merge alias");
          if (!scheduled.remove(new WindowKey(key, actual)))
            throw new IllegalStateException("Stock SESSION mapping has no matching timer");
          if (ordered.put(actual.getStart(), actual.getEnd()) != null)
            throw new IllegalStateException("Duplicate stock SESSION start");
        }
        Long previous = null;
        for (var window : ordered.entrySet()) {
          if (previous != null && previous >= window.getKey())
            throw new IllegalStateException("Overlapping stock SESSION mappings");
          previous = window.getValue();
        }
      }
    }
    if (!scheduled.isEmpty()) throw new IllegalStateException("Stock SESSION timer has no mapping");
    var remaining = new HashSet<>(aliases.keySet());
    try (var entries = backend.<TimeWindow>getKeysAndNamespaces(STATE)) {
      var iterator = entries.iterator();
      while (iterator.hasNext()) {
        var entry = iterator.next();
        if (!remaining.remove(new WindowKey(keyRows.toBinaryRow(entry.f0).copy(), entry.f1)))
          throw new IllegalStateException("Stock SESSION accumulator has no merge alias");
      }
    }
    if (!remaining.isEmpty())
      throw new IllegalStateException("Stock SESSION merge alias has no accumulator");
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
    for (var entry : aliases.entrySet()) {
      backend.setCurrentKey(entry.getKey().key);
      var value =
          backend
              .getPartitionedState(entry.getKey().window, new TimeWindow.Serializer(), descriptor())
              .value();
      if (value == null) throw new IllegalStateException("Stock SESSION has no accumulator");
      var row = new GenericRowData(bridgeType.getFieldCount());
      row.setField(0, nativeBound(entry.getValue().getStart()));
      row.setField(1, nativeBound(entry.getValue().getEnd()));
      for (int i = 0; i < keyGetters.length; i++)
        row.setField(2 + i, keyGetters[i].getFieldOrNull(entry.getKey().key));
      for (int i = 0; i < accGetters.length; i++)
        row.setField(2 + keyGetters.length + i, accGetters[i].getFieldOrNull(value));
      batch.add(row);
      if (batch.size() == CHUNK_ROWS) {
        importBatch(batch, handle);
        batch.clear();
      }
    }
    if (!batch.isEmpty()) importBatch(batch, handle);
    for (var view : distinctViews)
      view.importState(backend, keyType, handle, descriptor(), namespaces, true);
    if (!distinctViews.isEmpty()) Native.finishSessionCanonicalDistinct(handle);
    clear(backend);
  }

  private StockWindowDistinctView.Namespaces<TimeWindow> namespaces(
      CheckpointableKeyedStateBackend<RowData> backend, Map<WindowKey, TimeWindow> aliases) {
    var keyRows = new RowDataSerializer(keyType);
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

      public long nativeEnd(TimeWindow namespace, RowData key) {
        var actual = aliases.get(new WindowKey(keyRows.toBinaryRow(key), namespace));
        if (actual == null)
          throw new IllegalStateException("SESSION DISTINCT value has no merge alias");
        return nativeBound(actual.getEnd());
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

  private static final class WindowKey {
    final BinaryRowData key;
    final TimeWindow window;

    WindowKey(BinaryRowData key, TimeWindow window) {
      this.key = key;
      this.window = window;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof WindowKey
          && key.equals(((WindowKey) other).key)
          && window.equals(((WindowKey) other).window);
    }

    @Override
    public int hashCode() {
      return 31 * key.hashCode() + window.hashCode();
    }
  }
}
