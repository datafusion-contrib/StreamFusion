package tech.streamfusion.state;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released TVF slicing accumulators, clock-specific window timers and UNION watermark state. */
public final class StockSlicingWindowStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final String STATE = "window-aggs";
  private static final String EVENTS = "_timer_state/event_window-timers";
  private static final String PROCESSING = "_timer_state/processing_window-timers";
  private static final int CHUNK_ROWS = 1024;
  private final TypeSerializer<RowData> keys;
  private final RowType keyType;
  private final RowType accumulators;
  private final RowType bridgeType;
  private final RowType timerBridgeType;
  private final long slice;
  private final boolean shared;
  private final int countStar;
  private final ZoneId zone;
  private final int[] averageOffsets;
  private final List<StockWindowDistinctView> distinctViews;
  private final int scalarFields;
  private final WindowStart finalWindows;
  private final boolean processingTime;

  @FunctionalInterface
  public interface WindowStart extends java.io.Serializable {
    long start(long end);
  }

  private transient StockWindowWatermark watermark;
  private transient CheckpointableKeyedStateBackend<RowData> timerBackend;
  private transient KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, Long>> events;
  private transient KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, Long>>
      processing;

  public StockSlicingWindowStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulators,
      long size,
      String zone,
      int[] averageOffsets) {
    this(keys, keyType, accumulators, size, size, false, -1, zone, averageOffsets);
  }

  public StockSlicingWindowStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulators,
      long size,
      long slice,
      boolean shared,
      int countStar,
      String zone,
      int[] averageOffsets) {
    this(
        keys,
        keyType,
        accumulators,
        size,
        slice,
        shared,
        countStar,
        zone,
        averageOffsets,
        List.of(),
        accumulators.getFieldCount());
  }

  public StockSlicingWindowStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulators,
      long size,
      long slice,
      boolean shared,
      int countStar,
      String zone,
      int[] averageOffsets,
      List<StockWindowDistinctView> distinctViews,
      int scalarFields) {
    this(
        keys,
        keyType,
        accumulators,
        size,
        slice,
        shared,
        countStar,
        zone,
        averageOffsets,
        distinctViews,
        scalarFields,
        null);
  }

  public StockSlicingWindowStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulators,
      long size,
      long slice,
      boolean shared,
      int countStar,
      String zone,
      int[] averageOffsets,
      List<StockWindowDistinctView> distinctViews,
      int scalarFields,
      WindowStart finalWindows) {
    this(
        keys,
        keyType,
        accumulators,
        size,
        slice,
        shared,
        countStar,
        zone,
        averageOffsets,
        distinctViews,
        scalarFields,
        finalWindows,
        false);
  }

  public StockSlicingWindowStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulators,
      long size,
      long slice,
      boolean shared,
      int countStar,
      String zone,
      int[] averageOffsets,
      List<StockWindowDistinctView> distinctViews,
      int scalarFields,
      WindowStart finalWindows,
      boolean processingTime) {
    this.processingTime = processingTime;
    if (shared && finalWindows != null)
      throw new IllegalArgumentException("Final windows cannot use shared-slice state");
    this.finalWindows = finalWindows;
    if (size <= 0 || slice <= 0 || size % slice != 0)
      throw new IllegalArgumentException("Window size must be a positive multiple of its slice");
    this.keys = keys.duplicate();
    this.keyType = keyType;
    this.accumulators = accumulators;
    this.slice = slice;
    this.shared = shared;
    this.countStar = countStar;
    this.zone = ZoneId.of(zone);
    this.averageOffsets = averageOffsets.clone();
    this.distinctViews = List.copyOf(distinctViews);
    this.scalarFields = scalarFields;
    if (scalarFields < 0 || scalarFields + distinctViews.size() != accumulators.getFieldCount())
      throw new IllegalArgumentException("Unexpected scalar/window-view accumulator layout");
    for (int i = scalarFields; i < accumulators.getFieldCount(); i++) {
      var type = accumulators.getTypeAt(i);
      if (!(type instanceof org.apache.flink.table.types.logical.RawType)
          || !(((org.apache.flink.table.types.logical.RawType<?>) type).getTypeSerializer()
              instanceof org.apache.flink.table.dataview.NullSerializer))
        throw new IllegalArgumentException(
            "Window view placeholder must use the stock NULL serializer");
    }
    for (int offset : averageOffsets)
      if (offset < 0
          || offset + 1 >= scalarFields
          || !accumulators
              .getTypeAt(offset + 1)
              .is(org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT))
        throw new IllegalArgumentException("Invalid AVG accumulator offset");
    var fields = new ArrayList<RowType.RowField>();
    fields.add(new RowType.RowField("window_start", new BigIntType(false)));
    fields.add(new RowType.RowField("window_end", new BigIntType(false)));
    for (int i = 0; i < keyType.getFieldCount(); i++)
      fields.add(new RowType.RowField("key" + i, keyType.getTypeAt(i)));
    for (int i = 0; i < scalarFields; i++)
      fields.add(new RowType.RowField("acc" + i, accumulators.getTypeAt(i)));
    bridgeType = new RowType(fields);
    timerBridgeType = new RowType(fields.subList(1, 2 + keyType.getFieldCount()));
  }

  @Override
  public void configureNativeState(long handle, boolean persistent) {
    if (finalWindows != null) Native.setFixedWindowFinalWindows(handle, true);
    for (var view : distinctViews) view.configure(handle);
    if (shared)
      Native.configureSlicingWindowCanonical(
          handle,
          countStar,
          processingTime
              || tech.streamfusion.compat.FlinkPlanCompat.restoresWindowTimerWatermark());
    if (!processingTime) Native.restoreTumblingCanonicalWatermark(handle, restoredWatermark());
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    var states = new java.util.HashMap<String, StateDescriptor.Type>();
    states.put(STATE, StateDescriptor.Type.VALUE);
    for (var view : distinctViews) {
      for (var entry : view.stateDescriptors().entrySet())
        if (states.put(entry.getKey(), entry.getValue()) != null)
          throw new IllegalStateException("Duplicate window state descriptor: " + entry.getKey());
    }
    return Map.copyOf(states);
  }

  @Override
  public TypeSerializer<?> namespaceSerializer(String state) {
    if (!stateDescriptors().containsKey(state))
      throw new IllegalArgumentException("Unknown window state: " + state);
    return LongSerializer.INSTANCE;
  }

  @Override
  public Map<String, StockStateInventory.Timer> timerSchema() {
    return Map.of(
        EVENTS,
        processingTime
            ? StockStateInventory.Timer.empty(LongSerializer.INSTANCE)
            : StockStateInventory.Timer.consumed(LongSerializer.INSTANCE),
        PROCESSING,
        processingTime
            ? StockStateInventory.Timer.consumed(LongSerializer.INSTANCE)
            : StockStateInventory.Timer.empty(LongSerializer.INSTANCE));
  }

  @Override
  public void initializeOperatorState(OperatorStateStore store, boolean restored) throws Exception {
    watermark = new StockWindowWatermark(store, restored);
  }

  @Override
  public void snapshotOperatorState(long value) throws Exception {
    if (watermark == null)
      throw new IllegalStateException("Window operator state is not initialized");
    watermark.snapshot(value);
  }

  @Override
  public long restoredWatermark() {
    if (watermark == null)
      throw new IllegalStateException("Window operator state is not initialized");
    return watermark.restoredWatermark();
  }

  private long windowStart(long end) {
    return finalWindows == null ? Math.subtractExact(end, slice) : finalWindows.start(end);
  }

  private ValueStateDescriptor<RowData> descriptor() {
    return new ValueStateDescriptor<>(STATE, new RowDataSerializer(accumulators));
  }

  private void bindTimers(CheckpointableKeyedStateBackend<RowData> backend) {
    if (timerBackend == backend) return;
    events =
        backend.create(EVENTS, new TimerSerializer<>(keys.duplicate(), LongSerializer.INSTANCE));
    processing =
        backend.create(
            PROCESSING, new TimerSerializer<>(keys.duplicate(), LongSerializer.INSTANCE));
    timerBackend = backend;
  }

  private KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, Long>> activeTimers() {
    return processingTime ? processing : events;
  }

  private boolean live(long end) {
    return processingTime || deadline(end) > restoredWatermark();
  }

  @Override
  public void clearProjectionTimers(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    bindTimers(backend);
    while (events.poll() != null) {}
    while (processing.poll() != null) {}
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    clear(backend);
    var keyRows = new StockKeyProjection(keyType);
    var accRows = new RowDataSerializer(accumulators);
    var descriptor = descriptor();
    var getters = getters(bridgeType);
    Native.BatchReceiver receiver =
        (arrayAddress, schemaAddress) -> {
          try (var array = ArrowArray.wrap(arrayAddress);
              var schema = ArrowSchema.wrap(schemaAddress);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            var reader = ArrowConversion.createArrowReader(root, bridgeType);
            for (int index = 0; index < root.getRowCount(); index++) {
              var row = reader.read(index);
              long start = local(row.getLong(0));
              long end = local(row.getLong(1));
              if (start != windowStart(end))
                throw new IllegalStateException(
                    "Native window bounds disagree with stock namespace");
              var key = new GenericRowData(keyType.getFieldCount());
              for (int i = 0; i < key.getArity(); i++)
                key.setField(i, getters[i + 2].getFieldOrNull(row));
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
                  .getPartitionedState(end, LongSerializer.INSTANCE, descriptor)
                  .update(accRows.copy(value));
              if (!shared)
                activeTimers().add(new TimerHeapInternalTimer<>(deadline(end), binary, end));
            }
          } catch (Exception failure) {
            throw new IllegalStateException("Cannot export stock window state", failure);
          }
        };
    if (shared) {
      Native.exportSlicingWindowCanonical(handle, false, receiver);
      var timerGetters = getters(timerBridgeType);
      Native.exportSlicingWindowCanonical(
          handle,
          true,
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var schema = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              var reader = ArrowConversion.createArrowReader(root, timerBridgeType);
              for (int index = 0; index < root.getRowCount(); index++) {
                var row = reader.read(index);
                long end = local(row.getLong(0));
                var key = new GenericRowData(keyType.getFieldCount());
                for (int i = 0; i < key.getArity(); i++)
                  key.setField(i, timerGetters[1 + i].getFieldOrNull(row));
                activeTimers()
                    .add(new TimerHeapInternalTimer<>(deadline(end), keyRows.project(key), end));
              }
            } catch (Exception failure) {
              throw new IllegalStateException("Cannot export shared window timers", failure);
            }
          });
    } else Native.exportTumblingAggregatorCanonical(handle, receiver);
    for (var view : distinctViews) view.exportState(backend, keyType, handle, this::local);
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    configureNativeState(handle, persistent);
    StockStateInventory.validate(
        backend,
        stateDescriptors().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        new StockStateInventory.State(entry.getValue(), LongSerializer.INSTANCE))),
        timerSchema(),
        keys);
    bindTimers(backend);
    var keyRows = new RowDataSerializer(keyType);
    Set<WindowKey> scheduled = new HashSet<>();
    try (var timers =
        StockTimerSnapshot.<RowData, Long>iterator(backend, processingTime ? PROCESSING : EVENTS)) {
      while (timers.hasNext()) {
        var timer = timers.next();
        if (timer.getTimestamp() != deadline(timer.getNamespace())
            || !live(timer.getNamespace())
            || !scheduled.add(
                new WindowKey(keyRows.toBinaryRow(timer.getKey()).copy(), timer.getNamespace())))
          throw new IllegalStateException("Unexpected stock window timer");
      }
    }
    Set<BinaryRowData> scheduledKeys =
        scheduled.stream().map(timer -> timer.key).collect(Collectors.toSet());
    try (var entries = backend.<Long>getKeysAndNamespaces(STATE)) {
      var iterator = entries.iterator();
      while (iterator.hasNext()) {
        var entry = iterator.next();
        var key = keyRows.toBinaryRow(entry.f0).copy();
        boolean future = live(entry.f1);
        if (shared && future && !scheduledKeys.contains(key))
          throw new IllegalStateException("Stock shared window key has no pending timer");
        if (!shared && !scheduled.remove(new WindowKey(key, entry.f1)) && future)
          throw new IllegalStateException("Stock window accumulator has no matching timer");
      }
    }
    if (!shared && !scheduled.isEmpty())
      throw new IllegalStateException("Stock window timer has no accumulator");
    var keyGetters = getters(keyType);
    var accGetters =
        getters(
            RowType.of(
                accumulators
                    .getChildren()
                    .subList(0, scalarFields)
                    .toArray(new org.apache.flink.table.types.logical.LogicalType[0])));
    var descriptor = descriptor();
    var batch = new ArrayList<RowData>(CHUNK_ROWS);
    try (var entries = backend.<Long>getKeysAndNamespaces(STATE)) {
      var iterator = entries.iterator();
      while (iterator.hasNext()) {
        var entry = iterator.next();
        var key = keyRows.toBinaryRow(entry.f0).copy();
        long end = entry.f1;
        backend.setCurrentKey(key);
        var value = backend.getPartitionedState(end, LongSerializer.INSTANCE, descriptor).value();
        if (value == null) throw new IllegalStateException("Stock window has no accumulator");
        // Released global combination can retain expired unshared bytes without a timer.
        // A past shared slice can still contribute when another record schedules a later window.
        if (!shared && !live(end)) continue;
        var row = new GenericRowData(bridgeType.getFieldCount());
        row.setField(0, nativeBound(windowStart(end)));
        row.setField(1, nativeBound(end));
        for (int i = 0; i < keyGetters.length; i++)
          row.setField(2 + i, keyGetters[i].getFieldOrNull(key));
        for (int i = 0; i < accGetters.length; i++)
          row.setField(2 + keyGetters.length + i, accGetters[i].getFieldOrNull(value));
        batch.add(row);
        if (batch.size() == CHUNK_ROWS) {
          importBatch(batch, handle);
          batch.clear();
        }
      }
    }
    if (!batch.isEmpty()) importBatch(batch, handle);
    if (shared) {
      batch.clear();
      for (WindowKey timer : scheduled) {
        var row = new GenericRowData(timerBridgeType.getFieldCount());
        row.setField(0, nativeBound(timer.end));
        for (int i = 0; i < keyGetters.length; i++)
          row.setField(1 + i, keyGetters[i].getFieldOrNull(timer.key));
        batch.add(row);
        if (batch.size() == CHUNK_ROWS) {
          importTimers(batch, handle);
          batch.clear();
        }
      }
      if (!batch.isEmpty()) importTimers(batch, handle);
    }
    if (!processingTime) Native.restoreTumblingCanonicalWatermark(handle, restoredWatermark());
    for (var view : distinctViews)
      view.importState(
          backend, keyType, handle, descriptor, this::nativeBound, end -> shared || live(end));
    if (!distinctViews.isEmpty()) Native.finishWindowCanonicalDistinct(handle);
    clear(backend);
  }

  private void importBatch(List<RowData> rows, long handle) {
    try (VectorSchemaRoot root =
            RowDataArrowConverter.write(rows, bridgeType, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (shared)
        Native.importSlicingWindowCanonical(
            handle, keyType.getFieldCount(), false, array.memoryAddress(), schema.memoryAddress());
      else
        Native.importTumblingAggregatorCanonical(
            handle, keyType.getFieldCount(), array.memoryAddress(), schema.memoryAddress());
    }
  }

  private void importTimers(List<RowData> rows, long handle) {
    try (var root = RowDataArrowConverter.write(rows, timerBridgeType, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      Native.importSlicingWindowCanonical(
          handle, keyType.getFieldCount(), true, array.memoryAddress(), schema.memoryAddress());
    }
  }

  private long local(long bound) {
    return zone.getRules().isFixedOffset()
        ? TimeWindowUtil.toUtcTimestampMills(bound, zone)
        : bound;
  }

  private long nativeBound(long local) {
    return zone.getRules().isFixedOffset() ? TimeWindowUtil.toEpochMills(local, zone) : local;
  }

  private long deadline(long end) {
    return TimeWindowUtil.toEpochMillsForTimer(Math.subtractExact(end, 1), zone);
  }

  private static RowData.FieldGetter[] getters(RowType type) {
    var getters = new RowData.FieldGetter[type.getFieldCount()];
    for (int i = 0; i < getters.length; i++)
      getters[i] = RowData.createFieldGetter(type.getTypeAt(i), i);
    return getters;
  }

  private static final class WindowKey {
    final BinaryRowData key;
    final long end;

    WindowKey(BinaryRowData key, long end) {
      this.key = key;
      this.end = end;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof WindowKey
          && end == ((WindowKey) other).end
          && key.equals(((WindowKey) other).key);
    }

    @Override
    public int hashCode() {
      return 31 * key.hashCode() + Long.hashCode(end);
    }
  }
}
