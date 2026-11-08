package tech.streamfusion.state;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released window rank/dedup retained rows, event timers and UNION watermark. */
public final class StockWindowRankStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final String EVENTS = "_timer_state/event_window-timers";
  private static final String PROCESSING = "_timer_state/processing_window-timers";
  private static final int CHUNK_ROWS = 1024;

  @FunctionalInterface
  public interface WindowStart extends java.io.Serializable {
    long start(long end);
  }

  private final TypeSerializer<RowData> keys;
  private final RowType keyType;
  private final RowType rowType;
  private final RowType sortType;
  private final TypeSerializer<RowData> sortKeys;
  private final int[] keyColumns;
  private final int[] sortColumns;
  private final int startColumn;
  private final int endColumn;
  private final ZoneId zone;
  private final StockWindowRankStateContract.WindowStart windowStart;
  private final boolean deduplicate;
  private transient StockWindowWatermark watermark;
  private transient CheckpointableKeyedStateBackend<RowData> timerBackend;
  private transient KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, Long>> events;
  private transient KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, Long>>
      processing;

  public StockWindowRankStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      TypeSerializer<RowData> sortKeys,
      RowType sortType,
      RowType rowType,
      int[] keyColumns,
      int[] sortColumns,
      int startColumn,
      int endColumn,
      String zone,
      StockWindowRankStateContract.WindowStart windowStart) {
    this(
        keys,
        keyType,
        sortKeys,
        sortType,
        rowType,
        keyColumns,
        sortColumns,
        startColumn,
        endColumn,
        zone,
        windowStart,
        false);
  }

  public static StockWindowRankStateContract deduplicate(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType rowType,
      int[] keyColumns,
      int orderColumn,
      int startColumn,
      int endColumn,
      String zone,
      StockWindowRankStateContract.WindowStart windowStart) {
    return new StockWindowRankStateContract(
        keys,
        keyType,
        null,
        null,
        rowType,
        keyColumns,
        new int[] {orderColumn},
        startColumn,
        endColumn,
        zone,
        windowStart,
        true);
  }

  private StockWindowRankStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      TypeSerializer<RowData> sortKeys,
      RowType sortType,
      RowType rowType,
      int[] keyColumns,
      int[] sortColumns,
      int startColumn,
      int endColumn,
      String zone,
      StockWindowRankStateContract.WindowStart windowStart,
      boolean deduplicate) {
    this.keys = keys.duplicate();
    this.keyType = keyType;
    this.sortKeys = sortKeys == null ? null : sortKeys.duplicate();
    this.sortType = sortType;
    this.rowType = rowType;
    this.keyColumns = keyColumns.clone();
    this.sortColumns = sortColumns.clone();
    this.startColumn = startColumn;
    this.endColumn = endColumn;
    this.zone = ZoneId.of(zone);
    this.windowStart = Objects.requireNonNull(windowStart);
    this.deduplicate = deduplicate;
    if (keyColumns.length != keyType.getFieldCount()
        || (!deduplicate && sortColumns.length != sortType.getFieldCount())
        || startColumn < 0
        || endColumn < 0
        || startColumn >= rowType.getFieldCount()
        || endColumn >= rowType.getFieldCount())
      throw new IllegalArgumentException("Invalid stock window-rank layout");
  }

  private String stateName() {
    return deduplicate ? "window_deduplicate" : "window_rank";
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(stateName(), deduplicate ? StateDescriptor.Type.VALUE : StateDescriptor.Type.MAP);
  }

  @Override
  public TypeSerializer<?> namespaceSerializer(String state) {
    if (!state.equals(stateName()))
      throw new IllegalArgumentException("Unknown window rank state: " + state);
    return LongSerializer.INSTANCE;
  }

  @Override
  public Map<String, StockStateInventory.Timer> timerSchema() {
    return Map.of(
        EVENTS,
        StockStateInventory.Timer.consumed(LongSerializer.INSTANCE),
        PROCESSING,
        StockStateInventory.Timer.empty(LongSerializer.INSTANCE));
  }

  @Override
  public void initializeOperatorState(OperatorStateStore store, boolean restored) throws Exception {
    watermark = new StockWindowWatermark(store, restored);
  }

  @Override
  public void snapshotOperatorState(long value) throws Exception {
    watermark.snapshot(value);
  }

  @Override
  public long restoredWatermark() {
    return watermark.restoredWatermark();
  }

  @Override
  public void configureNativeState(long handle, boolean persistent) {
    long value = restoredWatermark();
    long offset =
        zone.getRules().isFixedOffset()
            ? zone.getRules().getOffset(java.time.Instant.EPOCH).getTotalSeconds() * 1000L
            : 0;
    if (value != Long.MIN_VALUE && value != Long.MAX_VALUE) {
      try {
        value = Math.addExact(value, offset + 1);
      } catch (ArithmeticException overflow) {
        value = offset + 1 > 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
      }
    }
    Native.restoreWindowRankerCanonicalFrontier(handle, value);
  }

  private MapStateDescriptor<RowData, List<RowData>> descriptor() {
    return new MapStateDescriptor<>(
        stateName(), sortKeys.duplicate(), new ListSerializer<>(new RowDataSerializer(rowType)));
  }

  private ValueStateDescriptor<RowData> valueDescriptor() {
    return new ValueStateDescriptor<>(stateName(), new RowDataSerializer(rowType));
  }

  @FunctionalInterface
  private interface RetainedRow {
    void accept(RowData sortKey, RowData row) throws Exception;
  }

  private void readRows(
      CheckpointableKeyedStateBackend<RowData> backend, long end, RetainedRow receiver)
      throws Exception {
    if (deduplicate) {
      var row =
          backend.getPartitionedState(end, LongSerializer.INSTANCE, valueDescriptor()).value();
      if (row == null)
        throw new IllegalStateException("Stock window-deduplication state has no row");
      receiver.accept(null, row);
      return;
    }
    boolean found = false;
    for (var group :
        backend.getPartitionedState(end, LongSerializer.INSTANCE, descriptor()).entries()) {
      if (group.getValue() == null || group.getValue().isEmpty())
        throw new IllegalStateException("Empty stock window-rank list");
      for (var row : group.getValue()) {
        receiver.accept(group.getKey(), row);
        found = true;
      }
    }
    if (!found) throw new IllegalStateException("Stock window-rank timer has no rows");
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

  private long millis(RowData row, int column) {
    if (row.isNullAt(column)) throw new IllegalStateException("NULL window-rank bound");
    return rowType.getTypeAt(column).is(org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT)
        ? row.getLong(column)
        : row.getTimestamp(column, 3).getMillisecond();
  }

  private long deadline(long end) {
    return TimeWindowUtil.toEpochMillsForTimer(Math.subtractExact(end, 1), zone);
  }

  private RowData project(RowData row, int[] columns) {
    var key = new GenericRowData(columns.length);
    for (int i = 0; i < columns.length; i++)
      key.setField(
          i,
          RowData.createFieldGetter(rowType.getTypeAt(columns[i]), columns[i]).getFieldOrNull(row));
    return key;
  }

  private void validate(RowData row, long end) {
    if (row.getRowKind() != RowKind.INSERT
        || (deduplicate && row.isNullAt(sortColumns[0]))
        || millis(row, endColumn) != end
        || millis(row, startColumn) != windowStart.start(end))
      throw new IllegalStateException("Stock window-rank row disagrees with its namespace");
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
    var keyProjection = new StockKeyProjection(keyType);
    var sortProjection = deduplicate ? null : new StockKeyProjection(sortType);
    var rows = new RowDataSerializer(rowType);
    Native.exportWindowRankerCanonical(
        handle,
        (arrayAddress, schemaAddress) -> {
          try (var array = ArrowArray.wrap(arrayAddress);
              var schema = ArrowSchema.wrap(schemaAddress);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            var reader = ArrowConversion.createArrowReader(root, rowType);
            for (int i = 0; i < root.getRowCount(); i++) {
              var row = reader.read(i);
              long end = millis(row, endColumn);
              validate(row, end);
              var key = keyProjection.project(project(row, keyColumns));
              backend.setCurrentKey(key);
              if (deduplicate) {
                var state =
                    backend.getPartitionedState(end, LongSerializer.INSTANCE, valueDescriptor());
                if (state.value() != null)
                  throw new IllegalStateException(
                      "Multiple retained rows for stock window deduplication");
                state.update(rows.copy(row));
              } else {
                var sort = sortProjection.project(project(row, sortColumns));
                var state = backend.getPartitionedState(end, LongSerializer.INSTANCE, descriptor());
                var previous = state.get(sort);
                var list = previous == null ? new ArrayList<RowData>() : new ArrayList<>(previous);
                list.add(rows.copy(row));
                state.put(sort, list);
              }
              events.add(new TimerHeapInternalTimer<>(deadline(end), key, end));
            }
          } catch (Exception failure) {
            throw new IllegalStateException("Cannot export stock window rank", failure);
          }
        });
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    configureNativeState(handle, persistent);
    StockStateInventory.validate(
        backend,
        Map.of(
            stateName(),
            new StockStateInventory.State(
                stateDescriptors().get(stateName()), LongSerializer.INSTANCE)),
        timerSchema(),
        keys);
    bindTimers(backend);
    var keyRows = new RowDataSerializer(keyType);
    Set<WindowKey> scheduled = new HashSet<>();
    try (var iterator = StockTimerSnapshot.<RowData, Long>iterator(backend, EVENTS)) {
      while (iterator.hasNext()) {
        var timer = iterator.next();
        if (timer.getTimestamp() != deadline(timer.getNamespace())
            || timer.getTimestamp() <= restoredWatermark()
            || !scheduled.add(
                new WindowKey(keyRows.toBinaryRow(timer.getKey()).copy(), timer.getNamespace())))
          throw new IllegalStateException("Unexpected stock window-rank timer");
      }
    }
    var keyProjection = new StockKeyProjection(keyType);
    var sortProjection = deduplicate ? null : new StockKeyProjection(sortType);
    var sortRows = deduplicate ? null : new RowDataSerializer(sortType);
    try (var entries = backend.<Long>getKeysAndNamespaces(stateName())) {
      var iterator = entries.iterator();
      while (iterator.hasNext()) {
        var entry = iterator.next();
        var key = keyRows.toBinaryRow(entry.f0).copy();
        if (!scheduled.remove(new WindowKey(key, entry.f1)))
          throw new IllegalStateException("Stock window-rank state has no matching timer");
        backend.setCurrentKey(key);
        readRows(
            backend,
            entry.f1,
            (sortKey, row) -> {
              validate(row, entry.f1);
              if (!key.equals(keyProjection.project(project(row, keyColumns)))
                  || (!deduplicate
                      && !sortRows
                          .toBinaryRow(sortKey)
                          .equals(sortProjection.project(project(row, sortColumns)))))
                throw new IllegalStateException("Stock window-rank row disagrees with its key");
            });
      }
    }
    if (!scheduled.isEmpty())
      throw new IllegalStateException("Stock window-rank timer has no state");
    var batch = new ArrayList<RowData>(CHUNK_ROWS);
    var serializer = new RowDataSerializer(rowType);
    try (var entries = backend.<Long>getKeysAndNamespaces(stateName())) {
      var iterator = entries.iterator();
      while (iterator.hasNext()) {
        var entry = iterator.next();
        backend.setCurrentKey(entry.f0);
        readRows(
            backend,
            entry.f1,
            (sortKey, row) -> {
              batch.add(serializer.copy(row));
              if (batch.size() == CHUNK_ROWS) {
                importBatch(batch, handle);
                batch.clear();
              }
            });
      }
    }
    if (!batch.isEmpty()) importBatch(batch, handle);
    clear(backend);
  }

  private void importBatch(List<RowData> rows, long handle) {
    try (var root = RowDataArrowConverter.write(rows, rowType, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      Native.importWindowRankerCanonical(handle, array.memoryAddress(), schema.memoryAddress());
    }
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
      return Objects.hash(key, end);
    }
  }
}
