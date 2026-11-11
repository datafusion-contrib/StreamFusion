package tech.streamfusion.state;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;

/** Released window rank/dedup retained rows, event timers and UNION watermark. */
public final class StockWindowRankStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final String EVENTS = "_timer_state/event_window-timers";
  private static final String PROCESSING = "_timer_state/processing_window-timers";

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

  private RowData retainedValue(CheckpointableKeyedStateBackend<RowData> backend, long end)
      throws Exception {
    var row = backend.getPartitionedState(end, LongSerializer.INSTANCE, valueDescriptor()).value();
    if (row == null) throw new IllegalStateException("Stock window-deduplication state has no row");
    return row;
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
    try (var staged =
        deduplicate
            ? null
            : new StockMapListSpool<RowData, RowData>(
                new RowDataSerializer(RowType.of(keyType, new BigIntType(false))),
                sortKeys,
                rows)) {
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
                  staged.append(GenericRowData.of(key, end), sort, row);
                }
                events.add(new TimerHeapInternalTimer<>(deadline(end), key, end));
              }
            } catch (Exception failure) {
              throw new IllegalStateException("Cannot export stock window rank", failure);
            }
          });
      if (staged != null)
        staged.visit(
            backend,
            (owner, sort, retained) -> {
              backend.setCurrentKey(owner.getRow(0, keyType.getFieldCount()));
              backend
                  .getPartitionedState(owner.getLong(1), LongSerializer.INSTANCE, descriptor())
                  .put(sort, retained);
            });
    }
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
    var keyProjection = new StockKeyProjection(keyType);
    var sortProjection = deduplicate ? null : new StockKeyProjection(sortType);
    var sortRows = deduplicate ? null : new RowDataSerializer(sortType);
    try (var inventory = new StockStateSpool(keyRows)) {
      long scheduled = 0, observed = 0;
      try (var iterator = StockTimerSnapshot.<RowData, Long>iterator(backend, EVENTS)) {
        while (iterator.hasNext()) {
          var timer = iterator.next();
          var key = keyRows.toBinaryRow(timer.getKey()).copy();
          inventory.addOwner(key);
          if (timer.getTimestamp() != deadline(timer.getNamespace())
              || timer.getTimestamp() <= restoredWatermark()
              || !inventory.add(1, key, timer.getNamespace()))
            throw new IllegalStateException("Unexpected stock window-rank timer");
          scheduled++;
        }
      }
      try (var entries = backend.<Long>getKeysAndNamespaces(stateName())) {
        var iterator = entries.iterator();
        while (iterator.hasNext()) {
          var entry = iterator.next();
          var key = keyRows.toBinaryRow(entry.f0).copy();
          if (!inventory.contains(1, key, entry.f1) || !inventory.add(2, key, entry.f1))
            throw new IllegalStateException("Stock window-rank state has no matching timer");
          observed++;
          if (deduplicate) {
            backend.setCurrentKey(key);
            var row = retainedValue(backend, entry.f1);
            validate(row, entry.f1);
            if (!key.equals(keyProjection.project(project(row, keyColumns))))
              throw new IllegalStateException("Stock window-rank row disagrees with its key");
          }
        }
      }
      if (observed != scheduled)
        throw new IllegalStateException("Stock window-rank timer has no state");
      if (!deduplicate) {
        StockMapListSnapshot.visit(
            backend,
            stateName(),
            keys,
            LongSerializer.INSTANCE,
            sortKeys,
            new RowDataSerializer(rowType),
            (owner, end, sort, encoded) -> {
              var key = keyRows.toBinaryRow(owner).copy();
              if (!inventory.contains(2, key, end))
                throw new IllegalStateException("Stock window-rank state has no matching timer");
              if (StockMapListSnapshot.size(encoded) <= 0)
                throw new IllegalStateException("Empty stock window-rank list");
              for (var row : StockMapListSnapshot.decode(encoded, new RowDataSerializer(rowType))) {
                validate(row, end);
                if (!key.equals(keyProjection.project(project(row, keyColumns)))
                    || !sortRows
                        .toBinaryRow(sort)
                        .equals(sortProjection.project(project(row, sortColumns))))
                  throw new IllegalStateException("Stock window-rank row disagrees with its key");
              }
              inventory.add(3, key, end);
            });
        try (var owners = inventory.owners()) {
          while (owners.hasNext()) {
            var key = owners.next();
            try (var windows = inventory.times(2, key)) {
              while (windows.hasNext())
                if (!inventory.contains(3, key, windows.next()))
                  throw new IllegalStateException("Stock window-rank timer has no rows");
            }
          }
        }
      }
    }
    var batch = new StockRowBatch(rowType, (rows, a, s) -> importBatch(rows, handle, a, s));
    if (deduplicate) {
      try (var entries = backend.<Long>getKeysAndNamespaces(stateName())) {
        var iterator = entries.iterator();
        while (iterator.hasNext()) {
          var entry = iterator.next();
          backend.setCurrentKey(entry.f0);
          batch.append(retainedValue(backend, entry.f1));
        }
      }
    } else {
      StockMapListSnapshot.visit(
          backend,
          stateName(),
          keys,
          LongSerializer.INSTANCE,
          sortKeys,
          new RowDataSerializer(rowType),
          (owner, end, sort, encoded) -> {
            for (var row : StockMapListSnapshot.decode(encoded, new RowDataSerializer(rowType)))
              batch.append(row);
          });
    }
    batch.flush();
    clear(backend);
  }

  private void importBatch(List<RowData> rows, long handle, long array, long schema) {
    Native.importWindowRankerCanonical(handle, array, schema);
  }
}
