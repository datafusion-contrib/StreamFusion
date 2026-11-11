package tech.streamfusion.state;

import java.time.ZoneId;
import java.util.*;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
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
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;

/** Released event-time window join lists and per-key/window firing timers. */
public final class StockWindowJoinStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;

  public static final class WindowSpec implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    private final long size, offset;
    private final boolean cumulative;

    private WindowSpec(long size, long offset, boolean cumulative) {
      if (size <= 0) throw new IllegalArgumentException("Window size must be positive");
      this.size = size;
      this.offset = offset;
      this.cumulative = cumulative;
    }

    public static WindowSpec fixed(long size) {
      return new WindowSpec(size, 0, false);
    }

    public static WindowSpec cumulative(long maxSize, long offset) {
      return new WindowSpec(maxSize, offset, true);
    }

    long start(long end) {
      return cumulative
          ? org.apache.flink.table.runtime.operators.window.TimeWindow.getWindowStartWithOffset(
              Math.subtractExact(end, 1), offset, size)
          : Math.subtractExact(end, size);
    }
  }

  private static final String EVENTS = "_timer_state/event_window-timers";
  private static final String PROCESSING = "_timer_state/processing_window-timers";
  private final TypeSerializer<RowData> keys;
  private final RowType keyType;
  private final RowType[] inputs;
  private final int[][] columns;
  private final int[] starts, ends;
  private final ZoneId zone;
  private final WindowSpec windowStart;
  private transient CheckpointableKeyedStateBackend<RowData> timerBackend;
  private transient KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, Long>> events;
  private transient KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, Long>>
      processing;

  public StockWindowJoinStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType left,
      RowType right,
      int[] leftKeys,
      int[] rightKeys,
      int leftStart,
      int leftEnd,
      int rightStart,
      int rightEnd,
      String zone,
      WindowSpec windowStart) {
    this.keys = keys.duplicate();
    this.keyType = keyType;
    inputs = new RowType[] {left, right};
    columns = new int[][] {leftKeys.clone(), rightKeys.clone()};
    starts = new int[] {leftStart, rightStart};
    ends = new int[] {leftEnd, rightEnd};
    this.zone = ZoneId.of(zone);
    this.windowStart = Objects.requireNonNull(windowStart);
    for (int side = 0; side < 2; side++) {
      if (columns[side].length != keyType.getFieldCount()
          || starts[side] < 0
          || ends[side] < 0
          || starts[side] >= inputs[side].getFieldCount()
          || ends[side] >= inputs[side].getFieldCount())
        throw new IllegalArgumentException("Invalid stock window-join layout");
      for (int i = 0; i < columns[side].length; i++)
        if (!inputs[side]
            .getTypeAt(columns[side][i])
            .copy(true)
            .equals(keyType.getTypeAt(i).copy(true)))
          throw new IllegalArgumentException("Window-join key type changed");
    }
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  private String name(int side) {
    return side == 0 ? "left-records" : "right-records";
  }

  private ListStateDescriptor<RowData> descriptor(int side) {
    return new ListStateDescriptor<>(name(side), new RowDataSerializer(inputs[side]));
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(name(0), StateDescriptor.Type.LIST, name(1), StateDescriptor.Type.LIST);
  }

  @Override
  public TypeSerializer<?> namespaceSerializer(String state) {
    if (!stateDescriptors().containsKey(state))
      throw new IllegalArgumentException("Unknown stock window-join state: " + state);
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

  private void timers(CheckpointableKeyedStateBackend<RowData> backend) {
    if (backend == timerBackend) return;
    events =
        backend.create(EVENTS, new TimerSerializer<>(keys.duplicate(), LongSerializer.INSTANCE));
    processing =
        backend.create(
            PROCESSING, new TimerSerializer<>(keys.duplicate(), LongSerializer.INSTANCE));
    timerBackend = backend;
  }

  private long deadline(long end) {
    return TimeWindowUtil.toEpochMillsForTimer(Math.subtractExact(end, 1), zone);
  }

  private long millis(RowData row, int side, int column) {
    if (row.isNullAt(column)) throw new IllegalStateException("NULL stock window-join bound");
    return inputs[side].getTypeAt(column).is(LogicalTypeRoot.BIGINT)
        ? row.getLong(column)
        : row.getTimestamp(column, 3).getMillisecond();
  }

  private BinaryRowData projectedKey(RowData row, int side, StockKeyProjection projection) {
    var selected = new GenericRowData(columns[side].length);
    for (int i = 0; i < columns[side].length; i++) {
      int column = columns[side][i];
      selected.setField(
          i, RowData.createFieldGetter(inputs[side].getTypeAt(column), column).getFieldOrNull(row));
    }
    return projection.project(selected);
  }

  private void validate(RowData row, int side, long end) {
    if (row.getRowKind() != RowKind.INSERT
        || millis(row, side, ends[side]) != end
        || millis(row, side, starts[side]) != windowStart.start(end))
      throw new IllegalStateException("Stock window-join row disagrees with its namespace");
  }

  private void preflight(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    var states = new HashMap<String, StockStateInventory.State>();
    stateDescriptors()
        .forEach(
            (name, type) ->
                states.put(name, new StockStateInventory.State(type, LongSerializer.INSTANCE)));
    StockStateInventory.validate(backend, states, timerSchema(), keys);
    timers(backend);
    var serializer = new RowDataSerializer(keyType);
    var projection = new StockKeyProjection(keyType);
    try (var inventory = new StockStateSpool(serializer)) {
      long scheduled = 0, observed = 0;
      try (var iterator = StockTimerSnapshot.<RowData, Long>iterator(backend, EVENTS)) {
        while (iterator.hasNext()) {
          var timer = iterator.next();
          var key = serializer.toBinaryRow(timer.getKey()).copy();
          if (timer.getTimestamp() != deadline(timer.getNamespace())
              || !inventory.add(1, key, timer.getNamespace()))
            throw new IllegalStateException("Unexpected stock window-join timer");
          scheduled++;
        }
      }
      for (int side = 0; side < 2; side++) {
        try (var stream = backend.<Long>getKeysAndNamespaces(name(side))) {
          var iterator = stream.iterator();
          while (iterator.hasNext()) {
            var entry = iterator.next();
            var key = serializer.toBinaryRow(entry.f0).copy();
            if (!inventory.contains(1, key, entry.f1))
              throw new IllegalStateException("Stock window-join state has no firing timer");
            backend.setCurrentKey(key);
            var rows =
                backend
                    .getPartitionedState(entry.f1, LongSerializer.INSTANCE, descriptor(side))
                    .get();
            boolean found = false;
            if (rows != null)
              for (RowData row : rows) {
                validate(row, side, entry.f1);
                if (!key.equals(projectedKey(row, side, projection)))
                  throw new IllegalStateException("Stock window-join row disagrees with its key");
                found = true;
              }
            if (!found) throw new IllegalStateException("Stock window-join namespace has no rows");
            if (inventory.add(2, key, entry.f1)) observed++;
          }
        }
      }
      if (observed != scheduled)
        throw new IllegalStateException("Stock window-join firing timer has no state");
    }
  }

  @Override
  public void clearProjectionTimers(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    timers(backend);
    while (events.poll() != null) {}
    while (processing.poll() != null) {}
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    clear(backend);
    var projection = new StockKeyProjection(keyType);
    for (int side = 0; side < 2; side++) {
      final int input = side;
      var serializer = new RowDataSerializer(inputs[side]);
      Native.exportWindowJoinCanonicalRows(
          handle,
          side == 0,
          (a, s) -> {
            try (var array = ArrowArray.wrap(a);
                var schema = ArrowSchema.wrap(s);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              var reader = ArrowConversion.createArrowReader(root, inputs[input]);
              for (int row = 0; row < root.getRowCount(); row++) {
                var value = reader.read(row);
                long end = millis(value, input, ends[input]);
                validate(value, input, end);
                var key = projectedKey(value, input, projection);
                backend.setCurrentKey(key);
                backend
                    .getPartitionedState(end, LongSerializer.INSTANCE, descriptor(input))
                    .add(serializer.copy(value));
                events.add(new TimerHeapInternalTimer<>(deadline(end), key, end));
              }
            } catch (Exception error) {
              throw new IllegalStateException("Cannot export stock window join", error);
            }
          });
    }
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    preflight(backend);
    for (int side = 0; side < 2; side++) {
      final int input = side;
      var batch = new StockRowBatch(inputs[side], (rows, a, s) -> send(handle, input, rows, a, s));
      try (var stream = backend.<Long>getKeysAndNamespaces(name(side))) {
        var iterator = stream.iterator();
        while (iterator.hasNext()) {
          var entry = iterator.next();
          backend.setCurrentKey(entry.f0);
          for (RowData row :
              backend
                  .getPartitionedState(entry.f1, LongSerializer.INSTANCE, descriptor(side))
                  .get()) batch.append(row);
        }
      }
      batch.flush();
    }
    clear(backend);
  }

  private void send(long handle, int side, List<RowData> rows, long array, long schema) {
    Native.importWindowJoinCanonicalRows(handle, side == 0, array, schema);
  }
}
