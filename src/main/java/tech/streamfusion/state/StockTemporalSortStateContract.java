package tech.streamfusion.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released rowtime-only temporal sort: full pending rows, empty key and actual emission cutoff. */
public final class StockTemporalSortStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final String ROWS = "dataState", LAST = "lastTriggeringTsState";
  private static final String EVENTS = "_timer_state/event_user-timers";
  private static final String PROCESSING = "_timer_state/processing_user-timers";
  private static final RowType EMPTY = RowType.of();
  private static final int TIMER_TIMES = 4;
  private final TypeSerializer<RowData> keys;
  private final RowType input;
  private final int timeColumn;
  private transient CheckpointableKeyedStateBackend<RowData> timerBackend;
  private transient KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, VoidNamespace>>
      timerQueue;

  public StockTemporalSortStateContract(
      TypeSerializer<RowData> keys, RowType input, int timeColumn) {
    if (timeColumn < 0 || timeColumn >= input.getFieldCount())
      throw new IllegalArgumentException("Temporal sort rowtime column missing");
    this.keys = keys.duplicate();
    this.input = input;
    this.timeColumn = timeColumn;
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(ROWS, StateDescriptor.Type.MAP, LAST, StateDescriptor.Type.VALUE);
  }

  @Override
  public Map<String, StockStateInventory.Timer> timerSchema() {
    return Map.of(
        EVENTS,
        StockStateInventory.Timer.consumed(VoidNamespaceSerializer.INSTANCE),
        PROCESSING,
        StockStateInventory.Timer.empty(VoidNamespaceSerializer.INSTANCE));
  }

  private static BinaryRowData emptyKey() {
    return new RowDataSerializer(EMPTY).toBinaryRow(new GenericRowData(0)).copy();
  }

  private static void checkKey(RowData key) {
    if (key.getArity() != 0 || key.getRowKind() != RowKind.INSERT)
      throw new IllegalArgumentException("Temporal sort requires the stock empty key");
  }

  private void own(CheckpointableKeyedStateBackend<RowData> backend) {
    if (!backend.getKeyGroupRange().contains(0))
      throw new IllegalArgumentException("Temporal sort does not own singleton key group zero");
    backend.setCurrentKey(emptyKey());
  }

  private MapState<Long, List<RowData>> rows(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new MapStateDescriptor<>(
            ROWS, LongSerializer.INSTANCE, new ListSerializer<>(new RowDataSerializer(input))));
  }

  private ValueState<Long> last(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new ValueStateDescriptor<>(LAST, LongSerializer.INSTANCE));
  }

  private KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, VoidNamespace>> events(
      CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    if (backend != timerBackend) {
      timerQueue =
          backend.create(
              EVENTS, new TimerSerializer<>(keys.duplicate(), VoidNamespaceSerializer.INSTANCE));
      timerBackend = backend;
    }
    return timerQueue;
  }

  private long time(RowData row) {
    if (row.isNullAt(timeColumn)) throw new IllegalArgumentException("NULL temporal sort rowtime");
    var type = input.getTypeAt(timeColumn);
    if (type instanceof TimestampType)
      return row.getTimestamp(timeColumn, ((TimestampType) type).getPrecision()).getMillisecond();
    if (type instanceof LocalZonedTimestampType)
      return row.getTimestamp(timeColumn, ((LocalZonedTimestampType) type).getPrecision())
          .getMillisecond();
    return row.getLong(timeColumn);
  }

  @Override
  public void clearProjectionTimers(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    own(backend);
    events(backend);
    var queue = events(backend);
    while (queue.poll() != null) {}
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    clear(backend);
    try (var lists =
        new StockMapListSpool<Long, RowData>(
            keys, LongSerializer.INSTANCE, new RowDataSerializer(input))) {
      var state = rows(backend);
      var timers = events(backend);
      Native.exportTemporalSortCanonicalRows(
          handle,
          (a, s) -> {
            try (var array = ArrowArray.wrap(a);
                var schema = ArrowSchema.wrap(s);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              var reader = ArrowConversion.createArrowReader(root, input);
              for (int i = 0; i < root.getRowCount(); i++) {
                var row = reader.read(i);
                if (row.getRowKind() != RowKind.INSERT)
                  throw new IllegalArgumentException(
                      "Temporal sort pending rows must be INSERT records");
                long timestamp = time(row);
                lists.append(emptyKey(), timestamp, row);
                timers.add(
                    new TimerHeapInternalTimer<>(timestamp, emptyKey(), VoidNamespace.INSTANCE));
              }
            } catch (Exception error) {
              throw new IllegalStateException("Cannot project temporal sort state", error);
            }
          });
      lists.visit(
          backend,
          (owner, timestamp, values) -> {
            backend.setCurrentKey(owner);
            state.put(timestamp, values);
          });
      long[] cutoff = Native.temporalSortCanonicalCutoff(handle);
      if (cutoff.length == 1) last(backend).update(cutoff[0]);
      else if (cutoff.length != 0)
        throw new IllegalArgumentException("Invalid temporal sort cutoff");
    }
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    var inventory =
        Map.of(
            ROWS,
            new StockStateInventory.State(
                StateDescriptor.Type.MAP, VoidNamespaceSerializer.INSTANCE),
            LAST,
            new StockStateInventory.State(
                StateDescriptor.Type.VALUE, VoidNamespaceSerializer.INSTANCE));
    StockStateInventory.validate(backend, inventory, timerSchema(), keys);
    for (String name : stateDescriptors().keySet())
      try (var keys = backend.getKeys(name, VoidNamespace.INSTANCE)) {
        keys.forEach(StockTemporalSortStateContract::checkKey);
      }
    own(backend);
    Long cutoff = last(backend).value();
    try (var spool = new StockStateSpool(keys)) {
      var saved = new StockMapListSnapshot(spool, new RowDataSerializer(input), 1);
      saved.capture(backend, ROWS, keys);
      try (var owners = spool.owners()) {
        while (owners.hasNext()) checkKey(owners.next());
      }
      var owner = emptyKey();
      try (var timestamps = saved.timestamps(owner)) {
        while (timestamps.hasNext()) {
          long timestamp = timestamps.next();
          if (saved.size(owner, timestamp) <= 0 || (cutoff != null && timestamp <= cutoff))
            throw new IllegalArgumentException("Temporal sort bucket is empty or already emitted");
          for (var row : saved.rows(owner, timestamp))
            if (row == null || row.getRowKind() != RowKind.INSERT || time(row) != timestamp)
              throw new IllegalArgumentException("Temporal sort row/time/kind changed");
        }
      }
      try (var iterator = StockTimerSnapshot.<RowData, VoidNamespace>iterator(backend, EVENTS)) {
        while (iterator.hasNext()) {
          var timer = iterator.next();
          checkKey(timer.getKey());
          if (timer.getNamespace() != VoidNamespace.INSTANCE
              || !spool.add(TIMER_TIMES, owner, timer.getTimestamp()))
            throw new IllegalArgumentException("Temporal sort timer identity changed");
        }
      }
      try (var timestamps = saved.sortedTimestamps(owner);
          var timers = spool.times(TIMER_TIMES, owner)) {
        while (timestamps.hasNext() && timers.hasNext()) {
          if (!timestamps.next().equals(timers.next()))
            throw new IllegalArgumentException(
                "Temporal sort pending timers disagree with input buckets");
        }
        if (timestamps.hasNext() || timers.hasNext())
          throw new IllegalArgumentException(
              "Temporal sort pending timers disagree with input buckets");
      }
      if (Native.temporalSortCanonicalCutoff(handle).length != 0)
        throw new IllegalStateException("Temporal sort import requires an empty target");
      Native.exportTemporalSortCanonicalRows(
          handle,
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var schema = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              if (root.getRowCount() != 0)
                throw new IllegalStateException("Temporal sort import requires an empty target");
            }
          });
      Native.importTemporalSortCanonicalCutoff(
          handle, cutoff == null ? new long[0] : new long[] {cutoff});
      var chunk = new ArrayList<RowData>(1024);
      long bytes = 0;
      var serializer = new RowDataSerializer(input);
      try (var timestamps = saved.sortedTimestamps(owner)) {
        while (timestamps.hasNext())
          for (var row : saved.rows(owner, timestamps.next())) {
            long size = serializer.toBinaryRow(row).getSizeInBytes();
            if (!chunk.isEmpty() && (chunk.size() == 1024 || bytes + size > 4L * 1024 * 1024)) {
              send(handle, chunk);
              chunk.clear();
              bytes = 0;
            }
            chunk.add(row);
            bytes += size;
          }
      }
      if (!chunk.isEmpty()) send(handle, chunk);
    }
    clear(backend);
  }

  private void send(long handle, List<RowData> rows) {
    try (VectorSchemaRoot root = RowDataArrowConverter.write(rows, input, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      Native.importTemporalSortCanonicalRows(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }
}
