package tech.streamfusion.state;

import java.util.*;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.*;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released temporal caches, independent probe cursors, cleanup clocks and physical callbacks. */
public final class StockTemporalJoinStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final String EVENTS = "_timer_state/event_timers";
  private static final String PROCESSING = "_timer_state/processing_timers";
  private static final int CHUNK = 1024;
  private static final String[] METADATA_FIELDS = {
    "__temporal_metadata_kind", "__temporal_metadata_value"
  };
  private static final int EVENT_CALLBACKS = 1, PROCESSING_CALLBACKS = 2;
  private final TypeSerializer<RowData> keys;
  private final RowType keyType;
  private final RowType[] inputs, transfers;
  private final int[][] columns;
  private final int[] times, precisions;
  private final boolean cleaning;
  private transient CheckpointableKeyedStateBackend<RowData> timerBackend;
  private transient KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, VoidNamespace>>
      events,
      processing;

  public StockTemporalJoinStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType left,
      RowType right,
      int[] leftKeys,
      int[] rightKeys,
      int leftTime,
      int rightTime,
      long retention) {
    this.keys = Objects.requireNonNull(keys).duplicate();
    this.keyType = keyType;
    inputs = new RowType[] {left, right};
    columns = new int[][] {leftKeys.clone(), rightKeys.clone()};
    times = new int[] {leftTime, rightTime};
    cleaning = retention > 1;
    precisions = new int[keyType.getFieldCount()];
    for (int field = 0; field < keyType.getFieldCount(); field++) {
      var type = keyType.getTypeAt(field);
      if (type.is(LogicalTypeFamily.CONSTRUCTED))
        throw new IllegalArgumentException("Temporal key needs a verified scalar layout");
      precisions[field] =
          type.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                  || type.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
              ? LogicalTypeChecks.getPrecision(type)
              : -1;
    }
    transfers = new RowType[2];
    for (int side = 0; side < 2; side++) {
      if (columns[side].length != keyType.getFieldCount()
          || times[side] < 0
          || times[side] >= inputs[side].getFieldCount())
        throw new IllegalArgumentException("Invalid temporal state layout");
      var time = inputs[side].getTypeAt(times[side]);
      if (!(time.is(LogicalTypeRoot.BIGINT)
          || (time.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                  || time.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE))
              && LogicalTypeChecks.getPrecision(time) <= 3))
        throw new IllegalArgumentException(
            "Temporal row time must have a compact stock representation");
      for (int field = 0; field < columns[side].length; field++) {
        int index = columns[side][field];
        if (index < 0
            || index >= inputs[side].getFieldCount()
            || !inputs[side]
                .getTypeAt(index)
                .copy(true)
                .equals(keyType.getTypeAt(field).copy(true)))
          throw new IllegalArgumentException("Temporal key type changed");
      }
      var fields = new ArrayList<>(inputs[side].getFields());
      add(fields, "__canonical_index__", new BigIntType(false));
      add(fields, "__canonical_kind__", new TinyIntType(false));
      transfers[side] = new RowType(fields);
    }
  }

  private static void add(List<RowType.RowField> fields, String name, LogicalType type) {
    var names = new HashSet<String>();
    for (var field : fields) names.add(field.getName());
    while (names.contains(name)) name += "_";
    fields.add(new RowType.RowField(name, type));
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    var result = new LinkedHashMap<String, StateDescriptor.Type>();
    result.put("left", StateDescriptor.Type.MAP);
    result.put("right", StateDescriptor.Type.MAP);
    result.put("next-index", StateDescriptor.Type.VALUE);
    result.put("timer", StateDescriptor.Type.VALUE);
    if (cleaning) result.put("cleanup-timestamp", StateDescriptor.Type.VALUE);
    return result;
  }

  @Override
  public Map<String, StockStateInventory.Timer> timerSchema() {
    return Map.of(
        EVENTS,
        StockStateInventory.Timer.consumed(VoidNamespaceSerializer.INSTANCE),
        PROCESSING,
        cleaning
            ? StockStateInventory.Timer.consumed(VoidNamespaceSerializer.INSTANCE)
            : StockStateInventory.Timer.empty(VoidNamespaceSerializer.INSTANCE));
  }

  @Override
  public void configureNativeState(long handle, boolean persistent) {
    Native.bindTemporalJoinCanonicalKeys(handle, precisions);
  }

  private MapState<Long, RowData> cache(CheckpointableKeyedStateBackend<RowData> backend, int side)
      throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new MapStateDescriptor<>(
            side == 0 ? "left" : "right",
            LongSerializer.INSTANCE,
            new RowDataSerializer(inputs[side])));
  }

  private ValueState<Long> value(CheckpointableKeyedStateBackend<RowData> backend, String name)
      throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new ValueStateDescriptor<>(name, LongSerializer.INSTANCE));
  }

  private void queues(CheckpointableKeyedStateBackend<RowData> backend) {
    if (timerBackend == backend) return;
    events =
        backend.create(
            EVENTS, new TimerSerializer<>(keys.duplicate(), VoidNamespaceSerializer.INSTANCE));
    processing =
        backend.create(
            PROCESSING, new TimerSerializer<>(keys.duplicate(), VoidNamespaceSerializer.INSTANCE));
    timerBackend = backend;
  }

  private BinaryRowData key(RowData row, StockKeyProjection projection) {
    var binary = new RowDataSerializer(keyType).toBinaryRow(row).copy();
    if (binary.getRowKind() != RowKind.INSERT || !binary.equals(projection.project(binary)))
      throw new IllegalStateException("Temporal key encoding changed");
    return binary;
  }

  private BinaryRowData projected(RowData row, int side, StockKeyProjection projection) {
    var selected = new GenericRowData(columns[side].length);
    for (int field = 0; field < columns[side].length; field++) {
      int index = columns[side][field];
      selected.setField(
          field,
          RowData.createFieldGetter(inputs[side].getTypeAt(index).copy(true), index)
              .getFieldOrNull(row));
    }
    return projection.project(selected);
  }

  private long millis(RowData row, int side) {
    int column = times[side];
    if (row.isNullAt(column)) throw new IllegalStateException("NULL temporal row time");
    return inputs[side].getTypeAt(column).is(LogicalTypeRoot.BIGINT)
        ? row.getLong(column)
        : row.getTimestamp(column, 3).getMillisecond();
  }

  private void callbacks(
      CheckpointableKeyedStateBackend<RowData> backend,
      String queue,
      StockKeyProjection projection,
      StockStateSpool retained,
      int section)
      throws Exception {
    try (var iterator = StockTimerSnapshot.<RowData, VoidNamespace>iterator(backend, queue)) {
      while (iterator.hasNext()) {
        var timer = iterator.next();
        var key = key(timer.getKey(), projection);
        retained.addOwner(key);
        if (!retained.add(section, key, timer.getTimestamp()))
          throw new IllegalStateException("Duplicate temporal callback");
      }
    }
  }

  private void preflight(CheckpointableKeyedStateBackend<RowData> backend, StockStateSpool retained)
      throws Exception {
    var expected = new HashMap<String, StockStateInventory.State>();
    stateDescriptors()
        .forEach(
            (name, kind) ->
                expected.put(
                    name, new StockStateInventory.State(kind, VoidNamespaceSerializer.INSTANCE)));
    StockStateInventory.validate(backend, expected, timerSchema(), keys);
    queues(backend);
    var projection = new StockKeyProjection(keyType);
    callbacks(backend, EVENTS, projection, retained, EVENT_CALLBACKS);
    callbacks(backend, PROCESSING, projection, retained, PROCESSING_CALLBACKS);
    for (String name : stateDescriptors().keySet()) {
      try (var stream = backend.getKeys(name, VoidNamespace.INSTANCE)) {
        var iterator = stream.iterator();
        while (iterator.hasNext()) retained.addOwner(key(iterator.next(), projection));
      }
    }
    var left = cache(backend, 0);
    var right = cache(backend, 1);
    var cursor = value(backend, "next-index");
    var registered = value(backend, "timer");
    ValueState<Long> cleanup = cleaning ? value(backend, "cleanup-timestamp") : null;
    try (var owners = retained.owners()) {
      while (owners.hasNext()) {
        var key = owners.next();
        backend.setCurrentKey(key);
        cursor.value();
        Long timer = registered.value();
        if (timer != null && !retained.contains(EVENT_CALLBACKS, key, timer))
          throw new IllegalStateException("Registered temporal value has no physical callback");
        Long deadline = cleaning ? cleanup.value() : null;
        try (var scheduled = retained.times(PROCESSING_CALLBACKS, key)) {
          if (deadline == null
              ? scheduled.hasNext()
              : !scheduled.hasNext()
                  || scheduled.next().longValue() != deadline
                  || scheduled.hasNext())
            throw new IllegalStateException(
                "Temporal cleanup value and physical callback disagree");
        }
        for (int side = 0; side < 2; side++) {
          for (var entry : (side == 0 ? left : right).entries()) {
            var row = entry.getValue();
            if (entry.getKey() == null
                || row == null
                || !projected(row, side, projection).equals(key)
                || side == 1 && entry.getKey() != millis(row, side))
              throw new IllegalStateException(
                  "Temporal row disagrees with its state key or build timestamp");
            millis(row, side);
          }
        }
      }
    }
  }

  @Override
  public void clearProjectionTimers(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    queues(backend);
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
      var values = cache(backend, side);
      Native.exportTemporalJoinCanonicalRows(
          handle,
          side == 0,
          (a, s) -> {
            try (var array = ArrowArray.wrap(a);
                var schema = ArrowSchema.wrap(s);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              var reader = ArrowConversion.createArrowReader(root, transfers[input]);
              for (int index = 0; index < root.getRowCount(); index++) {
                var row = reader.read(index);
                int arity = inputs[input].getFieldCount();
                var payload = new GenericRowData(arity);
                for (int field = 0; field < arity; field++)
                  payload.setField(
                      field,
                      RowData.createFieldGetter(inputs[input].getTypeAt(field).copy(true), field)
                          .getFieldOrNull(row));
                payload.setRowKind(RowKind.fromByteValue(row.getByte(arity + 1)));
                backend.setCurrentKey(projected(payload, input, projection));
                long mapKey = row.getLong(arity);
                if (values.contains(mapKey))
                  throw new IllegalStateException("Duplicate native temporal row map key");
                values.put(mapKey, serializer.toBinaryRow(payload).copy());
              }
            } catch (Exception failure) {
              throw new IllegalStateException("Cannot export temporal cache", failure);
            }
          });
    }
    StockKeyedLongRecords.export(
        backend,
        handle,
        keyType,
        METADATA_FIELDS,
        Native::exportTemporalJoinCanonicalRecords,
        (key, kind, number) -> {
          switch (kind) {
            case 0:
              if (number != 0) throw new IllegalArgumentException("Invalid temporal owner marker");
              break;
            case 1:
              value(backend, "next-index").update(number);
              break;
            case 2:
              value(backend, "timer").update(number);
              break;
            case 3:
              if (!cleaning)
                throw new IllegalStateException("Native cleanup clock with disabled retention");
              value(backend, "cleanup-timestamp").update(number);
              processing.add(
                  new TimerHeapInternalTimer<>(number, key.copy(), VoidNamespace.INSTANCE));
              break;
            case 4:
              events.add(new TimerHeapInternalTimer<>(number, key.copy(), VoidNamespace.INSTANCE));
              break;
            default:
              throw new IllegalArgumentException("Unknown temporal metadata kind");
          }
        });
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    try (var retained = new StockStateSpool(new RowDataSerializer(keyType))) {
      preflight(backend, retained);
      for (boolean validating : new boolean[] {true, false}) {
        var metadata =
            new StockKeyedLongRecords.Writer(
                handle,
                keyType,
                METADATA_FIELDS,
                validating,
                Native::importTemporalJoinCanonicalRecords);
        metadata.begin();
        for (int side = 0; side < 2; side++) {
          var rows = new ArrayList<RowData>(CHUNK);
          long bytes = 0;
          var values = cache(backend, side);
          var serializer = new RowDataSerializer(inputs[side]);
          try (var owners = retained.owners()) {
            while (owners.hasNext()) {
              backend.setCurrentKey(owners.next());
              for (var entry : values.entries()) {
                var payload = serializer.copy(entry.getValue());
                long size = serializer.toBinaryRow(payload).getSizeInBytes() + 32L;
                if (!rows.isEmpty() && (rows.size() == CHUNK || bytes + size > 4L * 1024 * 1024)) {
                  send(handle, side, rows, validating);
                  rows.clear();
                  bytes = 0;
                }
                int arity = inputs[side].getFieldCount();
                var row = new GenericRowData(arity + 2);
                for (int field = 0; field < arity; field++)
                  row.setField(
                      field,
                      RowData.createFieldGetter(inputs[side].getTypeAt(field).copy(true), field)
                          .getFieldOrNull(payload));
                row.setField(arity, entry.getKey());
                row.setField(arity + 1, payload.getRowKind().toByteValue());
                rows.add(row);
                bytes += size;
              }
            }
          }
          if (!rows.isEmpty()) send(handle, side, rows, validating);
        }
        try (var owners = retained.owners()) {
          while (owners.hasNext()) {
            var key = owners.next();
            backend.setCurrentKey(key);
            Long cursor = value(backend, "next-index").value(),
                timer = value(backend, "timer").value(),
                cleanup = cleaning ? value(backend, "cleanup-timestamp").value() : null;
            if (cursor == null
                && timer == null
                && cleanup == null
                && !retained.hasAny(EVENT_CALLBACKS, key)) continue;
            metadata.append(key, (byte) 0, 0);
            if (cursor != null) metadata.append(key, (byte) 1, cursor);
            if (timer != null) metadata.append(key, (byte) 2, timer);
            if (cleanup != null) metadata.append(key, (byte) 3, cleanup);
            try (var callbacks = retained.times(EVENT_CALLBACKS, key)) {
              while (callbacks.hasNext()) metadata.append(key, (byte) 4, callbacks.next());
            }
          }
        }
        metadata.finish();
      }
    }
  }

  private void send(long handle, int side, List<RowData> rows, boolean validating)
      throws Exception {
    try (var root = RowDataArrowConverter.write(rows, transfers[side], NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (validating)
        Native.validateTemporalJoinCanonicalRows(
            handle, side == 0, array.memoryAddress(), schema.memoryAddress());
      else
        Native.importTemporalJoinCanonicalRows(
            handle, side == 0, array.memoryAddress(), schema.memoryAddress());
    }
  }
}
