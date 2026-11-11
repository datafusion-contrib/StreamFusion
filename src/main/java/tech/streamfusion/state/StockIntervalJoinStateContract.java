package tech.streamfusion.state;

import java.util.*;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;

/**
 * Released interval caches, original payloads, outer-match flags and independent cleanup clocks.
 */
public final class StockIntervalJoinStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final String EVENTS = "_timer_state/event_user-timers";
  private static final String PROCESSING = "_timer_state/processing_user-timers";
  private static final RowType TIMER_TRANSFER =
      RowType.of(new VarBinaryType(VarBinaryType.MAX_LENGTH), new BigIntType());
  private final boolean processingTime;
  private final TypeSerializer<RowData> keys;
  private final RowType keyType;
  private final RowType[] inputs, transfers;
  private final int[][] columns;
  private final int[] times, keyDescriptor;
  private transient CheckpointableKeyedStateBackend<RowData> timerBackend;
  private transient KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, VoidNamespace>>
      events,
      processing;

  public StockIntervalJoinStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType left,
      RowType right,
      int[] leftKeys,
      int[] rightKeys,
      int leftTime,
      int rightTime) {
    this(keys, keyType, left, right, leftKeys, rightKeys, leftTime, rightTime, false);
  }

  public StockIntervalJoinStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType left,
      RowType right,
      int[] leftKeys,
      int[] rightKeys,
      int leftTime,
      int rightTime,
      boolean processingTime) {
    this.processingTime = processingTime;
    this.keys = Objects.requireNonNull(keys).duplicate();
    this.keyType = keyType;
    inputs = new RowType[] {left, right};
    columns = new int[][] {leftKeys.clone(), rightKeys.clone()};
    times = new int[] {leftTime, rightTime};
    transfers = new RowType[2];
    keyDescriptor = new int[keyType.getFieldCount() + 1];
    keyDescriptor[0] = Native.STOCK_KEY_PROJECTION;
    for (int i = 0; i < keyType.getFieldCount(); i++) {
      var type = keyType.getTypeAt(i);
      if (type.is(LogicalTypeFamily.CONSTRUCTED))
        throw new IllegalArgumentException("Interval key must have a verified scalar layout");
      keyDescriptor[i + 1] =
          type.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                  || type.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
              ? LogicalTypeChecks.getPrecision(type)
              : -1;
    }
    for (int side = 0; side < 2; side++) {
      if (columns[side].length != keyType.getFieldCount()
          || !processingTime && (times[side] < 0 || times[side] >= inputs[side].getFieldCount()))
        throw new IllegalArgumentException("Invalid interval-join state layout");
      var time = processingTime ? new BigIntType() : inputs[side].getTypeAt(times[side]);
      if (!(time.is(LogicalTypeRoot.BIGINT)
          || (time.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                  || time.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE))
              && LogicalTypeChecks.getPrecision(time) <= 3))
        throw new IllegalArgumentException("Stock event-time interval timestamps must be compact");
      for (int i = 0; i < columns[side].length; i++) {
        int column = columns[side][i];
        if (column < 0
            || column >= inputs[side].getFieldCount()
            || !inputs[side].getTypeAt(column).copy(true).equals(keyType.getTypeAt(i).copy(true)))
          throw new IllegalArgumentException("Interval-join key type changed");
      }
      var fields = new ArrayList<RowType.RowField>();
      for (var field : inputs[side].getFields()) {
        var physical = field.getType();
        if (processingTime && LogicalTypeChecks.isProctimeAttribute(physical))
          physical = physical.copy(true);
        fields.add(new RowType.RowField(field.getName(), physical));
      }
      if (processingTime)
        fields.add(new RowType.RowField("__canonical_clock__", new BigIntType(false)));
      fields.add(new RowType.RowField("__canonical_matched__", new BooleanType(false)));
      transfers[side] = new RowType(fields);
    }
  }

  public boolean processingTime() {
    return processingTime;
  }

  private int matchIndex(int side) {
    return inputs[side].getFieldCount() + (processingTime ? 1 : 0);
  }

  private KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, VoidNamespace>>
      callbacks() {
    return processingTime ? processing : events;
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  private String cacheName(int side) {
    return side == 0 ? "IntervalJoinLeftCache" : "IntervalJoinRightCache";
  }

  private String timerName(int side) {
    // Flink names a cleanup state after the opposite input that advances its expiration.
    return side == 0 ? "IntervalJoinRightTimerState" : "IntervalJoinLeftTimerState";
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private TypeSerializer<Tuple2<RowData, Boolean>> tuples(int side) {
    return new TupleSerializer<Tuple2<RowData, Boolean>>(
        (Class) Tuple2.class,
        new TypeSerializer<?>[] {new RowDataSerializer(inputs[side]), BooleanSerializer.INSTANCE});
  }

  private MapStateDescriptor<Long, List<Tuple2<RowData, Boolean>>> descriptor(int side) {
    return new MapStateDescriptor<>(
        cacheName(side), LongSerializer.INSTANCE, new ListSerializer<>(tuples(side)));
  }

  private MapState<Long, List<Tuple2<RowData, Boolean>>> cache(
      CheckpointableKeyedStateBackend<RowData> backend, int side) throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor(side));
  }

  private ValueState<Long> timer(CheckpointableKeyedStateBackend<RowData> backend, int side)
      throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new ValueStateDescriptor<>(timerName(side), LongSerializer.INSTANCE));
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(
        cacheName(0),
        StateDescriptor.Type.MAP,
        cacheName(1),
        StateDescriptor.Type.MAP,
        timerName(0),
        StateDescriptor.Type.VALUE,
        timerName(1),
        StateDescriptor.Type.VALUE);
  }

  @Override
  public Map<String, StockStateInventory.Timer> timerSchema() {
    return Map.of(
        processingTime ? PROCESSING : EVENTS,
        StockStateInventory.Timer.consumed(VoidNamespaceSerializer.INSTANCE),
        processingTime ? EVENTS : PROCESSING,
        StockStateInventory.Timer.empty(VoidNamespaceSerializer.INSTANCE));
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

  @Override
  public void configureNativeState(long handle, boolean persistent) {
    if (processingTime)
      Native.bindIntervalJoinCanonicalClock(
          handle, inputs[0].getFieldCount(), inputs[1].getFieldCount());
    Native.bindIntervalJoinCanonicalKeys(handle, keyDescriptor);
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

  private long millis(RowData row, int side) {
    int column = times[side];
    if (row.isNullAt(column)) throw new IllegalStateException("NULL stock interval timestamp");
    return inputs[side].getTypeAt(column).is(LogicalTypeRoot.BIGINT)
        ? row.getLong(column)
        : row.getTimestamp(column, 3).getMillisecond();
  }

  private BinaryRowData binaryKey(RowData key, StockKeyProjection projection) {
    var binary = new RowDataSerializer(keyType).toBinaryRow(key).copy();
    if (binary.getRowKind() != RowKind.INSERT || !binary.equals(projection.project(binary)))
      throw new IllegalStateException("Stock interval key encoding is not canonical");
    return binary;
  }

  private void preflight(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    var states = new HashMap<String, StockStateInventory.State>();
    stateDescriptors()
        .forEach(
            (name, kind) ->
                states.put(
                    name, new StockStateInventory.State(kind, VoidNamespaceSerializer.INSTANCE)));
    StockStateInventory.validate(backend, states, timerSchema(), keys);
    queues(backend);
    var projection = new StockKeyProjection(keyType);
    try (var inventory = new StockStateSpool(new RowDataSerializer(keyType))) {
      long scheduled = 0;
      try (var iterator =
          StockTimerSnapshot.<RowData, VoidNamespace>iterator(
              backend, processingTime ? PROCESSING : EVENTS)) {
        while (iterator.hasNext()) {
          var event = iterator.next();
          var key = binaryKey(event.getKey(), projection);
          inventory.addOwner(key);
          if (event.getNamespace() != VoidNamespace.INSTANCE
              || !inventory.add(1, key, event.getTimestamp()))
            throw new IllegalStateException("Duplicate or invalid stock interval cleanup callback");
          scheduled++;
        }
      }
      for (int side = 0; side < 2; side++) {
        // Register both exact value serializers before making any native changes.
        cache(backend, side);
        var deadline = timer(backend, side);
        try (var stream = backend.getKeys(timerName(side), VoidNamespace.INSTANCE)) {
          var iterator = stream.iterator();
          while (iterator.hasNext()) {
            var key = binaryKey(iterator.next(), projection);
            backend.setCurrentKey(key);
            Long time = deadline.value();
            if (time == null || !inventory.contains(1, key, time))
              throw new IllegalStateException(
                  "Stock interval deadline has no cleanup callback: deadline="
                      + time
                      + ", callbacks="
                      + scheduled
                      + ", keyHash="
                      + key.hashCode());
            inventory.add(2, key, time);
          }
        }
        final int input = side;
        StockMapListSnapshot.visit(
            backend,
            cacheName(side),
            keys,
            VoidNamespaceSerializer.INSTANCE,
            LongSerializer.INSTANCE,
            tuples(side),
            (owner, namespace, time, encoded) -> {
              var key = binaryKey(owner, projection);
              backend.setCurrentKey(key);
              if (deadline.value() == null)
                throw new IllegalStateException("Stock interval cache has no cleanup deadline");
              if (StockMapListSnapshot.size(encoded) <= 0)
                throw new IllegalStateException("Empty stock interval timestamp bucket");
              for (var pair : StockMapListSnapshot.decode(encoded, tuples(input))) {
                if (pair == null
                    || pair.f0 == null
                    || pair.f1 == null
                    || pair.f0.getRowKind() != RowKind.INSERT
                    || !processingTime && millis(pair.f0, input) != time
                    || !key.equals(projectedKey(pair.f0, input, projection)))
                  throw new IllegalStateException(
                      "Stock interval row disagrees with its cache key or timestamp");
              }
              inventory.add(3 + input, key, 0);
            });
        try (var stream = backend.getKeys(cacheName(side), VoidNamespace.INSTANCE)) {
          var iterator = stream.iterator();
          while (iterator.hasNext())
            if (!inventory.contains(3 + side, binaryKey(iterator.next(), projection), 0))
              throw new IllegalStateException("Empty stock interval cache");
        }
      }
      try (var owners = inventory.owners()) {
        while (owners.hasNext()) {
          var owner = owners.next();
          try (var times = inventory.times(1, owner)) {
            while (times.hasNext())
              if (!inventory.contains(2, owner, times.next()))
                throw new IllegalStateException(
                    "Stock interval callback has no owned cleanup deadline");
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
      try (var staged =
          new StockMapListSpool<Long, Tuple2<RowData, Boolean>>(
              keys, LongSerializer.INSTANCE, tuples(side))) {
        Native.exportIntervalJoinCanonicalRows(
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
                  var payload = new GenericRowData(inputs[input].getFieldCount());
                  for (int field = 0; field < payload.getArity(); field++)
                    payload.setField(
                        field,
                        RowData.createFieldGetter(inputs[input].getTypeAt(field).copy(true), field)
                            .getFieldOrNull(row));
                  if (row.isNullAt(matchIndex(input)))
                    throw new IllegalStateException("NULL native match flag");
                  long time =
                      processingTime ? row.getLong(payload.getArity()) : millis(payload, input);
                  staged.append(
                      projectedKey(payload, input, projection),
                      time,
                      Tuple2.of(
                          serializer.toBinaryRow(payload).copy(),
                          row.getBoolean(matchIndex(input))));
                }
              } catch (Exception failure) {
                throw new IllegalStateException("Cannot export stock interval cache", failure);
              }
            });
        staged.visit(
            backend,
            (owner, time, retained) -> {
              backend.setCurrentKey(owner);
              values.put(time, retained);
            });
      }
      var deadline = timer(backend, side);
      Native.exportIntervalJoinCanonicalTimers(
          handle,
          side == 0,
          (a, s) -> {
            try (var array = ArrowArray.wrap(a);
                var schema = ArrowSchema.wrap(s);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              var reader = ArrowConversion.createArrowReader(root, TIMER_TRANSFER);
              for (int index = 0; index < root.getRowCount(); index++) {
                var row = reader.read(index);
                byte[] bytes = row.getBinary(0);
                var key = new BinaryRowData(keyType.getFieldCount());
                key.pointTo(MemorySegmentFactory.wrap(bytes), 0, bytes.length);
                key = binaryKey(key, projection);
                backend.setCurrentKey(key);
                long time = row.getLong(1);
                if (deadline.value() != null)
                  throw new IllegalStateException("Duplicate native interval deadline");
                deadline.update(time);
                callbacks().add(new TimerHeapInternalTimer<>(time, key, VoidNamespace.INSTANCE));
              }
            } catch (Exception failure) {
              throw new IllegalStateException("Cannot export stock interval timers", failure);
            }
          });
    }
    preflight(backend);
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    preflight(backend);
    for (int side = 0; side < 2; side++) {
      final int input = side;
      var batch =
          new StockRowBatch(
              transfers[side], (rows, a, s) -> send(handle, input, rows, false, a, s));
      StockMapListSnapshot.visit(
          backend,
          cacheName(side),
          keys,
          VoidNamespaceSerializer.INSTANCE,
          LongSerializer.INSTANCE,
          tuples(side),
          (owner, namespace, time, encoded) -> {
            for (var pair : StockMapListSnapshot.decode(encoded, tuples(input))) {
              var row = new GenericRowData(transfers[input].getFieldCount());
              for (int field = 0; field < inputs[input].getFieldCount(); field++)
                row.setField(
                    field,
                    RowData.createFieldGetter(inputs[input].getTypeAt(field).copy(true), field)
                        .getFieldOrNull(pair.f0));
              if (processingTime) row.setField(inputs[input].getFieldCount(), time);
              row.setField(matchIndex(input), pair.f1);
              batch.append(row);
            }
          });
      batch.flush();
      var timers =
          new StockRowBatch(TIMER_TRANSFER, (rows, a, s) -> send(handle, input, rows, true, a, s));
      var deadline = timer(backend, side);
      var serializer = new RowDataSerializer(keyType);
      try (var stream = backend.getKeys(timerName(side), VoidNamespace.INSTANCE)) {
        var iterator = stream.iterator();
        while (iterator.hasNext()) {
          var key = serializer.toBinaryRow(iterator.next());
          backend.setCurrentKey(key);
          byte[] bytes =
              BinarySegmentUtils.copyToBytes(
                  key.getSegments(), key.getOffset(), key.getSizeInBytes());
          timers.append(GenericRowData.of(bytes, deadline.value()));
        }
      }
      timers.flush();
    }
    clear(backend);
  }

  private void send(
      long handle, int side, List<RowData> rows, boolean timers, long array, long schema) {
    if (timers) Native.importIntervalJoinCanonicalTimers(handle, side == 0, array, schema);
    else Native.importIntervalJoinCanonicalRows(handle, side == 0, array, schema);
  }
}
