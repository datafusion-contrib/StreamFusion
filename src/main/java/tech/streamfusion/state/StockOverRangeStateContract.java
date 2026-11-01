package tech.streamfusion.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released bounded event-time RANGE state, with raw generated accumulators and Void timers. */
public final class StockOverRangeStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final String ACC = "accState", INPUT = "inputState";
  private static final String CLEANUP = "cleanupTsState", LAST = "lastTriggeringTsState";
  private static final String EVENTS = "_timer_state/event_user-timers";
  private static final String PROCESSING = "_timer_state/processing_user-timers";
  private final TypeSerializer<RowData> keys;
  private final RowType keyType,
      inputType,
      accumulatorType,
      nativeAccumulatorType,
      nativeInputType,
      frameType,
      protocolType;
  private final int[] keyColumns;
  private final int timeColumn;
  private final int hiddenCount;
  private final int[] accumulatorFields;
  private final RowType constants;
  private final byte[] constantBytes;
  private transient RowData constantRow;
  private transient CheckpointableKeyedStateBackend<RowData> timerBackend;
  private transient KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, VoidNamespace>>
      events,
      processing;

  public StockOverRangeStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType inputType,
      RowType accumulatorType,
      int[] keyColumns,
      int timeColumn,
      int[] kinds,
      int[] widths,
      int hiddenCount,
      RowType constants,
      byte[] constantBytes) {
    this.keys = keys;
    this.keyType = keyType;
    this.inputType = inputType;
    this.accumulatorType = accumulatorType;
    this.hiddenCount = hiddenCount;
    this.keyColumns = keyColumns.clone();
    this.timeColumn = timeColumn;
    this.constants = constants;
    this.constantBytes = constantBytes == null ? null : constantBytes.clone();
    if (keyColumns.length != keyType.getFieldCount()
        || timeColumn < 0
        || timeColumn >= inputType.getFieldCount()
        || kinds.length == 0
        || kinds.length != widths.length)
      throw new IllegalArgumentException("Invalid bounded RANGE layout");
    int fields = 0;
    for (int i = 0; i < kinds.length; i++) {
      if (kinds[i] < 0
          || kinds[i] >= 100
          || !Set.of(0, 3, 4, 22).contains(kinds[i])
          || widths[i] != (kinds[i] == 0 || kinds[i] == 4 ? 2 : 1))
        throw new IllegalArgumentException("Unqualified bounded RANGE accumulator/view layout");
      fields += widths[i];
    }
    if (fields + (hiddenCount >= 0 ? 1 : 0) != accumulatorType.getFieldCount()
        || hiddenCount >= accumulatorType.getFieldCount()
        || (hiddenCount >= 0 && !accumulatorType.getTypeAt(hiddenCount).is(LogicalTypeRoot.BIGINT)))
      throw new IllegalArgumentException("Unowned RANGE accumulator fields");
    accumulatorFields =
        java.util.stream.IntStream.range(0, accumulatorType.getFieldCount())
            .filter(index -> index != hiddenCount)
            .toArray();
    nativeAccumulatorType =
        new RowType(
            java.util.Arrays.stream(accumulatorFields)
                .mapToObj(index -> accumulatorType.getFields().get(index))
                .collect(java.util.stream.Collectors.toList()));
    for (int i = 0; i < keyColumns.length; i++)
      if (keyColumns[i] < 0
          || keyColumns[i] >= inputType.getFieldCount()
          || !inputType.getTypeAt(keyColumns[i]).equals(keyType.getTypeAt(i)))
        throw new IllegalArgumentException("RANGE key layout changed");
    var nativeFields = new ArrayList<>(inputType.getFields());
    if (constants != null && constants.getFieldCount() > 0) {
      if (constantBytes == null || constantBytes.length == 0)
        throw new IllegalArgumentException("RANGE constants are missing");
      for (int i = 0; i < constants.getFieldCount(); i++)
        append(nativeFields, "__over_constant_" + i, constants.getTypeAt(i));
    }
    nativeInputType = new RowType(nativeFields);
    nativeFields.add(
        new RowType.RowField(RowDataArrowConverter.ROW_KIND_COLUMN, new TinyIntType()));
    for (String name : List.of("__over_frame_time", "__over_frame_clock", "__over_frame_position"))
      append(nativeFields, name, new BigIntType(false));
    for (int i = 0; i < fields; i++)
      append(nativeFields, "__over_frame_fold" + i, nativeAccumulatorType.getTypeAt(i).copy(true));
    append(nativeFields, "__over_frame_cleanup", new BigIntType());
    append(nativeFields, "__over_frame_last_trigger", new BigIntType());
    append(nativeFields, "__over_frame_timers", new ArrayType(new BigIntType()));
    append(nativeFields, "__over_frame_map_order", new ArrayType(new BigIntType()));
    frameType = new RowType(nativeFields);
    var protocol = new ArrayList<RowType.RowField>();
    for (int i = 0; i < keyType.getFieldCount(); i++)
      protocol.add(new RowType.RowField("key" + i, keyType.getTypeAt(i).copy(true)));
    protocol.add(new RowType.RowField("__over_range_cleanup", new BigIntType()));
    protocol.add(new RowType.RowField("__over_range_last", new BigIntType()));
    protocol.add(
        new RowType.RowField("__over_range_timers", new ArrayType(false, new BigIntType())));
    protocol.add(
        new RowType.RowField("__over_range_order", new ArrayType(false, new BigIntType())));
    protocolType = new RowType(protocol);
  }

  private static void append(List<RowType.RowField> fields, String base, LogicalType type) {
    String name = base;
    while (contains(fields, name)) name += "_";
    fields.add(new RowType.RowField(name, type));
  }

  private static boolean contains(List<RowType.RowField> fields, String name) {
    for (var field : fields) if (field.getName().equals(name)) return true;
    return false;
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(
        ACC,
        StateDescriptor.Type.VALUE,
        INPUT,
        StateDescriptor.Type.MAP,
        CLEANUP,
        StateDescriptor.Type.VALUE,
        LAST,
        StateDescriptor.Type.VALUE);
  }

  @Override
  public Map<String, StockStateInventory.Timer> timerSchema() {
    return Map.of(
        EVENTS,
        StockStateInventory.Timer.consumed(VoidNamespaceSerializer.INSTANCE),
        PROCESSING,
        StockStateInventory.Timer.empty(VoidNamespaceSerializer.INSTANCE));
  }

  @Override
  public void configureNativeState(long handle, boolean persistent) {
    Native.bindOverCanonicalFrameInputs(handle);
  }

  private void bind(CheckpointableKeyedStateBackend<RowData> backend) {
    if (backend == timerBackend) return;
    var serializer = new TimerSerializer<>(keys.duplicate(), VoidNamespaceSerializer.INSTANCE);
    events = backend.create(EVENTS, serializer);
    processing = backend.create(PROCESSING, serializer);
    timerBackend = backend;
  }

  private ValueState<RowData> accumulators(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new ValueStateDescriptor<>(ACC, new RowDataSerializer(accumulatorType)));
  }

  private ValueState<Long> scalar(CheckpointableKeyedStateBackend<RowData> backend, String name)
      throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new ValueStateDescriptor<>(name, LongSerializer.INSTANCE));
  }

  private MapState<Long, List<RowData>> inputs(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new MapStateDescriptor<>(
            INPUT,
            LongSerializer.INSTANCE,
            new ListSerializer<>(new RowDataSerializer(inputType))));
  }

  private List<BinaryRowData> stateKeys(
      CheckpointableKeyedStateBackend<RowData> backend, String name) throws Exception {
    var serializer = new RowDataSerializer(keyType);
    var result = new ArrayList<BinaryRowData>();
    try (var stream = backend.getKeys(name, VoidNamespace.INSTANCE)) {
      stream.forEach(key -> result.add(serializer.toBinaryRow(key).copy()));
    }
    return result;
  }

  private BinaryRowData key(RowData row) {
    var key = new GenericRowData(keyColumns.length);
    for (int i = 0; i < keyColumns.length; i++) key.setField(i, get(row, inputType, keyColumns[i]));
    return new RowDataSerializer(keyType).toBinaryRow(key).copy();
  }

  private static Object get(RowData row, RowType type, int index) {
    return RowData.createFieldGetter(type.getTypeAt(index), index).getFieldOrNull(row);
  }

  private long time(RowData row) {
    if (row.isNullAt(timeColumn)) throw new IllegalArgumentException("NULL RANGE rowtime");
    var type = inputType.getTypeAt(timeColumn);
    if (type instanceof TimestampType)
      return row.getTimestamp(timeColumn, ((TimestampType) type).getPrecision()).getMillisecond();
    if (type instanceof LocalZonedTimestampType)
      return row.getTimestamp(timeColumn, ((LocalZonedTimestampType) type).getPrecision())
          .getMillisecond();
    return row.getLong(timeColumn);
  }

  @Override
  public void clear(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    for (String name : List.of(ACC, INPUT, CLEANUP, LAST))
      for (var key : stateKeys(backend, name)) {
        backend.setCurrentKey(key);
        if (name.equals(ACC)) accumulators(backend).clear();
        else if (name.equals(INPUT)) inputs(backend).clear();
        else scalar(backend, name).clear();
      }
    bind(backend);
    while (events.poll() != null) {}
    while (processing.poll() != null) {}
  }

  @FunctionalInterface
  private interface Exporter {
    void run(long handle, Native.BatchReceiver receiver);
  }

  @FunctionalInterface
  private interface Importer {
    void run(long handle, long array, long schema);
  }

  @FunctionalInterface
  private interface RootConsumer {
    void accept(VectorSchemaRoot root) throws Exception;
  }

  private static void visit(Exporter exporter, long handle, RootConsumer accept) {
    exporter.run(
        handle,
        (a, s) -> {
          try (var array = ArrowArray.wrap(a);
              var schema = ArrowSchema.wrap(s);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            accept.accept(root);
          } catch (Exception error) {
            throw new IllegalStateException("Cannot project bounded RANGE state", error);
          }
        });
  }

  private static void send(
      Importer importer, long handle, RowType type, List<RowData> rows, boolean kinds) {
    if (rows.isEmpty()) return;
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED, kinds);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      importer.run(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }

  private static VectorSchemaRoot columns(VectorSchemaRoot root, int offset, int count) {
    var selected = new VectorSchemaRoot(root.getFieldVectors().subList(offset, offset + count));
    selected.setRowCount(root.getRowCount());
    return selected;
  }

  private void put(CheckpointableKeyedStateBackend<RowData> backend, RowData row) throws Exception {
    var key = key(row);
    long timestamp = time(row);
    backend.setCurrentKey(key);
    var state = inputs(backend);
    var peers = state.get(timestamp);
    peers = peers == null ? new ArrayList<>() : new ArrayList<>(peers);
    peers.add(new RowDataSerializer(inputType).copy(row));
    state.put(timestamp, peers);
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    clear(backend);
    var inputReaderType = inputType;
    visit(
        Native::exportOverCanonicalFrames,
        handle,
        root -> {
          int arity = root.getFieldVectors().size() - nativeAccumulatorType.getFieldCount() - 7;
          if (arity < nativeInputType.getFieldCount())
            throw new IllegalArgumentException("RANGE native input is incomplete");
          var expected = ArrowConversion.toArrowSchema(nativeAccumulatorType).getFields();
          for (int i = 0; i < expected.size(); i++)
            if (!expected
                .get(i)
                .getType()
                .equals(root.getSchema().getFields().get(arity + 3 + i).getType()))
              throw new IllegalArgumentException("RANGE raw accumulator type changed");
          var reader = ArrowConversion.createArrowReader(root, inputReaderType);
          BinaryRowData owner = null;
          for (int i = 0; i < root.getRowCount(); i++) {
            var row = reader.read(i);
            var key = key(row);
            if (owner != null && !owner.equals(key))
              throw new IllegalArgumentException("Mixed RANGE frame keys");
            owner = key;
            put(backend, row);
          }
          var raw =
              ArrowConversion.createArrowReader(
                  columns(root, arity + 3, nativeAccumulatorType.getFieldCount()),
                  nativeAccumulatorType);
          backend.setCurrentKey(owner);
          var saved = raw.read(root.getRowCount() - 1);
          var accumulator = new GenericRowData(accumulatorType.getFieldCount());
          for (int p = 0; p < accumulatorFields.length; p++)
            accumulator.setField(accumulatorFields[p], get(saved, nativeAccumulatorType, p));
          if (hiddenCount >= 0) accumulator.setField(hiddenCount, (long) root.getRowCount());
          accumulators(backend).update(new RowDataSerializer(accumulatorType).copy(accumulator));
        });
    visit(
        Native::exportOverPendingCanonical,
        handle,
        root -> {
          var reader = ArrowConversion.createArrowReader(root, inputType);
          for (int i = 0; i < root.getRowCount(); i++) put(backend, reader.read(i));
        });
    visit(
        Native::exportOverCanonicalRange,
        handle,
        root -> {
          var reader = ArrowConversion.createArrowReader(root, protocolType);
          for (int i = 0; i < root.getRowCount(); i++) {
            var row = reader.read(i);
            var keyRow = new GenericRowData(keyType.getFieldCount());
            for (int k = 0; k < keyType.getFieldCount(); k++)
              keyRow.setField(k, get(row, keyType, k));
            var owner = new RowDataSerializer(keyType).toBinaryRow(keyRow).copy();
            backend.setCurrentKey(owner);
            int k = keyType.getFieldCount();
            if (row.isNullAt(k)) scalar(backend, CLEANUP).clear();
            else scalar(backend, CLEANUP).update(row.getLong(k));
            if (row.isNullAt(k + 1)) scalar(backend, LAST).clear();
            else scalar(backend, LAST).update(row.getLong(k + 1));
            var order = row.getArray(k + 3);
            var state = inputs(backend);
            var saved = new LinkedHashMap<Long, List<RowData>>();
            for (int p = 0; p < order.size(); p++) {
              long timestamp = order.getLong(p);
              var peers = state.get(timestamp);
              if (peers == null || saved.put(timestamp, peers) != null)
                throw new IllegalArgumentException("RANGE map order has no input");
            }
            var timestamps = state.keys();
            if (timestamps != null)
              for (var timestamp : timestamps)
                if (!saved.containsKey(timestamp))
                  throw new IllegalArgumentException("RANGE input has no map order");
            state.clear();
            for (var entry : saved.entrySet()) state.put(entry.getKey(), entry.getValue());
            var timers = row.getArray(k + 2);
            for (int p = 0; p < timers.size(); p++)
              events.add(
                  new TimerHeapInternalTimer<>(timers.getLong(p), owner, VoidNamespace.INSTANCE));
          }
        });
  }

  private static final class Partition {
    final BinaryRowData key;
    final LinkedHashMap<Long, List<RowData>> rows = new LinkedHashMap<>();
    final TreeSet<Long> timers = new TreeSet<>();
    RowData accumulator;
    Long cleanup, last;

    Partition(BinaryRowData key) {
      this.key = key;
    }
  }

  private List<Partition> preflight(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    var inventory = new HashMap<String, StockStateInventory.State>();
    stateDescriptors()
        .forEach(
            (name, kind) ->
                inventory.put(
                    name, new StockStateInventory.State(kind, VoidNamespaceSerializer.INSTANCE)));
    StockStateInventory.validate(backend, inventory, timerSchema(), keys);
    bind(backend);
    var partitions = new LinkedHashMap<BinaryRowData, Partition>();
    for (String name : List.of(ACC, INPUT, CLEANUP, LAST))
      for (var key : stateKeys(backend, name)) partitions.computeIfAbsent(key, Partition::new);
    var keyRows = new RowDataSerializer(keyType);
    try (var iterator = events.iterator()) {
      while (iterator.hasNext()) {
        var timer = iterator.next();
        if (timer.getNamespace() != VoidNamespace.INSTANCE)
          throw new IllegalArgumentException("Unknown RANGE timer namespace");
        var key = keyRows.toBinaryRow(timer.getKey()).copy();
        if (!partitions.computeIfAbsent(key, Partition::new).timers.add(timer.getTimestamp()))
          throw new IllegalArgumentException("Duplicate RANGE timer");
      }
    }
    if (!processing.isEmpty())
      throw new IllegalArgumentException("RANGE has processing-time timers");
    var rowSerializer = new RowDataSerializer(inputType);
    var accSerializer = new RowDataSerializer(accumulatorType);
    for (var partition : partitions.values()) {
      backend.setCurrentKey(partition.key);
      partition.cleanup = scalar(backend, CLEANUP).value();
      partition.last = scalar(backend, LAST).value();
      var acc = accumulators(backend).value();
      partition.accumulator = acc == null ? null : accSerializer.copy(acc);
      var entries = inputs(backend).entries();
      if (entries != null)
        for (var entry : entries) {
          var timestamp = entry.getKey();
          var peers = entry.getValue();
          if (timestamp == null || peers == null || peers.isEmpty())
            throw new IllegalArgumentException("Empty RANGE timestamp bucket");
          var copied = new ArrayList<RowData>();
          for (var row : peers) {
            if (row.getRowKind() != RowKind.INSERT
                || !key(row).equals(partition.key)
                || time(row) != timestamp)
              throw new IllegalArgumentException("RANGE input disagrees with key/time/kind");
            copied.add(rowSerializer.copy(row));
          }
          partition.rows.put(timestamp, copied);
          if ((partition.last == null || timestamp > partition.last)
              && !partition.timers.contains(timestamp))
            throw new IllegalArgumentException("Pending RANGE input has no event timer");
        }
      if (partition.cleanup != null && !partition.timers.contains(partition.cleanup))
        throw new IllegalArgumentException("RANGE cleanup has no matching timer");
      if (partition.cleanup == null && (!partition.rows.isEmpty() || partition.accumulator != null))
        throw new IllegalArgumentException("RANGE input has no cleanup owner");
      boolean processed =
          partition.last != null
              && partition.rows.keySet().stream()
                  .anyMatch(timestamp -> timestamp <= partition.last);
      if (processed != (partition.accumulator != null))
        throw new IllegalArgumentException("RANGE accumulator/frame ownership changed");
      if (partition.accumulator != null && hiddenCount >= 0) {
        long count =
            partition.rows.entrySet().stream()
                .filter(entry -> entry.getKey() <= partition.last)
                .mapToLong(entry -> entry.getValue().size())
                .sum();
        if (partition.accumulator.isNullAt(hiddenCount)
            || partition.accumulator.getLong(hiddenCount) != count)
          throw new IllegalArgumentException(
              "RANGE hidden input count disagrees with retained rows");
      }
    }
    return new ArrayList<>(partitions.values());
  }

  private RowData nativeRow(RowData input) throws Exception {
    if (constants == null || constants.getFieldCount() == 0) return input;
    if (constantRow == null)
      constantRow =
          new RowDataSerializer(constants).deserialize(new DataInputDeserializer(constantBytes));
    var row = new GenericRowData(nativeInputType.getFieldCount());
    for (int i = 0; i < inputType.getFieldCount(); i++) row.setField(i, get(input, inputType, i));
    for (int i = 0; i < constants.getFieldCount(); i++)
      row.setField(inputType.getFieldCount() + i, get(constantRow, constants, i));
    return row;
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    var partitions = preflight(backend);
    for (var partition : partitions) {
      var processed = new ArrayList<RowData>();
      var pending = new ArrayList<RowData>();
      var ordered = new TreeMap<>(partition.rows);
      for (var entry : ordered.entrySet())
        for (var row : entry.getValue()) {
          var expanded = nativeRow(row);
          if (partition.last != null && entry.getKey() <= partition.last) processed.add(expanded);
          else pending.add(expanded);
        }
      if (!processed.isEmpty()) {
        var frames = new ArrayList<RowData>();
        int base = nativeInputType.getFieldCount();
        for (int i = 0; i < processed.size(); i++) {
          var input = processed.get(i);
          var row = new GenericRowData(frameType.getFieldCount());
          for (int p = 0; p < base; p++) row.setField(p, get(input, nativeInputType, p));
          row.setField(base, (byte) 0);
          long timestamp = time(input);
          row.setField(base + 1, timestamp);
          row.setField(base + 2, timestamp);
          row.setField(base + 3, (long) i);
          if (i + 1 == processed.size()) {
            for (int p = 0; p < accumulatorFields.length; p++)
              row.setField(
                  base + 4 + p, get(partition.accumulator, accumulatorType, accumulatorFields[p]));
            int p = base + 4 + accumulatorFields.length;
            row.setField(p, partition.cleanup);
            row.setField(p + 1, partition.last);
            row.setField(
                p + 2,
                new GenericArrayData(
                    partition.timers.stream().mapToLong(Long::longValue).toArray()));
            row.setField(
                p + 3,
                new GenericArrayData(
                    partition.rows.keySet().stream().mapToLong(Long::longValue).toArray()));
          }
          frames.add(row);
        }
        send(Native::importOverCanonicalFrames, handle, frameType, frames, false);
      } else {
        var row = new GenericRowData(protocolType.getFieldCount());
        int k = keyType.getFieldCount();
        for (int p = 0; p < k; p++) row.setField(p, get(partition.key, keyType, p));
        row.setField(k, partition.cleanup);
        row.setField(k + 1, partition.last);
        row.setField(
            k + 2,
            new GenericArrayData(partition.timers.stream().mapToLong(Long::longValue).toArray()));
        row.setField(
            k + 3,
            new GenericArrayData(
                partition.rows.keySet().stream().mapToLong(Long::longValue).toArray()));
        send(Native::importOverCanonicalRange, handle, protocolType, List.of(row), false);
      }
      send(Native::importOverPendingCanonical, handle, nativeInputType, pending, true);
    }
    clear(backend);
  }
}
