package tech.streamfusion.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

/** Shared raw arithmetic and retained-input projection for released bounded OVER operators. */
abstract class StockBoundedOverStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final int CHUNK_ROWS = 1024, CHUNK_BYTES = 4 * 1024 * 1024;
  private static final String ACC = "accState", INPUT = "inputState";
  private static final String LAST = "lastTriggeringTsState";
  private static final String COUNT = "processedCountState", SMALLEST = "smallestTSState";
  private final String cleanupState;
  private final boolean processingRows, eventRows, cleanupEnabled;
  private final long precedingRows;
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

  StockBoundedOverStateContract(
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
      byte[] constantBytes,
      long precedingRows,
      boolean processingRows,
      boolean eventRows,
      boolean cleanupEnabled) {
    if ((processingRows || eventRows) && precedingRows <= 0)
      throw new IllegalArgumentException("Bounded ROWS width must be positive");
    if (eventRows && (processingRows || cleanupEnabled))
      throw new IllegalArgumentException("Event ROWS processing cleanup is not qualified");
    this.processingRows = processingRows;
    this.eventRows = eventRows;
    this.precedingRows = precedingRows;
    this.cleanupEnabled = cleanupEnabled;
    cleanupState = processingRows ? "ProcTimeBoundedRowsOverCleanupTime" : "cleanupTsState";
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
      throw new IllegalArgumentException("Invalid bounded OVER layout");
    int fields = 0;
    for (int i = 0; i < kinds.length; i++) {
      if (kinds[i] < 0
          || kinds[i] >= 100
          || !Set.of(0, 3, 4, 22).contains(kinds[i])
          || widths[i] != (kinds[i] == 0 || kinds[i] == 4 ? 2 : 1))
        throw new IllegalArgumentException("Unqualified bounded OVER accumulator/view layout");
      fields += widths[i];
    }
    if (fields + (hiddenCount >= 0 ? 1 : 0) != accumulatorType.getFieldCount()
        || hiddenCount >= accumulatorType.getFieldCount()
        || (hiddenCount >= 0 && !accumulatorType.getTypeAt(hiddenCount).is(LogicalTypeRoot.BIGINT)))
      throw new IllegalArgumentException("Unowned bounded OVER accumulator fields");
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
        throw new IllegalArgumentException("bounded OVER key layout changed");
    var nativeFields = new ArrayList<>(inputType.getFields());
    if (constants != null && constants.getFieldCount() > 0) {
      if (constantBytes == null || constantBytes.length == 0)
        throw new IllegalArgumentException("bounded OVER constants are missing");
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
    if (processingRows) {
      protocol.add(new RowType.RowField("__over_proc_count", new BigIntType(false)));
      protocol.add(new RowType.RowField("__over_proc_smallest", new BigIntType(false)));
      protocol.add(new RowType.RowField("__over_proc_cleanup", new BigIntType()));
    } else {
      protocol.add(new RowType.RowField("__over_range_cleanup", new BigIntType()));
      protocol.add(new RowType.RowField("__over_range_last", new BigIntType()));
      protocol.add(
          new RowType.RowField("__over_range_timers", new ArrayType(false, new BigIntType())));
      protocol.add(
          new RowType.RowField("__over_range_order", new ArrayType(false, new BigIntType())));
    }
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
    var descriptors = new LinkedHashMap<String, StateDescriptor.Type>();
    descriptors.put(ACC, StateDescriptor.Type.VALUE);
    descriptors.put(INPUT, StateDescriptor.Type.MAP);
    if (cleanupEnabled) descriptors.put(cleanupState, StateDescriptor.Type.VALUE);
    if (processingRows || eventRows) descriptors.put(COUNT, StateDescriptor.Type.VALUE);
    if (processingRows) descriptors.put(SMALLEST, StateDescriptor.Type.VALUE);
    else descriptors.put(LAST, StateDescriptor.Type.VALUE);
    return descriptors;
  }

  @Override
  public Map<String, StockStateInventory.Timer> timerSchema() {
    return Map.of(
        EVENTS,
        processingRows
            ? StockStateInventory.Timer.empty(VoidNamespaceSerializer.INSTANCE)
            : StockStateInventory.Timer.consumed(VoidNamespaceSerializer.INSTANCE),
        PROCESSING,
        processingRows && cleanupEnabled
            ? StockStateInventory.Timer.consumed(VoidNamespaceSerializer.INSTANCE)
            : StockStateInventory.Timer.empty(VoidNamespaceSerializer.INSTANCE));
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

  private BinaryRowData key(RowData row) {
    var key = new GenericRowData(keyColumns.length);
    for (int i = 0; i < keyColumns.length; i++) key.setField(i, get(row, inputType, keyColumns[i]));
    return new RowDataSerializer(keyType).toBinaryRow(key).copy();
  }

  private static Object get(RowData row, RowType type, int index) {
    return RowData.createFieldGetter(type.getTypeAt(index), index).getFieldOrNull(row);
  }

  private long time(RowData row) {
    if (row.isNullAt(timeColumn)) throw new IllegalArgumentException("NULL bounded OVER rowtime");
    var type = inputType.getTypeAt(timeColumn);
    if (type instanceof TimestampType)
      return row.getTimestamp(timeColumn, ((TimestampType) type).getPrecision()).getMillisecond();
    if (type instanceof LocalZonedTimestampType)
      return row.getTimestamp(timeColumn, ((LocalZonedTimestampType) type).getPrecision())
          .getMillisecond();
    return row.getLong(timeColumn);
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
            throw new IllegalStateException("Cannot project bounded OVER state", error);
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

  @Override
  public void clearProjectionTimers(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    bind(backend);
    while (events.poll() != null) {}
    while (processing.poll() != null) {}
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    clear(backend);
    try (var lists =
        new StockMapListSpool<Long, RowData>(
            keys, LongSerializer.INSTANCE, new RowDataSerializer(inputType))) {
      var inputReaderType = inputType;
      visit(
          Native::exportOverCanonicalFrames,
          handle,
          root -> {
            int arity = root.getFieldVectors().size() - nativeAccumulatorType.getFieldCount() - 7;
            if (arity < nativeInputType.getFieldCount())
              throw new IllegalArgumentException("bounded OVER native input is incomplete");
            var expected = ArrowConversion.toArrowSchema(nativeAccumulatorType).getFields();
            for (int i = 0; i < expected.size(); i++)
              if (!expected
                  .get(i)
                  .getType()
                  .equals(root.getSchema().getFields().get(arity + 3 + i).getType()))
                throw new IllegalArgumentException("bounded OVER raw accumulator type changed");
            var reader = ArrowConversion.createArrowReader(root, inputReaderType);
            BinaryRowData owner = null;
            for (int i = 0; i < root.getRowCount(); i++) {
              var row = reader.read(i);
              var key = key(row);
              if (owner != null && !owner.equals(key))
                throw new IllegalArgumentException("Mixed bounded OVER frame keys");
              owner = key;
              if (processingRows) {
                var clock = (org.apache.arrow.vector.BigIntVector) root.getVector(arity + 1);
                if (clock.isNull(i))
                  throw new IllegalArgumentException("Missing retained processing clock");
                lists.append(key, clock.get(i), row);
              } else lists.append(key, time(row), row);
            }
            if ("false"
                .equals(
                    root.getSchema().getCustomMetadata().get("streamfusion.canonical.frame.final")))
              return;
            var positions = (org.apache.arrow.vector.BigIntVector) root.getVector(arity + 2);
            long retained = Math.addExact(positions.get(root.getRowCount() - 1), 1);
            var raw =
                ArrowConversion.createArrowReader(
                    columns(root, arity + 3, nativeAccumulatorType.getFieldCount()),
                    nativeAccumulatorType);
            backend.setCurrentKey(owner);
            var saved = raw.read(root.getRowCount() - 1);
            var accumulator = new GenericRowData(accumulatorType.getFieldCount());
            for (int p = 0; p < accumulatorFields.length; p++)
              accumulator.setField(accumulatorFields[p], get(saved, nativeAccumulatorType, p));
            if (hiddenCount >= 0) accumulator.setField(hiddenCount, retained);
            accumulators(backend).update(new RowDataSerializer(accumulatorType).copy(accumulator));
            if (eventRows) {
              scalar(backend, COUNT).update(Math.min(precedingRows, retained));
              scalar(backend, LAST).update(time(reader.read(root.getRowCount() - 1)));
            }
          });
      if (!processingRows) {
        visit(
            Native::exportOverPendingCanonical,
            handle,
            root -> {
              var reader = ArrowConversion.createArrowReader(root, inputType);
              for (int i = 0; i < root.getRowCount(); i++) {
                var row = reader.read(i);
                lists.append(key(row), time(row), row);
                if (eventRows)
                  events.add(
                      new TimerHeapInternalTimer<>(time(row), key(row), VoidNamespace.INSTANCE));
              }
            });
      }
      lists.visit(
          backend,
          (owner, timestamp, values) -> {
            backend.setCurrentKey(owner);
            inputs(backend).put(timestamp, values);
          });
      if (processingRows) {
        visit(
            Native::exportOverCanonicalProcessingRows,
            handle,
            root -> {
              var reader = ArrowConversion.createArrowReader(root, protocolType);
              for (int i = 0; i < root.getRowCount(); i++) {
                var row = reader.read(i);
                int k = keyType.getFieldCount();
                var owner = protocolKey(row);
                backend.setCurrentKey(owner);
                scalar(backend, COUNT).update(row.getLong(k));
                scalar(backend, SMALLEST).update(row.getLong(k + 1));
                if (cleanupEnabled) {
                  if (row.isNullAt(k + 2))
                    throw new IllegalArgumentException("Processing ROWS cleanup is missing");
                  long deadline = row.getLong(k + 2);
                  scalar(backend, cleanupState).update(deadline);
                  processing.add(
                      new TimerHeapInternalTimer<>(deadline, owner, VoidNamespace.INSTANCE));
                } else if (!row.isNullAt(k + 2))
                  throw new IllegalArgumentException("Unexpected processing ROWS cleanup");
              }
            });
        return;
      }
      if (eventRows) return;
      if (tech.streamfusion.compat.FlinkStateBackendCompat.serializesKeyedValues(backend)) {
        StockOverCallbacks.export(
            backend,
            handle,
            keyType,
            (owner, kind, timestamp) -> {
              switch (kind) {
                case StockOverCallbacks.CLEANUP:
                  scalar(backend, cleanupState).update(timestamp);
                  break;
                case StockOverCallbacks.LAST_TRIGGER:
                  scalar(backend, LAST).update(timestamp);
                  break;
                case StockOverCallbacks.EVENT:
                  events.add(
                      new TimerHeapInternalTimer<>(timestamp, owner, VoidNamespace.INSTANCE));
                  break;
                default:
                  throw new IllegalArgumentException("Unexpected RANGE processing callback");
              }
            });
        return;
      }
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
              if (row.isNullAt(k)) scalar(backend, cleanupState).clear();
              else scalar(backend, cleanupState).update(row.getLong(k));
              if (row.isNullAt(k + 1)) scalar(backend, LAST).clear();
              else scalar(backend, LAST).update(row.getLong(k + 1));
              if (backend instanceof org.apache.flink.runtime.state.heap.HeapKeyedStateBackend) {
                var order = row.getArray(k + 3);
                var state = inputs(backend);
                var saved = new LinkedHashMap<Long, List<RowData>>();
                for (int p = 0; p < order.size(); p++) {
                  long timestamp = order.getLong(p);
                  var peers = state.get(timestamp);
                  if (peers == null || saved.put(timestamp, peers) != null)
                    throw new IllegalArgumentException("bounded OVER map order has no input");
                }
                var timestamps = state.keys();
                if (timestamps != null)
                  for (var timestamp : timestamps)
                    if (!saved.containsKey(timestamp))
                      throw new IllegalArgumentException("bounded OVER input has no map order");
                state.clear();
                for (var entry : saved.entrySet()) state.put(entry.getKey(), entry.getValue());
              }
              var timers = row.getArray(k + 2);
              for (int p = 0; p < timers.size(); p++)
                events.add(
                    new TimerHeapInternalTimer<>(timers.getLong(p), owner, VoidNamespace.INSTANCE));
            }
          });
    }
  }

  private BinaryRowData protocolKey(RowData row) {
    var key = new GenericRowData(keyType.getFieldCount());
    for (int i = 0; i < keyType.getFieldCount(); i++) key.setField(i, get(row, keyType, i));
    return new RowDataSerializer(keyType).toBinaryRow(key).copy();
  }

  private static final class Partition {
    final BinaryRowData key;
    long bucketCount, retainedRows, pendingBuckets, timerCount;
    Long lastProcessed;
    RowData accumulator;
    Long cleanup, last, count, smallest;

    Partition(BinaryRowData key) {
      this.key = key;
    }
  }

  private void preflight(
      CheckpointableKeyedStateBackend<RowData> backend,
      StockStateSpool partitions,
      StockMapListSnapshot lists,
      long handle,
      boolean persistent)
      throws Exception {
    var inventory = new HashMap<String, StockStateInventory.State>();
    stateDescriptors()
        .forEach(
            (name, kind) ->
                inventory.put(
                    name, new StockStateInventory.State(kind, VoidNamespaceSerializer.INSTANCE)));
    StockStateInventory.validate(backend, inventory, timerSchema(), keys);
    lists.capture(backend, INPUT, keys);
    bind(backend);
    partitions.collectOwners(backend, stateDescriptors().keySet());
    try (var iterator =
        StockTimerSnapshot.<RowData, VoidNamespace>iterator(
            backend, processingRows ? PROCESSING : EVENTS)) {
      while (iterator.hasNext()) {
        var timer = iterator.next();
        if (timer.getNamespace() != VoidNamespace.INSTANCE)
          throw new IllegalArgumentException("Unknown bounded OVER timer namespace");
        partitions.addOwner(timer.getKey());
        if (!partitions.add(1, timer.getKey(), timer.getTimestamp()))
          throw new IllegalArgumentException("Duplicate bounded OVER timer");
      }
    }
    if (!(processingRows ? events : processing).isEmpty())
      throw new IllegalArgumentException("Unexpected bounded OVER timer domain");
    try (var owners = partitions.owners()) {
      while (owners.hasNext()) {
        var partition = readPartition(backend, partitions, lists, owners.next());
        transferFrames(lists, handle, partition, true);
        if (!processingRows && !eventRows)
          transferProtocol(partitions, lists, handle, partition, persistent, true);
        transferPending(lists, handle, partition, true);
      }
    }
  }

  private Partition readPartition(
      CheckpointableKeyedStateBackend<RowData> backend,
      StockStateSpool partitions,
      StockMapListSnapshot lists,
      RowData owner)
      throws Exception {
    var partition = new Partition(new RowDataSerializer(keyType).toBinaryRow(owner).copy());
    partition.timerCount = partitions.count(1, owner);
    var accSerializer = new RowDataSerializer(accumulatorType);
    backend.setCurrentKey(partition.key);
    partition.cleanup = cleanupEnabled ? scalar(backend, cleanupState).value() : null;
    if (processingRows || eventRows) partition.count = scalar(backend, COUNT).value();
    if (processingRows) {
      partition.smallest = scalar(backend, SMALLEST).value();
    } else partition.last = scalar(backend, LAST).value();
    var acc = accumulators(backend).value();
    partition.accumulator = acc == null ? null : accSerializer.copy(acc);
    try (var timestamps = lists.timestamps(owner)) {
      while (timestamps.hasNext()) {
        long timestamp = timestamps.next();
        int count = lists.size(owner, timestamp);
        if (count <= 0)
          throw new IllegalArgumentException(
              "Empty " + (processingRows ? "processing ROWS" : "RANGE") + " timestamp bucket");
        for (var row : lists.rows(owner, timestamp)) {
          if (row.getRowKind() != RowKind.INSERT
              || !key(row).equals(partition.key)
              || (!processingRows && time(row) != timestamp))
            throw new IllegalArgumentException("bounded OVER input disagrees with key/time/kind");
        }
        partition.bucketCount++;
        if (processingRows || partition.last != null && timestamp <= partition.last) {
          partition.retainedRows = Math.addExact(partition.retainedRows, count);
          if (partition.lastProcessed == null || timestamp > partition.lastProcessed)
            partition.lastProcessed = timestamp;
        } else partition.pendingBuckets++;
        if (eventRows && timestamp <= 0)
          throw new IllegalArgumentException("Event ROWS input must follow the initial cutoff");
        if (!processingRows
            && (partition.last == null || timestamp > partition.last)
            && !partitions.contains(1, owner, timestamp))
          throw new IllegalArgumentException("Pending bounded OVER input has no event timer");
      }
    }
    if (processingRows) {
      long retained = partition.retainedRows;
      if (partition.accumulator == null
          || retained == 0
          || partition.count == null
          || partition.smallest == null
          || partition.count <= 0
          || partition.count > precedingRows
          || partition.count != Math.min(retained, precedingRows)
          || (partition.smallest != Long.MAX_VALUE && !lists.contains(owner, partition.smallest)))
        throw new IllegalArgumentException(
            "Processing ROWS counter/minimum/frame ownership changed");
      if (cleanupEnabled != (partition.cleanup != null)
          || (cleanupEnabled
              ? partition.timerCount != 1 || !partitions.contains(1, owner, partition.cleanup)
              : partition.timerCount != 0))
        throw new IllegalArgumentException("Processing ROWS cleanup/timer ownership changed");
      if (hiddenCount >= 0
          && (partition.accumulator.isNullAt(hiddenCount)
              || partition.accumulator.getLong(hiddenCount) != retained))
        throw new IllegalArgumentException(
            "Processing ROWS hidden input count disagrees with retained rows");
      return partition;
    }
    if (eventRows) {
      long retained = partition.retainedRows;
      if (retained == 0
          ? partition.accumulator != null || partition.count != null || partition.last != null
          : partition.accumulator == null
              || partition.count == null
              || partition.count != Math.min(retained, precedingRows)
              || !partition.lastProcessed.equals(partition.last))
        throw new IllegalArgumentException("Event ROWS counter/cutoff/frame ownership changed");
      if (partition.timerCount != partition.pendingBuckets)
        throw new IllegalArgumentException("Event ROWS timer ownership changed");
      if (partition.accumulator != null
          && hiddenCount >= 0
          && (partition.accumulator.isNullAt(hiddenCount)
              || partition.accumulator.getLong(hiddenCount) != retained))
        throw new IllegalArgumentException(
            "Event ROWS hidden input count disagrees with retained rows");
      return partition;
    }
    if (partition.cleanup != null && !partitions.contains(1, owner, partition.cleanup))
      throw new IllegalArgumentException("bounded OVER cleanup has no matching timer");
    if (partition.cleanup == null && (partition.bucketCount != 0 || partition.accumulator != null))
      throw new IllegalArgumentException("bounded OVER input has no cleanup owner");
    if ((partition.retainedRows != 0) != (partition.accumulator != null))
      throw new IllegalArgumentException("bounded OVER accumulator/frame ownership changed");
    if (partition.accumulator != null && hiddenCount >= 0) {
      long count = partition.retainedRows;
      if (partition.accumulator.isNullAt(hiddenCount)
          || partition.accumulator.getLong(hiddenCount) != count)
        throw new IllegalArgumentException(
            "bounded OVER hidden input count disagrees with retained rows");
    }

    return partition;
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

  private void transferFrames(
      StockMapListSnapshot lists, long handle, Partition partition, boolean validating)
      throws Exception {
    long total = partition.retainedRows;
    if (total == 0) return;
    var frames = new ArrayList<RowData>(CHUNK_ROWS);
    var serializer = new RowDataSerializer(nativeInputType);
    long position = 0;
    long bytes = 0;
    int base = nativeInputType.getFieldCount();
    try (var timestamps = lists.sortedTimestamps(partition.key)) {
      while (timestamps.hasNext()) {
        long timestamp = timestamps.next();
        if (!processingRows && (partition.last == null || timestamp > partition.last)) continue;
        for (var original : lists.rows(partition.key, timestamp)) {
          var input = nativeRow(original);
          long size =
              serializer.toBinaryRow(input).getSizeInBytes() + 8L * frameType.getFieldCount();
          if (!frames.isEmpty() && (frames.size() >= CHUNK_ROWS || bytes + size > CHUNK_BYTES)) {
            sendFrameChunk(handle, frames, false, validating);
            frames.clear();
            bytes = 0;
          }
          var row = new GenericRowData(frameType.getFieldCount());
          for (int p = 0; p < base; p++) row.setField(p, get(input, nativeInputType, p));
          row.setField(base, (byte) 0);
          row.setField(base + 1, processingRows ? position : time(input));
          row.setField(base + 2, timestamp);
          row.setField(base + 3, position);
          if (++position == total) {
            for (int p = 0; p < accumulatorFields.length; p++)
              row.setField(
                  base + 4 + p, get(partition.accumulator, accumulatorType, accumulatorFields[p]));
            int p = base + 4 + accumulatorFields.length;
            if (processingRows || eventRows) {
              row.setField(p, partition.cleanup);
              if (!eventRows) row.setField(p + 1, partition.last);
            }
          }
          frames.add(row);
          bytes += size;
        }
      }
    }
    sendFrameChunk(handle, frames, true, validating);
  }

  private void sendFrameChunk(long handle, List<RowData> frames, boolean last, boolean validating) {
    send(
        (h, a, s) -> {
          if (!processingRows && !eventRows)
            Native.importOverCanonicalFramePayloadChunk(h, a, s, last, validating);
          else Native.importOverCanonicalFrameChunk(h, a, s, last, validating);
        },
        handle,
        frameType,
        frames,
        false);
  }

  private void transferProtocol(
      StockStateSpool partitions,
      StockMapListSnapshot lists,
      long handle,
      Partition partition,
      boolean persistent,
      boolean validating)
      throws Exception {
    int k = keyType.getFieldCount();
    if (processingRows) {
      var metadata = new GenericRowData(protocolType.getFieldCount());
      for (int p = 0; p < k; p++) metadata.setField(p, get(partition.key, keyType, p));
      metadata.setField(k, partition.count);
      metadata.setField(k + 1, partition.smallest);
      metadata.setField(k + 2, partition.cleanup);
      send(
          Native::importOverCanonicalProcessingRows,
          handle,
          protocolType,
          List.of(metadata),
          false);
    } else if (!eventRows) {
      var writer = new StockOverCallbacks.Writer(handle, keyType, validating);
      writer.append(partition.key, StockOverCallbacks.OWNER, partition.bucketCount);
      if (partition.cleanup != null)
        writer.append(partition.key, StockOverCallbacks.CLEANUP, partition.cleanup);
      if (partition.last != null)
        writer.append(partition.key, StockOverCallbacks.LAST_TRIGGER, partition.last);
      try (var times = partitions.times(1, partition.key)) {
        while (times.hasNext())
          writer.append(partition.key, StockOverCallbacks.EVENT, times.next());
      }
      try (var times =
          persistent ? lists.sortedTimestamps(partition.key) : lists.timestamps(partition.key)) {
        while (times.hasNext())
          writer.append(partition.key, StockOverCallbacks.MAP_ORDER, times.next());
      }
      writer.finish();
    }
  }

  private void transferPending(
      StockMapListSnapshot lists, long handle, Partition partition, boolean validating)
      throws Exception {
    if (processingRows) return;
    var rows = new ArrayList<RowData>(CHUNK_ROWS);
    var serializer = new RowDataSerializer(nativeInputType);
    long bytes = 0;
    try (var timestamps = lists.sortedTimestamps(partition.key)) {
      while (timestamps.hasNext()) {
        long timestamp = timestamps.next();
        if (partition.last != null && timestamp <= partition.last) continue;
        for (var input : lists.rows(partition.key, timestamp)) {
          var row = nativeRow(input);
          long size = serializer.toBinaryRow(row).getSizeInBytes();
          if (!rows.isEmpty() && (rows.size() >= CHUNK_ROWS || bytes + size > CHUNK_BYTES)) {
            send(
                validating
                    ? Native::validateOverPendingCanonical
                    : Native::importOverPendingCanonical,
                handle,
                nativeInputType,
                rows,
                true);
            rows.clear();
            bytes = 0;
          }
          rows.add(row);
          bytes += size;
        }
      }
    }
    send(
        validating ? Native::validateOverPendingCanonical : Native::importOverPendingCanonical,
        handle,
        nativeInputType,
        rows,
        true);
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    try (var partitions = new StockStateSpool(new RowDataSerializer(keyType))) {
      var lists = new StockMapListSnapshot(partitions, new RowDataSerializer(inputType), 2);
      preflight(backend, partitions, lists, handle, persistent);
      try (var owners = partitions.owners()) {
        while (owners.hasNext()) {
          var partition = readPartition(backend, partitions, lists, owners.next());
          transferFrames(lists, handle, partition, false);
          transferProtocol(partitions, lists, handle, partition, persistent, false);
          transferPending(lists, handle, partition, false);
        }
      }
    }
    clear(backend);
  }
}
