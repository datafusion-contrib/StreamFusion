package tech.streamfusion.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released event-time unbounded folds, pending buckets and physical event/cleanup timers. */
public final class StockOverEventFoldStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final String INPUT = "inputState";
  private static final String CLEANUP = "RowTimeUnboundedOverCleanupTime";
  private static final String EVENTS = "_timer_state/event_user-timers";
  private static final String PROCESSING = "_timer_state/processing_user-timers";
  private static final int CHUNK_ROWS = 1024;
  private final StockOverFoldStateContract folds;
  private final RowType keyType, inputType, nativeInputType, cleanupType, constants;
  private final byte[] constantBytes;
  private final int[] keyColumns;
  private final int timeColumn;
  private final boolean cleanupEnabled;
  private final boolean pollingTimers =
      tech.streamfusion.compat.FlinkPlanCompat.unboundedOverVersion() == 1;
  private transient RowData constantRow;
  private transient CheckpointableKeyedStateBackend<RowData> timerBackend;
  private transient KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, VoidNamespace>>
      events,
      processing;

  public StockOverEventFoldStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType inputType,
      RowType accumulatorType,
      int[] keyColumns,
      int timeColumn,
      int[] kinds,
      int[] widths,
      RowType constants,
      byte[] constantBytes,
      boolean cleanupEnabled) {
    folds = new StockOverFoldStateContract(keys, keyType, accumulatorType, kinds, widths, false);
    this.keyType = keyType;
    this.inputType = inputType;
    this.keyColumns = keyColumns.clone();
    this.timeColumn = timeColumn;
    this.constants = constants;
    this.constantBytes = constantBytes == null ? null : constantBytes.clone();
    this.cleanupEnabled = cleanupEnabled;
    if (keyColumns.length != keyType.getFieldCount()
        || timeColumn < 0
        || timeColumn >= inputType.getFieldCount())
      throw new IllegalArgumentException("Invalid event OVER input/key layout");
    for (int i = 0; i < keyColumns.length; i++)
      if (keyColumns[i] < 0
          || keyColumns[i] >= inputType.getFieldCount()
          || !inputType.getTypeAt(keyColumns[i]).equals(keyType.getTypeAt(i)))
        throw new IllegalArgumentException("Event OVER key type changed");
    var fields = new ArrayList<>(inputType.getFields());
    if (constants != null && constants.getFieldCount() > 0) {
      if (constantBytes == null || constantBytes.length == 0)
        throw new IllegalArgumentException("Event OVER constants are missing");
      var names = new java.util.HashSet<>(inputType.getFieldNames());
      for (int i = 0; i < constants.getFieldCount(); i++) {
        String name = "__over_constant_" + i;
        while (!names.add(name)) name += "_";
        fields.add(new RowType.RowField(name, constants.getTypeAt(i)));
      }
    }
    nativeInputType = new RowType(fields);
    var metadata = new ArrayList<RowType.RowField>();
    for (int i = 0; i < keyType.getFieldCount(); i++)
      metadata.add(new RowType.RowField("key" + i, keyType.getTypeAt(i).copy(true)));
    metadata.add(new RowType.RowField("__over_retention_stamp", new BigIntType(pollingTimers)));
    metadata.add(new RowType.RowField("__sf_over_cleanup_armed", new BooleanType(false)));
    if (pollingTimers)
      metadata.add(
          new RowType.RowField(
              "__sf_over_polling_timers",
              new org.apache.flink.table.types.logical.ArrayType(false, new BigIntType(false))));
    cleanupType = new RowType(metadata);
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return folds.keySerializer();
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    var result = new LinkedHashMap<>(folds.stateDescriptors());
    result.put(INPUT, StateDescriptor.Type.MAP);
    if (cleanupEnabled) result.put(CLEANUP, StateDescriptor.Type.VALUE);
    return result;
  }

  @Override
  public Map<String, StockStateInventory.Timer> timerSchema() {
    return Map.of(
        EVENTS,
        StockStateInventory.Timer.consumed(VoidNamespaceSerializer.INSTANCE),
        PROCESSING,
        cleanupEnabled
            ? StockStateInventory.Timer.consumed(VoidNamespaceSerializer.INSTANCE)
            : StockStateInventory.Timer.empty(VoidNamespaceSerializer.INSTANCE));
  }

  @Override
  public void configureNativeState(long handle, boolean persistent) {
    if (cleanupEnabled) Native.bindOverEventCleanupTimers(handle);
    if (pollingTimers) Native.bindOverEventPollingTimers(handle);
  }

  private void bind(CheckpointableKeyedStateBackend<RowData> backend) {
    if (backend == timerBackend) return;
    var serializer = new TimerSerializer<>(keySerializer(), VoidNamespaceSerializer.INSTANCE);
    events = backend.create(EVENTS, serializer);
    processing = backend.create(PROCESSING, serializer);
    timerBackend = backend;
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

  private ValueState<Long> cleanup(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new ValueStateDescriptor<>(CLEANUP, LongSerializer.INSTANCE));
  }

  private BinaryRowData checkedKey(RowData key) {
    if (key == null
        || key.getArity() != keyType.getFieldCount()
        || key.getRowKind() != RowKind.INSERT)
      throw new IllegalArgumentException("Invalid event OVER partition key");
    for (int i = 0; i < keyType.getFieldCount(); i++)
      if (!keyType.getTypeAt(i).isNullable() && key.isNullAt(i))
        throw new IllegalArgumentException("NULL in required event OVER key");
    return new RowDataSerializer(keyType).toBinaryRow(key).copy();
  }

  private static Object get(RowData row, RowType type, int index) {
    return row.isNullAt(index)
        ? null
        : RowData.createFieldGetter(type.getTypeAt(index), index).getFieldOrNull(row);
  }

  private BinaryRowData key(RowData row) {
    var value = new GenericRowData(keyColumns.length);
    for (int i = 0; i < keyColumns.length; i++)
      value.setField(i, get(row, inputType, keyColumns[i]));
    return new RowDataSerializer(keyType).toBinaryRow(value).copy();
  }

  private long time(RowData row) {
    if (row.isNullAt(timeColumn)) throw new IllegalArgumentException("NULL event OVER rowtime");
    var type = inputType.getTypeAt(timeColumn);
    if (type instanceof TimestampType)
      return row.getTimestamp(timeColumn, ((TimestampType) type).getPrecision()).getMillisecond();
    if (type instanceof LocalZonedTimestampType)
      return row.getTimestamp(timeColumn, ((LocalZonedTimestampType) type).getPrecision())
          .getMillisecond();
    return row.getLong(timeColumn);
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

  @FunctionalInterface
  private interface RootReceiver {
    void accept(VectorSchemaRoot root) throws Exception;
  }

  @FunctionalInterface
  private interface Exporter {
    void run(long handle, Native.BatchReceiver receiver);
  }

  @FunctionalInterface
  private interface Importer {
    void run(long handle, long array, long schema);
  }

  private static void visit(Exporter exporter, long handle, RootReceiver receiver) {
    exporter.run(
        handle,
        (array, schema) -> {
          try (var a = ArrowArray.wrap(array);
              var s = ArrowSchema.wrap(schema);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, a, s, NativeAllocator.DICTIONARIES)) {
            receiver.accept(root);
          } catch (Exception failure) {
            throw new IllegalStateException("Cannot transfer event OVER state", failure);
          }
        });
  }

  private static void send(Importer importer, long handle, RowType type, List<RowData> rows) {
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED);
        var a = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var s = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, a, s);
      importer.run(handle, a.memoryAddress(), s.memoryAddress());
    }
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
    folds.exportState(backend, handle, persistent);
    var serializer = new RowDataSerializer(inputType);
    visit(
        Native::exportOverPendingCanonical,
        handle,
        root -> {
          if (root.getFieldVectors().size() < nativeInputType.getFieldCount())
            throw new IllegalArgumentException("Event OVER pending input is incomplete");
          var reader = ArrowConversion.createArrowReader(root, nativeInputType);
          for (int i = 0; i < root.getRowCount(); i++) {
            var nativeRow = reader.read(i);
            var row = new GenericRowData(inputType.getFieldCount());
            for (int p = 0; p < inputType.getFieldCount(); p++)
              row.setField(p, get(nativeRow, inputType, p));
            var owner = key(row);
            var timestamp = time(row);
            backend.setCurrentKey(owner);
            var state = inputs(backend);
            var peers = state.get(timestamp);
            if (peers == null) {
              peers = new ArrayList<>();
              if (!pollingTimers)
                events.add(new TimerHeapInternalTimer<>(timestamp, owner, VoidNamespace.INSTANCE));
            }
            peers.add(serializer.copy(row));
            state.put(timestamp, peers);
          }
        });
    visit(
        Native::exportOverCleanupCanonical,
        handle,
        root -> {
          if (!cleanupEnabled && !pollingTimers && root.getRowCount() != 0)
            throw new IllegalArgumentException("Event OVER cleanup was not configured");
          if (root.getFieldVectors().size() != cleanupType.getFieldCount())
            throw new IllegalArgumentException("Event OVER cleanup ownership is missing");
          var reader = ArrowConversion.createArrowReader(root, cleanupType);
          for (int i = 0; i < root.getRowCount(); i++) {
            var row = reader.read(i);
            var owner = new GenericRowData(keyType.getFieldCount());
            for (int p = 0; p < keyType.getFieldCount(); p++)
              owner.setField(p, get(row, keyType, p));
            var binary = new RowDataSerializer(keyType).toBinaryRow(owner).copy();
            backend.setCurrentKey(binary);
            int arity = keyType.getFieldCount();
            if (!row.isNullAt(arity)) {
              if (!cleanupEnabled)
                throw new IllegalArgumentException("Unconfigured event OVER cleanup");
              long deadline = row.getLong(arity);
              cleanup(backend).update(deadline);
              if (row.getBoolean(arity + 1))
                processing.add(
                    new TimerHeapInternalTimer<>(deadline, binary, VoidNamespace.INSTANCE));
            } else if (row.getBoolean(arity + 1))
              throw new IllegalArgumentException("Cleanup timer has no deadline");
            if (pollingTimers) {
              var timers = row.getArray(arity + 2);
              for (int t = 0; t < timers.size(); t++)
                events.add(
                    new TimerHeapInternalTimer<>(
                        timers.getLong(t), binary, VoidNamespace.INSTANCE));
            }
          }
        });
  }

  private static final int EVENT_TIMES = 1, PROCESSING_TIMES = 2, PENDING_TIMES = 3, FOLDS = 4;

  private void preflight(
      CheckpointableKeyedStateBackend<RowData> backend, StockRestoreInventory partitions)
      throws Exception {
    var inventory = new HashMap<String, StockStateInventory.State>();
    stateDescriptors()
        .forEach(
            (name, kind) ->
                inventory.put(
                    name, new StockStateInventory.State(kind, VoidNamespaceSerializer.INSTANCE)));
    StockStateInventory.validate(backend, inventory, timerSchema(), keySerializer());
    bind(backend);
    for (String name : stateDescriptors().keySet())
      try (var source = backend.getKeys(name, VoidNamespace.INSTANCE)) {
        var keys = source.iterator();
        while (keys.hasNext()) {
          var key = checkedKey(keys.next());
          partitions.addOwner(key);
          if (name.equals("accState")) partitions.add(FOLDS, key, 0);
        }
      }
    for (boolean clock : new boolean[] {false, true})
      try (var timers =
          StockTimerSnapshot.<RowData, VoidNamespace>iterator(
              backend, clock ? PROCESSING : EVENTS)) {
        while (timers.hasNext()) {
          var timer = timers.next();
          if (timer.getNamespace() != VoidNamespace.INSTANCE)
            throw new IllegalArgumentException("Unknown event OVER timer namespace");
          var owner = checkedKey(timer.getKey());
          partitions.addOwner(owner);
          if (!partitions.add(clock ? PROCESSING_TIMES : EVENT_TIMES, owner, timer.getTimestamp()))
            throw new IllegalArgumentException("Duplicate event OVER timer");
        }
      }
    try (var owners = partitions.owners()) {
      while (owners.hasNext()) {
        var owner = owners.next();
        backend.setCurrentKey(owner);
        Long deadline = cleanupEnabled ? cleanup(backend).value() : null;
        boolean pending = false;
        var entries = inputs(backend).entries();
        if (entries != null)
          for (var entry : entries) {
            Long timestamp = entry.getKey();
            var peers = entry.getValue();
            if (timestamp == null
                || timestamp == Long.MIN_VALUE
                || peers == null
                || peers.isEmpty())
              throw new IllegalArgumentException("Empty or invalid event OVER timestamp bucket");
            for (var row : peers) {
              if (row == null
                  || row.getArity() != inputType.getFieldCount()
                  || row.getRowKind() != RowKind.INSERT
                  || !key(row).equals(owner)
                  || time(row) != timestamp)
                throw new IllegalArgumentException("Event OVER input disagrees with key/time/kind");
              for (int p = 0; p < inputType.getFieldCount(); p++)
                if (!inputType.getTypeAt(p).isNullable()
                    && !LogicalTypeChecks.isProctimeAttribute(inputType.getTypeAt(p))
                    && row.isNullAt(p))
                  throw new IllegalArgumentException("NULL in required event OVER input");
            }
            pending = true;
            partitions.add(PENDING_TIMES, owner, timestamp);
            if (!pollingTimers && !partitions.contains(EVENT_TIMES, owner, timestamp))
              throw new IllegalArgumentException(
                  "Event OVER pending buckets and event timers differ");
          }
        if (pollingTimers) {
          if (pending && !partitions.hasAny(EVENT_TIMES, owner))
            throw new IllegalArgumentException(
                "Event OVER pending buckets and event timers differ");
        } else
          try (var timers = partitions.times(EVENT_TIMES, owner)) {
            while (timers.hasNext())
              if (!partitions.contains(PENDING_TIMES, owner, timers.next()))
                throw new IllegalArgumentException(
                    "Event OVER pending buckets and event timers differ");
          }
        if (cleanupEnabled && deadline == null && (pending || partitions.contains(FOLDS, owner, 0)))
          throw new IllegalArgumentException("Event OVER state has no cleanup deadline");
        try (var timers = partitions.times(PROCESSING_TIMES, owner)) {
          if (timers.hasNext()
              && (!cleanupEnabled
                  || deadline == null
                  || timers.next().longValue() != deadline
                  || timers.hasNext()))
            throw new IllegalArgumentException(
                "Event OVER processing timer has no matching cleanup value");
        }
      }
    }
  }

  private void transferPending(
      CheckpointableKeyedStateBackend<RowData> backend,
      long handle,
      StockRestoreInventory partitions,
      boolean validating)
      throws Exception {
    var rows = new ArrayList<RowData>(CHUNK_ROWS);
    try (var owners = partitions.owners()) {
      while (owners.hasNext()) {
        var owner = owners.next();
        backend.setCurrentKey(owner);
        try (var timestamps = partitions.times(PENDING_TIMES, owner)) {
          while (timestamps.hasNext())
            for (var row : inputs(backend).get(timestamps.next())) {
              rows.add(nativeRow(row));
              if (rows.size() == CHUNK_ROWS) {
                send(
                    validating
                        ? Native::validateOverPendingCanonical
                        : Native::importOverPendingCanonical,
                    handle,
                    nativeInputType,
                    rows);
                rows.clear();
              }
            }
        }
      }
    }
    if (!rows.isEmpty())
      send(
          validating ? Native::validateOverPendingCanonical : Native::importOverPendingCanonical,
          handle,
          nativeInputType,
          rows);
  }

  private void transferCleanup(
      CheckpointableKeyedStateBackend<RowData> backend,
      long handle,
      StockRestoreInventory partitions,
      boolean validating)
      throws Exception {
    var rows = new ArrayList<RowData>(CHUNK_ROWS);
    try (var owners = partitions.owners()) {
      while (owners.hasNext()) {
        var owner = owners.next();
        backend.setCurrentKey(owner);
        Long deadline = cleanupEnabled ? cleanup(backend).value() : null;
        if (deadline == null && (!pollingTimers || !partitions.hasAny(EVENT_TIMES, owner)))
          continue;
        var row = new GenericRowData(cleanupType.getFieldCount());
        for (int p = 0; p < keyType.getFieldCount(); p++) row.setField(p, get(owner, keyType, p));
        row.setField(keyType.getFieldCount(), deadline);
        row.setField(keyType.getFieldCount() + 1, partitions.hasAny(PROCESSING_TIMES, owner));
        if (pollingTimers) {
          var timers = new ArrayList<Long>();
          try (var timestamps = partitions.times(EVENT_TIMES, owner)) {
            while (timestamps.hasNext()) timers.add(timestamps.next());
          }
          row.setField(
              keyType.getFieldCount() + 2,
              new org.apache.flink.table.data.GenericArrayData(timers.toArray(Long[]::new)));
        }
        rows.add(row);
        if (rows.size() == CHUNK_ROWS) {
          send(
              validating
                  ? Native::validateOverCleanupCanonical
                  : Native::importOverCleanupCanonical,
              handle,
              cleanupType,
              rows);
          rows.clear();
        }
      }
    }
    if (!rows.isEmpty())
      send(
          validating ? Native::validateOverCleanupCanonical : Native::importOverCleanupCanonical,
          handle,
          cleanupType,
          rows);
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    try (var partitions = new StockRestoreInventory(new RowDataSerializer(keyType))) {
      preflight(backend, partitions);
      folds.requireEmpty(handle);
      visit(
          Native::exportOverPendingCanonical,
          handle,
          root -> {
            if (root.getRowCount() != 0)
              throw new IllegalStateException("Event OVER import requires an empty pending target");
          });
      visit(
          Native::exportOverCleanupCanonical,
          handle,
          root -> {
            if (root.getRowCount() != 0)
              throw new IllegalStateException("Event OVER import requires an empty cleanup target");
          });
      for (boolean validating : new boolean[] {true, false}) {
        folds.transferState(backend, handle, validating);
        transferPending(backend, handle, partitions, validating);
        transferCleanup(backend, handle, partitions, validating);
      }
    }
    clear(backend);
  }
}
