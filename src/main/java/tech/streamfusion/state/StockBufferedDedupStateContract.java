package tech.streamfusion.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released event-time keep-first rows, emitted markers, and user-timers priority queues. */
public final class StockBufferedDedupStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final int CHUNK_ROWS = 1024;
  private static final String WAITING = "waiting-to-emit-on-timer";
  private static final String EMITTED = "already-emitted-state-boolean";
  private static final String EVENT_TIMERS = "_timer_state/event_user-timers";
  private static final String PROCESSING_TIMERS = "_timer_state/processing_user-timers";
  private final TypeSerializer<RowData> keySerializer;
  private final RowDataKeySelector keySelector;
  private final RowType keyType;
  private final RowType rowType;
  private final RowType bridgeType;
  private final int[] keys;
  private final int rowtimeColumn;
  private final boolean ttl;
  private transient CheckpointableKeyedStateBackend<RowData> timerBackend;
  private transient KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, VoidNamespace>>
      eventTimers;
  private transient KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, VoidNamespace>>
      processingTimers;

  public StockBufferedDedupStateContract(
      TypeSerializer<RowData> keySerializer,
      RowDataKeySelector keySelector,
      RowType keyType,
      RowType rowType,
      int[] keys,
      int rowtimeColumn,
      long retention) {
    this.keySerializer = keySerializer;
    this.keySelector = keySelector;
    this.keyType = keyType;
    this.rowType = rowType;
    this.keys = keys.clone();
    this.rowtimeColumn = rowtimeColumn;
    ttl = retention > 0;
    var fields = new ArrayList<RowType.RowField>();
    for (var field : rowType.getFields())
      fields.add(new RowType.RowField(field.getName(), field.getType().copy(true)));
    String suffix = "__canonical_marker_time";
    var names =
        fields.stream().map(RowType.RowField::getName).collect(java.util.stream.Collectors.toSet());
    while (names.contains(suffix)) suffix = "$" + suffix;
    fields.add(new RowType.RowField(suffix, new BigIntType(false)));
    bridgeType = new RowType(fields);
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(WAITING, StateDescriptor.Type.VALUE, EMITTED, StateDescriptor.Type.VALUE);
  }

  @Override
  public Map<String, StockStateInventory.Timer> timerSchema() {
    return Map.of(
        EVENT_TIMERS,
        StockStateInventory.Timer.consumed(VoidNamespaceSerializer.INSTANCE),
        PROCESSING_TIMERS,
        StockStateInventory.Timer.empty(VoidNamespaceSerializer.INSTANCE));
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keySerializer.duplicate();
  }

  private StockValueState<RowData> waiting(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return new StockValueState<>(backend, WAITING, new RowDataSerializer(rowType), false);
  }

  private StockValueState<Boolean> emitted(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return new StockValueState<>(backend, EMITTED, BooleanSerializer.INSTANCE, ttl);
  }

  private KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<RowData, VoidNamespace>> timers(
      CheckpointableKeyedStateBackend<RowData> backend, String name) {
    if (timerBackend != backend) {
      var events =
          backend.create(
              EVENT_TIMERS,
              new TimerSerializer<>(keySerializer.duplicate(), VoidNamespaceSerializer.INSTANCE));
      var processing =
          backend.create(
              PROCESSING_TIMERS,
              new TimerSerializer<>(keySerializer.duplicate(), VoidNamespaceSerializer.INSTANCE));
      eventTimers = events;
      processingTimers = processing;
      timerBackend = backend;
    }
    return name.equals(EVENT_TIMERS) ? eventTimers : processingTimers;
  }

  @Override
  public void clear(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    clearState(backend, WAITING, waiting(backend));
    clearState(backend, EMITTED, emitted(backend));
    var events = timers(backend, EVENT_TIMERS);
    while (events.poll() != null) {}
    var processing = timers(backend, PROCESSING_TIMERS);
    while (processing.poll() != null) {}
  }

  private static void clearState(
      CheckpointableKeyedStateBackend<RowData> backend, String name, StockValueState<?> state)
      throws Exception {
    List<RowData> keys;
    try (var stream = backend.getKeys(name, VoidNamespace.INSTANCE)) {
      keys =
          stream
              .map(backend.getKeySerializer()::copy)
              .collect(java.util.stream.Collectors.toList());
    }
    for (var key : keys) {
      backend.setCurrentKey(key);
      state.clear();
    }
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    clear(backend);
    var waiting = waiting(backend);
    var emitted = emitted(backend);
    var events = timers(backend, EVENT_TIMERS);
    var serializer = new RowDataSerializer(rowType);
    try (ArrowSchema schema = rowSchema();
        AutoCloseable release = schema::release) {
      for (boolean markers : new boolean[] {false, true}) {
        Native.exportBufferedDeduplicatorCanonical(
            handle,
            schema.memoryAddress(),
            markers,
            (arrayAddress, schemaAddress) -> {
              try (ArrowArray array = ArrowArray.wrap(arrayAddress);
                  ArrowSchema batchSchema = ArrowSchema.wrap(schemaAddress);
                  VectorSchemaRoot root =
                      Data.importVectorSchemaRoot(
                          NativeAllocator.SHARED,
                          array,
                          batchSchema,
                          NativeAllocator.DICTIONARIES)) {
                var reader =
                    ArrowConversion.createArrowReader(
                        root,
                        new RowType(bridgeType.getFields().subList(0, rowType.getFieldCount())));
                for (int index = 0; index < root.getRowCount(); index++) {
                  RowData row = reader.read(index);
                  RowData key = keySerializer.copy(keySelector.getKey(row));
                  backend.setCurrentKey(key);
                  if (markers)
                    emitted.update(
                        true, ((BigIntVector) root.getVector(rowType.getFieldCount())).get(index));
                  else {
                    RowData retained = serializer.copy(row);
                    waiting.update(retained, 0);
                    events.add(
                        new TimerHeapInternalTimer<>(
                            rowtime(retained), key, VoidNamespace.INSTANCE));
                  }
                }
              } catch (Exception failure) {
                throw new IllegalStateException("Cannot export buffered dedup state", failure);
              }
            });
      }
    }
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    StockStateInventory.validate(
        backend, stateDescriptors(), java.util.Set.of(EVENT_TIMERS), keySerializer);
    try (ArrowSchema schema = rowSchema();
        AutoCloseable release = schema::release) {
      for (boolean markers : new boolean[] {false, true})
        Native.exportBufferedDeduplicatorCanonical(
            handle,
            schema.memoryAddress(),
            markers,
            (arrayAddress, schemaAddress) -> {
              try (var array = ArrowArray.wrap(arrayAddress);
                  var exported = ArrowSchema.wrap(schemaAddress);
                  var root =
                      Data.importVectorSchemaRoot(
                          NativeAllocator.SHARED, array, exported, NativeAllocator.DICTIONARIES)) {
                if (root.getRowCount() != 0)
                  throw new IllegalStateException(
                      "Buffered canonical restore requires an empty native target");
              }
            });
      visit(backend, handle, schema, true);
      visit(backend, handle, schema, false);
    }
    clear(backend);
  }

  private void validateKey(RowData key) {
    if (key.getRowKind() != org.apache.flink.types.RowKind.INSERT)
      throw new IllegalStateException("Buffered dedup has a non-insert key");
    for (int field = 0; field < keyType.getFieldCount(); field++)
      if (!keyType.getTypeAt(field).isNullable() && key.isNullAt(field))
        throw new IllegalStateException("Buffered dedup has a NULL non-null key field");
  }

  private void visit(
      CheckpointableKeyedStateBackend<RowData> backend,
      long handle,
      ArrowSchema schema,
      boolean validateOnly)
      throws Exception {
    if (!timers(backend, PROCESSING_TIMERS).isEmpty())
      throw new IllegalStateException("Buffered dedup has unexpected processing-time timers");
    var scheduled = new HashMap<RowData, Long>();
    try (var iterator = timers(backend, EVENT_TIMERS).iterator()) {
      while (iterator.hasNext()) {
        var timer = iterator.next();
        validateKey(timer.getKey());
        if (!VoidNamespace.INSTANCE.equals(timer.getNamespace())
            || scheduled.put(keySerializer.copy(timer.getKey()), timer.getTimestamp()) != null)
          throw new IllegalStateException(
              "Buffered dedup has duplicate timers or an unexpected namespace");
      }
    }
    var visited = new java.util.HashSet<RowData>();
    var waiting = waiting(backend);
    var emitted = emitted(backend);
    var rowSerializer = new RowDataSerializer(rowType);
    var fields = new RowData.FieldGetter[rowType.getFieldCount()];
    for (int i = 0; i < fields.length; i++)
      fields[i] = RowData.createFieldGetter(rowType.getTypeAt(i), i);
    var keyFields = new RowData.FieldGetter[keys.length];
    for (int i = 0; i < keys.length; i++)
      keyFields[i] = RowData.createFieldGetter(keyType.getTypeAt(i), i);
    List<RowData> rows = new ArrayList<>(CHUNK_ROWS);
    try (var stream = backend.getKeys(WAITING, VoidNamespace.INSTANCE)) {
      var iterator = stream.iterator();
      while (iterator.hasNext()) {
        RowData key = keySerializer.copy(iterator.next());
        validateKey(key);
        if (!visited.add(key)) throw new IllegalStateException("Duplicate buffered dedup key");
        backend.setCurrentKey(key);
        var state = waiting.value();
        if (state == null || state.getUserValue() == null)
          throw new IllegalStateException("Buffered dedup has an empty pending entry");
        RowData value = rowSerializer.copy(state.getUserValue());
        if (value.getRowKind() != org.apache.flink.types.RowKind.INSERT
            || !key.equals(keySelector.getKey(value)))
          throw new IllegalStateException("Buffered dedup pending row and key disagree");
        if (value.isNullAt(rowtimeColumn))
          throw new IllegalStateException("Buffered dedup pending row has NULL time");
        Long time = scheduled.remove(key);
        if (time == null || time != rowtime(value))
          throw new IllegalStateException("Buffered dedup pending row and event timer disagree");
        var row = new GenericRowData(bridgeType.getFieldCount());
        for (int i = 0; i < fields.length; i++) row.setField(i, fields[i].getFieldOrNull(value));
        row.setField(fields.length, 0L);
        rows.add(row);
        if (rows.size() == CHUNK_ROWS) {
          importBatch(handle, schema, false, rows, validateOnly);
          rows.clear();
        }
      }
    }
    if (!scheduled.isEmpty())
      throw new IllegalStateException("Buffered dedup timer has no pending row");
    if (!rows.isEmpty()) {
      importBatch(handle, schema, false, rows, validateOnly);
      rows.clear();
    }
    try (var stream = backend.getKeys(EMITTED, VoidNamespace.INSTANCE)) {
      var iterator = stream.iterator();
      while (iterator.hasNext()) {
        RowData key = keySerializer.copy(iterator.next());
        validateKey(key);
        if (!visited.add(key))
          throw new IllegalStateException("Buffered dedup pending and emitted states overlap");
        backend.setCurrentKey(key);
        var state = emitted.value();
        if (state == null || !Boolean.TRUE.equals(state.getUserValue()))
          throw new IllegalStateException("Buffered dedup has an invalid emitted marker");
        var row = new GenericRowData(bridgeType.getFieldCount());
        for (int i = 0; i < keys.length; i++)
          row.setField(keys[i], keyFields[i].getFieldOrNull(key));
        if (!key.equals(keySelector.getKey(row)))
          throw new IllegalStateException("Buffered dedup marker has a non-canonical key");
        row.setField(fields.length, state.getLastAccessTimestamp());
        rows.add(row);
        if (rows.size() == CHUNK_ROWS) {
          importBatch(handle, schema, true, rows, validateOnly);
          rows.clear();
        }
      }
    }
    if (!rows.isEmpty()) importBatch(handle, schema, true, rows, validateOnly);
  }

  private long rowtime(RowData row) {
    var type = rowType.getTypeAt(rowtimeColumn);
    return type.is(org.apache.flink.table.types.logical.LogicalTypeFamily.TIMESTAMP)
        ? row.getTimestamp(rowtimeColumn, LogicalTypeChecks.getPrecision(type)).getMillisecond()
        : row.getLong(rowtimeColumn);
  }

  private ArrowSchema rowSchema() {
    var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
    try {
      Data.exportSchema(
          NativeAllocator.SHARED,
          ArrowConversion.toArrowSchema(rowType),
          NativeAllocator.DICTIONARIES,
          schema);
      return schema;
    } catch (RuntimeException | Error failure) {
      if (schema.snapshot().release != 0) schema.release();
      schema.close();
      throw failure;
    }
  }

  private void importBatch(
      long handle,
      ArrowSchema rowSchema,
      boolean markers,
      List<RowData> rows,
      boolean validateOnly) {
    try (VectorSchemaRoot root =
            RowDataArrowConverter.write(rows, bridgeType, NativeAllocator.SHARED);
        ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (validateOnly)
        Native.validateBufferedDeduplicatorCanonical(
            handle,
            rowSchema.memoryAddress(),
            markers,
            array.memoryAddress(),
            schema.memoryAddress());
      else
        Native.importBufferedDeduplicatorCanonical(
            handle,
            rowSchema.memoryAddress(),
            markers,
            array.memoryAddress(),
            schema.memoryAddress());
    }
  }
}
