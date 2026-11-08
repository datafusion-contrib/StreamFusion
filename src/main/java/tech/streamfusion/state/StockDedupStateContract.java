package tech.streamfusion.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released eager dedup state: a processing-time presence marker or a complete retained row. */
public final class StockDedupStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final int CHUNK_ROWS = 1024;
  private static final String STATE = "deduplicate-state";
  private final TypeSerializer<RowData> keySerializer;
  private final RowType keyType;
  private final RowType rowType;
  private final RowType bridgeType;
  private final boolean presenceOnly;
  private final int rowtimeColumn;
  private final long stateRetention;

  public StockDedupStateContract(
      TypeSerializer<RowData> keySerializer,
      RowType keyType,
      RowType rowType,
      boolean presenceOnly,
      int rowtimeColumn,
      long stateRetention) {
    if (rowtimeColumn < -1
        || rowtimeColumn >= rowType.getFieldCount()
        || rowtimeColumn >= 0
            && (presenceOnly
                || !rowType
                    .getTypeAt(rowtimeColumn)
                    .is(org.apache.flink.table.types.logical.LogicalTypeFamily.TIMESTAMP)))
      throw new IllegalArgumentException("unqualified eager dedup rowtime schema");
    this.keySerializer = keySerializer;
    this.keyType = keyType;
    this.rowType = rowType;
    this.presenceOnly = presenceOnly;
    this.rowtimeColumn = rowtimeColumn;
    this.stateRetention = stateRetention;
    var fields = new ArrayList<RowType.RowField>();
    for (var field : rowType.getFields())
      fields.add(
          new RowType.RowField(
              field.getName(), field.getType().copy(presenceOnly || field.getType().isNullable())));
    fields.add(
        new RowType.RowField(
            uniqueName(fields, "binary_key"), new VarBinaryType(false, VarBinaryType.MAX_LENGTH)));
    fields.add(new RowType.RowField(uniqueName(fields, "__rowtime__"), new BigIntType(false)));
    fields.add(new RowType.RowField(uniqueName(fields, "__ttl_ts__"), new BigIntType(false)));
    fields.add(new RowType.RowField(uniqueName(fields, "__stored_kind__"), new TinyIntType(false)));
    bridgeType = new RowType(fields);
  }

  private static String uniqueName(List<RowType.RowField> fields, String preferred) {
    var names =
        fields.stream().map(RowType.RowField::getName).collect(java.util.stream.Collectors.toSet());
    String name = preferred;
    while (names.contains(name)) name = "$" + name;
    return name;
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(STATE, StateDescriptor.Type.VALUE);
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keySerializer.duplicate();
  }

  @SuppressWarnings("unchecked")
  private TypeSerializer<Object> valueSerializer() {
    return (TypeSerializer<Object>)
        (TypeSerializer<?>)
            (presenceOnly ? BooleanSerializer.INSTANCE : new RowDataSerializer(rowType));
  }

  private StockValueState<Object> state(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return new StockValueState<>(backend, STATE, valueSerializer(), stateRetention > 0);
  }

  @Override
  public org.apache.flink.runtime.state.SavepointResources<RowData> snapshot(
      StockSnapshotContext context, long handle, boolean persistent) throws Exception {
    if (persistent)
      return context.nativeValue(
          STATE,
          valueSerializer(),
          stateRetention > 0,
          () -> withNativeSchema(
              schema -> Native.captureEagerDeduplicatorCanonicalSnapshot(handle, schema)),
          bridgeType,
          decoder());
    return context.value(
        STATE,
        valueSerializer(),
        stateRetention > 0,
        receiver -> visit(handle, persistent, receiver));
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    clear(backend);
    var state = state(backend);
    visit(
        handle,
        persistent,
        (key, value, written) -> {
          backend.setCurrentKey(key);
          state.update(value, written);
        });
  }

  private void visit(long handle, boolean persistent, StockValueSnapshot.Entry<Object> receiver)
      throws Exception {
    var decoder = decoder();
    scan(
        handle,
        persistent,
        (arrayAddress, schemaAddress) -> {
          try (var array = ArrowArray.wrap(arrayAddress);
              var schema = ArrowSchema.wrap(schemaAddress);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            var reader = ArrowConversion.createArrowReader(root, bridgeType);
            for (int index = 0; index < root.getRowCount(); index++)
              decoder.decode(reader.read(index), receiver);
          } catch (Exception failure) {
            throw new IllegalStateException("Cannot export stock dedup state", failure);
          }
        });
  }

  private StockNativeValueSnapshot.Decoder<Object> decoder() {
    int arity = rowType.getFieldCount();
    var serializer = new RowDataSerializer(rowType);
    var fields = new RowData.FieldGetter[arity];
    for (int field = 0; field < arity; field++)
      fields[field] = RowData.createFieldGetter(rowType.getTypeAt(field).copy(true), field);
    return (row, receiver) -> {
      byte[] encoded = row.getBinary(arity);
      var key = new BinaryRowData(keyType.getFieldCount());
      key.pointTo(MemorySegmentFactory.wrap(encoded), 0, encoded.length);
      if (presenceOnly) receiver.write(key, Boolean.TRUE, row.getLong(arity + 2));
      else {
        var payload = new GenericRowData(arity);
        for (int field = 0; field < arity; field++)
          payload.setField(field, fields[field].getFieldOrNull(row));
        RowData value = serializer.copy(payload);
        value.setRowKind(RowKind.fromByteValue(row.getByte(arity + 3)));
        receiver.write(key, value, row.getLong(arity + 2));
      }
    };
  }

  private void scan(long handle, boolean persistent, Native.BatchReceiver receiver) {
    withNativeSchema(
        schema -> {
          Native.exportEagerDeduplicatorCanonical(handle, persistent, schema, receiver);
          return null;
        });
  }

  private <T> T withNativeSchema(java.util.function.LongFunction<T> action) {
    try (ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try {
        Data.exportSchema(
            NativeAllocator.SHARED,
            ArrowConversion.toArrowSchema(rowType),
            NativeAllocator.DICTIONARIES,
            schema);
        return action.apply(schema.memoryAddress());
      } finally {
        if (schema.snapshot().release != 0) schema.release();
      }
    }
  }

  void requireEmptyStockState(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    state(backend);
    try (var keys = backend.getKeys(STATE, VoidNamespace.INSTANCE)) {
      if (keys.findAny().isPresent())
        throw new IllegalStateException("Stateless proctime dedup has unexpected retained rows");
    }
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    scan(
        handle,
        persistent,
        (arrayAddress, schemaAddress) -> {
          try (var array = ArrowArray.wrap(arrayAddress);
              var schema = ArrowSchema.wrap(schemaAddress);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            if (root.getRowCount() != 0)
              throw new IllegalStateException(
                  "Dedup canonical restore requires an empty native target");
          }
        });
    visit(backend, handle, persistent, true);
    visit(backend, handle, persistent, false);
  }

  private void visit(
      CheckpointableKeyedStateBackend<RowData> backend,
      long handle,
      boolean persistent,
      boolean validateOnly)
      throws Exception {
    var state = state(backend);
    var keyRows = new RowDataSerializer(keyType);
    var valueRows = new RowDataSerializer(rowType);
    int arity = rowType.getFieldCount();
    var rows = new ArrayList<RowData>(CHUNK_ROWS);
    var getters = new RowData.FieldGetter[arity];
    for (int field = 0; field < arity; field++)
      getters[field] = RowData.createFieldGetter(rowType.getTypeAt(field).copy(true), field);
    try (var keys = backend.getKeys(STATE, VoidNamespace.INSTANCE)) {
      var iterator = keys.iterator();
      while (iterator.hasNext()) {
        RowData key = iterator.next();
        backend.setCurrentKey(key);
        var stored = state.value();
        if (stored == null || stored.getUserValue() == null) continue;
        Object value =
            presenceOnly ? stored.getUserValue() : valueRows.copy((RowData) stored.getUserValue());
        GenericRowData row = new GenericRowData(arity + 4);
        if (!presenceOnly) {
          for (int field = 0; field < arity; field++) {
            row.setField(field, getters[field].getFieldOrNull((RowData) value));
          }
        }
        BinaryRowData binary = keyRows.toBinaryRow(key);
        row.setField(
            arity,
            BinarySegmentUtils.copyToBytes(
                binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()));
        row.setField(
            arity + 1,
            rowtimeColumn < 0
                ? 0L
                : ((RowData) value)
                    .getTimestamp(
                        rowtimeColumn,
                        org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision(
                            rowType.getTypeAt(rowtimeColumn)))
                    .getMillisecond());
        row.setField(arity + 2, stored.getLastAccessTimestamp());
        row.setField(
            arity + 3, presenceOnly ? (byte) 0 : ((RowData) value).getRowKind().toByteValue());
        rows.add(row);
        if (rows.size() == CHUNK_ROWS) {
          importBatch(rows, handle, persistent, validateOnly);
          rows.clear();
        }
      }
      if (!rows.isEmpty()) importBatch(rows, handle, persistent, validateOnly);
    }
  }

  private void importBatch(
      List<RowData> rows, long handle, boolean persistent, boolean validateOnly) {
    try (VectorSchemaRoot root =
            RowDataArrowConverter.write(rows, bridgeType, NativeAllocator.SHARED);
        ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (validateOnly)
        Native.validateEagerDeduplicatorCanonical(
            handle, persistent, array.memoryAddress(), schema.memoryAddress());
      else
        Native.importEagerDeduplicatorCanonical(
            handle, persistent, array.memoryAddress(), schema.memoryAddress());
    }
  }
}
