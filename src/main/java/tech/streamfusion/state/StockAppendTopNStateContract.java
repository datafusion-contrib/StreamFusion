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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.MapListRepresentation;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released append Top-N lists, fast Top-1 rows, and independent first-observed rank bounds. */
public final class StockAppendTopNStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final int CHUNK_ROWS = 1024;
  private final TypeSerializer<RowData> keys;
  private final RowType keyType;
  private final RowDataKeySelector keySelector;
  private final RowDataKeySelector sortSelector;
  private final RowType rowType;
  private final RowType sortType;
  private final RowType bridgeType;
  private final int[] sortColumns;
  private final boolean top1;
  private final boolean variableBound;
  private final boolean ttl;

  public StockAppendTopNStateContract(
      TypeSerializer<RowData> keys,
      RowDataKeySelector keySelector,
      RowDataKeySelector sortSelector,
      RowType keyType,
      RowType rowType,
      int[] sortColumns,
      boolean top1,
      boolean variableBound,
      boolean ttl) {
    this.keys = keys;
    this.keySelector = keySelector;
    this.sortSelector = sortSelector;
    this.keyType = keyType;
    this.rowType = rowType;
    this.sortColumns = sortColumns.clone();
    this.top1 = top1;
    this.variableBound = variableBound;
    this.ttl = ttl;
    sortType = (RowType) sortSelector.getProducedType().toLogicalType();
    var fields = new ArrayList<RowType.RowField>();
    for (var field : rowType.getFields())
      fields.add(new RowType.RowField(field.getName(), field.getType().copy(true)));
    add(fields, "binary_key", new VarBinaryType(false, VarBinaryType.MAX_LENGTH));
    add(fields, "__rank_end__", new BigIntType(false));
    add(fields, "__ttl_ts__", new BigIntType(false));
    add(fields, "__stored_kind__", new TinyIntType(false));
    bridgeType = new RowType(fields);
  }

  private static void add(
      List<RowType.RowField> fields,
      String name,
      org.apache.flink.table.types.logical.LogicalType type) {
    var names =
        fields.stream().map(RowType.RowField::getName).collect(java.util.stream.Collectors.toSet());
    while (names.contains(name)) name = "$" + name;
    fields.add(new RowType.RowField(name, type));
  }

  private String dataName() {
    return top1 ? "Top1-Rank-State" : "data-state-with-append";
  }

  @SuppressWarnings("unchecked")
  private MapState<RowData, Object> lists(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    TypeSerializer<?> serializer = new ListSerializer<>(new RowDataSerializer(rowType));
    if (ttl) serializer = new TtlSerializer<>(LongSerializer.INSTANCE, serializer);
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new MapStateDescriptor<>(
            dataName(), new RowDataSerializer(sortType), (TypeSerializer<Object>) serializer));
  }

  private StockValueState<RowData> winner(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return new StockValueState<>(backend, dataName(), new RowDataSerializer(rowType), ttl);
  }

  private StockValueState<Long> bound(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return new StockValueState<>(backend, "rankEnd", LongSerializer.INSTANCE, ttl);
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    var states = new java.util.HashMap<String, StateDescriptor.Type>();
    states.put(dataName(), top1 ? StateDescriptor.Type.VALUE : StateDescriptor.Type.MAP);
    if (variableBound) states.put("rankEnd", StateDescriptor.Type.VALUE);
    return Map.copyOf(states);
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  @Override
  public void clear(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    var winner = top1 ? winner(backend) : null;
    var lists = top1 ? null : lists(backend);
    try (var stream = backend.getKeys(dataName(), VoidNamespace.INSTANCE)) {
      for (var key : stream.collect(java.util.stream.Collectors.toList())) {
        backend.setCurrentKey(key);
        if (top1) winner.clear();
        else lists.clear();
      }
    }
    if (variableBound) {
      var bound = bound(backend);
      try (var stream = backend.getKeys("rankEnd", VoidNamespace.INSTANCE)) {
        for (var key : stream.collect(java.util.stream.Collectors.toList())) {
          backend.setCurrentKey(key);
          bound.clear();
        }
      }
    }
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    clear(backend);
    var winner = top1 ? winner(backend) : null;
    var lists = top1 ? null : lists(backend);
    var bound = variableBound ? bound(backend) : null;
    var serializer = new RowDataSerializer(rowType);
    var sortProjection = new StockKeyProjection(sortType);
    int arity = rowType.getFieldCount();
    try (ArrowSchema rowSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        AutoCloseable release =
            () -> {
              if (rowSchema.snapshot().release != 0) rowSchema.release();
            }) {
      Data.exportSchema(
          NativeAllocator.SHARED,
          MapListRepresentation.forIpc(ArrowConversion.toArrowSchema(rowType)),
          NativeAllocator.DICTIONARIES,
          rowSchema);
      Native.exportAppendTopNCanonical(
          handle,
          persistent,
          rowSchema.memoryAddress(),
          (arrayAddress, schemaAddress) -> {
            try (ArrowArray array = ArrowArray.wrap(arrayAddress);
                ArrowSchema schema = ArrowSchema.wrap(schemaAddress);
                VectorSchemaRoot root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              var reader = ArrowConversion.createArrowReader(root, bridgeType);
              for (int index = 0; index < root.getRowCount(); index++) {
                var row = reader.read(index);
                byte[] encoded = row.getBinary(arity);
                BinaryRowData key = new BinaryRowData(keyType.getFieldCount());
                key.pointTo(MemorySegmentFactory.wrap(encoded), 0, encoded.length);
                backend.setCurrentKey(key);
                byte kind = row.getByte(arity + 3);
                long written = row.getLong(arity + 2);
                if (kind == -1) {
                  if (bound == null)
                    throw new IllegalStateException("Unexpected variable rank bound");
                  bound.update(row.getLong(arity + 1), written);
                  continue;
                }
                RowData retained = copyPayload(row);
                validatePayload(key, null, retained);
                RowData payload = serializer.copy(retained);
                payload.setRowKind(RowKind.fromByteValue(kind));
                if (top1) winner.update(payload, written);
                else {
                  var sort = new GenericRowData(sortColumns.length);
                  for (int column = 0; column < sortColumns.length; column++)
                    sort.setField(
                        column,
                        RowData.createFieldGetter(
                                rowType.getTypeAt(sortColumns[column]), sortColumns[column])
                            .getFieldOrNull(payload));
                  RowData sortKey = sortProjection.project(sort);
                  Object old = lists.get(sortKey);
                  var rows = new ArrayList<RowData>();
                  if (old != null) rows.addAll(values(old));
                  rows.add(payload);
                  lists.put(sortKey, ttl ? new TtlValue<>(rows, written) : rows);
                }
              }
            } catch (Exception failure) {
              throw new IllegalStateException("Cannot project append Top-N state", failure);
            }
          });
    }
  }

  private GenericRowData copyPayload(RowData row) {
    var copy = new GenericRowData(rowType.getFieldCount());
    for (int column = 0; column < rowType.getFieldCount(); column++)
      copy.setField(
          column,
          RowData.createFieldGetter(bridgeType.getTypeAt(column), column).getFieldOrNull(row));
    return copy;
  }

  @SuppressWarnings("unchecked")
  private List<RowData> values(Object stored) {
    return (List<RowData>) (ttl ? ((TtlValue<?>) stored).getUserValue() : stored);
  }

  private long written(Object stored) {
    return ttl ? ((TtlValue<?>) stored).getLastAccessTimestamp() : 0;
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    StockStateInventory.validate(backend, stateDescriptors(), java.util.Set.of(), keys);
    try (ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        AutoCloseable release =
            () -> {
              if (schema.snapshot().release != 0) schema.release();
            }) {
      Data.exportSchema(
          NativeAllocator.SHARED,
          MapListRepresentation.forIpc(ArrowConversion.toArrowSchema(rowType)),
          NativeAllocator.DICTIONARIES,
          schema);
      Native.exportAppendTopNCanonical(
          handle,
          persistent,
          schema.memoryAddress(),
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var exported = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, exported, NativeAllocator.DICTIONARIES)) {
              if (root.getRowCount() != 0)
                throw new IllegalStateException("Append Top-N restore requires an empty target");
            }
          });
    }
    visit(backend, handle, persistent, true);
    visit(backend, handle, persistent, false);
    clear(backend);
  }

  private void validateKey(RowData key, RowType type) {
    if (key.getRowKind() != RowKind.INSERT)
      throw new IllegalStateException("Append Top-N key has non-insert row kind");
    for (int i = 0; i < type.getFieldCount(); i++)
      if (!type.getTypeAt(i).isNullable() && key.isNullAt(i))
        throw new IllegalStateException("Append Top-N key has a NULL non-null field");
  }

  private void validatePayload(RowData partition, RowData sort, RowData row) throws Exception {
    for (int i = 0; i < rowType.getFieldCount(); i++)
      if (!rowType.getTypeAt(i).isNullable()
          && row.isNullAt(i)
          && !org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute(
              rowType.getTypeAt(i)))
        throw new IllegalStateException("Append Top-N retained row has a NULL non-null field");
    if (!partition.equals(keySelector.getKey(row)))
      throw new IllegalStateException("Append Top-N partition key and retained row disagree");
    if (sort != null && !sort.equals(sortSelector.getKey(row)))
      throw new IllegalStateException("Append Top-N sort key and retained row disagree");
  }

  private void visit(
      CheckpointableKeyedStateBackend<RowData> backend,
      long handle,
      boolean persistent,
      boolean validateOnly)
      throws Exception {
    var winner = top1 ? winner(backend) : null;
    var lists = top1 ? null : lists(backend);
    var serializer = new RowDataSerializer(rowType);
    var keySerializer = keys.duplicate();
    var sortSerializer = new RowDataSerializer(sortType);
    var seen = new java.util.HashSet<RowData>();
    List<RowData> rows = new ArrayList<>(CHUNK_ROWS);
    try (var stream = backend.getKeys(dataName(), VoidNamespace.INSTANCE)) {
      var iterator = stream.iterator();
      while (iterator.hasNext()) {
        RowData key = keySerializer.copy(iterator.next());
        validateKey(key, keyType);
        if (!seen.add(key)) throw new IllegalStateException("Duplicate append Top-N partition key");
        backend.setCurrentKey(key);
        byte[] encoded = bytes(new RowDataSerializer(keyType).toBinaryRow(key));
        if (top1) {
          var value = winner.value();
          if (value == null || value.getUserValue() == null)
            throw new IllegalStateException("Append Top-1 has an empty winner");
          RowData payload = serializer.copy(value.getUserValue());
          validatePayload(key, null, payload);
          rows.add(
              transfer(
                  payload,
                  encoded,
                  0,
                  value.getLastAccessTimestamp(),
                  payload.getRowKind().toByteValue()));
          if (rows.size() == CHUNK_ROWS) {
            importBatch(handle, persistent, rows, validateOnly);
            rows.clear();
          }
        } else {
          var seenSorts = new java.util.HashSet<RowData>();
          for (var entry : lists.entries()) {
            RowData sort = sortSerializer.copy(entry.getKey());
            validateKey(sort, sortType);
            if (!seenSorts.add(sort))
              throw new IllegalStateException("Duplicate append Top-N sort key");
            if (entry.getValue() == null
                || values(entry.getValue()) == null
                || values(entry.getValue()).isEmpty())
              throw new IllegalStateException("Append Top-N has an empty retained list");
            for (var value : values(entry.getValue())) {
              if (value == null)
                throw new IllegalStateException("Append Top-N has a NULL retained row");
              RowData payload = serializer.copy(value);
              validatePayload(key, sort, payload);
              rows.add(
                  transfer(
                      payload,
                      encoded,
                      0,
                      written(entry.getValue()),
                      payload.getRowKind().toByteValue()));
              if (rows.size() == CHUNK_ROWS) {
                importBatch(handle, persistent, rows, validateOnly);
                rows.clear();
              }
            }
          }
        }
      }
    }
    if (variableBound) {
      var bound = bound(backend);
      seen.clear();
      try (var stream = backend.getKeys("rankEnd", VoidNamespace.INSTANCE)) {
        var iterator = stream.iterator();
        while (iterator.hasNext()) {
          RowData key = keySerializer.copy(iterator.next());
          validateKey(key, keyType);
          if (!seen.add(key)) throw new IllegalStateException("Duplicate append Top-N bound key");
          backend.setCurrentKey(key);
          var value = bound.value();
          if (value == null || value.getUserValue() == null)
            throw new IllegalStateException("Append Top-N has an empty rank bound");
          rows.add(
              transfer(
                  new GenericRowData(rowType.getFieldCount()),
                  bytes(new RowDataSerializer(keyType).toBinaryRow(key)),
                  value.getUserValue(),
                  value.getLastAccessTimestamp(),
                  (byte) -1));
          if (rows.size() == CHUNK_ROWS) {
            importBatch(handle, persistent, rows, validateOnly);
            rows.clear();
          }
        }
      }
    }
    if (!rows.isEmpty()) importBatch(handle, persistent, rows, validateOnly);
  }

  private static byte[] bytes(BinaryRowData row) {
    return BinarySegmentUtils.copyToBytes(row.getSegments(), row.getOffset(), row.getSizeInBytes());
  }

  private RowData transfer(RowData payload, byte[] key, long rankEnd, long time, byte kind) {
    int arity = rowType.getFieldCount();
    var row = new GenericRowData(arity + 4);
    for (int column = 0; column < arity; column++)
      row.setField(
          column,
          RowData.createFieldGetter(bridgeType.getTypeAt(column), column).getFieldOrNull(payload));
    row.setField(arity, key);
    row.setField(arity + 1, rankEnd);
    row.setField(arity + 2, time);
    row.setField(arity + 3, kind);
    return row;
  }

  private void importBatch(
      long handle, boolean persistent, List<RowData> rows, boolean validateOnly) {
    try (VectorSchemaRoot root =
            RowDataArrowConverter.write(rows, bridgeType, NativeAllocator.SHARED);
        ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED,
          MapListRepresentation.forExport(root),
          NativeAllocator.DICTIONARIES,
          array,
          schema);
      if (validateOnly)
        Native.validateAppendTopNCanonical(
            handle, persistent, array.memoryAddress(), schema.memoryAddress());
      else
        Native.importAppendTopNCanonical(
            handle, persistent, array.memoryAddress(), schema.memoryAddress());
    }
  }
}
