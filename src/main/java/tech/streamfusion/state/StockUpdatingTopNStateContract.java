package tech.streamfusion.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.MapListRepresentation;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released fixed UpdateFast row-key tuples, including the order within sort-key ties. */
public final class StockUpdatingTopNStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final int CHUNK_ROWS = 1024;
  private static final String DATA = "data-state-with-update";
  private final TypeSerializer<RowData> keys;
  private final RowType keyType;
  private final RowType rowType;
  private final RowType rowKeyType;
  private final RowType sortType;
  private final RowType bridgeType;
  private final RowDataKeySelector keySelector;
  private final RowDataKeySelector rowKeySelector;
  private final RowDataKeySelector sortSelector;
  private final boolean top1;

  public StockUpdatingTopNStateContract(
      TypeSerializer<RowData> keys,
      RowDataKeySelector keySelector,
      RowDataKeySelector rowKeySelector,
      RowDataKeySelector sortSelector,
      RowType keyType,
      RowType rowType,
      boolean top1) {
    this.keys = keys;
    this.keyType = keyType;
    this.rowType = rowType;
    this.keySelector = keySelector;
    this.rowKeySelector = rowKeySelector;
    this.sortSelector = sortSelector;
    this.top1 = top1;
    rowKeyType = (RowType) rowKeySelector.getProducedType().toLogicalType();
    sortType = (RowType) sortSelector.getProducedType().toLogicalType();
    var fields = new ArrayList<RowType.RowField>();
    for (var field : rowType.getFields())
      fields.add(new RowType.RowField(field.getName(), field.getType().copy(true)));
    for (LogicalType type :
        List.of(
            new VarBinaryType(false, VarBinaryType.MAX_LENGTH),
            new VarBinaryType(false, VarBinaryType.MAX_LENGTH),
            new IntType(false),
            new BigIntType(false),
            new TinyIntType(false))) {
      String name = "$stock_meta_" + fields.size();
      var names =
          fields.stream()
              .map(RowType.RowField::getName)
              .collect(java.util.stream.Collectors.toSet());
      while (names.contains(name)) name = "$" + name;
      fields.add(new RowType.RowField(name, type));
    }
    bridgeType = new RowType(fields);
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(
        top1 ? "Top1-Rank-State" : DATA,
        top1 ? StateDescriptor.Type.VALUE : StateDescriptor.Type.MAP);
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private TypeSerializer<Tuple2<RowData, Integer>> tupleSerializer() {
    return new TupleSerializer(
        (Class) Tuple2.class,
        new TypeSerializer<?>[] {new RowDataSerializer(rowType), IntSerializer.INSTANCE});
  }

  private MapState<RowData, Tuple2<RowData, Integer>> data(
      CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new MapStateDescriptor<>(DATA, new RowDataSerializer(rowKeyType), tupleSerializer()));
  }

  private StockValueState<RowData> winner(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return new StockValueState<>(backend, "Top1-Rank-State", new RowDataSerializer(rowType), false);
  }

  @Override
  public void clear(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    var data = top1 ? null : data(backend);
    var winner = top1 ? winner(backend) : null;
    try (var stream = backend.getKeys(top1 ? "Top1-Rank-State" : DATA, VoidNamespace.INSTANCE)) {
      for (var key : stream.map(keys::copy).collect(java.util.stream.Collectors.toList())) {
        backend.setCurrentKey(key);
        if (top1) winner.clear();
        else data.clear();
      }
    }
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    clear(backend);
    var data = top1 ? null : data(backend);
    var winner = top1 ? winner(backend) : null;
    var serializer = new RowDataSerializer(rowType);
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
      Native.exportUpdatingTopNCanonical(
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
                RowData row = reader.read(index);
                RowData key = binary(row.getBinary(arity), keyType.getFieldCount());
                validateKey(key, keyType);
                backend.setCurrentKey(key);
                RowData retained = payload(row);
                validatePayload(key, null, retained);
                RowData payload = serializer.copy(retained);
                payload.setRowKind(RowKind.fromByteValue(row.getByte(arity + 4)));
                if (row.getLong(arity + 3) != 0 || row.getInt(arity + 2) < 1)
                  throw new IllegalStateException("Invalid update rank export metadata");
                if (top1) {
                  if (winner.value() != null || row.getInt(arity + 2) != 1)
                    throw new IllegalStateException("Duplicate or invalid update Top-1 winner");
                  winner.update(payload, 0);
                } else {
                  RowData rowKey = binary(row.getBinary(arity + 1), rowKeyType.getFieldCount());
                  validateKey(rowKey, rowKeyType);
                  validatePayload(key, rowKey, payload);
                  if (data.contains(rowKey))
                    throw new IllegalStateException("Duplicate update rank row key");
                  data.put(rowKey, Tuple2.of(payload, row.getInt(arity + 2)));
                }
              }
            } catch (Exception failure) {
              throw new IllegalStateException("Cannot project released update rank state", failure);
            }
          });
    }
  }

  private GenericRowData payload(RowData row) {
    var payload = new GenericRowData(rowType.getFieldCount());
    for (int column = 0; column < rowType.getFieldCount(); column++)
      payload.setField(
          column,
          RowData.createFieldGetter(bridgeType.getTypeAt(column), column).getFieldOrNull(row));
    return payload;
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
      Native.exportUpdatingTopNCanonical(
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
                throw new IllegalStateException("Update rank restore requires an empty target");
            }
          });
    }
    visit(backend, handle, persistent, true);
    visit(backend, handle, persistent, false);
    clear(backend);
  }

  private void validateKey(RowData key, RowType type) {
    if (key.getRowKind() != RowKind.INSERT)
      throw new IllegalStateException("Update rank key has non-insert row kind");
    for (int i = 0; i < type.getFieldCount(); i++)
      if (!type.getTypeAt(i).isNullable() && key.isNullAt(i))
        throw new IllegalStateException("Update rank key has a NULL non-null field");
  }

  private void validatePayload(RowData partition, RowData rowKey, RowData row) throws Exception {
    for (int i = 0; i < rowType.getFieldCount(); i++)
      if (!rowType.getTypeAt(i).isNullable()
          && row.isNullAt(i)
          && !org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute(
              rowType.getTypeAt(i)))
        throw new IllegalStateException("Update rank retained row has a NULL non-null field");
    if (!partition.equals(keySelector.getKey(row)))
      throw new IllegalStateException("Update rank partition key differs from its payload");
    if (rowKey != null && !rowKey.equals(rowKeySelector.getKey(row)))
      throw new IllegalStateException("Update rank row key differs from its payload");
  }

  private void visit(
      CheckpointableKeyedStateBackend<RowData> backend,
      long handle,
      boolean persistent,
      boolean validateOnly)
      throws Exception {
    var data = top1 ? null : data(backend);
    var winner = top1 ? winner(backend) : null;
    var serializer = new RowDataSerializer(rowType);
    var keySerializer = keys.duplicate();
    var binaryKeys = new RowDataSerializer(keyType);
    var rowKeySerializer = new RowDataSerializer(rowKeyType);
    var sortSerializer = new RowDataSerializer(sortType);
    var seen = new java.util.HashSet<RowData>();
    List<RowData> pending = new ArrayList<>(CHUNK_ROWS);
    try (var stream = backend.getKeys(top1 ? "Top1-Rank-State" : DATA, VoidNamespace.INSTANCE)) {
      var iterator = stream.iterator();
      while (iterator.hasNext()) {
        RowData key = keySerializer.copy(iterator.next());
        validateKey(key, keyType);
        if (!seen.add(key)) throw new IllegalStateException("Duplicate update rank partition key");
        backend.setCurrentKey(key);
        byte[] encoded = bytes(binaryKeys.toBinaryRow(key));
        if (top1) {
          var value = winner.value();
          if (value == null || value.getUserValue() == null)
            throw new IllegalStateException("Update Top-1 has an empty winner");
          validatePayload(key, null, value.getUserValue());
          RowData row = serializer.copy(value.getUserValue());
          pending.add(
              transfer(
                  row,
                  encoded,
                  bytes(rowKeySerializer.toBinaryRow(rowKeySelector.getKey(row))),
                  1));
        } else {
          var groups =
              new HashMap<BinaryRowData, TreeMap<Integer, Tuple2<BinaryRowData, RowData>>>();
          var rowKeys = new java.util.HashSet<RowData>();
          for (var entry : data.entries()) {
            var value = entry.getValue();
            if (value == null || value.f0 == null || value.f1 == null || value.f1 < 1)
              throw new IllegalStateException("Invalid update rank tuple");
            var rowKey = rowKeySerializer.toBinaryRow(entry.getKey()).copy();
            validateKey(rowKey, rowKeyType);
            if (!rowKeys.add(rowKey))
              throw new IllegalStateException("Duplicate update rank row key");
            validatePayload(key, rowKey, value.f0);
            var sort = sortSerializer.toBinaryRow(sortSelector.getKey(value.f0)).copy();
            validateKey(sort, sortType);
            var ties = groups.computeIfAbsent(sort, ignored -> new TreeMap<>());
            if (ties.put(value.f1, Tuple2.of(rowKey, serializer.copy(value.f0))) != null)
              throw new IllegalStateException("Duplicate update rank inner position");
          }
          for (var ties : groups.values()) {
            int next = 1;
            for (var entry : ties.entrySet()) {
              if (entry.getKey() != next++)
                throw new IllegalStateException("Nonconsecutive update rank inner positions");
              pending.add(
                  transfer(
                      entry.getValue().f1, encoded, bytes(entry.getValue().f0), entry.getKey()));
              if (pending.size() == CHUNK_ROWS) {
                importBatch(handle, persistent, pending, validateOnly);
                pending.clear();
              }
            }
          }
        }
        if (pending.size() == CHUNK_ROWS) {
          importBatch(handle, persistent, pending, validateOnly);
          pending.clear();
        }
      }
    }
    if (!pending.isEmpty()) importBatch(handle, persistent, pending, validateOnly);
  }

  private RowData transfer(RowData payload, byte[] key, byte[] rowKey, int inner) {
    int arity = rowType.getFieldCount();
    var row = new GenericRowData(arity + 5);
    for (int column = 0; column < arity; column++)
      row.setField(
          column,
          RowData.createFieldGetter(bridgeType.getTypeAt(column), column).getFieldOrNull(payload));
    row.setField(arity, key);
    row.setField(arity + 1, rowKey);
    row.setField(arity + 2, inner);
    row.setField(arity + 3, 0L);
    row.setField(arity + 4, payload.getRowKind().toByteValue());
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
        Native.validateUpdatingTopNCanonical(
            handle, persistent, array.memoryAddress(), schema.memoryAddress());
      else
        Native.importUpdatingTopNCanonical(
            handle, persistent, array.memoryAddress(), schema.memoryAddress());
    }
  }

  private static BinaryRowData binary(byte[] bytes, int arity) {
    var row = new BinaryRowData(arity);
    row.pointTo(MemorySegmentFactory.wrap(bytes), 0, bytes.length);
    return row;
  }

  private static byte[] bytes(BinaryRowData row) {
    return BinarySegmentUtils.copyToBytes(row.getSegments(), row.getOffset(), row.getSizeInBytes());
  }
}
