package tech.streamfusion.state;

import java.io.Serializable;
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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
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
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Stock regular-join values, unique-key maps, or full-row multisets on each input side. */
public final class StockJoinStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final int CHUNK_ROWS = 1024;
  private final TypeSerializer<RowData> keys;
  private final RowType keyType;
  private final Side left;
  private final Side right;

  public StockJoinStateContract(
      TypeSerializer<RowData> keys, RowType keyType, SideSpec left, SideSpec right) {
    this.keys = keys;
    this.keyType = keyType;
    this.left = new Side("left-records", left);
    this.right = new Side("right-records", right);
  }

  public static final class SideSpec implements Serializable {
    private static final long serialVersionUID = 1L;
    final RowType rowType;
    final boolean joinKeyUnique;
    final RowType uniqueType;
    final KeySelector<RowData, RowData> uniqueSelector;
    final boolean outer;
    final long retention;

    public SideSpec(
        RowType rowType,
        boolean joinKeyUnique,
        RowType uniqueType,
        KeySelector<RowData, RowData> uniqueSelector,
        boolean outer,
        long retention) {
      if ((uniqueType == null) != (uniqueSelector == null))
        throw new IllegalArgumentException(
            "Join unique-key type and selector must be bound together");
      this.rowType = rowType;
      this.joinKeyUnique = joinKeyUnique;
      this.uniqueType = uniqueType;
      this.uniqueSelector = uniqueSelector;
      this.outer = outer;
      this.retention = retention;
    }
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(left.name, left.kind(), right.name, right.kind());
  }

  @Override
  public void clear(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    left.clear(backend);
    right.clear(backend);
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    clear(backend);
    left.exportState(backend, handle, persistent, true, keyType);
    right.exportState(backend, handle, persistent, false, keyType);
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    left.importState(backend, handle, persistent, true, keyType, true);
    right.importState(backend, handle, persistent, false, keyType, true);
    left.importState(backend, handle, persistent, true, keyType, false);
    right.importState(backend, handle, persistent, false, keyType, false);
  }

  private static final class Side implements Serializable {
    private static final long serialVersionUID = 1L;
    final String name;
    final RowType rowType;
    final RowType bridgeType;
    final RowType uniqueType;
    final KeySelector<RowData, RowData> uniqueSelector;
    final boolean scalar;
    final boolean multiset;
    final boolean outer;
    final boolean ttl;

    Side(String name, SideSpec spec) {
      this.name = name;
      rowType = spec.rowType;
      outer = spec.outer;
      ttl = spec.retention > 0;
      scalar = spec.joinKeyUnique;
      multiset = !scalar && spec.uniqueType == null;
      uniqueType = !scalar && !multiset ? spec.uniqueType : null;
      uniqueSelector = !scalar && !multiset ? spec.uniqueSelector : null;
      var fields = new ArrayList<>(rowType.getFields());
      for (LogicalType type :
          List.of(
              new VarBinaryType(false, VarBinaryType.MAX_LENGTH),
              new BigIntType(false),
              new IntType(false),
              new BigIntType(false))) {
        String fieldName = "$stock_meta_" + fields.size();
        var names =
            fields.stream()
                .map(RowType.RowField::getName)
                .collect(java.util.stream.Collectors.toSet());
        while (names.contains(fieldName)) fieldName = "$" + fieldName;
        fields.add(new RowType.RowField(fieldName, type));
      }
      bridgeType = new RowType(fields);
    }

    StateDescriptor.Type kind() {
      return scalar ? StateDescriptor.Type.VALUE : StateDescriptor.Type.MAP;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    TypeSerializer<Object> values() {
      TypeSerializer<?> value = multiset ? IntSerializer.INSTANCE : new RowDataSerializer(rowType);
      if (outer)
        value =
            new TupleSerializer(
                (Class) Tuple2.class, new TypeSerializer<?>[] {value, IntSerializer.INSTANCE});
      return (TypeSerializer<Object>) value;
    }

    StockValueState<Object> scalar(CheckpointableKeyedStateBackend<RowData> backend)
        throws Exception {
      return new StockValueState<>(backend, name, values(), ttl);
    }

    @SuppressWarnings("unchecked")
    MapState<RowData, Object> map(CheckpointableKeyedStateBackend<RowData> backend)
        throws Exception {
      TypeSerializer<?> serializer = values();
      if (ttl) serializer = new TtlSerializer<>(LongSerializer.INSTANCE, serializer);
      return backend.getPartitionedState(
          VoidNamespace.INSTANCE,
          VoidNamespaceSerializer.INSTANCE,
          new MapStateDescriptor<>(
              name,
              new RowDataSerializer(multiset ? rowType : uniqueType),
              (TypeSerializer<Object>) serializer));
    }

    void clear(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
      var value = scalar ? scalar(backend) : null;
      var map = scalar ? null : map(backend);
      List<RowData> keys;
      try (var stream = backend.getKeys(name, VoidNamespace.INSTANCE)) {
        keys =
            stream
                .map(key -> backend.getKeySerializer().copy(key))
                .collect(java.util.stream.Collectors.toList());
      }
      for (var key : keys) {
        backend.setCurrentKey(key);
        if (scalar) value.clear();
        else map.clear();
      }
    }

    void exportState(
        CheckpointableKeyedStateBackend<RowData> backend,
        long handle,
        boolean persistent,
        boolean left,
        RowType keyType)
        throws Exception {
      var scalarState = scalar ? scalar(backend) : null;
      var mapState = scalar ? null : map(backend);
      var rows = new RowDataSerializer(rowType);
      var mapKeys = scalar ? null : new RowDataSerializer(multiset ? rowType : uniqueType);
      int arity = rowType.getFieldCount();
      Native.exportUpdatingJoinCanonical(
          handle,
          persistent,
          left,
          (arrayAddress, schemaAddress) -> {
            try (ArrowArray array = ArrowArray.wrap(arrayAddress);
                ArrowSchema schema = ArrowSchema.wrap(schemaAddress);
                VectorSchemaRoot root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              var reader = ArrowConversion.createArrowReader(root, bridgeType);
              for (int index = 0; index < root.getRowCount(); index++) {
                RowData row = reader.read(index);
                byte[] bytes = row.getBinary(arity);
                var key = new BinaryRowData(keyType.getFieldCount());
                key.pointTo(MemorySegmentFactory.wrap(bytes), 0, bytes.length);
                backend.setCurrentKey(key);
                int count = Math.toIntExact(row.getLong(arity + 1));
                if (count <= 0 || !multiset && count != 1)
                  throw new IllegalStateException(
                      "Join row multiplicity does not fit the stock state shape");
                RowData payload = rows.copy(payload(row));
                int degree = row.getInt(arity + 2);
                long written = row.getLong(arity + 3);
                Object stored =
                    outer
                        ? Tuple2.of(multiset ? count : payload, degree)
                        : multiset ? count : payload;
                if (scalar) {
                  if (scalarState.value() != null)
                    throw new IllegalStateException("Multiple join rows under a unique join key");
                  scalarState.update(stored, written);
                } else {
                  RowData mapKey = multiset ? payload : uniqueSelector.getKey(payload);
                  mapKey = mapKeys.toBinaryRow(mapKey).copy();
                  if (mapState.contains(mapKey))
                    throw new IllegalStateException("Duplicate stock join map key");
                  mapState.put(mapKey, ttl ? new TtlValue<>(stored, written) : stored);
                }
              }
            } catch (Exception failure) {
              throw new IllegalStateException(
                  "Cannot project stock join state for " + name, failure);
            }
          });
    }

    RowData payload(RowData row) {
      var payload = new GenericRowData(rowType.getFieldCount());
      for (int field = 0; field < rowType.getFieldCount(); field++)
        payload.setField(
            field, RowData.createFieldGetter(rowType.getTypeAt(field), field).getFieldOrNull(row));
      return payload;
    }

    void importState(
        CheckpointableKeyedStateBackend<RowData> backend,
        long handle,
        boolean persistent,
        boolean left,
        RowType keyType,
        boolean validateOnly)
        throws Exception {
      var scalarState = scalar ? scalar(backend) : null;
      var mapState = scalar ? null : map(backend);
      var keySerializer = new RowDataSerializer(keyType);
      var rows = new RowDataSerializer(rowType);
      var uniqueKeys = uniqueType == null ? null : new RowDataSerializer(uniqueType);
      var pending = new ArrayList<RowData>(CHUNK_ROWS);
      try (var stream = backend.getKeys(name, VoidNamespace.INSTANCE)) {
        var iterator = stream.iterator();
        while (iterator.hasNext()) {
          RowData key = iterator.next();
          backend.setCurrentKey(key);
          BinaryRowData binary = keySerializer.toBinaryRow(key);
          byte[] encoded =
              BinarySegmentUtils.copyToBytes(
                  binary.getSegments(), binary.getOffset(), binary.getSizeInBytes());
          if (scalar) {
            var value = scalarState.value();
            if (value != null)
              pending.add(
                  transfer(
                      null, value.getUserValue(), value.getLastAccessTimestamp(), encoded, rows));
          } else {
            for (var entry : mapState.entries()) {
              Object raw = entry.getValue();
              long written = ttl ? ((TtlValue<?>) raw).getLastAccessTimestamp() : 0;
              Object value = ttl ? ((TtlValue<?>) raw).getUserValue() : raw;
              RowData transferred = transfer(entry.getKey(), value, written, encoded, rows);
              if (!multiset) {
                RowData payload = (RowData) (outer ? ((Tuple2<?, ?>) value).f0 : value);
                if (!uniqueKeys
                    .toBinaryRow(entry.getKey())
                    .equals(uniqueKeys.toBinaryRow(uniqueSelector.getKey(payload))))
                  throw new IllegalStateException("Join unique key differs from its payload");
              }
              pending.add(transferred);
              if (pending.size() == CHUNK_ROWS) {
                importBatch(handle, persistent, left, pending, validateOnly);
                pending.clear();
              }
            }
          }
          if (pending.size() == CHUNK_ROWS) {
            importBatch(handle, persistent, left, pending, validateOnly);
            pending.clear();
          }
        }
      }
      if (!pending.isEmpty()) importBatch(handle, persistent, left, pending, validateOnly);
    }

    RowData transfer(
        RowData mapKey, Object stored, long written, byte[] key, RowDataSerializer serializer) {
      Object value = outer ? ((Tuple2<?, ?>) stored).f0 : stored;
      int count = multiset ? (Integer) value : 1;
      int degree = outer ? (Integer) ((Tuple2<?, ?>) stored).f1 : -1;
      RowData payload = multiset ? mapKey : (RowData) value;
      if (count <= 0 || payload.getRowKind() != RowKind.INSERT)
        throw new IllegalStateException("Unsupported join row kind or multiplicity");
      payload = serializer.copy(payload);
      int arity = rowType.getFieldCount();
      var row = new GenericRowData(arity + 4);
      for (int field = 0; field < arity; field++)
        row.setField(
            field,
            RowData.createFieldGetter(rowType.getTypeAt(field), field).getFieldOrNull(payload));
      row.setField(arity, key);
      row.setField(arity + 1, (long) count);
      row.setField(arity + 2, degree);
      row.setField(arity + 3, written);
      return row;
    }

    void importBatch(
        long handle, boolean persistent, boolean left, List<RowData> rows, boolean validateOnly) {
      try (VectorSchemaRoot root =
              RowDataArrowConverter.write(rows, bridgeType, NativeAllocator.SHARED);
          ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
          ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
        Data.exportVectorSchemaRoot(
            NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
        if (validateOnly)
          Native.validateUpdatingJoinCanonical(
              handle, persistent, left, array.memoryAddress(), schema.memoryAddress());
        else
          Native.importUpdatingJoinCanonical(
              handle, persistent, left, array.memoryAddress(), schema.memoryAddress());
      }
    }
  }
}
