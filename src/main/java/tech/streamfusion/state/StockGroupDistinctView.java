package tech.streamfusion.state;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released shared DISTINCT views over native grouped membership tables. */
public final class StockGroupDistinctView implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final int CHUNK_ROWS = 1024;
  private final String name;
  private final TypeSerializer<Object> valueKeys;
  private final TypeSerializer<Object> counters;
  private final int[] aggregates;
  private final boolean counted;
  private final RowType bridgeType;

  @SuppressWarnings("unchecked")
  public StockGroupDistinctView(
      String name,
      LogicalType valueType,
      TypeSerializer<?> valueKeys,
      TypeSerializer<?> counters,
      int[] aggregates,
      boolean counted) {
    if (aggregates.length == 0) throw new IllegalArgumentException("Empty DISTINCT view");
    this.name = name;
    this.valueKeys = (TypeSerializer<Object>) valueKeys.duplicate();
    this.counters = (TypeSerializer<Object>) counters.duplicate();
    this.aggregates = aggregates.clone();
    this.counted = counted;
    bridgeType =
        RowType.of(
            new VarBinaryType(false, VarBinaryType.MAX_LENGTH),
            valueType.copy(true),
            new BigIntType(false));
  }

  @FunctionalInterface
  interface KeyValidator {
    void validate(RowData key);
  }

  Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(name, StateDescriptor.Type.MAP, name + "_null_state", StateDescriptor.Type.VALUE);
  }

  private MapState<Object, Object> state(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new ValueStateDescriptor<>(name + "_null_state", counters));
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new MapStateDescriptor<>(name, valueKeys, counters));
  }

  private org.apache.flink.api.common.state.ValueState<Object> nullState(
      CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new ValueStateDescriptor<>(name + "_null_state", counters));
  }

  void clear(CheckpointableKeyedStateBackend<RowData> backend, RowType keyType) throws Exception {
    var rows = new RowDataSerializer(keyType);
    var map = state(backend);
    List<RowData> keys = new ArrayList<>();
    try (var stream = backend.getKeys(name, VoidNamespace.INSTANCE)) {
      stream.forEach(key -> keys.add(rows.copy(key)));
    }
    for (var key : keys) {
      backend.setCurrentKey(key);
      map.clear();
    }
    keys.clear();
    var marker =
        backend.getPartitionedState(
            VoidNamespace.INSTANCE,
            VoidNamespaceSerializer.INSTANCE,
            new ValueStateDescriptor<>(name + "_null_state", counters));
    try (var stream = backend.getKeys(name + "_null_state", VoidNamespace.INSTANCE)) {
      stream.forEach(key -> keys.add(rows.copy(key)));
    }
    for (var key : keys) {
      backend.setCurrentKey(key);
      marker.clear();
    }
  }

  void exportState(
      CheckpointableKeyedStateBackend<RowData> backend,
      RowType keyType,
      long handle,
      boolean persistent)
      throws Exception {
    var map = state(backend);
    var marker = nullState(backend);
    var getter = RowData.createFieldGetter(bridgeType.getTypeAt(1), 1);
    for (int slot = 0; slot < aggregates.length; slot++) {
      final int position = slot;
      Native.exportGroupTypedCanonical(
          handle,
          persistent,
          aggregates[slot],
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var schema = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              if (root.getFieldVectors().size() != 3)
                throw new IllegalStateException("Unexpected native DISTINCT frame");
              var reader = ArrowConversion.createArrowReader(root, bridgeType);
              for (int i = 0; i < root.getRowCount(); i++) {
                var row = reader.read(i);
                byte[] bytes = row.getBinary(0);
                var key = new BinaryRowData(keyType.getFieldCount());
                key.pointTo(MemorySegmentFactory.wrap(bytes), 0, bytes.length);
                backend.setCurrentKey(key);
                Object value = getter.getFieldOrNull(row);
                long count = row.getLong(2);
                Object previous = value == null ? marker.value() : map.get(value);
                Object merged;
                if (counted && aggregates.length == 1) merged = count;
                else if (counted) {
                  long[] counts =
                      previous == null ? new long[aggregates.length] : ((long[]) previous).clone();
                  counts[position] = count;
                  merged = counts;
                } else if (count != 1)
                  throw new IllegalStateException("Invalid native membership bit");
                else if (aggregates.length <= Long.SIZE)
                  merged = (previous == null ? 0L : (Long) previous) | (1L << position);
                else {
                  long[] bits =
                      previous == null
                          ? new long[aggregates.length / Long.SIZE + 1]
                          : ((long[]) previous).clone();
                  bits[position / Long.SIZE] |= 1L << (position % Long.SIZE);
                  merged = bits;
                }
                if (value == null) marker.update(merged);
                else map.put(valueKeys.copy(value), merged);
              }
            } catch (Exception failure) {
              throw new IllegalStateException(
                  "Cannot export grouped DISTINCT view " + name, failure);
            }
          });
    }
  }

  private long membership(Object value, int slot) {
    if (counted) {
      if (aggregates.length == 1) {
        if (!(value instanceof Long)) throw new IllegalStateException("Invalid DISTINCT count");
        return (Long) value;
      }
      if (!(value instanceof long[]) || ((long[]) value).length != aggregates.length)
        throw new IllegalStateException("Invalid DISTINCT count array");
      return ((long[]) value)[slot];
    }
    if (aggregates.length <= Long.SIZE) {
      if (!(value instanceof Long)
          || (aggregates.length < Long.SIZE && ((Long) value) >>> aggregates.length != 0))
        throw new IllegalStateException("Invalid DISTINCT membership bits");
      return ((Long) value) >>> slot & 1;
    }
    if (!(value instanceof long[]) || ((long[]) value).length != aggregates.length / Long.SIZE + 1)
      throw new IllegalStateException("Invalid DISTINCT membership words");
    long[] bits = (long[]) value;
    if (bits[bits.length - 1] >>> (aggregates.length % Long.SIZE) != 0)
      throw new IllegalStateException("Unexpected DISTINCT membership bits");
    return bits[slot / Long.SIZE] >>> (slot % Long.SIZE) & 1;
  }

  void importState(
      CheckpointableKeyedStateBackend<RowData> backend,
      RowType keyType,
      StockValueState<RowData> accumulators,
      int[] scalarFields,
      long handle,
      boolean persistent,
      boolean validating,
      KeyValidator validate)
      throws Exception {
    var map = state(backend);
    var marker = nullState(backend);
    var serializer = new RowDataSerializer(keyType);
    var rows = new ArrayList<RowData>(CHUNK_ROWS);
    for (int slot = 0; slot < aggregates.length; slot++) {
      try (var stream = backend.getKeys(name, VoidNamespace.INSTANCE)) {
        var iterator = stream.iterator();
        while (iterator.hasNext()) {
          RowData key = iterator.next();
          validate.validate(key);
          backend.setCurrentKey(key);
          var entries = map.entries();
          if (entries == null) continue;
          var wrapped = accumulators.value();
          var binary = serializer.toBinaryRow(key);
          byte[] bytes =
              BinarySegmentUtils.copyToBytes(
                  binary.getSegments(), binary.getOffset(), binary.getSizeInBytes());
          for (var entry : entries) {
            if (entry.getKey() == null) continue;
            if (wrapped == null) throw new IllegalStateException("Orphan DISTINCT view " + name);
            long count = membership(entry.getValue(), slot);
            if (!counted && count == 0) continue;
            rows.add(GenericRowData.of(bytes, valueKeys.copy(entry.getKey()), count));
            if (rows.size() == CHUNK_ROWS) {
              batch(rows, handle, persistent, aggregates[slot], validating);
              rows.clear();
            }
          }
        }
      }
      try (var stream = backend.getKeys(name + "_null_state", VoidNamespace.INSTANCE)) {
        var iterator = stream.iterator();
        while (iterator.hasNext()) {
          RowData key = iterator.next();
          validate.validate(key);
          backend.setCurrentKey(key);
          Object value = marker.value();
          if (value == null) continue;
          if (accumulators.value() == null)
            throw new IllegalStateException("Orphan DISTINCT NULL marker " + name);
          long count = membership(value, slot);
          if (!counted && count == 0) continue;
          var binary = serializer.toBinaryRow(key);
          rows.add(
              GenericRowData.of(
                  BinarySegmentUtils.copyToBytes(
                      binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()),
                  null,
                  count));
          if (rows.size() == CHUNK_ROWS) {
            batch(rows, handle, persistent, aggregates[slot], validating);
            rows.clear();
          }
        }
      }
      if (!counted) {
        try (var stream = backend.getKeys("accState", VoidNamespace.INSTANCE)) {
          var iterator = stream.iterator();
          while (iterator.hasNext()) {
            backend.setCurrentKey(iterator.next());
            var accumulator = accumulators.value();
            if (accumulator == null) continue;
            long cardinality = 0;
            var entries = map.entries();
            if (entries != null)
              for (var entry : entries)
                if (entry.getKey() != null && membership(entry.getValue(), slot) != 0)
                  cardinality++;
            if (accumulator.getUserValue().getLong(scalarFields[aggregates[slot]]) != cardinality)
              throw new IllegalStateException("DISTINCT scalar and membership differ");
          }
        }
      }
      batch(rows, handle, persistent, aggregates[slot], validating);
      rows.clear();
    }
  }

  private void batch(
      List<RowData> rows, long handle, boolean persistent, int slot, boolean validating) {
    if (rows.isEmpty()) return;
    try (var root = RowDataArrowConverter.write(rows, bridgeType, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      Native.transferGroupTypedCanonical(
          handle, persistent, slot, array.memoryAddress(), schema.memoryAddress(), validating);
    }
  }
}
