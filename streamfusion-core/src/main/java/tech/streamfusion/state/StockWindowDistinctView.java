package tech.streamfusion.state;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongPredicate;
import java.util.function.LongUnaryOperator;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released append-only COUNT DISTINCT membership, shared by aggregates within each slice. */
public final class StockWindowDistinctView implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final int CHUNK_ROWS = 1024;
  private final String name;
  private final LogicalType valueType;
  private final TypeSerializer<Object> valueKeys;
  private final TypeSerializer<Object> counters;
  private final int[] aggregates;
  private final boolean counted;

  public StockWindowDistinctView(
      String name,
      LogicalType valueType,
      TypeSerializer<?> valueKeys,
      TypeSerializer<?> counters,
      int[] aggregates) {
    this(name, valueType, valueKeys, counters, aggregates, false);
  }

  @SuppressWarnings("unchecked")
  public StockWindowDistinctView(
      String name,
      LogicalType valueType,
      TypeSerializer<?> valueKeys,
      TypeSerializer<?> counters,
      int[] aggregates,
      boolean counted) {
    if (aggregates.length == 0) throw new IllegalArgumentException("Empty window DISTINCT view");
    this.name = name;
    this.valueType = valueType;
    this.valueKeys = (TypeSerializer<Object>) valueKeys.duplicate();
    this.counters = (TypeSerializer<Object>) counters.duplicate();
    this.aggregates = aggregates.clone();
    this.counted = counted;
  }

  interface Namespaces<N> {
    TypeSerializer<N> serializer();

    N stock(long end, RowData key) throws Exception;

    long nativeEnd(N namespace, RowData key) throws Exception;

    boolean live(N namespace);
  }

  private static Namespaces<Long> fixedNamespaces(
      LongUnaryOperator toStock, LongUnaryOperator toNative, LongPredicate live) {
    return new Namespaces<Long>() {
      public TypeSerializer<Long> serializer() {
        return LongSerializer.INSTANCE;
      }

      public Long stock(long end, RowData key) {
        return toStock.applyAsLong(end);
      }

      public long nativeEnd(Long namespace, RowData key) {
        return toNative.applyAsLong(namespace);
      }

      public boolean live(Long namespace) {
        return live.test(namespace);
      }
    };
  }

  void configure(long handle) {
    configure(handle, false);
  }

  void configure(long handle, boolean session) {
    for (int aggregate : aggregates) {
      if (session) Native.bindSessionCanonicalDistinct(handle, aggregate);
      else Native.bindWindowCanonicalDistinct(handle, aggregate);
    }
  }

  Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(name, StateDescriptor.Type.MAP, name + "_null_state", StateDescriptor.Type.VALUE);
  }

  private <N> MapState<Object, Object> state(
      CheckpointableKeyedStateBackend<RowData> backend, N namespace, TypeSerializer<N> serializer)
      throws Exception {
    backend.getPartitionedState(
        namespace, serializer, new ValueStateDescriptor<>(name + "_null_state", counters));
    return backend.getPartitionedState(
        namespace, serializer, new MapStateDescriptor<>(name, valueKeys, counters));
  }

  private RowType bridgeType(RowType keys) {
    var fields = new ArrayList<RowType.RowField>();
    fields.add(new RowType.RowField("window_end", new BigIntType(false)));
    for (int i = 0; i < keys.getFieldCount(); i++)
      fields.add(new RowType.RowField("key" + i, keys.getTypeAt(i)));
    fields.add(new RowType.RowField("value", valueType.copy(true)));
    if (counted) fields.add(new RowType.RowField("count", new BigIntType(false)));
    return new RowType(fields);
  }

  void clear(CheckpointableKeyedStateBackend<RowData> backend, RowType keyType) throws Exception {
    clear(backend, keyType, LongSerializer.INSTANCE);
  }

  <N> void clear(
      CheckpointableKeyedStateBackend<RowData> backend,
      RowType keyType,
      TypeSerializer<N> namespaces)
      throws Exception {
    var serializer = new RowDataSerializer(keyType);
    for (String stateName : stateDescriptors().keySet()) {
      var entries = new ArrayList<org.apache.flink.api.java.tuple.Tuple2<RowData, N>>();
      try (var stream = backend.<N>getKeysAndNamespaces(stateName)) {
        stream.forEach(
            entry ->
                entries.add(
                    org.apache.flink.api.java.tuple.Tuple2.of(
                        serializer.toBinaryRow(entry.f0).copy(), entry.f1)));
      }
      for (var entry : entries) {
        backend.setCurrentKey(entry.f0);
        if (stateName.equals(name)) state(backend, entry.f1, namespaces).clear();
        else
          backend
              .getPartitionedState(
                  entry.f1, namespaces, new ValueStateDescriptor<>(stateName, counters))
              .clear();
      }
    }
  }

  void exportState(
      CheckpointableKeyedStateBackend<RowData> backend,
      RowType keyType,
      long handle,
      LongUnaryOperator namespace)
      throws Exception {
    exportState(
        backend, keyType, handle, fixedNamespaces(namespace, value -> value, value -> true), false);
  }

  <N> void exportState(
      CheckpointableKeyedStateBackend<RowData> backend,
      RowType keyType,
      long handle,
      Namespaces<N> namespaces,
      boolean session)
      throws Exception {
    var type = bridgeType(keyType);
    var keyRows = new StockKeyProjection(keyType);
    var keyGetters = new RowData.FieldGetter[keyType.getFieldCount()];
    for (int i = 0; i < keyGetters.length; i++)
      keyGetters[i] = RowData.createFieldGetter(keyType.getTypeAt(i), 1 + i);
    var valueGetter = RowData.createFieldGetter(valueType, keyGetters.length + 1);
    for (int slot = 0; slot < aggregates.length; slot++) {
      final int position = slot;
      Native.BatchReceiver receiver =
          (arrayAddress, schemaAddress) -> {
            try (var array = ArrowArray.wrap(arrayAddress);
                var schema = ArrowSchema.wrap(schemaAddress);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
              var reader = ArrowConversion.createArrowReader(root, type);
              if (root.getFieldVectors().size() != type.getFieldCount())
                throw new IllegalStateException(
                    "Window DISTINCT counter layout differs from its native factory");
              for (int row = 0; row < root.getRowCount(); row++) {
                var input = reader.read(row);
                var key = new GenericRowData(keyGetters.length);
                for (int i = 0; i < keyGetters.length; i++)
                  key.setField(i, keyGetters[i].getFieldOrNull(input));
                backend.setCurrentKey(keyRows.project(key));
                var view =
                    state(
                        backend, namespaces.stock(input.getLong(0), key), namespaces.serializer());
                Object value = valueGetter.getFieldOrNull(input);
                if (value == null)
                  throw new IllegalStateException("NULL window DISTINCT membership");
                Object previous = view.get(value);
                Object merged;
                if (counted && aggregates.length == 1)
                  merged = input.getLong(keyGetters.length + 2);
                else if (counted) {
                  long[] counts =
                      previous == null ? new long[aggregates.length] : ((long[]) previous).clone();
                  counts[position] = input.getLong(keyGetters.length + 2);
                  merged = counts;
                } else if (aggregates.length <= Long.SIZE)
                  merged = (previous == null ? 0L : (Long) previous) | 1L << position;
                else {
                  long[] bits =
                      previous == null
                          ? new long[aggregates.length / Long.SIZE + 1]
                          : ((long[]) previous).clone();
                  bits[position / Long.SIZE] |= 1L << (position % Long.SIZE);
                  merged = bits;
                }
                view.put(valueKeys.copy(value), merged);
              }
            } catch (Exception failure) {
              throw new IllegalStateException(
                  "Cannot export window DISTINCT view " + name, failure);
            }
          };
      if (session) Native.exportSessionCanonicalDistinct(handle, aggregates[slot], receiver);
      else Native.exportWindowCanonicalDistinct(handle, aggregates[slot], receiver);
    }
  }

  private long membership(Object value, int slot) {
    if (counted) {
      if (aggregates.length == 1) {
        if (!(value instanceof Long))
          throw new IllegalStateException("Invalid window DISTINCT multiplicity");
        return (Long) value;
      }
      if (!(value instanceof long[]) || ((long[]) value).length != aggregates.length)
        throw new IllegalStateException("Invalid window DISTINCT counter array");
      return ((long[]) value)[slot];
    }
    if (aggregates.length <= Long.SIZE) {
      if (!(value instanceof Long)
          || (aggregates.length < Long.SIZE && ((Long) value) >>> aggregates.length != 0))
        throw new IllegalStateException(
            "Invalid window DISTINCT membership bits: " + value + ", slots=" + aggregates.length);
      return ((Long) value) >>> slot & 1;
    }
    if (!(value instanceof long[]) || ((long[]) value).length != aggregates.length / Long.SIZE + 1)
      throw new IllegalStateException("Invalid window DISTINCT membership words");
    long[] bits = (long[]) value;
    if (bits[bits.length - 1] >>> (aggregates.length % Long.SIZE) != 0)
      throw new IllegalStateException("Unexpected window DISTINCT membership bits");
    return bits[slot / Long.SIZE] >>> (slot % Long.SIZE) & 1;
  }

  void importState(
      CheckpointableKeyedStateBackend<RowData> backend,
      RowType keyType,
      long handle,
      ValueStateDescriptor<RowData> main,
      LongUnaryOperator namespace)
      throws Exception {
    importState(backend, keyType, handle, main, namespace, ignored -> true);
  }

  <N> void validateState(
      CheckpointableKeyedStateBackend<RowData> backend,
      ValueStateDescriptor<RowData> main,
      Namespaces<N> namespaces)
      throws Exception {
    for (String stateName : stateDescriptors().keySet()) {
      try (var stream = backend.<N>getKeysAndNamespaces(stateName)) {
        var iterator = stream.iterator();
        while (iterator.hasNext()) {
          var entry = iterator.next();
          backend.setCurrentKey(entry.f0);
          if (backend.getPartitionedState(entry.f1, namespaces.serializer(), main).value() == null)
            throw new IllegalStateException("Window DISTINCT view has no scalar accumulator");
          if (!stateName.equals(name)) {
            // COUNT ignores NULL in both bit-flag and multiplicity layouts. Bind the marker's
            // exact serializer without interpreting it as non-NULL membership.
            backend
                .getPartitionedState(
                    entry.f1,
                    namespaces.serializer(),
                    new ValueStateDescriptor<>(stateName, counters))
                .value();
          }
        }
      }
    }
    try (var stream = backend.<N>getKeysAndNamespaces(name)) {
      var iterator = stream.iterator();
      while (iterator.hasNext()) {
        var entry = iterator.next();
        backend.setCurrentKey(entry.f0);
        for (var member : state(backend, entry.f1, namespaces.serializer()).entries()) {
          if (member.getKey() == null)
            throw new IllegalStateException("NULL in window DISTINCT map");
          for (int slot = 0; slot < aggregates.length; slot++) membership(member.getValue(), slot);
        }
      }
    }
  }

  void importState(
      CheckpointableKeyedStateBackend<RowData> backend,
      RowType keyType,
      long handle,
      ValueStateDescriptor<RowData> main,
      LongUnaryOperator namespace,
      LongPredicate liveWindow)
      throws Exception {
    importState(
        backend,
        keyType,
        handle,
        main,
        fixedNamespaces(value -> value, namespace, liveWindow),
        false);
  }

  <N> void importState(
      CheckpointableKeyedStateBackend<RowData> backend,
      RowType keyType,
      long handle,
      ValueStateDescriptor<RowData> main,
      Namespaces<N> namespaces,
      boolean session)
      throws Exception {
    validateState(backend, main, namespaces);
    var type = bridgeType(keyType);
    var keyRows = new RowDataSerializer(keyType);
    var getters = new RowData.FieldGetter[keyType.getFieldCount()];
    for (int i = 0; i < getters.length; i++)
      getters[i] = RowData.createFieldGetter(keyType.getTypeAt(i), i);
    for (int slot = 0; slot < aggregates.length; slot++) {
      var rows = new ArrayList<RowData>(CHUNK_ROWS);
      try (var stream = backend.<N>getKeysAndNamespaces(name)) {
        var iterator = stream.iterator();
        while (iterator.hasNext()) {
          var entry = iterator.next();
          boolean live = namespaces.live(entry.f1);
          RowData key = keyRows.toBinaryRow(entry.f0).copy();
          backend.setCurrentKey(key);
          for (var member : state(backend, entry.f1, namespaces.serializer()).entries()) {
            if (member.getKey() == null)
              throw new IllegalStateException("NULL in window DISTINCT map");
            long count = membership(member.getValue(), slot);
            if (!live || count == 0) continue;
            var row = new GenericRowData(type.getFieldCount());
            row.setField(0, namespaces.nativeEnd(entry.f1, key));
            for (int i = 0; i < getters.length; i++)
              row.setField(1 + i, getters[i].getFieldOrNull(key));
            row.setField(1 + getters.length, valueKeys.copy(member.getKey()));
            if (counted) row.setField(2 + getters.length, count);
            rows.add(row);
            if (rows.size() == CHUNK_ROWS) {
              importBatch(rows, type, handle, aggregates[slot], session);
              rows.clear();
            }
          }
        }
      }
      if (!rows.isEmpty()) importBatch(rows, type, handle, aggregates[slot], session);
    }
  }

  private static void importBatch(
      List<RowData> rows, RowType type, long handle, int aggregate, boolean session) {
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (session)
        Native.importSessionCanonicalDistinct(
            handle, aggregate, array.memoryAddress(), schema.memoryAddress());
      else
        Native.importWindowCanonicalDistinct(
            handle, aggregate, array.memoryAddress(), schema.memoryAddress());
    }
  }
}
