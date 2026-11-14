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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released integral DISTINCT memberships alongside an unbounded scalar fold. */
public final class StockOverDistinctView implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final int CHUNK_ROWS = 1024;
  private final String name;
  private final TypeSerializer<Object> values, bits;
  private final int[] aggregates;
  private final RowType keyType, bridge;

  @SuppressWarnings("unchecked")
  public StockOverDistinctView(
      String name,
      RowType keyType,
      LogicalType valueType,
      TypeSerializer<?> values,
      TypeSerializer<?> bits,
      int[] aggregates) {
    if (!valueType.is(LogicalTypeRoot.INTEGER) && !valueType.is(LogicalTypeRoot.BIGINT)
        || aggregates.length == 0
        || aggregates.length > Long.SIZE
        || java.util.Arrays.stream(aggregates).anyMatch(index -> index < 0)
        || java.util.Arrays.stream(aggregates).distinct().count() != aggregates.length)
      throw new IllegalArgumentException("Unqualified OVER DISTINCT membership schema");
    this.name = name;
    this.keyType = keyType;
    this.values = (TypeSerializer<Object>) values.duplicate();
    this.bits = (TypeSerializer<Object>) bits.duplicate();
    this.aggregates = aggregates.clone();
    var fields = new ArrayList<RowType.RowField>();
    for (int index = 0; index < keyType.getFieldCount(); index++)
      fields.add(new RowType.RowField("key" + index, keyType.getTypeAt(index).copy(true)));
    fields.add(new RowType.RowField("value", valueType));
    bridge = new RowType(fields);
  }

  void validateAggregates(int[] kinds, boolean[] bound) {
    for (int index : aggregates) {
      if (index >= kinds.length || !supportsAggregate(kinds[index]) || bound[index])
        throw new IllegalArgumentException("Unqualified or duplicate OVER DISTINCT aggregate");
      bound[index] = true;
    }
  }

  public static boolean supportsAggregate(int kind) {
    return kind == 100 || kind == 103 || kind == 104 || kind == 122;
  }

  Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(name, StateDescriptor.Type.MAP, name + "_null_state", StateDescriptor.Type.VALUE);
  }

  void configure(long handle) {
    for (int index : aggregates) Native.bindOverCanonicalDistinct(handle, index);
  }

  private MapState<Object, Object> state(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    nullState(backend);
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new MapStateDescriptor<>(name, values, bits));
  }

  private ValueState<Object> nullState(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new ValueStateDescriptor<>(name + "_null_state", bits));
  }

  void requireEmpty(long handle) {
    for (int index : aggregates)
      visit(
          handle,
          index,
          row -> {
            throw new IllegalStateException(
                "OVER DISTINCT import requires an empty membership target");
          });
  }

  @FunctionalInterface
  private interface Receiver {
    void accept(RowData row) throws Exception;
  }

  private void visit(long handle, int index, Receiver receiver) {
    Native.exportOverCanonicalDistinct(
        handle,
        index,
        (arrayAddress, schemaAddress) -> {
          try (var array = ArrowArray.wrap(arrayAddress);
              var schema = ArrowSchema.wrap(schemaAddress);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            var expected = ArrowConversion.toArrowSchema(bridge).getFields();
            if (root.getRowCount() > CHUNK_ROWS || root.getFieldVectors().size() != expected.size())
              throw new IllegalStateException("OVER DISTINCT transfer arity changed");
            for (int field = 0; field < expected.size(); field++)
              if (!root.getSchema()
                  .getFields()
                  .get(field)
                  .getType()
                  .equals(expected.get(field).getType()))
                throw new IllegalStateException("OVER DISTINCT transfer type changed");
            var reader = ArrowConversion.createArrowReader(root, bridge);
            for (int row = 0; row < root.getRowCount(); row++) receiver.accept(reader.read(row));
          } catch (Exception failure) {
            throw new IllegalStateException("Cannot export OVER DISTINCT view " + name, failure);
          }
        });
  }

  void exportState(CheckpointableKeyedStateBackend<RowData> backend, long handle) throws Exception {
    var state = state(backend);
    var marker = nullState(backend);
    var keySerializer = new RowDataSerializer(keyType);
    var getter =
        RowData.createFieldGetter(
            bridge.getTypeAt(keyType.getFieldCount()).copy(true), keyType.getFieldCount());
    for (int slot = 0; slot < aggregates.length; slot++) {
      final long flag = 1L << slot;
      visit(
          handle,
          aggregates[slot],
          row -> {
            var key = new GenericRowData(keyType.getFieldCount());
            for (int field = 0; field < key.getArity(); field++)
              key.setField(
                  field,
                  RowData.createFieldGetter(keyType.getTypeAt(field).copy(true), field)
                      .getFieldOrNull(row));
            backend.setCurrentKey(keySerializer.toBinaryRow(key).copy());
            Object member = getter.getFieldOrNull(row);
            if (member == null) {
              if (!bridge.getTypeAt(keyType.getFieldCount()).isNullable())
                throw new IllegalStateException("NULL in a required DISTINCT argument");
              Object previous = marker.value();
              marker.update((previous == null ? 0L : (Long) previous) | flag);
            } else {
              Object previous = state.get(member);
              state.put(values.copy(member), (previous == null ? 0L : (Long) previous) | flag);
            }
          });
    }
  }

  void validate(
      CheckpointableKeyedStateBackend<RowData> backend,
      StockValueState<RowData> accumulators,
      int[] scalarFields,
      int[] kinds,
      RowType accumulatorType,
      java.util.function.Consumer<RowData> keyValidator)
      throws Exception {
    var map = state(backend);
    var marker = nullState(backend);
    for (String owner : List.of("accState", name, name + "_null_state"))
      try (var stream = backend.getKeys(owner, VoidNamespace.INSTANCE)) {
        var keys = stream.iterator();
        while (keys.hasNext()) {
          var key = keys.next();
          keyValidator.accept(key);
          backend.setCurrentKey(key);
          Object nullFlags = marker.value();
          if (nullFlags != null) {
            if (!bridge.getTypeAt(keyType.getFieldCount()).isNullable())
              throw new IllegalStateException("NULL membership for a required DISTINCT argument");
            validateFlags(nullFlags);
          }
          var wrapped = accumulators.value();
          if (wrapped == null && nullFlags != null)
            throw new IllegalStateException("OVER DISTINCT NULL membership has no scalar owner");
          var counts = new long[aggregates.length];
          var sums = new long[aggregates.length];
          var entries = map.entries();
          if (entries != null)
            for (var entry : entries) {
              if (entry.getKey() == null
                  || !(entry.getValue() instanceof Long)
                  || (bridge.getTypeAt(keyType.getFieldCount()).is(LogicalTypeRoot.INTEGER)
                      ? !(entry.getKey() instanceof Integer)
                      : !(entry.getKey() instanceof Long)))
                throw new IllegalStateException("Invalid OVER DISTINCT membership");
              long flags = validateFlags(entry.getValue());
              for (int slot = 0; slot < aggregates.length; slot++) {
                if ((flags >>> slot & 1) == 0) continue;
                counts[slot]++;
                sums[slot] += ((Number) entry.getKey()).longValue();
              }
            }
          for (int slot = 0; slot < aggregates.length; slot++) {
            if (wrapped == null) {
              if (counts[slot] != 0)
                throw new IllegalStateException("OVER DISTINCT membership has no scalar owner");
              continue;
            }
            var value = wrapped.getUserValue();
            int aggregate = aggregates[slot], field = scalarFields[aggregate];
            int kind = kinds[aggregate];
            if ((kind == 3 || kind == 4)
                && value.getLong(field + (kind == 4 ? 1 : 0)) != counts[slot])
              throw new IllegalStateException("OVER DISTINCT scalar and membership count differ");
            if (kind != 3) {
              Object expected =
                  kind == 0 && counts[slot] == 0
                      ? null
                      : accumulatorType.getTypeAt(field).is(LogicalTypeRoot.INTEGER)
                          ? (Object) (int) sums[slot]
                          : sums[slot];
              Object actual =
                  RowData.createFieldGetter(accumulatorType.getTypeAt(field).copy(true), field)
                      .getFieldOrNull(value);
              if (!java.util.Objects.equals(expected, actual))
                throw new IllegalStateException("OVER DISTINCT scalar and membership sum differ");
            }
          }
        }
      }
  }

  private long validateFlags(Object value) {
    if (!(value instanceof Long))
      throw new IllegalStateException("Invalid OVER DISTINCT membership bits");
    long flags = (Long) value;
    if (flags == 0 || aggregates.length < Long.SIZE && flags >>> aggregates.length != 0)
      throw new IllegalStateException("Invalid OVER DISTINCT membership bits");
    return flags;
  }

  void importState(CheckpointableKeyedStateBackend<RowData> backend, long handle) throws Exception {
    var map = state(backend);
    var marker = nullState(backend);
    var getters = new RowData.FieldGetter[keyType.getFieldCount()];
    for (int field = 0; field < getters.length; field++)
      getters[field] = RowData.createFieldGetter(keyType.getTypeAt(field).copy(true), field);
    for (int slot = 0; slot < aggregates.length; slot++) {
      var rows = new ArrayList<RowData>(CHUNK_ROWS);
      try (var stream = backend.getKeys(name, VoidNamespace.INSTANCE)) {
        var keys = stream.iterator();
        while (keys.hasNext()) {
          var key = keys.next();
          backend.setCurrentKey(key);
          var entries = map.entries();
          if (entries == null) continue;
          for (var entry : entries)
            if (((Long) entry.getValue() >>> slot & 1) != 0) {
              var row = new GenericRowData(bridge.getFieldCount());
              for (int field = 0; field < getters.length; field++)
                row.setField(field, getters[field].getFieldOrNull(key));
              row.setField(getters.length, values.copy(entry.getKey()));
              rows.add(row);
              if (rows.size() == CHUNK_ROWS) {
                transfer(handle, aggregates[slot], rows);
                rows.clear();
              }
            }
        }
      }
      if (bridge.getTypeAt(keyType.getFieldCount()).isNullable())
        try (var stream = backend.getKeys(name + "_null_state", VoidNamespace.INSTANCE)) {
          var keys = stream.iterator();
          while (keys.hasNext()) {
            var key = keys.next();
            backend.setCurrentKey(key);
            var flags = marker.value();
            if (flags == null || ((Long) flags >>> slot & 1) == 0) continue;
            var row = new GenericRowData(bridge.getFieldCount());
            for (int field = 0; field < getters.length; field++)
              row.setField(field, getters[field].getFieldOrNull(key));
            rows.add(row);
            if (rows.size() == CHUNK_ROWS) {
              transfer(handle, aggregates[slot], rows);
              rows.clear();
            }
          }
        }
      if (!rows.isEmpty()) transfer(handle, aggregates[slot], rows);
    }
  }

  private void transfer(long handle, int aggregate, List<RowData> rows) {
    try (var root = RowDataArrowConverter.write(rows, bridge, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      Native.importOverCanonicalDistinct(
          handle, aggregate, array.memoryAddress(), schema.memoryAddress());
    }
  }
}
