package tech.streamfusion.state;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Copies released unbounded scalar accumulators without replay or TTL clock refresh. */
public final class StockOverFoldStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final String STATE = "accState";
  private static final int CHUNK_ROWS = 1024;
  private final TypeSerializer<RowData> keys;
  private final RowType keyType, accumulatorType, bridge;
  private final int[] kinds, widths;
  private final boolean ttl;
  private final RowData.FieldGetter[] keyGetters, valueGetters;

  public StockOverFoldStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulatorType,
      int[] kinds,
      int[] widths,
      boolean ttl) {
    this.keys = keys.duplicate();
    this.keyType = keyType;
    this.accumulatorType = accumulatorType;
    this.kinds = kinds.clone();
    this.widths = widths.clone();
    this.ttl = ttl;
    if (keyType.getFieldCount() > 1
        || keyType.getChildren().stream().anyMatch(type -> !type.is(LogicalTypeRoot.BIGINT))
        || kinds.length == 0
        || kinds.length != widths.length
        || Arrays.stream(widths).sum() != accumulatorType.getFieldCount())
      throw new IllegalArgumentException("Unqualified unbounded OVER key or accumulator layout");
    int slot = 0;
    for (int i = 0; i < kinds.length; i++) {
      if (kinds[i] != 0 && kinds[i] != 3 && kinds[i] != 4 && kinds[i] != 22
          || widths[i] != (kinds[i] == 4 ? 2 : 1)
          || !numeric(accumulatorType.getTypeAt(slot))
          || kinds[i] == 3 && !accumulatorType.getTypeAt(slot).is(LogicalTypeRoot.BIGINT)
          || widths[i] == 2 && !accumulatorType.getTypeAt(slot + 1).is(LogicalTypeRoot.BIGINT))
        throw new IllegalArgumentException("Unqualified unbounded OVER scalar schema");
      slot += widths[i];
    }
    var fields = new ArrayList<RowType.RowField>();
    for (int i = 0; i < keyType.getFieldCount(); i++)
      fields.add(new RowType.RowField("key" + i, keyType.getTypeAt(i).copy(true)));
    for (int i = 0; i < accumulatorType.getFieldCount(); i++)
      fields.add(new RowType.RowField("state" + i, accumulatorType.getTypeAt(i).copy(true)));
    fields.add(new RowType.RowField("__over_retention_stamp", new BigIntType(false)));
    bridge = new RowType(fields);
    keyGetters = getters(keyType, 0);
    valueGetters = getters(accumulatorType, 0);
  }

  private static boolean numeric(LogicalType type) {
    return type.is(LogicalTypeRoot.BIGINT)
        || type.is(LogicalTypeRoot.FLOAT)
        || type.is(LogicalTypeRoot.DOUBLE);
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(STATE, StateDescriptor.Type.VALUE);
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  private StockValueState<RowData> state(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return new StockValueState<>(backend, STATE, new RowDataSerializer(accumulatorType), ttl);
  }

  private void validateKey(RowData key) {
    if (key == null
        || key.getArity() != keyType.getFieldCount()
        || key.getRowKind() != RowKind.INSERT)
      throw new IllegalStateException("Invalid unbounded OVER partition key");
    for (int i = 0; i < keyType.getFieldCount(); i++)
      if (!keyType.getTypeAt(i).isNullable() && key.isNullAt(i))
        throw new IllegalStateException("NULL in a required OVER key");
  }

  private void validateAccumulator(RowData value) {
    if (value == null
        || value.getArity() != accumulatorType.getFieldCount()
        || value.getRowKind() != RowKind.INSERT)
      throw new IllegalStateException("Invalid unbounded OVER accumulator");
    int slot = 0;
    for (int i = 0; i < kinds.length; i++) {
      if (kinds[i] != 0 && value.isNullAt(slot) || widths[i] == 2 && value.isNullAt(slot + 1))
        throw new IllegalStateException("NULL in a required unbounded OVER scalar");
      slot += widths[i];
    }
  }

  @FunctionalInterface
  private interface Receiver {
    void accept(RowData row) throws Exception;
  }

  private void receive(long arrayAddress, long schemaAddress, Receiver receiver) {
    try (var array = ArrowArray.wrap(arrayAddress);
        var schema = ArrowSchema.wrap(schemaAddress);
        var root =
            Data.importVectorSchemaRoot(
                NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
      var expected = ArrowConversion.toArrowSchema(bridge).getFields();
      if (root.getFieldVectors().size() != expected.size())
        throw new IllegalStateException("Unbounded OVER transfer arity changed");
      for (int i = 0; i < expected.size(); i++)
        if (!expected.get(i).getType().equals(root.getSchema().getFields().get(i).getType()))
          throw new IllegalStateException("Unbounded OVER transfer type changed");
      var reader = ArrowConversion.createArrowReader(root, bridge);
      for (int i = 0; i < root.getRowCount(); i++) receiver.accept(reader.read(i));
    } catch (Exception failure) {
      throw new IllegalStateException("Cannot transfer stock unbounded OVER state", failure);
    }
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    clear(backend);
    var state = state(backend);
    var keyRows = new RowDataSerializer(keyType);
    var getters = getters(accumulatorType, keyType.getFieldCount());
    Native.exportOverFoldCanonical(
        handle,
        (array, schema) ->
            receive(
                array,
                schema,
                row -> {
                  var key = copy(row, keyGetters);
                  var accumulator = copy(row, getters);
                  validateKey(key);
                  validateAccumulator(accumulator);
                  backend.setCurrentKey(keyRows.toBinaryRow(key).copy());
                  state.update(accumulator, row.getLong(bridge.getFieldCount() - 1));
                }));
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    StockStateInventory.validate(backend, stateDescriptors(), Set.of(), keys);
    requireEmpty(handle);
    transferState(backend, handle, true);
    transferState(backend, handle, false);
    clear(backend);
  }

  void requireEmpty(long handle) {
    Native.exportOverFoldCanonical(
        handle,
        (array, schema) ->
            receive(
                array,
                schema,
                row -> {
                  throw new IllegalStateException("Unbounded OVER import requires an empty target");
                }));
  }

  void transferState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean validating)
      throws Exception {
    var state = state(backend);
    var rows = new ArrayList<RowData>(CHUNK_ROWS);
    try (var stream = backend.getKeys(STATE, VoidNamespace.INSTANCE)) {
      var iterator = stream.iterator();
      while (iterator.hasNext()) {
        RowData key = iterator.next();
        validateKey(key);
        backend.setCurrentKey(key);
        var stored = state.value();
        if (stored == null) continue;
        var value = stored.getUserValue();
        validateAccumulator(value);
        var row = new GenericRowData(bridge.getFieldCount());
        for (int i = 0; i < keyGetters.length; i++)
          row.setField(i, keyGetters[i].getFieldOrNull(key));
        for (int i = 0; i < valueGetters.length; i++)
          row.setField(keyGetters.length + i, valueGetters[i].getFieldOrNull(value));
        row.setField(bridge.getFieldCount() - 1, stored.getLastAccessTimestamp());
        rows.add(row);
        if (rows.size() == CHUNK_ROWS) {
          transfer(handle, rows, validating);
          rows.clear();
        }
      }
    }
    if (!rows.isEmpty()) transfer(handle, rows, validating);
  }

  private void transfer(long handle, List<RowData> rows, boolean validating) {
    try (var root = RowDataArrowConverter.write(rows, bridge, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (validating)
        Native.validateOverFoldCanonical(handle, array.memoryAddress(), schema.memoryAddress());
      else Native.importOverFoldCanonical(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }

  @Override
  public void clear(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    var state = state(backend);
    var copier = new RowDataSerializer(keyType);
    List<RowData> savedKeys;
    try (var stream = backend.getKeys(STATE, VoidNamespace.INSTANCE)) {
      savedKeys = stream.map(copier::copy).collect(java.util.stream.Collectors.toList());
    }
    for (var key : savedKeys) {
      backend.setCurrentKey(key);
      state.clear();
    }
  }

  private static RowData.FieldGetter[] getters(RowType type, int offset) {
    var getters = new RowData.FieldGetter[type.getFieldCount()];
    for (int i = 0; i < getters.length; i++)
      getters[i] = RowData.createFieldGetter(type.getTypeAt(i), offset + i);
    return getters;
  }

  private static GenericRowData copy(RowData row, RowData.FieldGetter[] getters) {
    var result = new GenericRowData(getters.length);
    for (int i = 0; i < getters.length; i++) result.setField(i, getters[i].getFieldOrNull(row));
    return result;
  }
}
