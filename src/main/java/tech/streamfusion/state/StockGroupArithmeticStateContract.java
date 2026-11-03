package tech.streamfusion.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Projects actual generated arithmetic accumulator slots, including hidden record counts. */
public final class StockGroupArithmeticStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final int CHUNK_ROWS = 1024;
  private static final String STATE = "accState";
  private final TypeSerializer<RowData> keys;
  private final RowType keyType, accumulatorType, bridgeType;
  private final int[] kinds, scalarFields, countFields;
  private final int recordCountField;
  private final boolean ttl;

  public StockGroupArithmeticStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulatorType,
      int[] kinds,
      int[] scalarFields,
      int[] countFields,
      int recordCountField,
      boolean ttl) {
    this.keys = keys.duplicate();
    this.keyType = keyType;
    this.accumulatorType = accumulatorType;
    this.kinds = kinds.clone();
    this.scalarFields = scalarFields.clone();
    this.countFields = countFields.clone();
    this.recordCountField = recordCountField;
    this.ttl = ttl;
    if (kinds.length == 0
        || kinds.length != scalarFields.length
        || kinds.length != countFields.length)
      throw new IllegalArgumentException("Invalid aggregate field mapping");
    if (accumulatorType.getChildren().stream().anyMatch(type -> !type.is(LogicalTypeRoot.BIGINT)))
      throw new IllegalArgumentException("Arithmetic state requires BIGINT accumulator fields");
    boolean[] covered = new boolean[accumulatorType.getFieldCount()];
    cover(covered, recordCountField);
    var fields = new ArrayList<LogicalType>();
    fields.add(new VarBinaryType(false, VarBinaryType.MAX_LENGTH));
    fields.add(new BigIntType(false));
    fields.add(new BigIntType(false));
    for (int i = 0; i < kinds.length; i++) {
      if (kinds[i] != 0 && kinds[i] != 3 && kinds[i] != 4 && kinds[i] != 22)
        throw new IllegalArgumentException("Unsupported arithmetic aggregate kind");
      cover(covered, scalarFields[i]);
      if (kinds[i] == 0 || kinds[i] == 4) cover(covered, countFields[i]);
      else if (countFields[i] != -1)
        throw new IllegalArgumentException("COUNT and SUM0 have no separate physical count");
      fields.add(new BigIntType(kinds[i] == 0));
      fields.add(new BigIntType(false));
    }
    for (boolean present : covered)
      if (!present) throw new IllegalArgumentException("Unmapped physical accumulator field");
    bridgeType = RowType.of(fields.toArray(new LogicalType[0]));
  }

  private static void cover(boolean[] fields, int index) {
    if (index < 0 || index >= fields.length)
      throw new IllegalArgumentException("Invalid accumulator field index");
    fields[index] = true;
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(STATE, StateDescriptor.Type.VALUE);
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  @Override
  public void configureNativeState(long handle, boolean persistent) {
    Native.configureGroupArithmeticCanonical(handle, persistent, kinds);
  }

  private StockValueState<RowData> state(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return new StockValueState<>(backend, STATE, new RowDataSerializer(accumulatorType), ttl);
  }

  @Override
  public void clear(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    var state = state(backend);
    try (var stream = backend.getKeys(STATE, VoidNamespace.INSTANCE)) {
      for (var key : stream.map(keys::copy).collect(java.util.stream.Collectors.toList())) {
        backend.setCurrentKey(key);
        state.clear();
      }
    }
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    configureNativeState(handle, persistent);
    clear(backend);
    var state = state(backend);
    Native.exportGroupArithmeticCanonical(
        handle,
        persistent,
        (arrayAddress, schemaAddress) -> {
          try (var array = ArrowArray.wrap(arrayAddress);
              var schema = ArrowSchema.wrap(schemaAddress);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            var reader = ArrowConversion.createArrowReader(root, bridgeType);
            for (int index = 0; index < root.getRowCount(); index++) {
              RowData row = reader.read(index);
              byte[] encoded = row.getBinary(0);
              var key = new BinaryRowData(keyType.getFieldCount());
              key.pointTo(MemorySegmentFactory.wrap(encoded), 0, encoded.length);
              validateKey(key);
              var accumulator = new GenericRowData(accumulatorType.getFieldCount());
              boolean[] assigned = new boolean[accumulator.getArity()];
              assign(accumulator, assigned, recordCountField, row.getLong(1));
              for (int i = 0; i < kinds.length; i++) {
                assign(
                    accumulator,
                    assigned,
                    scalarFields[i],
                    row.isNullAt(3 + 2 * i) ? null : row.getLong(3 + 2 * i));
                if (countFields[i] >= 0)
                  assign(accumulator, assigned, countFields[i], row.getLong(4 + 2 * i));
              }
              validateAccumulator(accumulator);
              backend.setCurrentKey(key);
              state.update(accumulator, row.getLong(2));
            }
          } catch (Exception failure) {
            throw new IllegalStateException("Cannot project stock group accumulators", failure);
          }
        });
  }

  private static void assign(GenericRowData row, boolean[] assigned, int field, Long value) {
    if (assigned[field] && !java.util.Objects.equals(row.getField(field), value))
      throw new IllegalStateException("Inconsistent aliased accumulator fields");
    row.setField(field, value);
    assigned[field] = true;
  }

  private void validateKey(RowData key) {
    if (key == null
        || key.getArity() != keyType.getFieldCount()
        || key.getRowKind() != RowKind.INSERT)
      throw new IllegalStateException("Invalid group state key");
    for (int i = 0; i < keyType.getFieldCount(); i++)
      if (!keyType.getTypeAt(i).isNullable() && key.isNullAt(i))
        throw new IllegalStateException("NULL in a non-null group key");
  }

  private void validateAccumulator(RowData row) {
    if (row == null
        || row.getArity() != accumulatorType.getFieldCount()
        || row.getRowKind() != RowKind.INSERT)
      throw new IllegalStateException("Invalid physical group accumulator");
    for (int i = 0; i < row.getArity(); i++)
      if (!accumulatorType.getTypeAt(i).isNullable() && row.isNullAt(i))
        throw new IllegalStateException("NULL in a non-null accumulator field");
    if (row.isNullAt(recordCountField)) throw new IllegalStateException("NULL group record count");
    for (int i = 0; i < kinds.length; i++) {
      if (kinds[i] != 0 && row.isNullAt(scalarFields[i]))
        throw new IllegalStateException("NULL in a required accumulator scalar");
      if (countFields[i] >= 0 && row.isNullAt(countFields[i]))
        throw new IllegalStateException("NULL in a required accumulator count");
    }
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    configureNativeState(handle, persistent);
    StockStateInventory.validate(backend, stateDescriptors(), Set.of(), keys);
    Native.exportGroupArithmeticCanonical(
        handle,
        persistent,
        (arrayAddress, schemaAddress) -> {
          try (var array = ArrowArray.wrap(arrayAddress);
              var schema = ArrowSchema.wrap(schemaAddress);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            if (root.getRowCount() != 0)
              throw new IllegalStateException("Group canonical restore requires an empty target");
          }
        });
    visit(backend, handle, persistent, true);
    visit(backend, handle, persistent, false);
    clear(backend);
  }

  private void visit(
      CheckpointableKeyedStateBackend<RowData> backend,
      long handle,
      boolean persistent,
      boolean validating)
      throws Exception {
    var state = state(backend);
    var binary = new RowDataSerializer(keyType);
    List<RowData> rows = new ArrayList<>(CHUNK_ROWS);
    try (var stream = backend.getKeys(STATE, VoidNamespace.INSTANCE)) {
      var iterator = stream.iterator();
      while (iterator.hasNext()) {
        RowData key = iterator.next();
        validateKey(key);
        backend.setCurrentKey(key);
        var wrapped = state.value();
        if (wrapped == null) continue;
        RowData accumulator = wrapped.getUserValue();
        validateAccumulator(accumulator);
        var encoded = binary.toBinaryRow(key);
        var row = new GenericRowData(bridgeType.getFieldCount());
        row.setField(
            0,
            BinarySegmentUtils.copyToBytes(
                encoded.getSegments(), encoded.getOffset(), encoded.getSizeInBytes()));
        row.setField(1, accumulator.getLong(recordCountField));
        row.setField(2, wrapped.getLastAccessTimestamp());
        for (int i = 0; i < kinds.length; i++) {
          row.setField(
              3 + 2 * i,
              accumulator.isNullAt(scalarFields[i]) ? null : accumulator.getLong(scalarFields[i]));
          row.setField(
              4 + 2 * i,
              countFields[i] >= 0
                  ? accumulator.getLong(countFields[i])
                  : kinds[i] == 3 ? accumulator.getLong(scalarFields[i]) : 0L);
        }
        rows.add(row);
        if (rows.size() == CHUNK_ROWS) {
          batch(handle, persistent, rows, validating);
          rows.clear();
        }
      }
    }
    if (!rows.isEmpty()) batch(handle, persistent, rows, validating);
  }

  private void batch(long handle, boolean persistent, List<RowData> rows, boolean validating) {
    try (var root = RowDataArrowConverter.write(rows, bridgeType, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (validating)
        Native.validateGroupArithmeticCanonical(
            handle, persistent, array.memoryAddress(), schema.memoryAddress());
      else
        Native.importGroupArithmeticCanonical(
            handle, persistent, array.memoryAddress(), schema.memoryAddress());
    }
  }
}
