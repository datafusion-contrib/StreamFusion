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

/** Projects released typed scalar accumulators and shared DISTINCT views. */
public final class StockGroupTypedStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final int CHUNK_ROWS = 1024;
  private static final String STATE = "accState";
  private final TypeSerializer<RowData> keys;
  private final RowType keyType, accumulatorType, bridgeType;
  private final int[] kinds, scalarFields, countFields;
  private final int recordCountField;
  private final boolean ttl, globalMerge;
  private final List<StockGroupDistinctView> views;

  public StockGroupTypedStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulatorType,
      int[] kinds,
      int[] scalarFields,
      int[] countFields,
      int recordCountField,
      boolean ttl,
      List<StockGroupDistinctView> views) {
    this(
        keys,
        keyType,
        accumulatorType,
        kinds,
        scalarFields,
        countFields,
        recordCountField,
        ttl,
        views,
        false);
  }

  public StockGroupTypedStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulatorType,
      int[] kinds,
      int[] scalarFields,
      int[] countFields,
      int recordCountField,
      boolean ttl,
      List<StockGroupDistinctView> views,
      boolean globalMerge) {
    this.globalMerge = globalMerge;
    if (globalMerge && (recordCountField >= 0 || !views.isEmpty()))
      throw new IllegalArgumentException(
          "Global scalar merge requires append-only accumulators without views");
    this.keys = keys.duplicate();
    this.keyType = keyType;
    this.accumulatorType = accumulatorType;
    this.kinds = kinds.clone();
    this.scalarFields = scalarFields.clone();
    this.countFields = countFields.clone();
    this.recordCountField = recordCountField;
    this.ttl = ttl;
    this.views = List.copyOf(views);
    if (ttl && !views.isEmpty())
      throw new IllegalArgumentException("DISTINCT TTL requires per-entry timestamp projection");
    if (kinds.length == 0
        || kinds.length != scalarFields.length
        || kinds.length != countFields.length)
      throw new IllegalArgumentException("Invalid aggregate field mapping");

    boolean[] covered = new boolean[accumulatorType.getFieldCount()];
    if (recordCountField >= 0) {
      cover(covered, recordCountField);
      if (!accumulatorType.getTypeAt(recordCountField).is(LogicalTypeRoot.BIGINT))
        throw new IllegalArgumentException("Record counter must use BIGINT");
    } else if (recordCountField != -1)
      throw new IllegalArgumentException("Invalid input counter field");
    var fields = new ArrayList<LogicalType>();
    fields.add(new VarBinaryType(false, VarBinaryType.MAX_LENGTH));
    fields.add(new BigIntType(false));
    fields.add(new BigIntType(false));
    for (int i = 0; i < kinds.length; i++) {
      if (kinds[i] != 0
          && kinds[i] != 3
          && kinds[i] != 4
          && kinds[i] != 10
          && kinds[i] != 11
          && kinds[i] != 22
          && kinds[i] != 7
          && kinds[i] != 9) throw new IllegalArgumentException("Unsupported scalar aggregate kind");
      if (kinds[i] == 9 && recordCountField >= 0)
        throw new IllegalArgumentException("SUM DISTINCT requires append-only stock scalar state");
      if ((kinds[i] == 10 || kinds[i] == 11) && recordCountField >= 0)
        throw new IllegalArgumentException("Scalar MIN/MAX requires append-only input");
      cover(covered, scalarFields[i]);
      if (kinds[i] == 4 || kinds[i] == 0 && recordCountField >= 0) {
        cover(covered, countFields[i]);
        if (!accumulatorType.getTypeAt(countFields[i]).is(LogicalTypeRoot.BIGINT))
          throw new IllegalArgumentException("Intermediate count must use BIGINT");
      } else if (countFields[i] != -1)
        throw new IllegalArgumentException("Aggregate has no separate physical count");
      var scalarType = accumulatorType.getTypeAt(scalarFields[i]);
      switch (scalarType.getTypeRoot()) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case FLOAT:
        case DOUBLE:
          break;
        case DATE:
        case TIME_WITHOUT_TIME_ZONE:
        case BOOLEAN:
        case VARCHAR:
          if (kinds[i] != 10 && kinds[i] != 11)
            throw new IllegalArgumentException("Typed nonnumeric state requires append MIN/MAX");
          break;
        default:
          throw new IllegalArgumentException("Unqualified scalar accumulator type: " + scalarType);
      }
      if ((kinds[i] == 3 || kinds[i] == 7 || kinds[i] == 22)
          && !scalarType.is(LogicalTypeRoot.BIGINT))
        throw new IllegalArgumentException("COUNT requires the released BIGINT accumulator");
      fields.add(scalarType.copy(nullableScalar(kinds[i])));
      fields.add(new BigIntType(false));
    }
    int placeholders = 0;
    for (int i = 0; i < covered.length; i++) {
      if (covered[i]) continue;
      var type = accumulatorType.getTypeAt(i);
      if (!(type instanceof org.apache.flink.table.types.logical.RawType)
          || !(((org.apache.flink.table.types.logical.RawType<?>) type).getTypeSerializer()
              instanceof org.apache.flink.table.dataview.NullSerializer))
        throw new IllegalArgumentException("Unmapped scalar or non-stock view placeholder");
      placeholders++;
    }
    if (placeholders != views.size())
      throw new IllegalArgumentException(
          "View placeholders differ from the generated stock layout");
    bridgeType = RowType.of(fields.toArray(new LogicalType[0]));
  }

  private static boolean nullableScalar(int kind) {
    return kind == 0 || kind == 9 || kind == 10 || kind == 11;
  }

  private static void cover(boolean[] fields, int index) {
    if (index < 0 || index >= fields.length)
      throw new IllegalArgumentException("Invalid accumulator field index");
    fields[index] = true;
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    Map<String, StateDescriptor.Type> result = new java.util.LinkedHashMap<>();
    result.put(STATE, StateDescriptor.Type.VALUE);
    for (var view : views)
      for (var entry : view.stateDescriptors().entrySet())
        if (result.putIfAbsent(entry.getKey(), entry.getValue()) != null)
          throw new IllegalArgumentException("Duplicate stock state descriptor " + entry.getKey());
    return result;
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  @Override
  public void configureNativeState(long handle, boolean persistent) {
    if (globalMerge) Native.configureGroupTypedGlobalCanonical(handle, persistent);
    Native.configureGroupTypedCanonical(handle, persistent, kinds, recordCountField < 0);
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
    for (var view : views) view.clear(backend, keyType);
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    configureNativeState(handle, persistent);
    clear(backend);
    var state = state(backend);
    Native.exportGroupTypedCanonical(
        handle,
        persistent,
        -1,
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
              if (recordCountField >= 0)
                assign(accumulator, assigned, recordCountField, row.getLong(1));
              for (int i = 0; i < kinds.length; i++) {
                var getter = RowData.createFieldGetter(bridgeType.getTypeAt(3 + 2 * i), 3 + 2 * i);
                assign(accumulator, assigned, scalarFields[i], getter.getFieldOrNull(row));
                if (countFields[i] >= 0)
                  assign(accumulator, assigned, countFields[i], row.getLong(4 + 2 * i));
              }
              for (int field = 0; field < assigned.length; field++)
                if (!assigned[field])
                  accumulator.setField(
                      field,
                      org.apache.flink.table.data.binary.BinaryRawValueData.fromBytes(
                          new byte[] {0}));
              validateAccumulator(accumulator);
              backend.setCurrentKey(key);
              state.update(accumulator, row.getLong(2));
            }
          } catch (Exception failure) {
            throw new IllegalStateException("Cannot project stock group accumulators", failure);
          }
        });
    for (var view : views) view.exportState(backend, keyType, handle, persistent);
  }

  private static void assign(GenericRowData row, boolean[] assigned, int field, Object value) {
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
    if (recordCountField >= 0 && row.isNullAt(recordCountField))
      throw new IllegalStateException("NULL group record count");
    for (int i = 0; i < kinds.length; i++) {
      if (!nullableScalar(kinds[i]) && row.isNullAt(scalarFields[i]))
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
    Native.exportGroupTypedCanonical(
        handle,
        persistent,
        -1,
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
    for (var view : views)
      view.importState(
          backend,
          keyType,
          state(backend),
          scalarFields,
          kinds,
          handle,
          persistent,
          true,
          this::validateKey);
    visit(backend, handle, persistent, false);
    for (var view : views)
      view.importState(
          backend,
          keyType,
          state(backend),
          scalarFields,
          kinds,
          handle,
          persistent,
          false,
          this::validateKey);
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
        row.setField(1, recordCountField >= 0 ? accumulator.getLong(recordCountField) : 1L);
        row.setField(2, wrapped.getLastAccessTimestamp());
        for (int i = 0; i < kinds.length; i++) {
          row.setField(
              3 + 2 * i,
              RowData.createFieldGetter(accumulatorType.getTypeAt(scalarFields[i]), scalarFields[i])
                  .getFieldOrNull(accumulator));
          row.setField(
              4 + 2 * i,
              countFields[i] >= 0
                  ? accumulator.getLong(countFields[i])
                  : kinds[i] == 7
                      ? 0L
                      : nullableScalar(kinds[i])
                          ? (accumulator.isNullAt(scalarFields[i]) ? 0L : 1L)
                          : (kinds[i] == 3 || kinds[i] == 22)
                              ? accumulator.getLong(scalarFields[i])
                              : 0L);
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
      Native.transferGroupTypedCanonical(
          handle, persistent, -1, array.memoryAddress(), schema.memoryAddress(), validating);
    }
  }
}
