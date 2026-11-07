package tech.streamfusion.state;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
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
import org.apache.flink.table.data.binary.BinaryRawValueData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Projects released retracting extrema headers and independently retained map entries. */
public final class StockGroupExtremaStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final String STATE = "accState";
  private static final int CHUNK_ROWS = 1024;
  private final TypeSerializer<RowData> keys;
  private final RowType keyType, accumulatorType, headerType, entryType;
  private final int[] kinds, scalarFields, countFields;
  private final LogicalType[] valueTypes;
  private final String[] mapNames;
  private final TypeSerializer<?>[] mapKeySerializers, mapValueSerializers;
  private final int recordCountField;
  private final boolean ttl;

  public StockGroupExtremaStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulatorType,
      int[] kinds,
      int[] scalarFields,
      int[] countFields,
      String[] mapNames,
      TypeSerializer<?>[] mapKeySerializers,
      TypeSerializer<?>[] mapValueSerializers,
      int recordCountField,
      boolean ttl) {
    this.keys = keys.duplicate();
    this.keyType = keyType;
    this.accumulatorType = accumulatorType;
    this.kinds = kinds.clone();
    this.scalarFields = scalarFields.clone();
    this.countFields = countFields.clone();
    this.mapNames = mapNames.clone();
    this.mapKeySerializers = mapKeySerializers.clone();
    this.mapValueSerializers = mapValueSerializers.clone();
    this.recordCountField = recordCountField;
    this.ttl = ttl;
    this.valueTypes = new LogicalType[kinds.length];
    if (kinds.length == 0
        || kinds.length != scalarFields.length
        || kinds.length != countFields.length
        || kinds.length != mapNames.length
        || kinds.length != mapKeySerializers.length
        || kinds.length != mapValueSerializers.length
        || Arrays.stream(kinds).noneMatch(StockGroupExtremaStateContract::mapped))
      throw new IllegalArgumentException("Invalid extrema field mapping");
    if (keyType.getFieldCount() > 1
        || keyType.getChildren().stream()
            .anyMatch(
                type -> !type.is(LogicalTypeRoot.INTEGER) && !type.is(LogicalTypeRoot.BIGINT)))
      throw new IllegalArgumentException("Extrema requires one INT/BIGINT key or a global group");
    boolean[] covered = new boolean[accumulatorType.getFieldCount()];
    cover(covered, recordCountField, LogicalTypeRoot.BIGINT);
    var names = new java.util.HashSet<String>();
    var fields = new ArrayList<LogicalType>();
    fields.add(new VarBinaryType(false, VarBinaryType.MAX_LENGTH));
    fields.add(new BigIntType(false));
    fields.add(new BigIntType(false));
    for (int i = 0; i < kinds.length; i++) {
      if (mapped(kinds[i])) {
        cover(covered, scalarFields[i], LogicalTypeRoot.STRUCTURED_TYPE);
        var children = accumulatorType.getTypeAt(scalarFields[i]).getChildren();
        if (!(accumulatorType.getTypeAt(scalarFields[i]) instanceof StructuredType)
            || children.size() != 3
            || !supportsValueType(children.get(0))
            || !children.get(1).is(LogicalTypeRoot.BIGINT)
            || !(children.get(2) instanceof RawType)
            || !((RawType<?>) children.get(2))
                .getTypeSerializer()
                .getClass()
                .getName()
                .equals("org.apache.flink.table.dataview.NullSerializer")
            || countFields[i] != -1
            || mapNames[i] == null
            || mapNames[i].isEmpty()
            || mapNames[i].equals(STATE)
            || mapKeySerializers[i] == null
            || mapValueSerializers[i] == null
            || !names.add(mapNames[i]))
          throw new IllegalArgumentException("Invalid released extrema accumulator or map binding");
        valueTypes[i] = children.get(0);
        this.mapKeySerializers[i] = mapKeySerializers[i].duplicate();
        this.mapValueSerializers[i] = mapValueSerializers[i].duplicate();
      } else {
        if (kinds[i] != 0 && kinds[i] != 3 && kinds[i] != 4 && kinds[i] != 22
            || mapNames[i] != null
            || mapKeySerializers[i] != null
            || mapValueSerializers[i] != null)
          throw new IllegalArgumentException("Unsupported scalar in extrema state");
        cover(covered, scalarFields[i], LogicalTypeRoot.BIGINT);
        if (kinds[i] == 0 || kinds[i] == 4) cover(covered, countFields[i], LogicalTypeRoot.BIGINT);
        else if (countFields[i] != -1)
          throw new IllegalArgumentException("Unexpected scalar count field");
      }
      fields.add(new BigIntType(kinds[i] == 0 || mapped(kinds[i])));
      fields.add(new BigIntType(false));
    }
    for (boolean field : covered)
      if (!field) throw new IllegalArgumentException("Unmapped generated accumulator field");
    fields.add(new BooleanType(false));
    headerType = RowType.of(fields.toArray(new LogicalType[0]));
    entryType =
        RowType.of(
            new VarBinaryType(false, VarBinaryType.MAX_LENGTH),
            new BigIntType(false),
            new BigIntType(false),
            new BigIntType(false),
            new BigIntType(false));
  }

  public static boolean supportsValueType(LogicalType type) {
    switch (type.getTypeRoot()) {
      case BIGINT:
      case INTEGER:
      case SMALLINT:
      case TINYINT:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case BOOLEAN:
        return true;
      default:
        return false;
    }
  }

  private static long toCarrier(Object value, LogicalType type) {
    switch (type.getTypeRoot()) {
      case BIGINT:
        return (Long) value;
      case INTEGER:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
        return (Integer) value;
      case SMALLINT:
        return (Short) value;
      case TINYINT:
        return (Byte) value;
      case BOOLEAN:
        return (Boolean) value ? 1L : 0L;
      default:
        throw new IllegalStateException("Unsupported extrema value type");
    }
  }

  private static Object fromCarrier(long value, LogicalType type) {
    switch (type.getTypeRoot()) {
      case BIGINT:
        return value;
      case INTEGER:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
        return Math.toIntExact(value);
      case SMALLINT:
        if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) return (short) value;
        break;
      case TINYINT:
        if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) return (byte) value;
        break;
      case BOOLEAN:
        if (value == 0 || value == 1) return value == 1;
        break;
      default:
        break;
    }
    throw new IllegalStateException("Extrema carrier outside its logical type");
  }

  private static boolean mapped(int kind) {
    return kind == 38 || kind == 39;
  }

  private void cover(boolean[] covered, int index, LogicalTypeRoot root) {
    if (index < 0 || index >= covered.length || !accumulatorType.getTypeAt(index).is(root))
      throw new IllegalArgumentException("Invalid generated accumulator field");
    covered[index] = true;
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    var states = new LinkedHashMap<String, StateDescriptor.Type>();
    states.put(STATE, StateDescriptor.Type.VALUE);
    for (String map : mapNames) if (map != null) states.put(map, StateDescriptor.Type.MAP);
    return states;
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  @Override
  public void configureNativeState(long handle, boolean persistent) {
    Native.configureGroupExtremaCanonical(handle, persistent, kinds);
  }

  private StockValueState<RowData> state(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return new StockValueState<>(backend, STATE, new RowDataSerializer(accumulatorType), ttl);
  }

  @SuppressWarnings("unchecked")
  private StockMapState<Object, Long>[] maps(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    var states = (StockMapState<Object, Long>[]) new StockMapState<?, ?>[kinds.length];
    for (int i = 0; i < kinds.length; i++)
      if (mapped(kinds[i]))
        states[i] =
            new StockMapState<>(
                backend,
                mapNames[i],
                (TypeSerializer<Object>) mapKeySerializers[i],
                (TypeSerializer<Long>) mapValueSerializers[i],
                ttl);
    return states;
  }

  private List<RowData> sourceKeys(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    var all = new LinkedHashMap<ByteBuffer, RowData>();
    for (String name : stateDescriptors().keySet()) {
      try (var stream = backend.getKeys(name, VoidNamespace.INSTANCE)) {
        var iterator = stream.iterator();
        while (iterator.hasNext()) {
          var key = keys.copy(iterator.next());
          all.putIfAbsent(ByteBuffer.wrap(encode(key)), key);
        }
      }
    }
    return new ArrayList<>(all.values());
  }

  @Override
  public void clear(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    var state = state(backend);
    var maps = maps(backend);
    for (var key : sourceKeys(backend)) {
      backend.setCurrentKey(key);
      state.clear();
      for (var map : maps) if (map != null) map.clear();
    }
  }

  private BinaryRowData decode(byte[] encoded) {
    var key = new BinaryRowData(keyType.getFieldCount());
    key.pointTo(MemorySegmentFactory.wrap(encoded), 0, encoded.length);
    validateKey(key);
    return key;
  }

  private byte[] encode(RowData key) {
    validateKey(key);
    var binary = new RowDataSerializer(keyType).toBinaryRow(key);
    return BinarySegmentUtils.copyToBytes(
        binary.getSegments(), binary.getOffset(), binary.getSizeInBytes());
  }

  private void validateKey(RowData key) {
    if (key == null
        || key.getArity() != keyType.getFieldCount()
        || key.getRowKind() != RowKind.INSERT)
      throw new IllegalStateException("Invalid extrema group key");
    for (int i = 0; i < keyType.getFieldCount(); i++)
      if (!keyType.getTypeAt(i).isNullable() && key.isNullAt(i))
        throw new IllegalStateException("NULL in a required extrema key");
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void validateAccumulator(RowData row) {
    if (row == null
        || row.getArity() != accumulatorType.getFieldCount()
        || row.getRowKind() != RowKind.INSERT
        || row.isNullAt(recordCountField))
      throw new IllegalStateException("Invalid extrema accumulator");
    for (int i = 0; i < row.getArity(); i++)
      if (!accumulatorType.getTypeAt(i).isNullable() && row.isNullAt(i))
        throw new IllegalStateException("NULL in a required accumulator field");
    for (int i = 0; i < kinds.length; i++) {
      int field = scalarFields[i];
      if (mapped(kinds[i])) {
        if (row.isNullAt(field)) throw new IllegalStateException("Missing extrema header");
        var nested = row.getRow(field, 3);
        var raw = (RawType) accumulatorType.getTypeAt(field).getChildren().get(2);
        if (nested.getRowKind() != RowKind.INSERT
            || nested.isNullAt(1)
            || nested.isNullAt(2)
            || !Arrays.equals(
                new byte[] {0}, nested.getRawValue(2).toBytes(raw.getTypeSerializer())))
          throw new IllegalStateException("Invalid extrema cache, map size or RAW placeholder");
      } else if (kinds[i] != 0 && row.isNullAt(field)
          || countFields[i] >= 0 && row.isNullAt(countFields[i]))
        throw new IllegalStateException("NULL in a required extrema scalar");
    }
  }

  private static void assign(GenericRowData row, boolean[] assigned, int field, Object value) {
    if (assigned[field] && !java.util.Objects.equals(row.getField(field), value))
      throw new IllegalStateException("Inconsistent shared accumulator fields");
    assigned[field] = true;
    row.setField(field, value);
  }

  @FunctionalInterface
  private interface RowReceiver {
    void accept(RowData row) throws Exception;
  }

  private void receive(long arrayAddress, long schemaAddress, RowType type, RowReceiver receiver) {
    try (var array = ArrowArray.wrap(arrayAddress);
        var schema = ArrowSchema.wrap(schemaAddress);
        var root =
            Data.importVectorSchemaRoot(
                NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
      var reader = ArrowConversion.createArrowReader(root, type);
      for (int i = 0; i < root.getRowCount(); i++) receiver.accept(reader.read(i));
    } catch (Exception failure) {
      throw new IllegalStateException("Cannot transfer stock extrema state", failure);
    }
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    configureNativeState(handle, persistent);
    clear(backend);
    var state = state(backend);
    var maps = maps(backend);
    Native.exportGroupExtremaCanonical(
        handle,
        persistent,
        (array, schema) ->
            receive(
                array,
                schema,
                headerType,
                row -> {
                  if (!row.getBoolean(headerType.getFieldCount() - 1)) return;
                  var accumulator = new GenericRowData(accumulatorType.getFieldCount());
                  var assigned = new boolean[accumulator.getArity()];
                  assign(accumulator, assigned, recordCountField, row.getLong(1));
                  for (int i = 0; i < kinds.length; i++) {
                    Long value = row.isNullAt(3 + 2 * i) ? null : row.getLong(3 + 2 * i);
                    if (mapped(kinds[i]))
                      assign(
                          accumulator,
                          assigned,
                          scalarFields[i],
                          GenericRowData.of(
                              value == null ? null : fromCarrier(value, valueTypes[i]),
                              row.getLong(4 + 2 * i),
                              BinaryRawValueData.fromBytes(new byte[] {0})));
                    else {
                      assign(accumulator, assigned, scalarFields[i], value);
                      if (countFields[i] >= 0)
                        assign(accumulator, assigned, countFields[i], row.getLong(4 + 2 * i));
                    }
                  }
                  validateAccumulator(accumulator);
                  backend.setCurrentKey(decode(row.getBinary(0)));
                  state.update(accumulator, row.getLong(2));
                }),
        (array, schema) ->
            receive(
                array,
                schema,
                entryType,
                row -> {
                  int index = Math.toIntExact(row.getLong(1));
                  if (index < 0 || index >= maps.length || maps[index] == null)
                    throw new IllegalStateException("Unbound extrema view");
                  backend.setCurrentKey(decode(row.getBinary(0)));
                  maps[index].put(
                      fromCarrier(row.getLong(2), valueTypes[index]),
                      row.getLong(3),
                      row.getLong(4));
                }));
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    configureNativeState(handle, persistent);
    StockStateInventory.validate(backend, stateDescriptors(), Set.of(), keys);
    Native.BatchReceiver reject =
        (array, schema) ->
            receive(
                array,
                schema,
                headerType,
                row -> {
                  throw new IllegalStateException(
                      "Extrema canonical restore requires an empty target");
                });
    Native.exportGroupExtremaCanonical(handle, persistent, reject, reject);
    var state = state(backend);
    var maps = maps(backend);
    var source = sourceKeys(backend);
    for (boolean validating : new boolean[] {true, false}) {
      visitHeaders(backend, state, source, handle, persistent, validating);
      visitEntries(backend, maps, source, handle, persistent, validating);
    }
    clear(backend);
  }

  private void visitHeaders(
      CheckpointableKeyedStateBackend<RowData> backend,
      StockValueState<RowData> state,
      List<RowData> source,
      long handle,
      boolean persistent,
      boolean validating)
      throws Exception {
    var rows = new ArrayList<RowData>(CHUNK_ROWS);
    for (var key : source) {
      backend.setCurrentKey(key);
      var wrapped = state.value();
      RowData accumulator = wrapped == null ? null : wrapped.getUserValue();
      if (wrapped != null) validateAccumulator(accumulator);
      var row = new GenericRowData(headerType.getFieldCount());
      row.setField(0, encode(key));
      row.setField(1, accumulator == null ? 0L : accumulator.getLong(recordCountField));
      row.setField(2, accumulator == null ? 0L : wrapped.getLastAccessTimestamp());
      for (int i = 0; i < kinds.length; i++) {
        if (accumulator == null) {
          row.setField(3 + 2 * i, kinds[i] == 0 || mapped(kinds[i]) ? null : Long.valueOf(0));
          row.setField(4 + 2 * i, 0L);
        } else if (mapped(kinds[i])) {
          var nested = accumulator.getRow(scalarFields[i], 3);
          row.setField(
              3 + 2 * i,
              nested.isNullAt(0)
                  ? null
                  : toCarrier(
                      RowData.createFieldGetter(valueTypes[i], 0).getFieldOrNull(nested),
                      valueTypes[i]));
          row.setField(4 + 2 * i, nested.getLong(1));
        } else {
          row.setField(
              3 + 2 * i,
              accumulator.isNullAt(scalarFields[i]) ? null : accumulator.getLong(scalarFields[i]));
          row.setField(
              4 + 2 * i,
              countFields[i] >= 0
                  ? accumulator.getLong(countFields[i])
                  : kinds[i] == 3 ? accumulator.getLong(scalarFields[i]) : 0L);
        }
      }
      row.setField(headerType.getFieldCount() - 1, accumulator != null);
      rows.add(row);
      if (rows.size() == CHUNK_ROWS) {
        batch(handle, persistent, false, rows, validating);
        rows.clear();
      }
    }
    if (!rows.isEmpty()) batch(handle, persistent, false, rows, validating);
  }

  private void visitEntries(
      CheckpointableKeyedStateBackend<RowData> backend,
      StockMapState<Object, Long>[] maps,
      List<RowData> source,
      long handle,
      boolean persistent,
      boolean validating)
      throws Exception {
    var rows = new ArrayList<RowData>(CHUNK_ROWS);
    for (var key : source) {
      backend.setCurrentKey(key);
      byte[] encoded = encode(key);
      for (int i = 0; i < maps.length; i++)
        if (maps[i] != null) {
          for (var entry : maps[i].entries()) {
            if (entry.getKey() == null
                || entry.getValue() == null
                || entry.getValue().getUserValue() == null)
              throw new IllegalStateException("NULL extrema map entry");
            rows.add(
                GenericRowData.of(
                    encoded,
                    (long) i,
                    toCarrier(entry.getKey(), valueTypes[i]),
                    entry.getValue().getUserValue(),
                    entry.getValue().getLastAccessTimestamp()));
            if (rows.size() == CHUNK_ROWS) {
              batch(handle, persistent, true, rows, validating);
              rows.clear();
            }
          }
        }
    }
    if (!rows.isEmpty()) batch(handle, persistent, true, rows, validating);
  }

  private void batch(
      long handle, boolean persistent, boolean entries, List<RowData> rows, boolean validating) {
    try (var root =
            RowDataArrowConverter.write(
                rows, entries ? entryType : headerType, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (validating)
        Native.validateGroupExtremaCanonical(
            handle, persistent, entries, array.memoryAddress(), schema.memoryAddress());
      else
        Native.importGroupExtremaCanonical(
            handle, persistent, entries, array.memoryAddress(), schema.memoryAddress());
    }
  }
}
