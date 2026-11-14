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
  private final int[] nativeFields, decimalSumCounts, decimalSumAggregates;
  private final boolean ttl, preserveRowKeys;
  private final List<StockOverDistinctView> views;
  private final RowData.FieldGetter[] keyGetters, valueGetters;

  public StockOverFoldStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulatorType,
      int[] kinds,
      int[] widths,
      boolean ttl) {
    this(keys, keyType, accumulatorType, kinds, widths, ttl, List.of());
  }

  public StockOverFoldStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulatorType,
      int[] kinds,
      int[] widths,
      boolean ttl,
      List<StockOverDistinctView> views) {
    this(keys, keyType, accumulatorType, kinds, widths, ttl, views, false);
  }

  public StockOverFoldStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType accumulatorType,
      int[] kinds,
      int[] widths,
      boolean ttl,
      List<StockOverDistinctView> views,
      boolean preserveRowKeys) {
    this.preserveRowKeys = preserveRowKeys;
    this.views = List.copyOf(views);
    boolean rowKeys = keyType.getChildren().stream().anyMatch(StockKeyedRowSchema::containsRow);
    if (rowKeys && !preserveRowKeys)
      throw new IllegalArgumentException("Unqualified processing OVER ROW partition layout");
    boolean[] distinct = new boolean[kinds.length];
    if (ttl && !views.isEmpty())
      throw new IllegalArgumentException("OVER DISTINCT TTL lifetimes are not bound");
    for (var view : views) view.validateAggregates(kinds, distinct);
    for (int index = 0; index < kinds.length; index++)
      if (kinds[index] >= 100 && !distinct[index])
        throw new IllegalArgumentException("OVER DISTINCT membership view is missing");
    this.keys = keys.duplicate();
    this.keyType = keyType;
    this.accumulatorType = accumulatorType;
    this.kinds = Arrays.stream(kinds).map(kind -> kind >= 100 ? kind - 100 : kind).toArray();
    kinds = this.kinds;
    this.widths = widths.clone();
    this.ttl = ttl;
    if (keyType.getChildren().stream()
            .anyMatch(
                type ->
                    !(preserveRowKeys
                        ? supportsProcessingPartitionKey(type)
                        : supportsPartitionKey(type)))
        || kinds.length == 0
        || kinds.length != widths.length
        || Arrays.stream(widths).sum() + views.size() != accumulatorType.getFieldCount())
      throw new IllegalArgumentException("Unqualified unbounded OVER key or accumulator layout");
    int scalarCount = Arrays.stream(widths).sum();
    for (int field = scalarCount; field < accumulatorType.getFieldCount(); field++) {
      var type = accumulatorType.getTypeAt(field);
      if (!(type instanceof org.apache.flink.table.types.logical.RawType)
          || !(((org.apache.flink.table.types.logical.RawType<?>) type).getTypeSerializer()
              instanceof org.apache.flink.table.dataview.NullSerializer))
        throw new IllegalArgumentException("Unqualified OVER DISTINCT placeholder");
    }
    int slot = 0;
    for (int i = 0; i < kinds.length; i++) {
      boolean extreme = kinds[i] == 1 || kinds[i] == 2;
      if (kinds[i] != 0 && kinds[i] != 3 && kinds[i] != 4 && kinds[i] != 22 && !extreme
          || widths[i] != (kinds[i] == 4 ? 2 : 1)
          || !(extreme
              ? supportsExtremumType(accumulatorType.getTypeAt(slot))
              : numeric(accumulatorType.getTypeAt(slot)))
          || kinds[i] == 3 && !accumulatorType.getTypeAt(slot).is(LogicalTypeRoot.BIGINT)
          || widths[i] == 2 && !accumulatorType.getTypeAt(slot + 1).is(LogicalTypeRoot.BIGINT))
        throw new IllegalArgumentException("Unqualified unbounded OVER scalar schema");
      slot += widths[i];
    }
    var fields = new ArrayList<RowType.RowField>();
    for (int i = 0; i < keyType.getFieldCount(); i++)
      fields.add(new RowType.RowField("key" + i, keyType.getTypeAt(i).copy(true)));
    nativeFields = new int[scalarCount];
    decimalSumCounts = new int[scalarCount];
    decimalSumAggregates = new int[scalarCount];
    Arrays.fill(decimalSumCounts, -1);
    Arrays.fill(decimalSumAggregates, -1);
    slot = 0;
    int nativeSlot = 0;
    for (int i = 0; i < kinds.length; i++) {
      for (int p = 0; p < widths[i]; p++) {
        nativeFields[slot + p] = keyType.getFieldCount() + nativeSlot;
        fields.add(
            new RowType.RowField(
                "state" + nativeSlot++, accumulatorType.getTypeAt(slot + p).copy(true)));
      }
      if (kinds[i] == 0 && accumulatorType.getTypeAt(slot).is(LogicalTypeRoot.DECIMAL)) {
        decimalSumCounts[slot] = keyType.getFieldCount() + nativeSlot;
        if (distinct[i]) decimalSumAggregates[slot] = i;
        fields.add(new RowType.RowField("state" + nativeSlot++, new BigIntType()));
      }
      slot += widths[i];
    }
    fields.add(new RowType.RowField("__over_retention_stamp", new BigIntType(false)));
    bridge = new RowType(fields);
    keyGetters = getters(keyType, 0);
    valueGetters = Arrays.copyOf(getters(accumulatorType, 0), scalarCount);
  }

  private static boolean numeric(LogicalType type) {
    return type.is(LogicalTypeRoot.TINYINT)
        || type.is(LogicalTypeRoot.SMALLINT)
        || type.is(LogicalTypeRoot.INTEGER)
        || type.is(LogicalTypeRoot.BIGINT)
        || type.is(LogicalTypeRoot.FLOAT)
        || type.is(LogicalTypeRoot.DOUBLE)
        || type instanceof org.apache.flink.table.types.logical.DecimalType
            && ((org.apache.flink.table.types.logical.DecimalType) type).getPrecision() == 38;
  }

  public static boolean supportsPartitionKey(LogicalType type) {
    switch (type.getTypeRoot()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case BOOLEAN:
      case CHAR:
      case VARCHAR:
      case DECIMAL:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return true;
      default:
        return false;
    }
  }

  public static boolean supportsProcessingPartitionKey(LogicalType type) {
    return type.is(LogicalTypeRoot.ROW)
        ? type.getChildren().stream()
            .allMatch(StockOverFoldStateContract::supportsProcessingPartitionKey)
        : supportsPartitionKey(type);
  }

  private org.apache.arrow.vector.types.pojo.Schema bridgeSchema() {
    return preserveRowKeys
        ? ArrowConversion.toBinaryRowKeySchema(
            bridge, java.util.stream.IntStream.range(0, keyType.getFieldCount()).toArray())
        : ArrowConversion.toArrowSchema(bridge);
  }

  public static boolean supportsExtremumType(LogicalType type) {
    switch (type.getTypeRoot()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case CHAR:
      case VARCHAR:
      case DECIMAL:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return true;
      default:
        return false;
    }
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    var result = new java.util.LinkedHashMap<String, StateDescriptor.Type>();
    result.put(STATE, StateDescriptor.Type.VALUE);
    for (var view : views)
      view.stateDescriptors()
          .forEach(
              (name, type) -> {
                if (result.putIfAbsent(name, type) != null)
                  throw new IllegalArgumentException("Duplicate OVER DISTINCT state name: " + name);
              });
    return Map.copyOf(result);
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
      if (kinds[i] != 0
              && kinds[i] != 1
              && kinds[i] != 2
              && !accumulatorType.getTypeAt(slot).is(LogicalTypeRoot.DECIMAL)
              && value.isNullAt(slot)
          || widths[i] == 2 && value.isNullAt(slot + 1))
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
      var expected = bridgeSchema().getFields();
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
  public org.apache.flink.runtime.state.SavepointResources<RowData> snapshot(
      StockSnapshotContext context, long handle, boolean persistent) throws Exception {
    if (persistent && views.isEmpty())
      return context.nativeValue(
          STATE,
          new RowDataSerializer(accumulatorType),
          ttl,
          () -> Native.captureOverFoldCanonicalSnapshot(handle),
          bridge,
          decoder());
    return null;
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    configureNativeState(handle, persistent);
    clear(backend);
    var state = state(backend);
    visit(
        handle,
        persistent,
        (key, value, written) -> {
          backend.setCurrentKey(key);
          state.update(value, written);
        });
    for (var view : views) view.exportState(backend, handle);
  }

  @Override
  public void configureNativeState(long handle, boolean persistent) {
    for (var view : views) view.configure(handle);
  }

  private void visit(
      long handle, boolean persistent, StockNativeValueSnapshot.Entry<RowData> receiver)
      throws Exception {
    var decoder = decoder();
    Native.exportOverFoldCanonical(
        handle, (array, schema) -> receive(array, schema, row -> decoder.decode(row, receiver)));
  }

  private StockNativeValueSnapshot.Decoder<RowData> decoder() {
    var keyRows = new StockKeyProjection(keyType);
    var getters = new RowData.FieldGetter[nativeFields.length];
    for (int i = 0; i < getters.length; i++)
      getters[i] =
          RowData.createFieldGetter(accumulatorType.getTypeAt(i).copy(true), nativeFields[i]);
    return (row, receiver) -> {
      var key = copy(row, keyGetters);
      var accumulator = new GenericRowData(accumulatorType.getFieldCount());
      for (int i = 0; i < nativeFields.length; i++) {
        boolean emptySum = decimalSumCounts[i] >= 0 && row.getLong(decimalSumCounts[i]) == 0;
        accumulator.setField(i, emptySum ? null : getters[i].getFieldOrNull(row));
      }
      for (int field = nativeFields.length; field < accumulator.getArity(); field++)
        accumulator.setField(
            field, org.apache.flink.table.data.binary.BinaryRawValueData.fromBytes(new byte[] {0}));
      validateKey(key);
      validateAccumulator(accumulator);
      receiver.write(keyRows.project(key), accumulator, row.getLong(bridge.getFieldCount() - 1));
    };
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    StockStateInventory.validate(backend, stateDescriptors(), Set.of(), keys);
    configureNativeState(handle, persistent);
    requireEmpty(handle);
    transferState(backend, handle, true);
    int[] scalarFields = new int[kinds.length];
    for (int index = 1; index < kinds.length; index++)
      scalarFields[index] = scalarFields[index - 1] + widths[index - 1];
    for (var view : views)
      view.validate(
          backend, state(backend), scalarFields, kinds, accumulatorType, this::validateKey);
    transferState(backend, handle, false);
    for (var view : views) view.importState(backend, handle);
    if (!views.isEmpty()) Native.finishOverCanonicalDistinct(handle);
    clear(backend);
  }

  void requireEmpty(long handle) {
    for (var view : views) view.requireEmpty(handle);
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
          row.setField(nativeFields[i], valueGetters[i].getFieldOrNull(value));
        for (int i = 0; i < decimalSumCounts.length; i++)
          if (decimalSumCounts[i] >= 0) {
            // Stock SUM omits its count. DISTINCT membership can recover it without replaying sums.
            long count = value.isNullAt(i) ? 0L : 1L;
            if (decimalSumAggregates[i] >= 0) {
              count = -1;
              for (var view : views) {
                count = view.cardinality(backend, decimalSumAggregates[i]);
                if (count >= 0) break;
              }
              if (count < 0) throw new IllegalStateException("Missing decimal DISTINCT SUM view");
            }
            row.setField(decimalSumCounts[i], count);
          }
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
    try (var root =
            preserveRowKeys
                ? StockKeyedRowSchema.write(rows, bridge, bridgeSchema())
                : RowDataArrowConverter.write(rows, bridge, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (validating)
        Native.validateOverFoldCanonical(handle, array.memoryAddress(), schema.memoryAddress());
      else Native.importOverFoldCanonical(handle, array.memoryAddress(), schema.memoryAddress());
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
