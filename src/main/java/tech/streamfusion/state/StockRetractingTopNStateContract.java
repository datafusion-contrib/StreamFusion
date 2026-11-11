package tech.streamfusion.state;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
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
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.ComparableRecordComparator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.SortedMapSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.MapListRepresentation;
import tech.streamfusion.compat.FlinkStateBackendCompat;
import tech.streamfusion.operator.NativeAllocator;

/** Independent released retracting row lists, counts and bounds, without rewriting their ages. */
public final class StockRetractingTopNStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final byte COUNTS = -1, COUNT = -2, LIST = -3, BOUND = -4;
  private static final String DATA = "data-state", SORTED = "sorted-map";
  private final TypeSerializer<RowData> keys;
  private final RowDataKeySelector partitionSelector;
  private final RowDataKeySelector sortSelector;
  private final RowType keyType, rowType, sortType, bridgeType;
  private final Comparator<RowData> comparator;
  private final boolean variable, ttl;

  public StockRetractingTopNStateContract(
      TypeSerializer<RowData> keys,
      RowDataKeySelector partitionSelector,
      RowDataKeySelector sortSelector,
      RowType keyType,
      RowType rowType,
      ComparableRecordComparator comparator,
      boolean variable,
      boolean ttl) {
    this.keys = keys;
    this.partitionSelector = partitionSelector;
    this.sortSelector = sortSelector;
    this.keyType = keyType;
    this.rowType = rowType;
    this.comparator = comparator;
    this.variable = variable;
    this.ttl = ttl;
    sortType = (RowType) sortSelector.getProducedType().toLogicalType();
    var fields = new ArrayList<RowType.RowField>();
    for (var field : rowType.getFields())
      fields.add(new RowType.RowField(field.getName(), field.getType().copy(true)));
    for (LogicalType type :
        List.of(
            new VarBinaryType(false, VarBinaryType.MAX_LENGTH),
            new BigIntType(false),
            new BigIntType(false),
            new TinyIntType(false))) add(fields, type);
    for (LogicalType type : sortType.getChildren()) add(fields, type.copy(true));
    bridgeType = new RowType(fields);
  }

  private static void add(List<RowType.RowField> fields, LogicalType type) {
    String name = "$stock_meta_" + fields.size();
    var names =
        fields.stream().map(RowType.RowField::getName).collect(java.util.stream.Collectors.toSet());
    while (names.contains(name)) name = "$" + name;
    fields.add(new RowType.RowField(name, type));
  }

  @Override
  public void configureNativeState(long handle, boolean persistent) {
    Native.enableStockRetractingTopNState(handle, persistent);
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    var states = new java.util.HashMap<String, StateDescriptor.Type>();
    states.put(DATA, StateDescriptor.Type.MAP);
    states.put(SORTED, StateDescriptor.Type.VALUE);
    if (variable) states.put("rankEnd", StateDescriptor.Type.VALUE);
    return Map.copyOf(states);
  }

  @SuppressWarnings("unchecked")
  private MapState<RowData, Object> data(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    TypeSerializer<?> values = new ListSerializer<>(new RowDataSerializer(rowType));
    if (ttl) values = new TtlSerializer<>(LongSerializer.INSTANCE, values);
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new MapStateDescriptor<>(
            DATA, new RowDataSerializer(sortType), (TypeSerializer<Object>) values));
  }

  private StockValueState<SortedMap<RowData, Long>> counts(
      CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    return new StockValueState<>(
        backend,
        SORTED,
        new SortedMapSerializer<>(
            comparator, new RowDataSerializer(sortType), LongSerializer.INSTANCE),
        ttl);
  }

  private StockValueState<Long> bound(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return new StockValueState<>(backend, "rankEnd", LongSerializer.INSTANCE, ttl);
  }

  private static void validateKey(RowData key, RowType type) {
    if (key == null || key.getArity() != type.getFieldCount() || key.getRowKind() != RowKind.INSERT)
      throw new IllegalStateException("Invalid retracting state key");
    for (int column = 0; column < type.getFieldCount(); column++)
      if (!type.getTypeAt(column).isNullable() && key.isNullAt(column))
        throw new IllegalStateException("NULL in a non-null retracting key");
  }

  private void validatePayload(RowData partition, RowData sort, RowData row) throws Exception {
    if (row == null || row.getArity() != rowType.getFieldCount() || row.getRowKind() == null)
      throw new IllegalStateException("Invalid retracting retained row");
    for (int column = 0; column < rowType.getFieldCount(); column++)
      if (!rowType.getTypeAt(column).isNullable()
          && row.isNullAt(column)
          && !org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute(
              rowType.getTypeAt(column)))
        throw new IllegalStateException("NULL in a non-null retracting row");
    var keys = new StockKeyProjection(keyType);
    var sorts = new StockKeyProjection(sortType);
    if (!keys.project(partition).equals(keys.project(partitionSelector.getKey(row)))
        || !sorts.project(sort).equals(sorts.project(sortSelector.getKey(row))))
      throw new IllegalStateException("Retracting row differs from its partition or sort key");
  }

  private RowData section(RowData bridge, int offset, RowType type) {
    var result = new GenericRowData(type.getFieldCount());
    for (int column = 0; column < type.getFieldCount(); column++)
      result.setField(
          column,
          RowData.createFieldGetter(bridgeType.getTypeAt(offset + column), offset + column)
              .getFieldOrNull(bridge));
    return result;
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    configureNativeState(handle, persistent);
    clear(backend);
    var data = data(backend);
    var counts = counts(backend);
    var bound = variable ? bound(backend) : null;
    var rows = new RowDataSerializer(rowType);
    var sorts = new StockKeyProjection(sortType);
    int arity = rowType.getFieldCount();
    @SuppressWarnings({"unchecked", "rawtypes"})
    var counterSerializer =
        new TupleSerializer<Tuple2<RowData, Long>>(
            (Class) Tuple2.class,
            new TypeSerializer<?>[] {new RowDataSerializer(sortType), LongSerializer.INSTANCE});
    try (var staged =
            new StockMapListSpool<RowData, RowData>(keys, new RowDataSerializer(sortType), rows);
        var counters =
            new StockMapListSpool<Long, Tuple2<RowData, Long>>(
                keys, LongSerializer.INSTANCE, counterSerializer)) {
      export(
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
                var row = reader.read(index);
                var encoded = row.getBinary(arity);
                var key = new BinaryRowData(keyType.getFieldCount());
                key.pointTo(MemorySegmentFactory.wrap(encoded), 0, encoded.length);
                validateKey(key, keyType);
                backend.setCurrentKey(key);
                byte tag = row.getByte(arity + 3);
                long age = row.getLong(arity + 2);
                long number = row.getLong(arity + 1);
                if (!ttl && age != 0)
                  throw new IllegalStateException("Unexpected retracting TTL age");
                if (tag == COUNTS) {
                  if (!counters.start(key, 0L, age))
                    throw new IllegalStateException("Duplicate retracting count map");
                } else if (tag == BOUND) {
                  if (bound == null || bound.value() != null)
                    throw new IllegalStateException("Unexpected retracting bound");
                  bound.update(number, age);
                } else {
                  RowData sort = section(row, arity + 4, sortType);
                  validateKey(sort, sortType);
                  sort = sorts.project(sort);
                  if (tag == COUNT) {
                    var stamp = counters.metadata(key, 0L);
                    if (stamp == null || stamp != age)
                      throw new IllegalStateException(
                          "Missing, inconsistent or duplicate retracting counter");
                    counters.append(key, 0L, Tuple2.of(sort, number));
                  } else if (tag == LIST) {
                    if (!staged.start(key, sort, age))
                      throw new IllegalStateException("Duplicate retracting data list");
                  } else if (tag >= 0 && tag <= 3) {
                    var stamp = staged.metadata(key, sort);
                    if (stamp == null || stamp != age)
                      throw new IllegalStateException(
                          "Missing or inconsistent retracting list header");
                    RowData payload = section(row, 0, rowType);
                    validatePayload(key, sort, payload);
                    payload = rows.copy(payload);
                    payload.setRowKind(RowKind.fromByteValue(tag));
                    staged.append(key, sort, payload);
                  } else throw new IllegalStateException("Invalid retracting transfer tag");
                }
              }
            } catch (Exception failure) {
              throw new IllegalStateException("Cannot project released retracting state", failure);
            }
          });
      staged.visitWithMetadata(
          backend,
          (owner, sort, retained, age) -> {
            backend.setCurrentKey(owner);
            data.put(sort, ttl ? new TtlValue<>(retained, age) : retained);
          });
      counters.visitWithMetadata(
          backend,
          (owner, unused, entries, age) -> {
            backend.setCurrentKey(owner);
            SortedMap<RowData, Long> ordered = new StockSortedMap<>(entries, comparator);
            if (!FlinkStateBackendCompat.serializesKeyedValues(backend))
              ordered = new TreeMap<>(ordered);
            counts.update(ordered, age);
          });
    }
  }

  private void export(long handle, boolean persistent, Native.BatchReceiver receiver)
      throws Exception {
    try (var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        AutoCloseable release =
            () -> {
              if (schema.snapshot().release != 0) schema.release();
            }) {
      Data.exportSchema(
          NativeAllocator.SHARED,
          MapListRepresentation.forIpc(ArrowConversion.toArrowSchema(rowType)),
          NativeAllocator.DICTIONARIES,
          schema);
      Native.exportRetractingTopNCanonical(handle, persistent, schema.memoryAddress(), receiver);
    }
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    configureNativeState(handle, persistent);
    StockStateInventory.validate(backend, stateDescriptors(), java.util.Set.of(), keys);
    export(
        handle,
        persistent,
        (arrayAddress, schemaAddress) -> {
          try (var array = ArrowArray.wrap(arrayAddress);
              var schema = ArrowSchema.wrap(schemaAddress);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            if (root.getRowCount() != 0)
              throw new IllegalStateException("Retracting restore requires an empty target");
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
    var binary = new RowDataSerializer(keyType);
    var sortRows = new RowDataSerializer(sortType);
    var sorts = new StockKeyProjection(sortType);
    var rows = new RowDataSerializer(rowType);
    var pending =
        new StockRowBatch(
            bridgeType,
            true,
            (transferred, array, schema) ->
                batch(handle, persistent, transferred, validating, array, schema));
    var ownerType =
        new RowDataSerializer(RowType.of(keyType, new VarBinaryType(VarBinaryType.MAX_LENGTH)));
    try (var inventory = new StockStateSpool(ownerType)) {
      for (String name : stateDescriptors().keySet())
        try (var stream = backend.getKeys(name, VoidNamespace.INSTANCE)) {
          var iterator = stream.iterator();
          while (iterator.hasNext()) validateKey(iterator.next(), keyType);
        }
      TypeSerializer<?> countType =
          new SortedMapSerializer<>(comparator, sortRows, LongSerializer.INSTANCE);
      if (ttl) countType = new TtlSerializer<>(LongSerializer.INSTANCE, countType);
      StockMapListSnapshot.visitRaw(
          backend,
          SORTED,
          keys,
          VoidNamespaceSerializer.INSTANCE,
          null,
          countType,
          (key, namespace, unused, value) -> {
            validateKey(key, keyType);
            if (!inventory.add(1, GenericRowData.of(key, new byte[0]), 0))
              throw new IllegalStateException("Duplicate retracting count map");
            byte[] owner = bytes(binary.toBinaryRow(key));
            var counters = StockRankValues.counters(value, ttl, sortRows);
            pending.append(transfer(owner, null, null, COUNTS, 0, counters.written));
            for (var entry : counters) {
              validateKey(entry.f0, sortType);
              var sort = sorts.project(entry.f0);
              if (entry.f1 == null || !inventory.add(2, GenericRowData.of(key, bytes(sort)), 0))
                throw new IllegalStateException("Invalid retracting counter");
              pending.append(transfer(owner, sort, null, COUNT, entry.f1, counters.written));
            }
          });
      if (variable) {
        var bound = bound(backend);
        try (var stream = backend.getKeys("rankEnd", VoidNamespace.INSTANCE)) {
          var iterator = stream.iterator();
          while (iterator.hasNext()) {
            var key = keys.copy(iterator.next());
            validateKey(key, keyType);
            backend.setCurrentKey(key);
            var value = bound.value();
            if (value == null || value.getUserValue() == null)
              throw new IllegalStateException("NULL retracting bound");
            if (!inventory.add(3, GenericRowData.of(key, new byte[0]), 0))
              throw new IllegalStateException("Duplicate retracting bound");
            pending.append(
                transfer(
                    bytes(binary.toBinaryRow(key)),
                    null,
                    null,
                    BOUND,
                    value.getUserValue(),
                    value.getLastAccessTimestamp()));
          }
        }
      }
      TypeSerializer<?> listType = new ListSerializer<>(rows);
      if (ttl) listType = new TtlSerializer<>(LongSerializer.INSTANCE, listType);
      StockMapListSnapshot.visitRaw(
          backend,
          DATA,
          keys,
          VoidNamespaceSerializer.INSTANCE,
          sortRows,
          listType,
          (key, namespace, sourceSort, value) -> {
            validateKey(key, keyType);
            validateKey(sourceSort, sortType);
            var sort = sorts.project(sourceSort);
            if (!inventory.add(4, GenericRowData.of(key, bytes(sort)), 0))
              throw new IllegalStateException("Invalid retracting data list");
            byte[] owner = bytes(binary.toBinaryRow(key));
            var list = StockRankValues.list(value, ttl, rows);
            pending.append(transfer(owner, sort, null, LIST, 0, list.written));
            for (var payload : list) {
              validatePayload(key, sort, payload);
              pending.append(
                  transfer(
                      owner, sort, payload, payload.getRowKind().toByteValue(), 0, list.written));
            }
          });
      pending.flush();
    }
  }

  private RowData transfer(
      byte[] key, RowData sort, RowData payload, byte tag, long number, long age) {
    int arity = rowType.getFieldCount();
    var result = new GenericRowData(bridgeType.getFieldCount());
    if (payload != null)
      for (int column = 0; column < arity; column++)
        result.setField(
            column,
            RowData.createFieldGetter(bridgeType.getTypeAt(column), column)
                .getFieldOrNull(payload));
    result.setField(arity, key);
    result.setField(arity + 1, number);
    result.setField(arity + 2, age);
    result.setField(arity + 3, tag);
    if (sort != null)
      for (int column = 0; column < sortType.getFieldCount(); column++)
        result.setField(
            arity + 4 + column,
            RowData.createFieldGetter(sortType.getTypeAt(column), column).getFieldOrNull(sort));
    return result;
  }

  private static byte[] bytes(BinaryRowData row) {
    return BinarySegmentUtils.copyToBytes(row.getSegments(), row.getOffset(), row.getSizeInBytes());
  }

  private void batch(
      long handle,
      boolean persistent,
      List<RowData> rows,
      boolean validating,
      long array,
      long schema) {
    if (validating) Native.validateRetractingTopNCanonical(handle, persistent, array, schema);
    else Native.importRetractingTopNCanonical(handle, persistent, array, schema);
  }
}
