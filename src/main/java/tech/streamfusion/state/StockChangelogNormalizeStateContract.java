package tech.streamfusion.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;

/** The released normalizer's full last row and original write age. */
public final class StockChangelogNormalizeStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final int CHUNK_ROWS = 1024;
  private static final String STATE = "deduplicate-state";
  private final TypeSerializer<RowData> keySerializer;
  private final RowType keyType;
  private final RowType rowType;
  private final RowType bridgeType;
  private final boolean ttl;

  public StockChangelogNormalizeStateContract(
      TypeSerializer<RowData> keySerializer, RowType keyType, RowType rowType, boolean ttl) {
    this.keySerializer = keySerializer;
    this.keyType = keyType;
    this.rowType = rowType;
    this.ttl = ttl;
    var fields = new ArrayList<>(rowType.getFields());
    add(fields, "binary_key", new VarBinaryType(false, VarBinaryType.MAX_LENGTH));
    add(fields, "__rowtime__", new BigIntType(false));
    add(fields, "__ttl_ts__", new BigIntType(false));
    add(fields, "__stored_kind__", new TinyIntType(false));
    bridgeType = new RowType(fields);
  }

  private static void add(
      List<RowType.RowField> fields,
      String name,
      org.apache.flink.table.types.logical.LogicalType type) {
    var names =
        fields.stream().map(RowType.RowField::getName).collect(java.util.stream.Collectors.toSet());
    while (names.contains(name)) name = "$" + name;
    fields.add(new RowType.RowField(name, type));
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(STATE, StateDescriptor.Type.VALUE);
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keySerializer.duplicate();
  }

  private StockValueState<RowData> state(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return new StockValueState<>(backend, STATE, new RowDataSerializer(rowType), ttl);
  }

  @Override
  public void clear(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    var state = state(backend);
    try (var keys = backend.getKeys(STATE, VoidNamespace.INSTANCE)) {
      for (var key :
          keys.map(backend.getKeySerializer()::copy)
              .collect(java.util.stream.Collectors.toList())) {
        backend.setCurrentKey(key);
        state.clear();
      }
    }
  }

  private org.apache.arrow.vector.types.pojo.Schema arrowSchema(RowType type) {
    var schema = ArrowConversion.toArrowSchema(type);
    return keyType.getChildren().stream()
            .anyMatch(StockChangelogNormalizeStateContract::containsRow)
        ? tech.streamfusion.arrow.NestedRepresentation.withBinaryRows(schema)
        : schema;
  }

  private static boolean containsRow(org.apache.flink.table.types.logical.LogicalType type) {
    return type.is(org.apache.flink.table.types.logical.LogicalTypeRoot.ROW)
        || type.getChildren().stream().anyMatch(StockChangelogNormalizeStateContract::containsRow);
  }

  private VectorSchemaRoot writeRows(List<RowData> rows) {
    var root = VectorSchemaRoot.create(arrowSchema(bridgeType), NativeAllocator.SHARED);
    try {
      var writer = ArrowConversion.createRowDataArrowWriter(root, bridgeType);
      for (var row : rows) writer.write(row);
      writer.finish();
      return root;
    } catch (RuntimeException | Error failure) {
      root.close();
      throw failure;
    }
  }

  private void exportNative(long handle, boolean persistent, Native.BatchReceiver receiver) {
    try (ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try {
        Data.exportSchema(
            NativeAllocator.SHARED, arrowSchema(rowType), NativeAllocator.DICTIONARIES, schema);
        Native.exportChangelogNormalizerCanonical(
            handle, persistent, schema.memoryAddress(), receiver);
      } finally {
        if (schema.snapshot().release != 0) schema.release();
      }
    }
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    clear(backend);
    var state = state(backend);
    int arity = rowType.getFieldCount();
    var serializer = new RowDataSerializer(rowType);
    exportNative(
        handle,
        persistent,
        (arrayAddress, schemaAddress) -> {
          try (ArrowArray array = ArrowArray.wrap(arrayAddress);
              ArrowSchema schema = ArrowSchema.wrap(schemaAddress);
              VectorSchemaRoot root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            var reader = ArrowConversion.createArrowReader(root, bridgeType);
            var getters = getters();
            for (int index = 0; index < root.getRowCount(); index++) {
              var row = reader.read(index);
              byte[] encoded = row.getBinary(arity);
              var key = new BinaryRowData(keyType.getFieldCount());
              key.pointTo(MemorySegmentFactory.wrap(encoded), 0, encoded.length);
              backend.setCurrentKey(key);
              var payload = new GenericRowData(arity);
              for (int field = 0; field < arity; field++)
                payload.setField(field, getters[field].getFieldOrNull(row));
              state.update(serializer.copy(payload), row.getLong(arity + 2));
            }
          } catch (Exception failure) {
            throw new IllegalStateException("Cannot project stock normalizer rows", failure);
          }
        });
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    exportNative(
        handle,
        persistent,
        (arrayAddress, schemaAddress) -> {
          try (ArrowArray array = ArrowArray.wrap(arrayAddress);
              ArrowSchema schema = ArrowSchema.wrap(schemaAddress);
              VectorSchemaRoot root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            if (root.getRowCount() != 0)
              throw new IllegalStateException(
                  "Normalizer canonical restore requires an empty native target");
          }
        });
    visit(backend, handle, persistent, true);
    visit(backend, handle, persistent, false);
  }

  private RowData.FieldGetter[] getters() {
    var getters = new RowData.FieldGetter[rowType.getFieldCount()];
    for (int field = 0; field < getters.length; field++)
      getters[field] = RowData.createFieldGetter(rowType.getTypeAt(field), field);
    return getters;
  }

  private void visit(
      CheckpointableKeyedStateBackend<RowData> backend,
      long handle,
      boolean persistent,
      boolean validateOnly)
      throws Exception {
    var state = state(backend);
    var keyRows = new RowDataSerializer(keyType);
    var rowSerializer = new RowDataSerializer(rowType);
    var getters = getters();
    int arity = rowType.getFieldCount();
    var rows = new ArrayList<RowData>(CHUNK_ROWS);
    try (var stream = backend.getKeys(STATE, VoidNamespace.INSTANCE)) {
      var keys = stream.iterator();
      while (keys.hasNext()) {
        var key = keys.next();
        backend.setCurrentKey(key);
        var stored = state.value();
        if (stored == null || stored.getUserValue() == null) continue;
        RowData value = rowSerializer.copy(stored.getUserValue());
        var row = new GenericRowData(arity + 4);
        for (int field = 0; field < arity; field++)
          row.setField(field, getters[field].getFieldOrNull(value));
        var binary = keyRows.toBinaryRow(key);
        row.setField(
            arity,
            BinarySegmentUtils.copyToBytes(
                binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()));
        row.setField(arity + 1, 0L);
        row.setField(arity + 2, stored.getLastAccessTimestamp());
        row.setField(arity + 3, value.getRowKind().toByteValue());
        rows.add(row);
        if (rows.size() == CHUNK_ROWS) {
          importBatch(handle, persistent, rows, validateOnly);
          rows.clear();
        }
      }
    }
    if (!rows.isEmpty()) importBatch(handle, persistent, rows, validateOnly);
  }

  private void importBatch(
      long handle, boolean persistent, List<RowData> rows, boolean validateOnly) {
    try (VectorSchemaRoot root = writeRows(rows);
        ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (validateOnly)
        Native.validateChangelogNormalizerCanonical(
            handle, persistent, array.memoryAddress(), schema.memoryAddress());
      else
        Native.importChangelogNormalizerCanonical(
            handle, persistent, array.memoryAddress(), schema.memoryAddress());
    }
  }
}
