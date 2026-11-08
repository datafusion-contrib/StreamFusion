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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released arrival-ordered first-N counter, independent of the retained input schema. */
public final class StockFirstNStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final int CHUNK_ROWS = 1024;
  private static final String STATE = "counterState";
  private static final RowType BRIDGE =
      RowType.of(
          new VarBinaryType(false, VarBinaryType.MAX_LENGTH),
          new IntType(false),
          new BigIntType(false));
  private final TypeSerializer<RowData> keys;
  private final RowType keyType;
  private final boolean ttl;

  public StockFirstNStateContract(TypeSerializer<RowData> keys, RowType keyType, boolean ttl) {
    this.keys = keys;
    this.keyType = keyType;
    this.ttl = ttl;
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    return Map.of(STATE, StateDescriptor.Type.VALUE);
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  private StockValueState<Integer> state(CheckpointableKeyedStateBackend<RowData> backend)
      throws Exception {
    return new StockValueState<>(backend, STATE, IntSerializer.INSTANCE, ttl);
  }

  @Override
  public org.apache.flink.runtime.state.SavepointResources<RowData> snapshot(
      StockSnapshotContext context, long handle, boolean persistent) throws Exception {
    if (persistent)
      return context.nativeValue(
          STATE,
          IntSerializer.INSTANCE,
          ttl,
          () -> Native.captureFirstNCanonicalSnapshot(handle),
          BRIDGE,
          this::decode);
    return context.value(STATE, IntSerializer.INSTANCE, ttl, receiver -> visit(handle, receiver));
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    clear(backend);
    var state = state(backend);
    visit(
        handle,
        (key, value, written) -> {
          backend.setCurrentKey(key);
          state.update(value, written);
        });
  }

  private void visit(long handle, StockValueSnapshot.Entry<Integer> receiver) throws Exception {
    Native.exportFirstNCanonical(
        handle,
        (arrayAddress, schemaAddress) -> {
          try (ArrowArray array = ArrowArray.wrap(arrayAddress);
              ArrowSchema schema = ArrowSchema.wrap(schemaAddress);
              VectorSchemaRoot root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            var reader = ArrowConversion.createArrowReader(root, BRIDGE);
            for (int index = 0; index < root.getRowCount(); index++) {
              decode(reader.read(index), receiver);
            }
          } catch (Exception failure) {
            throw new IllegalStateException("Cannot project stock first-N counters", failure);
          }
        });
  }

  private void decode(RowData row, StockValueSnapshot.Entry<Integer> receiver) throws Exception {
    byte[] encoded = row.getBinary(0);
    var key = new BinaryRowData(keyType.getFieldCount());
    key.pointTo(MemorySegmentFactory.wrap(encoded), 0, encoded.length);
    receiver.write(key, row.getInt(1), row.getLong(2));
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    Native.exportFirstNCanonical(
        handle,
        (arrayAddress, schemaAddress) -> {
          try (ArrowArray array = ArrowArray.wrap(arrayAddress);
              ArrowSchema schema = ArrowSchema.wrap(schemaAddress);
              VectorSchemaRoot root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            if (root.getRowCount() != 0)
              throw new IllegalStateException(
                  "First-N canonical restore requires an empty native target");
          }
        });
    visit(backend, handle, true);
    visit(backend, handle, false);
  }

  private void visit(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean validateOnly)
      throws Exception {
    var state = state(backend);
    var serializer = new RowDataSerializer(keyType);
    List<RowData> rows = new ArrayList<>(CHUNK_ROWS);
    try (var stream = backend.getKeys(STATE, VoidNamespace.INSTANCE)) {
      var keys = stream.iterator();
      while (keys.hasNext()) {
        var key = keys.next();
        backend.setCurrentKey(key);
        var count = state.value();
        if (count == null) continue;
        var binary = serializer.toBinaryRow(key);
        byte[] encoded =
            BinarySegmentUtils.copyToBytes(
                binary.getSegments(), binary.getOffset(), binary.getSizeInBytes());
        rows.add(GenericRowData.of(encoded, count.getUserValue(), count.getLastAccessTimestamp()));
        if (rows.size() == CHUNK_ROWS) {
          importBatch(handle, rows, validateOnly);
          rows.clear();
        }
      }
    }
    if (!rows.isEmpty()) importBatch(handle, rows, validateOnly);
  }

  private static void importBatch(long handle, List<RowData> rows, boolean validateOnly) {
    try (VectorSchemaRoot root = RowDataArrowConverter.write(rows, BRIDGE, NativeAllocator.SHARED);
        ArrowArray array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (validateOnly)
        Native.validateFirstNCanonical(handle, array.memoryAddress(), schema.memoryAddress());
      else Native.importFirstNCanonical(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }
}
