package tech.streamfusion.state;

import java.util.ArrayList;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Shared bounded flat OVER metadata bridge, independent of the number of timers per owner. */
final class StockOverCallbacks {
  static final byte CLEANUP = 0, LAST_TRIGGER = 1, EVENT = 2, PROCESSING = 3;

  private StockOverCallbacks() {}

  @FunctionalInterface
  interface Receiver {
    void accept(BinaryRowData owner, byte kind, long timestamp) throws Exception;
  }

  private static RowType type(RowType keys) {
    var fields = new ArrayList<RowType.RowField>();
    for (int p = 0; p < keys.getFieldCount(); p++)
      fields.add(new RowType.RowField("key" + p, keys.getTypeAt(p).copy(true)));
    fields.add(new RowType.RowField("__over_callback_kind", new TinyIntType(false)));
    fields.add(new RowType.RowField("__over_callback_time", new BigIntType(false)));
    return new RowType(fields);
  }

  static final class Writer {
    private final long handle;
    private final RowType type;
    private final boolean validating;
    private final RowDataSerializer serializer;
    private final RowData.FieldGetter[] keys;
    private final ArrayList<RowData> rows = new ArrayList<>(1024);
    private long bytes;

    Writer(long handle, RowType keys, boolean validating) {
      this.handle = handle;
      this.type = type(keys);
      this.validating = validating;
      this.serializer = new RowDataSerializer(type);
      this.keys = new RowData.FieldGetter[keys.getFieldCount()];
      for (int i = 0; i < this.keys.length; i++)
        this.keys[i] = RowData.createFieldGetter(keys.getTypeAt(i).copy(true), i);
    }

    void append(RowData owner, byte kind, long timestamp) throws Exception {
      var row = new GenericRowData(type.getFieldCount());
      for (int i = 0; i < keys.length; i++) row.setField(i, keys[i].getFieldOrNull(owner));
      row.setField(keys.length, kind);
      row.setField(keys.length + 1, timestamp);
      long size = serializer.toBinaryRow(row).getSizeInBytes();
      if (!rows.isEmpty() && (rows.size() == 1024 || bytes + size > 4L * 1024 * 1024)) send(false);
      rows.add(row);
      bytes += size;
    }

    void finish() {
      send(true);
    }

    private void send(boolean last) {
      try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED);
          var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
          var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
        Data.exportVectorSchemaRoot(
            NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
        Native.importOverCanonicalCallbacks(
            handle, array.memoryAddress(), schema.memoryAddress(), last, validating);
      }
      rows.clear();
      bytes = 0;
    }
  }

  static void export(
      CheckpointableKeyedStateBackend<RowData> backend,
      long handle,
      RowType keys,
      Receiver receiver) {
    var type = type(keys);
    var serializer = new RowDataSerializer(keys);
    Native.exportOverCanonicalCallbacks(
        handle,
        (a, s) -> {
          try (var array = ArrowArray.wrap(a);
              var schema = ArrowSchema.wrap(s);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            if (root.getFieldVectors().size() != type.getFieldCount())
              throw new IllegalArgumentException("OVER callback schema changed");
            var reader = ArrowConversion.createArrowReader(root, type);
            for (int i = 0; i < root.getRowCount(); i++) {
              var row = reader.read(i);
              var owner = new GenericRowData(keys.getFieldCount());
              for (int p = 0; p < keys.getFieldCount(); p++)
                owner.setField(
                    p,
                    RowData.createFieldGetter(keys.getTypeAt(p).copy(true), p).getFieldOrNull(row));
              var binary = serializer.toBinaryRow(owner).copy();
              backend.setCurrentKey(binary);
              int base = keys.getFieldCount();
              if (row.isNullAt(base) || row.isNullAt(base + 1))
                throw new IllegalArgumentException("NULL OVER callback record");
              byte kind = row.getByte(base);
              if (kind < CLEANUP || kind > PROCESSING)
                throw new IllegalArgumentException("Unknown OVER callback kind");
              receiver.accept(binary, kind, row.getLong(base + 1));
            }
          } catch (Exception failure) {
            throw new IllegalStateException("Cannot export OVER callbacks", failure);
          }
        });
  }
}
