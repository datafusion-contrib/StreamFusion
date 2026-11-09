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

/** Shared flat OVER metadata bridge; timer lists never cross the export boundary. */
final class StockOverCallbacks {
  static final byte CLEANUP = 0, LAST_TRIGGER = 1, EVENT = 2, PROCESSING = 3;

  private StockOverCallbacks() {}

  @FunctionalInterface
  interface Receiver {
    void accept(BinaryRowData owner, byte kind, long timestamp) throws Exception;
  }

  static void export(
      CheckpointableKeyedStateBackend<RowData> backend,
      long handle,
      RowType keys,
      Receiver receiver) {
    var fields = new ArrayList<RowType.RowField>();
    for (int p = 0; p < keys.getFieldCount(); p++)
      fields.add(new RowType.RowField("key" + p, keys.getTypeAt(p).copy(true)));
    fields.add(new RowType.RowField("__over_callback_kind", new TinyIntType(false)));
    fields.add(new RowType.RowField("__over_callback_time", new BigIntType(false)));
    var type = new RowType(fields);
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
