package tech.streamfusion.state;

import java.util.ArrayList;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.arrow.MapListRepresentation;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Owned rows with bounded count and payload, allowing one oversized row. */
final class StockRowBatch {
  @FunctionalInterface
  interface Receiver {
    void accept(long array, long schema);
  }

  private final RowType type;
  private final RowDataSerializer serializer;

  @FunctionalInterface
  interface RowsReceiver {
    void accept(java.util.List<RowData> rows, long array, long schema);
  }

  private final RowsReceiver receiver;
  private final boolean mapLists;
  private final ArrayList<RowData> rows = new ArrayList<>(1024);
  private long bytes;

  StockRowBatch(RowType type, Receiver receiver) {
    this(type, false, receiver);
  }

  StockRowBatch(RowType type, boolean mapLists, Receiver receiver) {
    this(type, mapLists, (rows, array, schema) -> receiver.accept(array, schema));
  }

  StockRowBatch(RowType type, RowsReceiver receiver) {
    this(type, false, receiver);
  }

  StockRowBatch(RowType type, boolean mapLists, RowsReceiver receiver) {
    this.mapLists = mapLists;
    this.type = type;
    this.serializer = new RowDataSerializer(type);
    this.receiver = receiver;
  }

  void append(RowData row) {
    long size = serializer.toBinaryRow(row).getSizeInBytes();
    if (!rows.isEmpty() && (rows.size() == 1024 || bytes + size > 4L * 1024 * 1024)) flush();
    rows.add(serializer.copy(row));
    bytes += size;
  }

  void flush() {
    if (rows.isEmpty()) return;
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED,
          mapLists ? MapListRepresentation.forExport(root) : root,
          NativeAllocator.DICTIONARIES,
          array,
          schema);
      receiver.accept(rows, array.memoryAddress(), schema.memoryAddress());
    }
    rows.clear();
    bytes = 0;
  }
}
