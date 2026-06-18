package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;

/**
 * Global half of two-phase window aggregation: merges the partials emitted by the local half
 * (shuffled by key) into the window they belong to and, on a watermark, emits the final per-window
 * rows. Input rows are {@code [key?, partial, slice_end]}; output matches the host's global output.
 */
public class NativeGlobalWindowAggregateOperator extends NativeWindowOperatorBase {

  private final int keyColumn;
  private final int partialColumn;
  private final int sliceEndColumn;

  public NativeGlobalWindowAggregateOperator(
      long windowMillis,
      int keyColumn,
      int partialColumn,
      int sliceEndColumn,
      int aggregateKind,
      String timeZoneId,
      int batchSize) {
    super("streamfusion-global-window-state", windowMillis, aggregateKind, timeZoneId, batchSize);
    this.keyColumn = keyColumn;
    this.partialColumn = partialColumn;
    this.sliceEndColumn = sliceEndColumn;
  }

  @Override
  protected void pushBatch(List<RowData> rows) {
    boolean keyed = keyColumn >= 0;
    BigIntVector partial = new BigIntVector("partial", allocator);
    BigIntVector sliceEnd = new BigIntVector("slice_end", allocator);
    BigIntVector key = keyed ? new BigIntVector("key", allocator) : null;
    List<FieldVector> vectors = keyed ? List.of(key, partial, sliceEnd) : List.of(partial, sliceEnd);
    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      partial.allocateNew(rows.size());
      sliceEnd.allocateNew(rows.size());
      if (keyed) {
        key.allocateNew(rows.size());
      }
      for (int i = 0; i < rows.size(); i++) {
        RowData row = rows.get(i);
        partial.set(i, row.getLong(partialColumn));
        sliceEnd.set(i, row.getLong(sliceEndColumn));
        if (keyed) {
          key.set(i, row.getLong(keyColumn));
        }
      }
      root.setRowCount(rows.size());
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      Native.updatePartialTumblingAggregator(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }

  @Override
  protected void emitClosedWindows(long watermark) {
    emitFinal(watermark, keyColumn);
  }
}
