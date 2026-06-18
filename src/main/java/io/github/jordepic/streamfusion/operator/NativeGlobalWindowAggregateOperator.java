package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.util.ArrayList;
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
 * rows. Input rows are {@code [key?, partial0..partialN-1, slice_end]}; output matches the host.
 */
public class NativeGlobalWindowAggregateOperator extends NativeWindowOperatorBase {

  private final int keyColumn;
  private final int[] partialColumns;
  private final int sliceEndColumn;

  public NativeGlobalWindowAggregateOperator(
      long windowMillis,
      long slideMillis,
      int keyColumn,
      int[] partialColumns,
      int sliceEndColumn,
      int[] aggregateKinds,
      String timeZoneId,
      int batchSize) {
    super(
        "streamfusion-global-window-state",
        windowMillis,
        slideMillis,
        aggregateKinds,
        timeZoneId,
        batchSize);
    this.keyColumn = keyColumn;
    this.partialColumns = partialColumns;
    this.sliceEndColumn = sliceEndColumn;
  }

  @Override
  protected void pushBatch(List<RowData> rows) {
    boolean keyed = keyColumn >= 0;
    int aggregates = partialColumns.length;
    BigIntVector key = keyed ? new BigIntVector("key", allocator) : null;
    BigIntVector[] partials = new BigIntVector[aggregates];
    for (int a = 0; a < aggregates; a++) {
      partials[a] = new BigIntVector("partial" + a, allocator);
    }
    BigIntVector sliceEnd = new BigIntVector("slice_end", allocator);

    List<FieldVector> vectors = new ArrayList<>();
    if (keyed) {
      vectors.add(key);
    }
    for (BigIntVector partial : partials) {
      vectors.add(partial);
    }
    vectors.add(sliceEnd);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      if (keyed) {
        key.allocateNew(rows.size());
      }
      for (BigIntVector partial : partials) {
        partial.allocateNew(rows.size());
      }
      sliceEnd.allocateNew(rows.size());
      for (int i = 0; i < rows.size(); i++) {
        RowData row = rows.get(i);
        if (keyed) {
          key.set(i, row.getLong(keyColumn));
        }
        for (int a = 0; a < aggregates; a++) {
          partials[a].set(i, row.getLong(partialColumns[a]));
        }
        sliceEnd.set(i, row.getLong(sliceEndColumn));
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
