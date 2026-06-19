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

  private final int[] keyColumns;
  private final int[] keyTypes;
  private final int[] partialColumns;
  private final int sliceEndColumn;

  public NativeGlobalWindowAggregateOperator(
      long windowMillis,
      long slideMillis,
      int[] keyColumns,
      int[] keyTypes,
      int[] partialColumns,
      int sliceEndColumn,
      int[] aggregateKinds,
      String timeZoneId,
      int batchSize) {
    // Two-phase is restricted to bigint partials, so the merge accumulators are bigint.
    super(
        "streamfusion-global-window-state",
        windowMillis,
        slideMillis,
        TYPE_BIGINT,
        aggregateKinds,
        timeZoneId,
        batchSize);
    this.keyColumns = keyColumns;
    this.keyTypes = keyTypes;
    this.partialColumns = partialColumns;
    this.sliceEndColumn = sliceEndColumn;
  }

  @Override
  protected void pushBatch(List<RowData> rows) {
    int keyCount = keyColumns.length;
    int aggregates = partialColumns.length;
    BigIntVector[] keys = new BigIntVector[keyCount];
    for (int j = 0; j < keyCount; j++) {
      keys[j] = new BigIntVector("key" + j, allocator);
    }
    BigIntVector[] partials = new BigIntVector[aggregates];
    for (int a = 0; a < aggregates; a++) {
      partials[a] = new BigIntVector("partial" + a, allocator);
    }
    BigIntVector sliceEnd = new BigIntVector("slice_end", allocator);

    List<FieldVector> vectors = new ArrayList<>();
    for (BigIntVector key : keys) {
      vectors.add(key);
    }
    for (BigIntVector partial : partials) {
      vectors.add(partial);
    }
    vectors.add(sliceEnd);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      for (BigIntVector key : keys) {
        key.allocateNew(rows.size());
      }
      for (BigIntVector partial : partials) {
        partial.allocateNew(rows.size());
      }
      sliceEnd.allocateNew(rows.size());
      for (int i = 0; i < rows.size(); i++) {
        RowData row = rows.get(i);
        for (int j = 0; j < keyCount; j++) {
          keys[j].set(i, readKey(row, keyColumns[j], keyTypes[j]));
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
    emitFinal(watermark, keyTypes);
  }
}
