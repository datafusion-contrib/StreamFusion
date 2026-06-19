package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
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
      int valueType,
      int[] aggregateKinds,
      String timeZoneId,
      int batchSize) {
    super(
        "streamfusion-global-window-state",
        windowMillis,
        slideMillis,
        valueType,
        aggregateKinds,
        timeZoneId,
        batchSize);
    this.keyColumns = keyColumns;
    this.keyTypes = keyTypes;
    this.partialColumns = partialColumns;
    this.sliceEndColumn = sliceEndColumn;
  }

  /**
   * Whether aggregate {@code a}'s partial rides as a double. Sum/min/max over a double value carry a
   * double partial; count's partial is always a bigint, as are all partials over a bigint value.
   */
  private boolean partialIsDouble(int a) {
    return valueType == TYPE_DOUBLE && aggregateKinds[a] != KIND_COUNT;
  }

  @Override
  protected void pushBatch(List<RowData> rows) {
    int keyCount = keyColumns.length;
    int aggregates = partialColumns.length;
    FieldVector[] keys = new FieldVector[keyCount];
    for (int j = 0; j < keyCount; j++) {
      keys[j] = newKeyVector("key" + j, keyTypes[j]);
    }
    FieldVector[] partials = new FieldVector[aggregates];
    for (int a = 0; a < aggregates; a++) {
      partials[a] =
          partialIsDouble(a)
              ? new Float8Vector("partial" + a, allocator)
              : new BigIntVector("partial" + a, allocator);
    }
    BigIntVector sliceEnd = new BigIntVector("slice_end", allocator);

    List<FieldVector> vectors = new ArrayList<>();
    for (FieldVector key : keys) {
      vectors.add(key);
    }
    for (FieldVector partial : partials) {
      vectors.add(partial);
    }
    vectors.add(sliceEnd);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      sliceEnd.allocateNew(rows.size());
      for (int i = 0; i < rows.size(); i++) {
        RowData row = rows.get(i);
        for (int j = 0; j < keyCount; j++) {
          setKey(keys[j], i, row, keyColumns[j], keyTypes[j]);
        }
        for (int a = 0; a < aggregates; a++) {
          if (partialIsDouble(a)) {
            ((Float8Vector) partials[a]).setSafe(i, row.getDouble(partialColumns[a]));
          } else {
            ((BigIntVector) partials[a]).setSafe(i, row.getLong(partialColumns[a]));
          }
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
