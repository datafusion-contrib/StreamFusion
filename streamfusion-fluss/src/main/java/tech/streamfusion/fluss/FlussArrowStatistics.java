package tech.streamfusion.fluss;

import java.io.IOException;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.record.LogRecordBatchStatisticsWriter;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;

/** Columnar bounds collection with the released SDK's statistics wire serialization. */
final class FlussArrowStatistics {
  private final RowType type;
  private final int[] columns;
  private final InternalRow.FieldGetter[] getters;
  private final LogRecordBatchStatisticsWriter writer;

  FlussArrowStatistics(RowType type, int[] columns) {
    this.type = type;
    this.columns = columns.clone();
    getters =
        java.util.Arrays.stream(columns)
            .mapToObj(i -> InternalRow.createFieldGetter(type.getTypeAt(i), i))
            .toArray(InternalRow.FieldGetter[]::new);
    writer = new LogRecordBatchStatisticsWriter(type, columns);
  }

  byte[] collect(VectorSchemaRoot root) throws IOException {
    var allocator = root.getFieldVectors().get(0).getAllocator();
    int[] indices;
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
      indices = NativeFluss.statistics(array.memoryAddress(), schema.memoryAddress(), columns);
    }
    var reader = ArrowConversion.createArrowReader(root, FlinkConversions.toFlinkRowType(type));
    Object[] min = new Object[columns.length];
    Object[] max = new Object[columns.length];
    int[] nullCounts = new int[columns.length];
    for (int i = 0; i < columns.length; i++) {
      if (indices[3 * i] >= 0)
        min[i] = getters[i].getFieldOrNull(new FlinkAsFlussRow(reader.read(indices[3 * i])));
      if (indices[3 * i + 1] >= 0)
        max[i] = getters[i].getFieldOrNull(new FlinkAsFlussRow(reader.read(indices[3 * i + 1])));
      nullCounts[i] = indices[3 * i + 2];
    }
    var output = new MemorySegmentOutputView(256);
    writer.writeStatistics(GenericRow.of(min), GenericRow.of(max), nullCounts, output);
    return output.getCopyOfBuffer();
  }
}
