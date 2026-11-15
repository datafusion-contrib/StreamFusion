package tech.streamfusion.paimon;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.types.RowType;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowWriter;
import tech.streamfusion.operator.NativeAllocator;

/** Copies reused Paimon rows at the format boundary, after Java has finalized their file order. */
final class PaimonRowBatch implements AutoCloseable {
  private final org.apache.arrow.memory.BufferAllocator allocator;
  private final VectorSchemaRoot root;
  private final ArrowWriter<RowData> writer;
  private final FlinkRowData view = new FlinkRowData(null);
  private InternalRow currentRow;
  private int count;

  PaimonRowBatch(RowType type) {
    var logical = LogicalTypeConversion.toLogicalType(type);
    allocator = NativeAllocator.SHARED.newChildAllocator("paimon-file-rows", 0, Long.MAX_VALUE);
    try {
      root = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(logical), allocator);
    } catch (Throwable failure) {
      try {
        allocator.close();
      } catch (Throwable cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
    try {
      writer = ArrowConversion.createRowDataArrowWriter(root, logical);
      for (int column = 0; column < type.getFieldCount(); column++) {
        switch (type.getTypeAt(column).getTypeRoot()) {
          case CHAR:
          case VARCHAR:
            writer.getFieldWriters()[column] =
                stringWriter((org.apache.arrow.vector.VarCharVector) root.getVector(column));
            break;
          default:
            break;
        }
      }
    } catch (Throwable failure) {
      try {
        close();
      } catch (Throwable cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  void add(InternalRow row) {
    currentRow = row;
    try {
      writer.write(view.replace(row));
      count++;
    } finally {
      currentRow = null;
      view.replace(null);
    }
  }

  private tech.streamfusion.arrow.writers.ArrowFieldWriter<RowData> stringWriter(
      org.apache.arrow.vector.VarCharVector vector) {
    return new tech.streamfusion.arrow.writers.ArrowFieldWriter<RowData>(vector) {
      @Override
      public void doWrite(RowData ignored, int ordinal) {
        if (currentRow.isNullAt(ordinal)) {
          vector.setNull(getCount());
          return;
        }
        var value = currentRow.getString(ordinal);
        var segments = value.getSegments();
        if (segments.length == 1 && !segments[0].isOffHeap())
          vector.setSafe(
              getCount(), segments[0].getArray(), value.getOffset(), value.getSizeInBytes());
        else vector.setSafe(getCount(), value.toBytes());
      }
    };
  }

  int size() {
    return count;
  }

  boolean isEmpty() {
    return count == 0;
  }

  long allocatedBytes() {
    return allocator.getAllocatedMemory();
  }

  boolean isFull() {
    return count >= 4096 || allocatedBytes() >= (8L << 20);
  }

  VectorSchemaRoot finish() {
    writer.finish();
    return root;
  }

  void reset() {
    writer.reset();
    count = 0;
    if (allocator.getAllocatedMemory() >= (8L << 20)) root.clear();
  }

  public void close() {
    try (var ownedAllocator = allocator;
        var ownedRoot = root) {}
  }
}
