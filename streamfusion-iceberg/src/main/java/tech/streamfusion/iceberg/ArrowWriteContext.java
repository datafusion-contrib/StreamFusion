package tech.streamfusion.iceberg;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;
import tech.streamfusion.operator.NativeAllocator;

/** Batch-scoped selections must drain before the task writer releases borrowed buffers. */
final class ArrowWriteContext {
  interface PendingWrite {
    void drain() throws IOException;

    default void abort() throws IOException {}
  }

  private final Set<PendingWrite> pending = new LinkedHashSet<>();
  private VectorSchemaRoot root;
  private RowData row;
  private int position;
  private long nativeBatch;

  void begin(VectorSchemaRoot root) {
    if (this.root != null) throw new IllegalStateException("Nested Iceberg Arrow write");
    this.root = root;
  }

  void position(int position, RowData row) {
    this.position = position;
    this.row = row;
  }

  VectorSchemaRoot root() {
    if (root == null) throw new IllegalStateException("Iceberg appender outside Arrow write");
    return root;
  }

  long nativeBatch(long writer) {
    if (nativeBatch == 0) {
      var root = root();
      var allocator = root.getVector(0).getAllocator();
      try (var array = ArrowArray.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array);
        try {
          nativeBatch = NativeIceberg.retainWriteBatch(writer, array.memoryAddress());
        } finally {
          if (array.snapshot().release != 0) array.release();
        }
      }
    }
    return nativeBatch;
  }

  int position(RowData accepted) {
    if (row != accepted || root == null) {
      throw new IllegalArgumentException("Iceberg data appender requires the current Arrow view");
    }
    return position;
  }

  void pending(PendingWrite write) {
    pending.add(write);
  }

  void drain() throws IOException {
    for (PendingWrite write : pending) write.drain();
    pending.clear();
  }

  void clear() {
    try {
      if (nativeBatch != 0) {
        long current = nativeBatch;
        nativeBatch = 0;
        NativeIceberg.releaseWriteBatch(current);
      }
    } finally {
      pending.clear();
      root = null;
      row = null;
    }
  }

  void fail(Throwable failure) {
    for (PendingWrite write : pending) {
      try {
        write.abort();
      } catch (Throwable cleanup) {
        failure.addSuppressed(cleanup);
      }
    }
  }
}
