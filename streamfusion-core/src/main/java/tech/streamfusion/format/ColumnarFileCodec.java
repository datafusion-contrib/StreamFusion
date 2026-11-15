package tech.streamfusion.format;

import java.io.IOException;
import java.io.Serializable;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import tech.streamfusion.operator.NativeAllocator;

/**
 * Optional file codecs consume Arrow columns and produce Arrow C Data; the host retains stream
 * ownership.
 */
public interface ColumnarFileCodec extends Serializable {
  Encoder createEncoder(
      org.apache.arrow.vector.types.pojo.Schema schema,
      int[] partitions,
      String[] keys,
      String[] values,
      boolean changelog,
      java.io.OutputStream output)
      throws IOException;

  interface Encoder extends AutoCloseable {
    void write(long array, int[] selected, int offset, int count) throws IOException;

    default void write(VectorSchemaRoot batch, int[] selected, int offset, int count)
        throws IOException {
      var allocator =
          batch.getFieldVectors().isEmpty()
              ? NativeAllocator.SHARED
              : batch.getVector(0).getAllocator();
      try (var logical = tech.streamfusion.arrow.ArrowFileBatch.of(batch);
          var array = ArrowArray.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, logical.root(), NativeAllocator.DICTIONARIES, array);
        try {
          write(array.memoryAddress(), selected, offset, count);
        } finally {
          if (array.snapshot().release != 0) array.release();
        }
      }
    }

    long estimatedBytes();

    /** Current encoder buffers, excluding bytes already written to the host output. */
    default long bufferedBytes() {
      throw new UnsupportedOperationException("Encoder memory accounting is unavailable");
    }

    void finish() throws IOException;

    @Override
    void close();
  }

  long createDecoder(Object input, long length, long schema, String[] names, int batchRows);

  boolean next(long handle, long array, long schema);

  long readerMemory(long handle);

  void closeDecoder(long handle);

  @FunctionalInterface
  interface BatchExporter {
    boolean next(long array, long schema);
  }

  /** Transfers a native file batch to Java, releasing partial exports on every failure path. */
  static VectorSchemaRoot importBatch(BatchExporter exporter) {
    try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = org.apache.arrow.c.ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      try {
        if (!exporter.next(array.memoryAddress(), schema.memoryAddress())) return null;
        var root =
            VectorSchemaRoot.create(
                Data.importSchema(
                    NativeAllocator.SHARED,
                    org.apache.arrow.c.ArrowSchema.wrap(schema.memoryAddress()),
                    NativeAllocator.DICTIONARIES),
                NativeAllocator.SHARED);
        try {
          Data.importIntoVectorSchemaRoot(
              NativeAllocator.SHARED,
              ArrowArray.wrap(array.memoryAddress()),
              root,
              NativeAllocator.DICTIONARIES);
          return root;
        } catch (Throwable failure) {
          root.close();
          throw failure;
        }
      } finally {
        if (array.snapshot().release != 0) array.release();
        if (schema.snapshot().release != 0) schema.release();
      }
    }
  }
}
