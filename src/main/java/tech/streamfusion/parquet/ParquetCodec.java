package tech.streamfusion.parquet;

import tech.streamfusion.format.ColumnarFileCodec;

/** Adapts the optional Parquet module to the shared columnar file lifecycle. */
public final class ParquetCodec implements ColumnarFileCodec {

  public Encoder createEncoder(
      org.apache.arrow.vector.types.pojo.Schema schema,
      int[] partitions,
      String[] keys,
      String[] values,
      boolean changelog,
      java.io.OutputStream output) {

    schema = tech.streamfusion.arrow.ArrowFileBatch.schema(schema);
    long handle;
    var allocator = tech.streamfusion.operator.NativeAllocator.SHARED;
    try (var exported = org.apache.arrow.c.ArrowSchema.allocateNew(allocator)) {
      org.apache.arrow.c.Data.exportSchema(
          allocator, schema, tech.streamfusion.operator.NativeAllocator.DICTIONARIES, exported);
      handle =
          NativeParquet.createParquetEncoder(
              exported.memoryAddress(),
              partitions,
              keys,
              values,
              changelog,
              output,
              new byte[1 << 20]);
    }
    return new Encoder() {
      private boolean closed;

      public void write(long array, int[] rows, int offset, int count) {
        if (closed) throw new IllegalStateException("Parquet writer is closed");
        NativeParquet.parquetEncoderWrite(handle, array, rows, offset, count);
      }

      public long estimatedBytes() {
        return NativeParquet.parquetEncoderEstimatedBytes(handle);
      }

      public long bufferedBytes() {
        return NativeParquet.parquetEncoderBufferedBytes(handle);
      }

      public void finish() {
        NativeParquet.parquetEncoderFinish(handle);
      }

      public void close() {
        if (!closed) {
          closed = true;
          NativeParquet.closeParquetEncoder(handle);
        }
      }
    };
  }

  public long createDecoder(Object in, long length, long schema, String[] names, int rows) {
    return NativeParquet.createParquetDecoder(in, length, schema, names, rows);
  }

  public boolean next(long h, long a, long s) {
    return NativeParquet.parquetDecoderNext(h, a, s);
  }

  public long readerMemory(long h) {
    return NativeParquet.parquetDecoderMaxRowGroupBytes(h);
  }

  public void closeDecoder(long h) {
    NativeParquet.closeParquetDecoder(h);
  }
}
