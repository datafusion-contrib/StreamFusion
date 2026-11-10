package tech.streamfusion.hudi;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowFileBatch;
import tech.streamfusion.format.ColumnarFileCodec;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;
import tech.streamfusion.parquet.NativeParquet;

/** Parquet batches over Hudi's authenticated Java storage, without payload rows. */
final class HudiParquetReader implements AutoCloseable {
  private final SeekableDataInputStream input;
  private final byte[] chunk = new byte[64 * 1024];
  private long handle;
  private TaskOffHeapMemory.Reservation memory;

  HudiParquetReader(HoodieStorage storage, StoragePath path, RowType output, int rows)
      throws IOException {
    input = storage.openSeekable(path, true);
    try (var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED,
          ArrowFileBatch.schema(ArrowConversion.toArrowSchema(output)),
          NativeAllocator.DICTIONARIES,
          schema);
      handle =
          NativeParquet.createParquetDecoder(
              this,
              storage.getPathInfo(path).getLength(),
              schema.memoryAddress(),
              output.getFieldNames().toArray(new String[0]),
              rows);
      memory =
          TaskOffHeapMemory.reserve(
              "hudi-parquet",
              path.toString(),
              Math.addExact(chunk.length, NativeParquet.parquetDecoderMaxRowGroupBytes(handle)));
    } catch (Throwable failure) {
      try {
        close();
      } catch (Throwable closing) {
        failure.addSuppressed(closing);
      }
      throw failure;
    }
  }

  /** The native decoder borrows the destination only until this synchronous call returns. */
  public void readFully(long position, ByteBuffer target) throws IOException {
    input.seek(position);
    while (target.hasRemaining()) {
      int count = input.read(chunk, 0, Math.min(chunk.length, target.remaining()));
      if (count < 0) throw new EOFException("Truncated Hudi Parquet file");
      if (count == 0) throw new IOException("Hudi storage read made no progress");
      target.put(chunk, 0, count);
    }
  }

  VectorSchemaRoot next() {
    if (handle == 0) throw new IllegalStateException("Hudi Parquet reader is closed");
    return ColumnarFileCodec.importBatch(
        (array, schema) -> NativeParquet.parquetDecoderNext(handle, array, schema));
  }

  @Override
  public void close() throws IOException {
    try {
      if (handle != 0) {
        NativeParquet.closeParquetDecoder(handle);
        handle = 0;
      }
    } finally {
      try {
        input.close();
      } finally {
        if (memory != null) memory.close();
      }
    }
  }
}
