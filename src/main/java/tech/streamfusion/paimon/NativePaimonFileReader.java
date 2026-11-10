package tech.streamfusion.paimon;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import tech.streamfusion.format.ColumnarFileCodec;
import tech.streamfusion.operator.NativeAllocator;

/** Native columnar batches over Paimon's authenticated, seekable FileIO. */
public final class NativePaimonFileReader implements AutoCloseable {
  private final ColumnarFileCodec codec;
  private final SeekableInputStream input;
  private final byte[] chunk = new byte[64 * 1024];
  private long handle;

  public NativePaimonFileReader(
      ColumnarFileCodec codec,
      FileIO io,
      Path path,
      long length,
      Schema output,
      String[] names,
      int batchRows)
      throws IOException {
    this.codec = codec;
    input = io.newInputStream(path);
    try (ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED,
          tech.streamfusion.arrow.ArrowFileBatch.schema(output),
          NativeAllocator.DICTIONARIES,
          schema);
      handle = codec.createDecoder(this, length, schema.memoryAddress(), names, batchRows);
    } catch (Throwable failure) {
      input.close();
      throw failure;
    }
  }

  /**
   * Called synchronously by the native decoder; the direct buffer is borrowed only for this call.
   */
  public void readFully(long position, ByteBuffer target) throws IOException {
    input.seek(position);
    while (target.hasRemaining()) {
      int count = input.read(chunk, 0, Math.min(chunk.length, target.remaining()));
      if (count < 0) {
        throw new EOFException("Truncated Paimon columnar file");
      }
      if (count == 0) {
        throw new IOException("Paimon input made no progress");
      }
      target.put(chunk, 0, count);
    }
  }

  public VectorSchemaRoot next() {
    return importBatch(this::exportNext);
  }

  public boolean exportNext(long array, long schema) {
    if (handle == 0) {
      throw new IllegalStateException("Paimon columnar reader is closed");
    }
    return codec.next(handle, array, schema);
  }

  public long readerMemory() {
    if (handle == 0) throw new IllegalStateException("Paimon columnar reader is closed");
    return codec.readerMemory(handle);
  }

  @FunctionalInterface
  interface BatchExporter {
    boolean next(long array, long schema);
  }

  static VectorSchemaRoot importBatch(BatchExporter exporter) {
    return ColumnarFileCodec.importBatch(exporter::next);
  }

  @Override
  public void close() throws IOException {
    try {
      if (handle != 0) {
        codec.closeDecoder(handle);
        handle = 0;
      }
    } finally {
      input.close();
    }
  }
}
