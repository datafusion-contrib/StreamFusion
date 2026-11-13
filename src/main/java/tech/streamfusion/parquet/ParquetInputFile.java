package tech.streamfusion.parquet;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;

/** Host-owned random access shared by footer discovery and the native batch decoder. */
public final class ParquetInputFile implements AutoCloseable {
  private final FSDataInputStream input;
  private final byte[] chunk = new byte[64 * 1024];

  public ParquetInputFile(Path path) throws IOException {
    input = path.getFileSystem().open(path);
  }

  public void readFully(long offset, ByteBuffer target) throws IOException {
    input.seek(offset);
    while (target.hasRemaining()) {
      int count = input.read(chunk, 0, Math.min(chunk.length, target.remaining()));
      if (count < 0) throw new EOFException("Truncated Parquet file");
      if (count == 0) throw new IOException("Parquet input made no progress");
      target.put(chunk, 0, count);
    }
  }

  @Override
  public void close() throws IOException {
    input.close();
  }
}
