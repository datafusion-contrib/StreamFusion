package tech.streamfusion.parquet;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Host input fixture for measuring the production Rust reader's synchronous JNI callbacks. */
public final class ParquetBenchmarkInput implements AutoCloseable {
  private final byte[] bytes;
  private final Path path;
  private final FileChannel channel;
  private long reads;
  private long bytesRead;
  private boolean closed;

  public ParquetBenchmarkInput(byte[] encoded, boolean filesystem) throws IOException {
    bytes = encoded.clone();
    if (filesystem) {
      path = Files.createTempFile("streamfusion-parquet-criterion-", ".parquet");
      try {
        Files.write(path, bytes);
        channel = FileChannel.open(path, StandardOpenOption.READ);
      } catch (IOException failure) {
        try {
          Files.deleteIfExists(path);
        } catch (IOException cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
    } else {
      path = null;
      channel = null;
    }
  }

  public void readFully(long position, ByteBuffer target) throws IOException {
    if (closed) {
      throw new IOException("Benchmark input is closed");
    }
    int size = target.remaining();
    if (position < 0 || position > bytes.length || size > bytes.length - position) {
      throw new EOFException("Read outside benchmark input");
    }
    if (channel == null) {
      target.put(bytes, (int) position, size);
    } else {
      long offset = position;
      while (target.hasRemaining()) {
        int count = channel.read(target, offset);
        if (count <= 0) {
          throw new EOFException("Incomplete benchmark file read");
        }
        offset += count;
      }
    }
    reads++;
    bytesRead += size;
  }

  public long reads() {
    return reads;
  }

  public long bytesRead() {
    return bytesRead;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    if (channel != null) {
      try {
        channel.close();
      } finally {
        Files.deleteIfExists(path);
      }
    }
  }
}
