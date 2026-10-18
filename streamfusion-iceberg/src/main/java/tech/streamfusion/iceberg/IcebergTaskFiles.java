package tech.streamfusion.iceberg;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SeekableInputStream;

/** Task-local positional reads retain the host's filesystem and credential configuration. */
final class IcebergTaskFiles implements java.io.Closeable {
  private final FileIO io;
  private final Map<String, Long> lengths;
  private final Map<String, Input> streams = new HashMap<>();
  private final Object lifecycle = new Object();
  private volatile boolean closed;

  IcebergTaskFiles(FileIO io, Map<String, Long> lengths) {
    this.io = io;
    this.lengths = Map.copyOf(lengths);
  }

  public long length(String path) throws IOException {
    if (closed) throw new IOException("Iceberg task input is closed");
    Long length = lengths.get(path);
    if (length == null) throw new IOException("File is outside the Java-planned Iceberg task");
    return length;
  }

  private Input input(String path, long length) throws IOException {
    synchronized (lifecycle) {
      if (closed) throw new IOException("Iceberg task input is closed");
      var existing = streams.get(path);
      if (existing != null) return existing;
    }
    var opened = new Input(io.newInputFile(path, length).newStream());
    Input selected;
    synchronized (lifecycle) {
      selected = closed ? null : streams.putIfAbsent(path, opened);
      if (!closed && selected == null) return opened;
    }
    opened.stream.close();
    if (selected == null) throw new IOException("Iceberg task input is closed");
    return selected;
  }

  /** The direct buffer is borrowed for this synchronous callback only. */
  public void readFully(String path, long position, ByteBuffer target) throws IOException {
    long length = length(path);
    if (position < 0 || position > length || target.remaining() > length - position) {
      throw new EOFException("Iceberg read exceeds the planned file size");
    }
    var input = input(path, length);
    synchronized (input) {
      if (closed) throw new IOException("Iceberg task input is closed");
      input.stream.seek(position);
      while (target.hasRemaining()) {
        int count =
            input.stream.read(
                input.transfer, 0, Math.min(input.transfer.length, target.remaining()));
        if (count < 0) throw new EOFException("Truncated Iceberg file");
        if (count == 0) throw new IOException("Iceberg input made no progress");
        target.put(input.transfer, 0, count);
      }
    }
  }

  @Override
  public void close() throws IOException {
    List<Input> inputs;
    synchronized (lifecycle) {
      if (closed) return;
      closed = true;
      inputs = List.copyOf(streams.values());
      streams.clear();
    }
    IOException failure = null;
    for (var input : inputs) {
      try {
        input.stream.close();
      } catch (IOException e) {
        if (failure == null) failure = e;
        else failure.addSuppressed(e);
      }
    }
    if (failure != null) throw failure;
  }

  private static final class Input {
    private final SeekableInputStream stream;
    private final byte[] transfer = new byte[64 * 1024];

    private Input(SeekableInputStream stream) {
      this.stream = stream;
    }
  }
}
