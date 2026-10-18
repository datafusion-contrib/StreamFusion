package tech.streamfusion.iceberg;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Owns host IO and unclaimed fetched batches until reader cancellation. */
final class IcebergReadCancellation implements AutoCloseable {
  private final Set<Closeable> active = new HashSet<>();
  private boolean closed;

  void add(Closeable resource) throws IOException {
    synchronized (this) {
      if (!closed) {
        active.add(resource);
        return;
      }
    }
    resource.close();
    throw new IOException("Iceberg source reader is closed");
  }

  synchronized void remove(Closeable resource) {
    active.remove(resource);
  }

  @Override
  public void close() throws IOException {
    List<Closeable> resources;
    synchronized (this) {
      if (closed) return;
      closed = true;
      resources = List.copyOf(active);
      active.clear();
    }
    IOException failure = null;
    for (var resource : resources) {
      try {
        resource.close();
      } catch (Exception cause) {
        IOException e =
            cause instanceof IOException
                ? (IOException) cause
                : new IOException("Cannot close Iceberg source resource", cause);
        if (failure == null) failure = e;
        else failure.addSuppressed(e);
      }
    }
    if (failure != null) throw failure;
  }
}
