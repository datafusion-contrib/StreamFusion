package tech.streamfusion.fluss;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.shaded.netty4.io.netty.util.concurrent.Future;

/** Concurrent readers and writers share transport; the last owner closes it synchronously. */
final class FlussConnections {
  private static final Map<Map<String, String>, Entry> CONNECTIONS = new HashMap<>();

  private FlussConnections() {}

  static Lease acquire(Configuration config) throws IOException {
    Configuration transport = new Configuration(config);
    if (!transport.contains(ConfigOptions.NETTY_CLIENT_ALLOCATOR_HEAP_BUFFER_FIRST)
        && Boolean.parseBoolean(
            System.getProperty("streamfusion.fluss.direct-receive.enabled", "true"))) {
      transport.set(ConfigOptions.NETTY_CLIENT_ALLOCATOR_HEAP_BUFFER_FIRST, false);
    }
    Map<String, String> settings = new HashMap<>(transport.toMap());
    settings.put(
        "streamfusion.frame-aware-receive",
        System.getProperty("streamfusion.fluss.frame-aware-receive.enabled", "true"));
    Map<String, String> key = Map.copyOf(settings);
    if (!Boolean.parseBoolean(
        System.getProperty("streamfusion.fluss.connection-sharing.enabled", "true"))) {
      return new Lease(null, new Entry(transport));
    }
    synchronized (CONNECTIONS) {
      Entry entry = CONNECTIONS.get(key);
      if (entry == null) {
        entry = new Entry(transport);
        CONNECTIONS.put(key, entry);
      } else {
        entry.owners++;
      }
      return new Lease(key, entry);
    }
  }

  private static final class Entry {
    final FlussConnection connection;
    int owners = 1;

    Entry(Configuration config) throws IOException {
      connection = (FlussConnection) ConnectionFactory.createConnection(new Configuration(config));
      if (!config.getBoolean(ConfigOptions.NETTY_CLIENT_ALLOCATOR_HEAP_BUFFER_FIRST)
          && Boolean.parseBoolean(
              System.getProperty("streamfusion.fluss.frame-aware-receive.enabled", "true"))) {
        try {
          FlussDirectReceive.install(connection);
        } catch (ReflectiveOperationException | RuntimeException failure) {
          try {
            connection.close(Duration.ZERO);
          } catch (Exception close) {
            failure.addSuppressed(close);
          }
          throw new IOException("Released Fluss 1.0 direct receive hook is unavailable", failure);
        }
      }
    }
  }

  static final class Lease implements AutoCloseable {
    private final Map<String, String> key;
    private final Entry entry;
    private final AtomicBoolean released = new AtomicBoolean();

    private Lease(Map<String, String> key, Entry entry) {
      this.key = key;
      this.entry = entry;
    }

    FlussConnection connection() {
      return entry.connection;
    }

    @Override
    public void close() throws Exception {
      if (!released.compareAndSet(false, true)) return;
      if (key != null) {
        synchronized (CONNECTIONS) {
          if (--entry.owners != 0) return;
          CONNECTIONS.remove(key, entry);
        }
      }
      Future<?> termination = null;
      try {
        if (Boolean.parseBoolean(
            System.getProperty("streamfusion.fluss.fast-close.enabled", "true"))) {
          termination =
              FlussDirectReceive.bootstrap(entry.connection)
                  .config()
                  .group()
                  .shutdownGracefully(0, 10, TimeUnit.SECONDS);
        }
      } finally {
        try {
          entry.connection.close(Duration.ZERO);
        } finally {
          if (termination != null) termination.sync();
        }
      }
    }
  }
}
