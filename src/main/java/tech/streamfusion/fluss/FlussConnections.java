package tech.streamfusion.fluss;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.config.Configuration;

/** Concurrent readers and writers share transport; the last owner closes it synchronously. */
final class FlussConnections {
  private static final Map<Map<String, String>, Entry> CONNECTIONS = new HashMap<>();

  private FlussConnections() {}

  static Lease acquire(Configuration config) throws IOException {
    Map<String, String> key = Map.copyOf(config.toMap());
    if (!Boolean.parseBoolean(
        System.getProperty("streamfusion.fluss.connection-sharing.enabled", "true"))) {
      return new Lease(null, new Entry(config));
    }
    synchronized (CONNECTIONS) {
      Entry entry = CONNECTIONS.get(key);
      if (entry == null) {
        entry = new Entry(config);
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
      entry.connection.close(Duration.ZERO);
    }
  }
}
