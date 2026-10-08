package tech.streamfusion.fluss;

import java.util.Set;

/** Options whose observable behavior stays with the released Java connection. */
public final class FlussClientOptions {
  private FlussClientOptions() {}

  private static final Set<String> SUPPORTED =
      Set.of(
          "bootstrap.servers",
          "client.id",
          "client.connect-timeout",
          "client.request-timeout",
          "netty.client.num-network-threads",
          "netty.client.allocator.heap-buffer-first",
          "client.security.protocol",
          "client.security.enable-plugin-discovery",
          "client.security.sasl.mechanism",
          "client.security.sasl.jaas.config",
          "client.security.sasl.username",
          "client.security.sasl.password",
          "client.writer.dynamic-create-partition.enabled");

  public static boolean supported(String key) {
    return SUPPORTED.contains(key);
  }
}
