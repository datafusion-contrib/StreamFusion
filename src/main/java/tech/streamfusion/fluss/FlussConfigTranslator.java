package tech.streamfusion.fluss;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.TimeUtils;

/**
 * Translates the Fluss Java/Flink table options that belong to the Fluss client into the field names
 * used by {@code fluss-rs::Config}. Flink source-coordination options are deliberately kept out of
 * the native config: those decide startup offsets, partition discovery, split assignment, and
 * checkpoints on the JVM side. Writer/lookup client options are likewise dropped without falling
 * back — they configure the write and lookup paths, which a read-only source never executes. Any
 * other {@code client.*} option without a known fluss-rs mapping produces a fallback, so a setting
 * the native path cannot honor declines instead of silently diverging.
 */
public final class FlussConfigTranslator {

  private FlussConfigTranslator() {}

  public static final class Result {
    private final Map<String, String> config;
    private final Map<String, String> coordinationOptions;
    /** Why the native reader can't be used for these settings; null when translated. */
    public final String fallbackReason;

    private Result(
        Map<String, String> config, Map<String, String> coordinationOptions, String fallbackReason) {
      this.config = config;
      this.coordinationOptions = coordinationOptions;
      this.fallbackReason = fallbackReason;
    }

    static Result translated(Map<String, String> config, Map<String, String> coordinationOptions) {
      return new Result(config, coordinationOptions, null);
    }

    static Result fallback(String reason) {
      return new Result(null, Map.of(), reason);
    }

    public Map<String, String> config() {
      return config;
    }

    public Map<String, String> coordinationOptions() {
      return coordinationOptions;
    }
  }

  private static final Map<String, String> DIRECT =
      Map.ofEntries(
          Map.entry("bootstrap.servers", "bootstrap_servers"),
          Map.entry("client.scanner.log.max-poll-records", "scanner_log_max_poll_records"),
          Map.entry("client.scanner.remote-log.prefetch-num", "scanner_remote_log_prefetch_num"),
          Map.entry("client.remote-file.download-thread-num", "remote_file_download_thread_num"),
          Map.entry("client.security.protocol", "security_protocol"),
          Map.entry("client.security.sasl.mechanism", "security_sasl_mechanism"),
          Map.entry("client.security.sasl.username", "security_sasl_username"),
          Map.entry("client.security.sasl.password", "security_sasl_password"));

  private static final Map<String, String> MEMORY =
      Map.ofEntries(
          Map.entry("client.scanner.log.fetch.max-bytes", "scanner_log_fetch_max_bytes"),
          Map.entry(
              "client.scanner.log.fetch.max-bytes-for-bucket",
              "scanner_log_fetch_max_bytes_for_bucket"),
          Map.entry("client.scanner.log.fetch.min-bytes", "scanner_log_fetch_min_bytes"));

  private static final Map<String, String> DURATION =
      Map.ofEntries(
          Map.entry("client.connect-timeout", "connect_timeout_ms"),
          Map.entry("client.scanner.log.fetch.wait-max-time", "scanner_log_fetch_wait_max_time_ms"));

  private static final String[] COORDINATION = {
    "scan.startup.mode",
    "scan.startup.timestamp",
    "scan.partition.discovery.interval",
    "scan.kv.snapshot.lease.id",
    "scan.kv.snapshot.lease.duration"
  };

  private static final String[] NO_RUST_CONFIG = {
    "client.id",
    "client.request-timeout",
    "client.scanner.log.check-crc",
    "client.scanner.io.tmpdir",
    "client.metrics.enabled",
    "client.security.sasl.jaas.config"
  };

  // Write/lookup-path client options: legitimate table/client properties, but they configure paths a
  // read-only source never executes, so they are ignored rather than translated or fallback-triggering.
  private static final String[] READ_IRRELEVANT_PREFIXES = {"client.writer.", "client.lookup."};

  /** Translates known client options, or returns a fallback reason for a setting not safely mirrored. */
  public static Result translate(Map<String, String> options) {
    Map<String, String> out = new LinkedHashMap<>();
    Map<String, String> coordination = new LinkedHashMap<>();

    for (String key : COORDINATION) {
      if (options.containsKey(key)) {
        coordination.put(key, options.get(key));
      }
    }

    for (String key : NO_RUST_CONFIG) {
      if (options.containsKey(key)) {
        return Result.fallback("no fluss-rs Config field for " + key);
      }
    }

    for (String key : options.keySet()) {
      if (key.startsWith("client.")
          && !DIRECT.containsKey(key)
          && !MEMORY.containsKey(key)
          && !DURATION.containsKey(key)
          && !readIrrelevant(key)) {
        return Result.fallback("unrecognized Fluss client option " + key);
      }
    }

    for (Map.Entry<String, String> entry : DIRECT.entrySet()) {
      if (options.containsKey(entry.getKey())) {
        out.put(entry.getValue(), options.get(entry.getKey()));
      }
    }
    for (Map.Entry<String, String> entry : MEMORY.entrySet()) {
      if (options.containsKey(entry.getKey())) {
        out.put(entry.getValue(), Long.toString(parseMemoryBytes(options.get(entry.getKey()))));
      }
    }
    for (Map.Entry<String, String> entry : DURATION.entrySet()) {
      if (options.containsKey(entry.getKey())) {
        out.put(entry.getValue(), Long.toString(parseDurationMillis(options.get(entry.getKey()))));
      }
    }

    String protocol = out.get("security_protocol");
    if (protocol != null
        && !protocol.equalsIgnoreCase("plaintext")
        && !protocol.equalsIgnoreCase("sasl")) {
      return Result.fallback(
          "fluss-rs supports only the PLAINTEXT and SASL security protocols, not " + protocol);
    }
    String mechanism = out.getOrDefault("security_sasl_mechanism", "PLAIN");
    if (protocol != null
        && protocol.equalsIgnoreCase("sasl")
        && !mechanism.equalsIgnoreCase("PLAIN")) {
      return Result.fallback("fluss-rs currently supports only PLAIN SASL");
    }

    return Result.translated(out, coordination);
  }

  private static boolean readIrrelevant(String key) {
    for (String prefix : READ_IRRELEVANT_PREFIXES) {
      if (key.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private static long parseMemoryBytes(String value) {
    return MemorySize.parseBytes(value);
  }

  private static long parseDurationMillis(String value) {
    return TimeUtils.parseDuration(value).toMillis();
  }
}
