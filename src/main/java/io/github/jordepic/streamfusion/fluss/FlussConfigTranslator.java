package io.github.jordepic.streamfusion.fluss;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Translates the Fluss Java/Flink table options that belong to the Fluss client into the field names
 * used by {@code fluss-rs::Config}. Flink source-coordination options are deliberately kept out of
 * the native config: those decide startup offsets, partition discovery, split assignment, and
 * checkpoints on the JVM side.
 */
public final class FlussConfigTranslator {

  private FlussConfigTranslator() {}

  public static final class Result {
    private final Map<String, String> config;
    private final Map<String, String> coordinationOptions;
    private final String fallbackReason;

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

    public boolean isTranslated() {
      return config != null;
    }

    public Map<String, String> config() {
      return config;
    }

    public Map<String, String> coordinationOptions() {
      return coordinationOptions;
    }

    public Optional<String> fallbackReason() {
      return Optional.ofNullable(fallbackReason);
    }
  }

  private static final Map<String, String> DIRECT =
      Map.ofEntries(
          Map.entry("bootstrap.servers", "bootstrap_servers"),
          Map.entry("client.scanner.log.max-poll-records", "scanner_log_max_poll_records"),
          Map.entry("client.scanner.remote-log.prefetch-num", "scanner_remote_log_prefetch_num"),
          Map.entry("client.remote-file.download-thread-num", "remote_file_download_thread_num"),
          Map.entry("client.writer.dynamic-batch-size.enabled", "writer_dynamic_batch_size_enabled"),
          Map.entry("client.writer.bucket.no-key-assigner", "writer_bucket_no_key_assigner"),
          Map.entry("client.writer.acks", "writer_acks"),
          Map.entry("client.writer.retries", "writer_retries"),
          Map.entry("client.writer.enable-idempotence", "writer_enable_idempotence"),
          Map.entry(
              "client.writer.max-inflight-requests-per-bucket",
              "writer_max_inflight_requests_per_bucket"),
          Map.entry("client.lookup.queue-size", "lookup_queue_size"),
          Map.entry("client.lookup.max-batch-size", "lookup_max_batch_size"),
          Map.entry("client.lookup.max-inflight-requests", "lookup_max_inflight_requests"),
          Map.entry("client.lookup.max-retries", "lookup_max_retries"),
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
          Map.entry("client.scanner.log.fetch.min-bytes", "scanner_log_fetch_min_bytes"),
          Map.entry("client.writer.request-max-size", "writer_request_max_size"),
          Map.entry("client.writer.batch-size", "writer_batch_size"),
          Map.entry("client.writer.buffer.memory-size", "writer_buffer_memory_size"));

  private static final Map<String, String> DURATION =
      Map.ofEntries(
          Map.entry("client.connect-timeout", "connect_timeout_ms"),
          Map.entry("client.scanner.log.fetch.wait-max-time", "scanner_log_fetch_wait_max_time_ms"),
          Map.entry("client.writer.batch-timeout", "writer_batch_timeout_ms"),
          Map.entry("client.writer.buffer.wait-timeout", "writer_buffer_wait_timeout_ms"),
          Map.entry("client.lookup.batch-timeout", "lookup_batch_timeout_ms"));

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
      if (key.startsWith("client.") && !isKnownClientOption(key)) {
        return Result.fallback("unmapped Fluss client option " + key);
      }
    }

    for (Map.Entry<String, String> entry : DIRECT.entrySet()) {
      if (options.containsKey(entry.getKey())) {
        out.put(entry.getValue(), translateDirect(entry.getKey(), options.get(entry.getKey())));
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
    String mechanism = out.getOrDefault("security_sasl_mechanism", "PLAIN");
    if (protocol != null
        && protocol.equalsIgnoreCase("sasl")
        && !mechanism.equalsIgnoreCase("PLAIN")) {
      return Result.fallback("fluss-rs currently supports only PLAIN SASL");
    }

    return Result.translated(out, coordination);
  }

  private static boolean isKnownClientOption(String key) {
    return DIRECT.containsKey(key)
        || MEMORY.containsKey(key)
        || DURATION.containsKey(key)
        || contains(NO_RUST_CONFIG, key);
  }

  private static boolean contains(String[] values, String target) {
    for (String value : values) {
      if (value.equals(target)) {
        return true;
      }
    }
    return false;
  }

  private static String translateDirect(String javaKey, String value) {
    if ("client.writer.bucket.no-key-assigner".equals(javaKey)) {
      return value.trim().toLowerCase().replace('-', '_');
    }
    return value;
  }

  private static long parseMemoryBytes(String value) {
    String normalized = value.trim().toLowerCase().replaceAll("\\s+", "");
    long multiplier = 1;
    String number = normalized;
    if (normalized.endsWith("bytes")) {
      number = normalized.substring(0, normalized.length() - "bytes".length());
    } else if (normalized.endsWith("byte")) {
      number = normalized.substring(0, normalized.length() - "byte".length());
    } else if (normalized.endsWith("kib")) {
      number = normalized.substring(0, normalized.length() - 3);
      multiplier = 1024L;
    } else if (normalized.endsWith("kb") || normalized.endsWith("k")) {
      number = normalized.replaceAll("k[b]?$", "");
      multiplier = 1024L;
    } else if (normalized.endsWith("mib")) {
      number = normalized.substring(0, normalized.length() - 3);
      multiplier = 1024L * 1024L;
    } else if (normalized.endsWith("mb") || normalized.endsWith("m")) {
      number = normalized.replaceAll("m[b]?$", "");
      multiplier = 1024L * 1024L;
    } else if (normalized.endsWith("gib")) {
      number = normalized.substring(0, normalized.length() - 3);
      multiplier = 1024L * 1024L * 1024L;
    } else if (normalized.endsWith("gb") || normalized.endsWith("g")) {
      number = normalized.replaceAll("g[b]?$", "");
      multiplier = 1024L * 1024L * 1024L;
    } else if (normalized.endsWith("b")) {
      number = normalized.substring(0, normalized.length() - 1);
    }
    return Long.parseLong(number) * multiplier;
  }

  private static long parseDurationMillis(String value) {
    String trimmed = value.trim();
    if (trimmed.toUpperCase().startsWith("P")) {
      return Duration.parse(trimmed).toMillis();
    }
    String normalized = trimmed.toLowerCase().replaceAll("\\s+", "");
    if (normalized.endsWith("ms")) {
      return Long.parseLong(normalized.substring(0, normalized.length() - 2));
    }
    if (normalized.endsWith("s")) {
      return Long.parseLong(normalized.substring(0, normalized.length() - 1)) * 1000L;
    }
    if (normalized.endsWith("min")) {
      return Long.parseLong(normalized.substring(0, normalized.length() - 3)) * 60_000L;
    }
    if (normalized.endsWith("m")) {
      return Long.parseLong(normalized.substring(0, normalized.length() - 1)) * 60_000L;
    }
    if (normalized.endsWith("h")) {
      return Long.parseLong(normalized.substring(0, normalized.length() - 1)) * 3_600_000L;
    }
    return Long.parseLong(normalized);
  }
}
