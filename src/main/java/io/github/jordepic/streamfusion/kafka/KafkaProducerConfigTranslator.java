package io.github.jordepic.streamfusion.kafka;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Normalizes the properties used by Flink's Java commit producer and translates the data-plane
 * subset to librdkafka. Supplied properties are never silently dropped: each is translated,
 * explicitly handled by StreamFusion, or rejected so the planner can retain the stock Java sink.
 */
public final class KafkaProducerConfigTranslator {

  public static final int ABI_VERSION = 1;
  public static final int FLINK_TRANSACTION_TIMEOUT_MS = 3_600_000;

  private static final Set<String> PASSTHROUGH =
      Set.of(
          "bootstrap.servers",
          "client.id",
          "acks",
          "retries",
          "compression.type",
          "batch.size",
          "linger.ms",
          "delivery.timeout.ms",
          "request.timeout.ms",
          "retry.backoff.ms",
          "retry.backoff.max.ms",
          "max.in.flight.requests.per.connection",
          "enable.idempotence",
          "transaction.timeout.ms",
          "metadata.max.age.ms",
          "connections.max.idle.ms",
          "socket.connection.setup.timeout.ms",
          "reconnect.backoff.ms",
          "reconnect.backoff.max.ms",
          "client.dns.lookup",
          "security.protocol",
          "sasl.mechanism",
          "sasl.kerberos.service.name",
          "sasl.kerberos.kinit.cmd",
          "sasl.kerberos.min.time.before.relogin",
          "ssl.key.password",
          "ssl.cipher.suites",
          "ssl.endpoint.identification.algorithm",
          "enable.metrics.push");

  private static final Set<String> SECURITY_INPUTS =
      Set.of(
          "sasl.jaas.config",
          "ssl.truststore.location",
          "ssl.truststore.type",
          "ssl.keystore.location",
          "ssl.keystore.type",
          "ssl.key.password");

  private static final Set<String> STREAMFUSION_LIMITS =
      Set.of("max.block.ms", "max.request.size", "buffer.memory");

  private static final Map<String, String> DEFAULTS = new LinkedHashMap<>();

  static {
    DEFAULTS.put("acks", "all");
    DEFAULTS.put("retries", String.valueOf(Integer.MAX_VALUE));
    DEFAULTS.put("compression.type", "none");
    // batch.size is deliberately NOT defaulted across: kafka-clients' 16384 is a per-partition
    // accumulator target, while librdkafka's batch.size caps the whole MessageSet — pinning the
    // Java default measured ~4x slower produce throughput (2026-07-19 producer probe). librdkafka
    // keeps its own 1MB default; an explicit user value still passes through by name.
    DEFAULTS.put("linger.ms", "5");
    DEFAULTS.put("delivery.timeout.ms", "120000");
    DEFAULTS.put("request.timeout.ms", "30000");
    DEFAULTS.put("retry.backoff.ms", "100");
    DEFAULTS.put("retry.backoff.max.ms", "1000");
    DEFAULTS.put("max.in.flight.requests.per.connection", "5");
    DEFAULTS.put("enable.idempotence", "true");
    DEFAULTS.put("transaction.timeout.ms", String.valueOf(FLINK_TRANSACTION_TIMEOUT_MS));
    DEFAULTS.put("metadata.max.age.ms", "300000");
    DEFAULTS.put("connections.max.idle.ms", "540000");
    DEFAULTS.put("socket.connection.setup.timeout.ms", "10000");
    DEFAULTS.put("reconnect.backoff.ms", "50");
    DEFAULTS.put("reconnect.backoff.max.ms", "1000");
    DEFAULTS.put("client.dns.lookup", "use_all_dns_ips");
    DEFAULTS.put("security.protocol", "PLAINTEXT");
    DEFAULTS.put("enable.metrics.push", "true");
  }

  private KafkaProducerConfigTranslator() {}

  public static final class Result {
    private final Properties javaProperties;
    private final Map<String, String> nativeConfig;
    private final long maxBlockMs;
    private final int maxRequestSize;
    private final long bufferMemory;
    private final String fallbackReason;

    private Result(
        Properties javaProperties,
        Map<String, String> nativeConfig,
        long maxBlockMs,
        int maxRequestSize,
        long bufferMemory,
        String fallbackReason) {
      this.javaProperties = javaProperties;
      this.nativeConfig = nativeConfig;
      this.maxBlockMs = maxBlockMs;
      this.maxRequestSize = maxRequestSize;
      this.bufferMemory = bufferMemory;
      this.fallbackReason = fallbackReason;
    }

    static Result translated(
        Properties javaProperties,
        Map<String, String> nativeConfig,
        long maxBlockMs,
        int maxRequestSize,
        long bufferMemory) {
      return new Result(
          javaProperties, nativeConfig, maxBlockMs, maxRequestSize, bufferMemory, null);
    }

    static Result fallback(String reason) {
      return new Result(null, null, 0, 0, 0, reason);
    }

    public boolean isTranslated() {
      return nativeConfig != null;
    }

    public Properties javaProperties() {
      return javaProperties;
    }

    public Map<String, String> nativeConfig() {
      return nativeConfig;
    }

    public long maxBlockMs() {
      return maxBlockMs;
    }

    public int maxRequestSize() {
      return maxRequestSize;
    }

    public long bufferMemory() {
      return bufferMemory;
    }

    public Optional<String> fallbackReason() {
      return Optional.ofNullable(fallbackReason);
    }
  }

  public static Result translate(Properties supplied) {
    for (String key : supplied.stringPropertyNames()) {
      String reason = unsupportedReason(key, supplied.getProperty(key));
      if (reason != null) {
        return Result.fallback(reason);
      }
    }

    Properties javaProperties = new Properties();
    javaProperties.putAll(supplied);
    javaProperties.putIfAbsent(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    javaProperties.putIfAbsent(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    javaProperties.putIfAbsent(
        ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(FLINK_TRANSACTION_TIMEOUT_MS));

    if (!ByteArraySerializer.class
            .getName()
            .equals(javaProperties.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG))
        || !ByteArraySerializer.class
            .getName()
            .equals(javaProperties.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG))) {
      return Result.fallback("custom Kafka serializers require the Java producer");
    }

    try {
      // Applies kafka-clients' own type/range/idempotence validation before native planning.
      new ProducerConfig(javaProperties);
    } catch (RuntimeException invalid) {
      return Result.fallback("invalid Kafka producer configuration: " + invalid.getMessage());
    }

    Map<String, String> out = new LinkedHashMap<>();
    if (supplied.containsKey("bootstrap.servers")) {
      out.put("bootstrap.servers", supplied.getProperty("bootstrap.servers"));
    }
    DEFAULTS.forEach((key, value) -> out.put(key, javaProperties.getProperty(key, value)));
    for (String key : PASSTHROUGH) {
      if (javaProperties.containsKey(key)) {
        out.put(key, javaProperties.getProperty(key));
      }
    }

    translateSocketBuffer(
        javaProperties, out, "send.buffer.bytes", "socket.send.buffer.bytes", 131072);
    translateSocketBuffer(
        javaProperties, out, "receive.buffer.bytes", "socket.receive.buffer.bytes", 32768);

    int maxRequestSize = integer(javaProperties, "max.request.size", 1_048_576);
    long maxBlockMs = longValue(javaProperties, "max.block.ms", 60_000);
    long bufferMemory = longValue(javaProperties, "buffer.memory", 32L * 1024 * 1024);
    out.put("message.max.bytes", String.valueOf(maxRequestSize));
    // Java's accumulator is bounded by bytes alone (buffer.memory); librdkafka additionally caps
    // message count at 100K by default, which binds ~3x earlier for small records and turns into
    // producer backpressure Java would not apply. Disable the count cap so the byte budget is the
    // only bound, matching kafka-clients semantics.
    out.put("queue.buffering.max.messages", "0");
    out.put(
        "queue.buffering.max.kbytes", String.valueOf(Math.max(1, (bufferMemory + 1023) / 1024)));

    String codec = out.get("compression.type");
    String levelKey = "compression." + codec.toLowerCase(java.util.Locale.ROOT) + ".level";
    if (javaProperties.containsKey(levelKey)) {
      out.put("compression.level", javaProperties.getProperty(levelKey));
    }

    if ("0".equals(javaProperties.getProperty("batch.size"))) {
      return Result.fallback("batch.size=0 has no librdkafka equivalent");
    }

    String security = KafkaConfigTranslator.translateSecurity(javaProperties, out);
    if (security != null) {
      return Result.fallback(security);
    }
    if (out.containsKey("sasl.mechanism")) {
      out.put("sasl.mechanisms", out.remove("sasl.mechanism"));
    }

    // Runtime-owned settings are appended after user translation and cannot be overridden. The
    // statistics tick is the only channel librdkafka reveals the producer id/epoch through, and
    // producer warm-up blocks on the first tick, so the interval directly bounds warm-up latency.
    out.put("partitioner", "murmur2_random");
    out.put("statistics.interval.ms", "100");
    // kafka-clients hardcodes TCP_NODELAY on every connection; librdkafka leaves Nagle enabled by
    // default, which stalls the produce request/ack cycle behind delayed ACKs.
    out.put("socket.nagle.disable", "true");
    return Result.translated(javaProperties, out, maxBlockMs, maxRequestSize, bufferMemory);
  }

  private static String unsupportedReason(String key, String value) {
    if (ProducerConfig.TRANSACTIONAL_ID_CONFIG.equals(key)) {
      return "transactional.id is owned by Flink's transaction naming strategy";
    }
    if ("transaction.two.phase.commit.enable".equals(key) && Boolean.parseBoolean(value)) {
      return "transaction.two.phase.commit.enable is not supported by the native producer";
    }
    if ("enable.idempotence".equals(key) && !Boolean.parseBoolean(value)) {
      return "enable.idempotence=false is incompatible with exactly-once production";
    }
    if ("partitioner.class".equals(key) || "interceptor.classes".equals(key)) {
      return key + " requires the Java producer";
    }
    if ("socket.connection.setup.timeout.max.ms".equals(key)
        || "metadata.max.idle.ms".equals(key)
        || "security.providers".equals(key)
        || key.startsWith("metrics.")) {
      return "no librdkafka parity for " + key;
    }
    if ("partitioner.adaptive.partitioning.enable".equals(key)) {
      return Boolean.parseBoolean(value) ? null : "non-default adaptive partitioning requires Java";
    }
    if ("partitioner.availability.timeout.ms".equals(key)) {
      return "0".equals(value) ? null : "partition availability timeout requires Java";
    }
    if ("partitioner.ignore.keys".equals(key)) {
      return Boolean.parseBoolean(value) ? "partitioner.ignore.keys=true requires Java" : null;
    }
    if (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG.equals(key)
        || ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG.equals(key)
        || PASSTHROUGH.contains(key)
        || SECURITY_INPUTS.contains(key)
        || STREAMFUSION_LIMITS.contains(key)
        || "send.buffer.bytes".equals(key)
        || "receive.buffer.bytes".equals(key)
        || key.matches("compression\\.(gzip|lz4|zstd)\\.level")
        || "transaction.two.phase.commit.enable".equals(key)) {
      return null;
    }
    return "unsupported Kafka producer property " + key;
  }

  private static void translateSocketBuffer(
      Properties properties,
      Map<String, String> out,
      String javaKey,
      String nativeKey,
      int javaDefault) {
    int value = integer(properties, javaKey, javaDefault);
    out.put(nativeKey, String.valueOf(value < 0 ? 0 : value));
  }

  private static int integer(Properties properties, String key, int defaultValue) {
    return Integer.parseInt(properties.getProperty(key, String.valueOf(defaultValue)));
  }

  private static long longValue(Properties properties, String key, long defaultValue) {
    return Long.parseLong(properties.getProperty(key, String.valueOf(defaultValue)));
  }

  /**
   * The exact supplied keys covered by the initial native contract, used by upgrade guard tests.
   */
  static Set<String> classifiedKeys() {
    Set<String> keys = new LinkedHashSet<>(PASSTHROUGH);
    keys.addAll(SECURITY_INPUTS);
    keys.addAll(STREAMFUSION_LIMITS);
    keys.addAll(
        Set.of(
            "send.buffer.bytes",
            "receive.buffer.bytes",
            "key.serializer",
            "value.serializer",
            "transactional.id",
            "transaction.two.phase.commit.enable",
            "partitioner.class",
            "interceptor.classes",
            "partitioner.adaptive.partitioning.enable",
            "partitioner.availability.timeout.ms",
            "partitioner.ignore.keys",
            "socket.connection.setup.timeout.max.ms",
            "metadata.max.idle.ms",
            "security.providers",
            "compression.gzip.level",
            "compression.lz4.level",
            "compression.zstd.level"));
    return keys;
  }
}
