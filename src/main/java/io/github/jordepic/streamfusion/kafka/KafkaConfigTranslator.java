package io.github.jordepic.streamfusion.kafka;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Translates a Flink Kafka consumer's {@code Properties} into an equivalent librdkafka configuration
 * for a native consumer, or reports why it cannot — in which case the source falls back to the shallow
 * (Flink-client) path. The goal is behavioral parity, so beyond renaming keys it pins the Java client's
 * default for keys whose librdkafka default differs (the silent-divergence trap, e.g. {@code
 * isolation.level}), and it refuses (falls back) on anything it cannot map faithfully rather than
 * guessing — the same conservative stance as the expression layer's opt-in incompatibility.
 *
 * <p>Pure {@code Properties} → {@code Map} (no Kafka-client or connector classes), so it is unit-tested
 * without a broker. SASL/SSL material that needs real conversion (JKS→PEM) or an unrecognized JAAS
 * login module falls back; the recognized cases (PLAIN/SCRAM credentials, Kerberos keytab) are mapped.
 */
public final class KafkaConfigTranslator {

  private KafkaConfigTranslator() {}

  /** Either a librdkafka config (translation succeeded) or a reason the source must fall back. */
  public static final class Result {
    private final Map<String, String> config;
    private final String fallbackReason;

    private Result(Map<String, String> config, String fallbackReason) {
      this.config = config;
      this.fallbackReason = fallbackReason;
    }

    static Result translated(Map<String, String> config) {
      return new Result(config, null);
    }

    static Result fallback(String reason) {
      return new Result(null, reason);
    }

    public boolean isTranslated() {
      return config != null;
    }

    /** The librdkafka config; present only when {@link #isTranslated()}. */
    public Map<String, String> config() {
      return config;
    }

    /** Why the native consumer can't be used for these settings; present only on fallback. */
    public Optional<String> fallbackReason() {
      return Optional.ofNullable(fallbackReason);
    }
  }

  // Keys copied verbatim — same name and same meaning in both clients.
  private static final String[] PASSTHROUGH = {
    "bootstrap.servers",
    "group.id",
    "group.instance.id",
    "client.id",
    "client.rack",
    "enable.auto.commit",
    "fetch.min.bytes",
    "fetch.max.bytes",
    "max.partition.fetch.bytes",
    "max.poll.interval.ms",
    "session.timeout.ms",
    "heartbeat.interval.ms",
    "request.timeout.ms",
    "retry.backoff.ms",
    "retry.backoff.max.ms",
    "security.protocol",
    "sasl.mechanism", // also emitted under the librdkafka plural name below
    "sasl.kerberos.service.name",
    "sasl.kerberos.kinit.cmd",
    "sasl.kerberos.min.time.before.relogin",
    "ssl.key.password",
    "ssl.keystore.password",
    "ssl.cipher.suites",
    "ssl.endpoint.identification.algorithm",
  };

  // Java key -> librdkafka key, value copied unchanged.
  private static final Map<String, String> RENAMED =
      Map.of(
          "fetch.max.wait.ms", "fetch.wait.max.ms",
          "sasl.mechanism", "sasl.mechanisms",
          "send.buffer.bytes", "socket.send.buffer.bytes",
          "receive.buffer.bytes", "socket.receive.buffer.bytes");

  // Keys whose librdkafka default differs from the Java client's *in a way that affects what data you
  // get*: copy the user's value, or pin the Java default when unset, so behavior matches what a Flink
  // user expects. Keyed by the Java property name -> (librdkafka key, java default).
  //
  // Note: socket send/receive buffer sizes are deliberately NOT pinned here. They only affect
  // throughput, not correctness, and librdkafka's default (OS-auto-tuned) outperforms Java's small
  // fixed defaults — pinning Java's 64KB receive buffer measurably throttled the native consumer. The
  // user's explicit value is still honored via the rename above; we just don't force Java's default.
  private static final Map<String, String[]> DEFAULT_PINS =
      new LinkedHashMap<>(
          Map.of(
              "isolation.level", new String[] {"isolation.level", "read_uncommitted"},
              "check.crcs", new String[] {"check.crcs", "true"},
              "allow.auto.create.topics", new String[] {"allow.auto.create.topics", "true"},
              "connections.max.idle.ms", new String[] {"connections.max.idle.ms", "540000"},
              "metadata.max.age.ms", new String[] {"metadata.max.age.ms", "300000"},
              "socket.connection.setup.timeout.ms",
                  new String[] {"socket.connection.setup.timeout.ms", "10000"},
              "reconnect.backoff.ms", new String[] {"reconnect.backoff.ms", "50"},
              "reconnect.backoff.max.ms", new String[] {"reconnect.backoff.max.ms", "1000"}));

  // Java keys with no librdkafka analog: their presence (non-default) forces a fallback.
  private static final String[] NO_ANALOG = {
    "ssl.protocol",
    "ssl.enabled.protocols",
    "ssl.keymanager.algorithm",
    "ssl.trustmanager.algorithm",
  };

  public static Result translate(Properties props) {
    Map<String, String> out = new LinkedHashMap<>();

    for (String key : NO_ANALOG) {
      if (props.containsKey(key)) {
        return Result.fallback("no librdkafka equivalent for " + key);
      }
    }

    for (String key : PASSTHROUGH) {
      if (props.containsKey(key)) {
        out.put(key, props.getProperty(key));
      }
    }
    RENAMED.forEach(
        (javaKey, nativeKey) -> {
          if (props.containsKey(javaKey)) {
            out.put(nativeKey, props.getProperty(javaKey));
          }
        });
    DEFAULT_PINS.forEach(
        (javaKey, pin) -> {
          if (props.containsKey(javaKey)) {
            out.put(pin[0], props.getProperty(javaKey));
          } else {
            out.putIfAbsent(pin[0], pin[1]);
          }
        });

    if (props.containsKey("auto.offset.reset")) {
      String reset = mapAutoOffsetReset(props.getProperty("auto.offset.reset"));
      if (reset == null) {
        return Result.fallback("unmappable auto.offset.reset=" + props.getProperty("auto.offset.reset"));
      }
      out.put("auto.offset.reset", reset);
    }

    String sasl = sasl(props, out);
    if (sasl != null) {
      return Result.fallback(sasl);
    }
    String ssl = ssl(props, out);
    if (ssl != null) {
      return Result.fallback(ssl);
    }

    return Result.translated(out);
  }

  /** Java reset strategies → librdkafka names; null if there is no equivalent. */
  private static String mapAutoOffsetReset(String value) {
    switch (value.toLowerCase()) {
      case "earliest":
        return "smallest";
      case "latest":
        return "largest";
      case "none":
        return "error";
      default:
        return null; // e.g. by_duration:...
    }
  }

  private static final Pattern JAAS_OPTION =
      Pattern.compile("(\\w[\\w.]*)\\s*=\\s*\"?([^\"\\s;]+)\"?");

  /**
   * Parses {@code sasl.jaas.config} into librdkafka SASL keys. Recognizes PLAIN/SCRAM (username +
   * password) and Kerberos (keytab + principal); returns a fallback reason for an unrecognized login
   * module or a malformed config, and {@code null} on success (or when SASL isn't configured).
   */
  private static String sasl(Properties props, Map<String, String> out) {
    String jaas = props.getProperty("sasl.jaas.config");
    if (jaas == null) {
      return null;
    }
    String module = jaas.trim().split("\\s+", 2)[0];
    Map<String, String> options = new LinkedHashMap<>();
    Matcher matcher = JAAS_OPTION.matcher(jaas);
    while (matcher.find()) {
      options.put(matcher.group(1).toLowerCase(), matcher.group(2));
    }
    if (module.endsWith("PlainLoginModule") || module.endsWith("ScramLoginModule")) {
      if (!options.containsKey("username") || !options.containsKey("password")) {
        return "sasl.jaas.config missing username/password";
      }
      out.put("sasl.username", options.get("username"));
      out.put("sasl.password", options.get("password"));
      return null;
    }
    if (module.endsWith("Krb5LoginModule")) {
      if (options.containsKey("keytab")) {
        out.put("sasl.kerberos.keytab", options.get("keytab"));
      }
      if (options.containsKey("principal")) {
        out.put("sasl.kerberos.principal", options.get("principal"));
      }
      return null;
    }
    return "unrecognized SASL login module " + module;
  }

  /**
   * Maps SSL trust/key material. PEM stores pass through to librdkafka's PEM paths; JKS/PKCS12 stores
   * need conversion that is not done here, so they fall back. Returns a fallback reason or {@code null}.
   */
  private static String ssl(Properties props, Map<String, String> out) {
    String trustType = props.getProperty("ssl.truststore.type", "JKS");
    if (props.containsKey("ssl.truststore.location")) {
      if (!"PEM".equalsIgnoreCase(trustType)) {
        return "ssl.truststore.type=" + trustType + " needs JKS->PEM conversion (not yet supported)";
      }
      out.put("ssl.ca.location", props.getProperty("ssl.truststore.location"));
    }
    String keyType = props.getProperty("ssl.keystore.type", "JKS");
    if (props.containsKey("ssl.keystore.location")) {
      if (!"PEM".equalsIgnoreCase(keyType)) {
        return "ssl.keystore.type=" + keyType + " needs JKS->PEM conversion (not yet supported)";
      }
      out.put("ssl.certificate.location", props.getProperty("ssl.keystore.location"));
    }
    return null;
  }
}
