package io.github.jordepic.streamfusion.kafka;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Translates a Flink Kafka consumer's {@code Properties} into an equivalent librdkafka
 * configuration for a native consumer, or reports why it cannot — in which case the source falls
 * back to the shallow (Flink-client) path. The goal is behavioral parity, so beyond renaming keys
 * it pins the Java client's default for keys whose librdkafka default differs (the
 * silent-divergence trap, e.g. {@code isolation.level}), and it refuses (falls back) on anything it
 * cannot map faithfully rather than guessing — the same conservative stance as the expression
 * layer's opt-in incompatibility.
 *
 * <p>Pure {@code Properties} → {@code Map} (no Kafka-client or connector classes), so it is
 * unit-tested without a broker. SASL/SSL material that needs real conversion (JKS→PEM) or an
 * unrecognized JAAS login module falls back; the recognized cases (PLAIN/SCRAM credentials,
 * Kerberos keytab) are mapped.
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
    "check.crcs",
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
    "client.dns.lookup",
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

  // Keys whose librdkafka default differs from the Java client's *in a way that affects what data
  // you
  // get*: copy the user's value, or pin the Java default when unset, so behavior matches what a
  // Flink
  // user expects. Keyed by the Java property name -> (librdkafka key, java default).
  //
  // Note: socket send/receive buffer sizes are deliberately NOT pinned here. They only affect
  // throughput, not correctness, and librdkafka's default (OS-auto-tuned) outperforms Java's small
  // fixed defaults — pinning Java's 64KB receive buffer measurably throttled the native consumer.
  // The
  // user's explicit value is still honored via the rename above; we just don't force Java's
  // default.
  //
  // check.crcs is likewise not pinned (librdkafka default: false, the posture Arroyo ships with).
  // CRC verification is corruption-detection robustness, not a results-affecting semantic, and
  // librdkafka has no hardware CRC32C on ARM (x86 SSE4.2 only) — the software fallback measurably
  // taxes the delivery thread while the JVM's intrinsic CRC32C is nearly free. An explicit user
  // value is still copied verbatim. Recorded in divergences/.
  private static final Map<String, String[]> DEFAULT_PINS =
      new LinkedHashMap<>(
          Map.of(
              "isolation.level", new String[] {"isolation.level", "read_uncommitted"},
              "allow.auto.create.topics", new String[] {"allow.auto.create.topics", "true"},
              "connections.max.idle.ms", new String[] {"connections.max.idle.ms", "540000"},
              "metadata.max.age.ms", new String[] {"metadata.max.age.ms", "300000"},
              "socket.connection.setup.timeout.ms",
                  new String[] {"socket.connection.setup.timeout.ms", "10000"},
              "reconnect.backoff.ms", new String[] {"reconnect.backoff.ms", "50"},
              "reconnect.backoff.max.ms", new String[] {"reconnect.backoff.max.ms", "1000"}));

  // Security material consumed (converted, not copied) by translateSecurity.
  private static final Set<String> SECURITY_INPUTS =
      Set.of(
          "sasl.jaas.config",
          "ssl.truststore.location",
          "ssl.truststore.type",
          "ssl.keystore.location",
          "ssl.keystore.type");

  // Keys owned by the Java side of the native source — the shared Flink enumerator (discovery,
  // startup-offset resolution) and the reader wrapper — or forced by Flink's own builder. They are
  // deliberately not forwarded to librdkafka: the machinery they configure either runs on the JVM
  // for the native path too, or never engages because both clients use manual assignment.
  private static final Set<String> JAVA_OWNED =
      Set.of(
          // Flink KafkaSource keys injected by the connector/builder, not kafka-clients keys.
          "client.id.prefix",
          "partition.discovery.interval.ms",
          "register.consumer.metrics",
          "commit.offsets.on.checkpoint",
          // Flink always forces ByteArray deserializers; user values are overridden on the Java
          // path as well.
          "key.deserializer",
          "value.deserializer",
          // Subscribe/group-membership machinery: both the Java KafkaSource and the native reader
          // use manual assignment, and topic discovery runs in the shared Java enumerator.
          "exclude.internal.topics",
          "group.protocol",
          "group.remote.assignor",
          "partition.assignment.strategy",
          "share.acknowledgement.mode",
          "share.acquire.mode",
          // Java-consumer call scheduling with no data-affecting semantics on the native reader:
          // it batches by its own poll cap, and enumerator-side API calls keep Java behavior.
          "max.poll.records",
          "default.api.timeout.ms",
          // enable.auto.commit is forced false on both paths, so the interval never engages.
          "auto.commit.interval.ms");

  // Java keys with no faithful librdkafka analog: their presence forces a fallback.
  private static final Set<String> NO_ANALOG =
      Set.of(
          "ssl.protocol",
          "ssl.enabled.protocols",
          "ssl.keymanager.algorithm",
          "ssl.trustmanager.algorithm",
          "ssl.engine.factory.class",
          "ssl.provider",
          "ssl.secure.random.implementation",
          "ssl.keystore.key",
          "ssl.keystore.certificate.chain",
          "ssl.truststore.certificates",
          "ssl.truststore.password",
          "sasl.client.callback.handler.class",
          "sasl.kerberos.ticket.renew.window.factor",
          "sasl.kerberos.ticket.renew.jitter",
          "interceptor.classes",
          "metric.reporters",
          "config.providers",
          "security.providers",
          "metadata.recovery.strategy",
          "metadata.recovery.rebootstrap.trigger.ms",
          "socket.connection.setup.timeout.max.ms");

  /**
   * Why {@code key} cannot run natively, or null when it is classified (translated, converted
   * security material, or deliberately Java-owned). Vanilla Flink forwards arbitrary
   * {@code properties.*} keys and kafka-clients merely warns on unknown ones; the native path is
   * fail-closed instead — an unclassified key keeps the whole table on Flink with this reason,
   * because a key librdkafka interprets differently is worse than one it does not know.
   */
  private static String unsupportedReason(String key) {
    if (NO_ANALOG.contains(key)) {
      return "no librdkafka equivalent for " + key;
    }
    if (key.startsWith("sasl.login.") || key.startsWith("sasl.oauthbearer.")) {
      return "OAUTHBEARER/login-callback SASL requires the Java consumer (" + key + ")";
    }
    if (key.startsWith("metrics.") || key.startsWith("internal.")) {
      return "no librdkafka equivalent for " + key;
    }
    for (String passthrough : PASSTHROUGH) {
      if (passthrough.equals(key)) {
        return null;
      }
    }
    if (RENAMED.containsKey(key)
        || DEFAULT_PINS.containsKey(key)
        || SECURITY_INPUTS.contains(key)
        || JAVA_OWNED.contains(key)
        || "auto.offset.reset".equals(key)) {
      return null;
    }
    return "unsupported Kafka consumer property " + key;
  }

  /** The supplied keys the native contract covers, used by the upgrade guard test. */
  static Set<String> classifiedKeys() {
    Set<String> keys = new java.util.LinkedHashSet<>(java.util.List.of(PASSTHROUGH));
    keys.addAll(RENAMED.keySet());
    keys.addAll(DEFAULT_PINS.keySet());
    keys.addAll(SECURITY_INPUTS);
    keys.addAll(JAVA_OWNED);
    keys.add("auto.offset.reset");
    return keys;
  }

  public static Result translate(Properties props) {
    Map<String, String> out = new LinkedHashMap<>();

    for (String key : props.stringPropertyNames()) {
      String reason = unsupportedReason(key);
      if (reason != null) {
        return Result.fallback(reason);
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
        return Result.fallback(
            "unmappable auto.offset.reset=" + props.getProperty("auto.offset.reset"));
      }
      out.put("auto.offset.reset", reset);
    }

    String security = translateSecurity(props, out);
    if (security != null) {
      return Result.fallback(security);
    }

    return Result.translated(out);
  }

  /**
   * Adds the shared Java-to-librdkafka SASL/SSL settings. Returns a fallback reason, or {@code
   * null} when the security configuration is representable. Producer and consumer translation
   * deliberately share this narrow conversion while retaining separate property allowlists and
   * defaults.
   */
  static String translateSecurity(Properties props, Map<String, String> out) {
    String sasl = sasl(props, out);
    return sasl != null ? sasl : ssl(props, out);
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
   * password) and Kerberos (keytab + principal); returns a fallback reason for an unrecognized
   * login module or a malformed config, and {@code null} on success (or when SASL isn't
   * configured).
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
   * Maps SSL trust/key material. PEM stores pass through to librdkafka's PEM paths; JKS/PKCS12
   * stores need conversion that is not done here, so they fall back. Returns a fallback reason or
   * {@code null}.
   */
  private static String ssl(Properties props, Map<String, String> out) {
    String trustType = props.getProperty("ssl.truststore.type", "JKS");
    if (props.containsKey("ssl.truststore.location")) {
      if (!"PEM".equalsIgnoreCase(trustType)) {
        return "ssl.truststore.type="
            + trustType
            + " needs JKS->PEM conversion (not yet supported)";
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
