package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-kafka")
class KafkaConfigTranslatorTest {

  private static Properties props(String... kv) {
    Properties p = new Properties();
    for (int i = 0; i < kv.length; i += 2) {
      p.setProperty(kv[i], kv[i + 1]);
    }
    return p;
  }

  private static Map<String, String> translated(Properties p) {
    KafkaConfigTranslator.Result r = KafkaConfigTranslator.translate(p);
    assertTrue(
        r.fallbackReason == null, () -> "expected translation, got fallback: " + r.fallbackReason);
    return r.config();
  }

  private static String fallback(Properties p) {
    KafkaConfigTranslator.Result r = KafkaConfigTranslator.translate(p);
    assertTrue(r.fallbackReason != null, "expected fallback, got translation");
    return r.fallbackReason;
  }

  @Test
  void passesThroughSameNameKeys() {
    Map<String, String> c =
        translated(props("bootstrap.servers", "b:9092", "group.id", "g", "fetch.min.bytes", "1024"));
    assertEquals("b:9092", c.get("bootstrap.servers"));
    assertEquals("g", c.get("group.id"));
    assertEquals("1024", c.get("fetch.min.bytes"));
  }

  @Test
  void pinsJavaDefaultsForDivergentKeysWhenUnset() {
    // librdkafka would otherwise default these the other way — the silent-divergence trap.
    Map<String, String> c = translated(props("group.id", "g"));
    assertEquals("read_uncommitted", c.get("isolation.level"));
    assertEquals("true", c.get("allow.auto.create.topics"));
    assertEquals("540000", c.get("connections.max.idle.ms"));
    assertEquals("300000", c.get("metadata.max.age.ms"));
    assertEquals("10000", c.get("socket.connection.setup.timeout.ms"));
    assertEquals("50", c.get("reconnect.backoff.ms"));
    assertEquals("1000", c.get("reconnect.backoff.max.ms"));
    // Socket buffer sizes are NOT pinned — they only affect throughput, and librdkafka's OS-tuned
    // default beats Java's small fixed default, so we leave librdkafka to choose.
    assertNull(c.get("socket.send.buffer.bytes"));
    assertNull(c.get("socket.receive.buffer.bytes"));
    // check.crcs is NOT pinned either (librdkafka default false, as Arroyo ships): CRC verification
    // is robustness, not a results-affecting semantic, and librdkafka's software CRC32C on ARM
    // measurably taxes delivery. An explicit user value still passes through (see below).
    assertNull(c.get("check.crcs"));
  }

  @Test
  void userCheckCrcsPassesThrough() {
    assertEquals("true", translated(props("check.crcs", "true")).get("check.crcs"));
  }

  @Test
  void userValueOverridesPinnedDefault() {
    assertEquals(
        "read_committed", translated(props("isolation.level", "read_committed")).get("isolation.level"));
  }

  @Test
  void renamesKeys() {
    Map<String, String> c =
        translated(
            props(
                "fetch.max.wait.ms", "250",
                "send.buffer.bytes", "262144",
                "receive.buffer.bytes", "131072"));
    assertEquals("250", c.get("fetch.wait.max.ms"));
    assertEquals("262144", c.get("socket.send.buffer.bytes")); // user value, not pinned default
    assertEquals("131072", c.get("socket.receive.buffer.bytes"));
    assertNull(c.get("fetch.max.wait.ms"));
    assertNull(c.get("send.buffer.bytes"));
  }

  @Test
  void mapsAutoOffsetResetValues() {
    assertEquals("smallest", translated(props("auto.offset.reset", "earliest")).get("auto.offset.reset"));
    assertEquals("largest", translated(props("auto.offset.reset", "latest")).get("auto.offset.reset"));
    assertEquals("error", translated(props("auto.offset.reset", "none")).get("auto.offset.reset"));
  }

  @Test
  void fallsBackOnUnmappableAutoOffsetReset() {
    assertTrue(fallback(props("auto.offset.reset", "by_duration:PT1H")).contains("auto.offset.reset"));
  }

  @Test
  void parsesPlainJaasIntoCredentials() {
    Map<String, String> c =
        translated(
            props(
                "security.protocol", "SASL_SSL",
                "sasl.mechanism", "PLAIN",
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required"
                    + " username=\"alice\" password=\"s3cret\";"));
    assertEquals("SASL_SSL", c.get("security.protocol"));
    assertEquals("PLAIN", c.get("sasl.mechanisms")); // renamed plural
    assertEquals("PLAIN", c.get("sasl.mechanism"));
    assertEquals("alice", c.get("sasl.username"));
    assertEquals("s3cret", c.get("sasl.password"));
  }

  @Test
  void parsesScramJaasIntoCredentials() {
    Map<String, String> c =
        translated(
            props(
                "security.protocol", "SASL_SSL",
                "sasl.mechanism", "SCRAM-SHA-512",
                "sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required"
                    + " username=\"alice\" password=\"s3cret\";"));
    assertEquals("SCRAM-SHA-512", c.get("sasl.mechanisms"));
    assertEquals("alice", c.get("sasl.username"));
    assertEquals("s3cret", c.get("sasl.password"));
  }

  /**
   * JAAS quoted values may hold spaces, semicolons, equals signs, and backslash-escaped quotes —
   * the grammar is kafka-clients' own parser, so any credential the Java client accepts must
   * translate byte for byte (a truncated password would fail SASL at runtime on a config stock
   * Flink accepts).
   */
  @Test
  void parsesQuotedJaasValuesWithSpacesSemicolonsAndEscapes() {
    Map<String, String> c =
        translated(
            props(
                "security.protocol", "SASL_PLAINTEXT",
                "sasl.mechanism", "PLAIN",
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required"
                    + " username=\"al ice\" password=\"p w;r=d \\\" quote\";"));
    assertEquals("al ice", c.get("sasl.username"));
    assertEquals("p w;r=d \" quote", c.get("sasl.password"));
  }

  /** A config the Java client itself cannot parse falls back — never a guessed credential. */
  @Test
  void fallsBackOnJaasTheJavaClientRejects() {
    // Missing the terminating semicolon.
    assertTrue(
        fallback(
                props(
                    "security.protocol", "SASL_PLAINTEXT",
                    "sasl.mechanism", "PLAIN",
                    "sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required"
                        + " username=\"alice\" password=\"s3cret\""))
            .contains("not parseable"));
    // Two login modules where the client demands exactly one.
    assertTrue(
        fallback(
                props(
                    "security.protocol", "SASL_PLAINTEXT",
                    "sasl.mechanism", "PLAIN",
                    "sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required"
                        + " username=\"a\" password=\"b\";"
                        + " org.apache.kafka.common.security.plain.PlainLoginModule required"
                        + " username=\"c\" password=\"d\";"))
            .contains("not parseable"));
  }

  @Test
  void fallsBackOnKerberos() {
    // Explicit GSSAPI, whatever the login module says.
    assertTrue(
        fallback(
                props(
                    "security.protocol", "SASL_PLAINTEXT",
                    "sasl.mechanism", "GSSAPI",
                    "sasl.jaas.config",
                    "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true"
                        + " keyTab=\"/etc/security/keytabs/svc.keytab\""
                        + " principal=\"svc@EXAMPLE.COM\";"))
            .contains("GSSAPI"));
    // The Java client's default mechanism is GSSAPI, so SASL without a mechanism is Kerberos too.
    assertTrue(fallback(props("security.protocol", "SASL_SSL")).contains("GSSAPI"));
    // A Kerberos login module with a non-GSSAPI mechanism is still not runnable natively.
    assertTrue(
        fallback(
                props(
                    "security.protocol", "SASL_PLAINTEXT",
                    "sasl.mechanism", "PLAIN",
                    "sasl.jaas.config",
                    "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true;"))
            .contains("Krb5LoginModule"));
    assertTrue(
        fallback(props("sasl.kerberos.service.name", "kafka"))
            .contains("sasl.kerberos.service.name"));
  }

  @Test
  void fallsBackOnSaslWithoutCredentials() {
    assertTrue(
        fallback(props("security.protocol", "SASL_SSL", "sasl.mechanism", "PLAIN"))
            .contains("sasl.jaas.config"));
  }

  @Test
  void fallsBackOnUnrecognizedLoginModule() {
    assertTrue(
        fallback(
                props(
                    "security.protocol", "SASL_PLAINTEXT",
                    "sasl.mechanism", "PLAIN",
                    "sasl.jaas.config", "com.example.CustomLoginModule required token=\"x\";"))
            .contains("CustomLoginModule"));
  }

  @Test
  void mapsPemTruststoreToCaLocation() {
    Map<String, String> c =
        translated(
            props(
                "security.protocol", "SSL",
                "ssl.truststore.type", "PEM",
                "ssl.truststore.location", "/certs/ca.pem"));
    assertEquals("/certs/ca.pem", c.get("ssl.ca.location"));
  }

  @Test
  void probesThePlatformCaBundleWhenSslHasNoTruststore() {
    // The statically-linked OpenSSL has no CA directory baked in; without explicit trust material
    // librdkafka must probe the platform bundle or Linux fails certificate verification.
    assertEquals("probe", translated(props("security.protocol", "SSL")).get("ssl.ca.location"));
    Map<String, String> withTrust =
        translated(
            props(
                "security.protocol", "SSL",
                "ssl.truststore.type", "PEM",
                "ssl.truststore.location", "/certs/ca.pem"));
    assertEquals("/certs/ca.pem", withTrust.get("ssl.ca.location"));
    assertFalse(translated(props("bootstrap.servers", "b:9092")).containsKey("ssl.ca.location"));
  }

  @Test
  void mapsPemKeystoreToCertificateAndKey() {
    Map<String, String> c =
        translated(
            props(
                "security.protocol", "SSL",
                "ssl.keystore.type", "PEM",
                "ssl.keystore.location", "/certs/client.pem",
                "ssl.keystore.password", "ignored-for-pem"));
    assertEquals("/certs/client.pem", c.get("ssl.certificate.location"));
    assertEquals("/certs/client.pem", c.get("ssl.key.location"));
    assertFalse(c.containsKey("ssl.keystore.password"));
  }

  @Test
  void fallsBackOnJksTruststore() {
    // default ssl.truststore.type is JKS
    assertTrue(fallback(props("ssl.truststore.location", "/certs/truststore.jks")).contains("JKS"));
  }

  @Test
  void fallsBackOnKeyWithNoLibrdkafkaAnalog() {
    assertTrue(
        fallback(props("ssl.trustmanager.algorithm", "PKIX")).contains("ssl.trustmanager.algorithm"));
  }

  @Test
  void refusesJavaClientPluginsAndUnknownProperties() {
    assertTrue(fallback(props("interceptor.classes", "com.example.I")).contains("interceptor"));
    assertTrue(
        fallback(props("sasl.login.callback.handler.class", "com.example.H"))
            .contains("sasl.login.callback.handler.class"));
    assertTrue(
        fallback(props("sasl.oauthbearer.token.endpoint.url", "https://idp/token"))
            .contains("sasl.oauthbearer.token.endpoint.url"));
    assertTrue(fallback(props("metrics.recording.level", "DEBUG")).contains("metrics."));
    assertTrue(
        fallback(props("unknown.property", "value")).contains("unknown.property"));
  }

  @Test
  void ignoresJavaOwnedCoordinationKeysWithoutForwardingThem() {
    Map<String, String> c =
        translated(
            props(
                "bootstrap.servers", "b:9092",
                "partition.discovery.interval.ms", "60000",
                "client.id.prefix", "my-source",
                "commit.offsets.on.checkpoint", "true",
                "max.poll.records", "100",
                "partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor"));
    assertEquals("b:9092", c.get("bootstrap.servers"));
    assertFalse(c.containsKey("partition.discovery.interval.ms"));
    assertFalse(c.containsKey("client.id.prefix"));
    assertFalse(c.containsKey("commit.offsets.on.checkpoint"));
    assertFalse(c.containsKey("max.poll.records"));
    assertFalse(c.containsKey("partition.assignment.strategy"));
  }

  @Test
  void everyKafka42ConsumerPropertyIsEitherClassifiedOrExplicitlyFallsBack() {
    java.util.Set<String> flinkSourceKeys =
        java.util.Set.of(
            "client.id.prefix",
            "partition.discovery.interval.ms",
            "register.consumer.metrics",
            "commit.offsets.on.checkpoint");
    java.util.Set<String> nonKafkaKeys =
        new java.util.LinkedHashSet<>(KafkaConfigTranslator.classifiedKeys());
    nonKafkaKeys.removeAll(org.apache.kafka.clients.consumer.ConsumerConfig.configNames());
    nonKafkaKeys.removeAll(flinkSourceKeys);
    assertTrue(
        nonKafkaKeys.isEmpty(),
        () -> "classified keys absent from Kafka 4.2 and Flink's source options: " + nonKafkaKeys);
    for (String key : org.apache.kafka.clients.consumer.ConsumerConfig.configNames()) {
      if (KafkaConfigTranslator.classifiedKeys().contains(key)) {
        continue;
      }
      java.util.Properties input = props("bootstrap.servers", "b:9092");
      input.setProperty(key, "__explicit__");
      KafkaConfigTranslator.Result result = KafkaConfigTranslator.translate(input);
      assertTrue(
          result.fallbackReason != null,
          () -> "new ConsumerConfig key needs classification: " + key);
      assertTrue(result.fallbackReason.contains(key));
    }
  }
}
