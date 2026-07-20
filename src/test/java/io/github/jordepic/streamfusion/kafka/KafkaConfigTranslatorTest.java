package io.github.jordepic.streamfusion.kafka;

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
    assertTrue(r.isTranslated(), () -> "expected translation, got fallback: " + r.fallbackReason());
    return r.config();
  }

  private static String fallback(Properties p) {
    KafkaConfigTranslator.Result r = KafkaConfigTranslator.translate(p);
    assertFalse(r.isTranslated(), "expected fallback, got translation");
    return r.fallbackReason().orElseThrow();
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
  void parsesKerberosJaasIntoKeytabAndPrincipal() {
    Map<String, String> c =
        translated(
            props(
                "security.protocol", "SASL_PLAINTEXT",
                "sasl.mechanism", "GSSAPI",
                "sasl.kerberos.service.name", "kafka",
                "sasl.jaas.config",
                "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true"
                    + " keyTab=\"/etc/security/keytabs/svc.keytab\""
                    + " principal=\"svc@TRADING.IMC.INTRA\";"));
    assertEquals("kafka", c.get("sasl.kerberos.service.name"));
    assertEquals("/etc/security/keytabs/svc.keytab", c.get("sasl.kerberos.keytab"));
    assertEquals("svc@TRADING.IMC.INTRA", c.get("sasl.kerberos.principal"));
  }

  @Test
  void fallsBackOnUnrecognizedLoginModule() {
    assertTrue(
        fallback(props("sasl.jaas.config", "com.example.CustomLoginModule required token=\"x\";"))
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
      assertFalse(
          result.isTranslated(), () -> "new ConsumerConfig key needs classification: " + key);
      assertTrue(result.fallbackReason().orElseThrow().contains(key));
    }
  }
}
