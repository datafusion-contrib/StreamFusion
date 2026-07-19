package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-kafka")
class KafkaProducerConfigTranslatorTest {

  @Test
  void pinsFlinkAndKafka42DefaultsThatDifferFromLibrdkafka() {
    KafkaProducerConfigTranslator.Result result = translated(props("bootstrap.servers", "b:9092"));
    Map<String, String> nativeConfig = result.nativeConfig();
    assertEquals("3600000", nativeConfig.get("transaction.timeout.ms"));
    assertEquals("16384", nativeConfig.get("batch.size"));
    assertEquals("5", nativeConfig.get("linger.ms"));
    assertEquals("120000", nativeConfig.get("delivery.timeout.ms"));
    assertEquals("32768", nativeConfig.get("queue.buffering.max.kbytes"));
    assertEquals("1048576", nativeConfig.get("message.max.bytes"));
    assertEquals("murmur2_random", nativeConfig.get("partitioner"));
    assertEquals("100", nativeConfig.get("statistics.interval.ms"));
    assertEquals("3600000", result.javaProperties().getProperty("transaction.timeout.ms"));
  }

  @Test
  void translatesLimitsAliasesAndCompressionLevel() {
    KafkaProducerConfigTranslator.Result result =
        translated(
            props(
                "bootstrap.servers", "b:9092",
                "send.buffer.bytes", "-1",
                "receive.buffer.bytes", "65536",
                "buffer.memory", "33554433",
                "max.block.ms", "1234",
                "max.request.size", "999999",
                "compression.type", "zstd",
                "compression.zstd.level", "7"));
    assertEquals("0", result.nativeConfig().get("socket.send.buffer.bytes"));
    assertEquals("65536", result.nativeConfig().get("socket.receive.buffer.bytes"));
    assertEquals("32769", result.nativeConfig().get("queue.buffering.max.kbytes"));
    assertEquals("999999", result.nativeConfig().get("message.max.bytes"));
    assertEquals("7", result.nativeConfig().get("compression.level"));
    assertEquals(1234, result.maxBlockMs());
  }

  @Test
  void parsesSharedSaslCredentials() {
    KafkaProducerConfigTranslator.Result result =
        translated(
            props(
                "bootstrap.servers", "b:9092",
                "security.protocol", "SASL_SSL",
                "sasl.mechanism", "SCRAM-SHA-512",
                "sasl.jaas.config",
                    "org.apache.kafka.common.security.scram.ScramLoginModule required"
                        + " username=\"alice\" password=\"secret\";"));
    assertEquals("SCRAM-SHA-512", result.nativeConfig().get("sasl.mechanisms"));
    assertEquals("alice", result.nativeConfig().get("sasl.username"));
    assertEquals("secret", result.nativeConfig().get("sasl.password"));
  }

  @Test
  void refusesPropertiesThatCannotBeHonored() {
    assertFallback("partitioner.class", "com.example.CustomPartitioner");
    assertFallback("interceptor.classes", "com.example.Interceptor");
    assertFallback("transactional.id", "user-owned");
    assertFallback("socket.connection.setup.timeout.max.ms", "30000");
    assertFallback("partitioner.ignore.keys", "true");
    assertFallback("unknown.property", "value");
  }

  @Test
  void refusesKafkaSemanticsLibrdkafkaCannotExpress() {
    assertFallback("batch.size", "0");
    assertFallback("enable.idempotence", "false");
    assertFallback("transaction.two.phase.commit.enable", "true");
  }

  @Test
  void everyKafka42ProducerPropertyIsEitherClassifiedOrExplicitlyFallsBack() {
    java.util.Set<String> nonKafkaKeys =
        new java.util.LinkedHashSet<>(KafkaProducerConfigTranslator.classifiedKeys());
    nonKafkaKeys.removeAll(ProducerConfig.configNames());
    assertTrue(
        nonKafkaKeys.isEmpty(), () -> "classified keys absent from Kafka 4.2: " + nonKafkaKeys);
    for (String key : ProducerConfig.configNames()) {
      if (KafkaProducerConfigTranslator.classifiedKeys().contains(key)) {
        continue;
      }
      Properties input = props("bootstrap.servers", "b:9092");
      input.setProperty(key, "__explicit__");
      KafkaProducerConfigTranslator.Result result = KafkaProducerConfigTranslator.translate(input);
      assertFalse(
          result.isTranslated(), () -> "new ProducerConfig key needs classification: " + key);
      assertTrue(result.fallbackReason().orElseThrow().contains(key));
    }
  }

  private static KafkaProducerConfigTranslator.Result translated(Properties properties) {
    KafkaProducerConfigTranslator.Result result =
        KafkaProducerConfigTranslator.translate(properties);
    assertTrue(result.isTranslated(), () -> result.fallbackReason().orElse("missing config"));
    return result;
  }

  private static void assertFallback(String key, String value) {
    KafkaProducerConfigTranslator.Result result =
        KafkaProducerConfigTranslator.translate(props("bootstrap.servers", "b:9092", key, value));
    assertFalse(result.isTranslated(), () -> "expected fallback for " + key);
    assertTrue(result.fallbackReason().orElseThrow().contains(key));
  }

  private static Properties props(String... values) {
    Properties properties = new Properties();
    for (int i = 0; i < values.length; i += 2) {
      properties.setProperty(values[i], values[i + 1]);
    }
    return properties;
  }
}
