package io.github.jordepic.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.junit.jupiter.api.Test;

class KafkaSinkTranslatorTest {

  @Test
  void preservesTheStockExactlyOnceWriterContract() {
    KafkaSinkTranslator.Result result =
        KafkaSinkTranslator.translate(
            Map.of(
                "connector", "kafka",
                "topic", "output",
                "properties.bootstrap.servers", "broker:9092",
                "properties.compression.type", "lz4",
                "format", "json",
                "sink.delivery-guarantee", "exactly-once",
                "sink.transactional-id-prefix", "orders",
                "sink.parallelism", "3"));

    assertTrue(result.isTranslated(), () -> result.fallbackReason().orElse("unknown fallback"));
    assertEquals(DeliveryGuarantee.EXACTLY_ONCE, result.planned().deliveryGuarantee);
    assertEquals("orders", result.planned().transactionalIdPrefix);
    assertEquals("lz4", result.planned().producerProperties.getProperty("compression.type"));
    assertEquals(
        "lz4", result.planned().nativeProducerConfig.nativeConfig().get("compression.type"));
    assertEquals(3, result.planned().parallelism);
  }

  @Test
  void declinesShapesWhoseRowwiseSemanticsAreNotYetModeled() {
    Map<String, String> base =
        Map.of(
            "topic", "output",
            "properties.bootstrap.servers", "broker:9092",
            "format", "json");
    assertFallback(with(base, "key.format", "json"), "key format");
    assertFallback(with(base, "sink.partitioner", "fixed"), "partitioner");
    assertFallback(with(base, "sink.buffer-flush.max-rows", "10"), "buffer");
    assertFallback(with(base, "topic", "a;b"), "one fixed topic");
    assertFallback(with(base, "format", "avro"), "not yet natively encoded");
  }

  @Test
  void requiresAStableTransactionalPrefixForExactlyOnce() {
    KafkaSinkTranslator.Result result =
        KafkaSinkTranslator.translate(
            Map.of(
                "topic", "output",
                "properties.bootstrap.servers", "broker:9092",
                "format", "json",
                "sink.delivery-guarantee", "exactly-once"));
    assertFalse(result.isTranslated());
    assertTrue(result.fallbackReason().orElseThrow().contains("transactional-id-prefix"));
  }

  @Test
  void fallsBackWhenAProducerPropertyCannotRunNatively() {
    KafkaSinkTranslator.Result result =
        KafkaSinkTranslator.translate(
            Map.of(
                "topic", "output",
                "properties.bootstrap.servers", "broker:9092",
                "properties.interceptor.classes", "com.example.AuditInterceptor",
                "format", "json",
                "sink.delivery-guarantee", "exactly-once",
                "sink.transactional-id-prefix", "orders"));
    assertFalse(result.isTranslated());
    assertTrue(result.fallbackReason().orElseThrow().contains("interceptor.classes"));
  }

  private static void assertFallback(Map<String, String> options, String expected) {
    KafkaSinkTranslator.Result result = KafkaSinkTranslator.translate(options);
    assertFalse(result.isTranslated());
    assertTrue(result.fallbackReason().orElseThrow().contains(expected));
  }

  private static Map<String, String> with(Map<String, String> base, String key, String value) {
    java.util.HashMap<String, String> copy = new java.util.HashMap<>(base);
    copy.put(key, value);
    return copy;
  }
}
