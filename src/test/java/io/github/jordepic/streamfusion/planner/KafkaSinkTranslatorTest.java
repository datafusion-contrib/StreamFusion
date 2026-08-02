package io.github.jordepic.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.format.EncodeFormat;
import java.util.Map;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
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

    assertTrue(result.fallbackReason == null, () -> result.fallbackReason);
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
    assertFallback(with(base, "key.format", "json"), "keyed ordinary");
    assertFallback(with(base, "sink.partitioner", "fixed"), "partitioner");
    assertFallback(with(base, "sink.buffer-flush.max-rows", "10"), "buffer");
    assertFallback(with(base, "topic", "a;b"), "one fixed topic");
  }

  /** The encode-format seam: only resolvable (format, options, row) triples produce an instance. */
  @Test
  void resolvesOnlyNativelyEncodedFormatInstances() {
    RowType rowType = RowType.of(false, new BigIntType());
    assertNotNull(EncodeFormat.of("json", Map.of(), rowType));
    // A format with no installed provider (or no native serializer) declines, never errors.
    assertNull(EncodeFormat.of("sequence-file", Map.of(), rowType));
    assertNull(EncodeFormat.of(null, Map.of(), rowType));
    assertNull(EncodeFormat.of("json", Map.of("timestamp-format.standard", "RFC-1123"), rowType));
    assertNull(EncodeFormat.of("json", Map.of("encode.ignore-null-fields", "yes"), rowType));
    EncodeFormat iso =
        EncodeFormat.of(
            "json",
            Map.of(
                "timestamp-format.standard", "ISO-8601",
                "encode.ignore-null-fields", "TRUE",
                "encode.decimal-as-plain-number", "false"),
            rowType);
    assertNotNull(iso);
    assertTrue(iso.options.contains("timestamp-format=ISO-8601"));
    assertTrue(iso.options.contains("encode.ignore-null-fields=true"));
    assertFalse(iso.options.contains("decimal-as-plain-number"));
  }

  /** The provider-backed avro instances: derivation-based gates plus the confluent subject. */
  @Test
  void resolvesAvroFormatInstancesThroughTheirProviders() {
    RowType rowType = RowType.of(false, new BigIntType());
    EncodeFormat avro = EncodeFormat.of("avro", Map.of(), rowType);
    assertNotNull(avro);
    assertTrue(avro.options.startsWith("avro-schema={\"type\":\"record\""));
    assertNull(EncodeFormat.of("avro", Map.of("encoding", "json"), rowType));
    // Legacy mapping (the default) cannot derive TIMESTAMP_LTZ; the corrected mapping can.
    RowType ltz = RowType.of(false, new LocalZonedTimestampType(3));
    assertNull(EncodeFormat.of("avro", Map.of(), ltz));
    assertNotNull(EncodeFormat.of("avro", Map.of("timestamp_mapping.legacy", "false"), ltz));

    Map<String, String> confluent =
        Map.of("url", "http://registry:8081", "schema-registry.subject", "t-value");
    assertNotNull(EncodeFormat.of("avro-confluent", confluent, rowType));
    // avro-confluent hard-wires the legacy mapping, so TIMESTAMP_LTZ never resolves.
    assertNull(EncodeFormat.of("avro-confluent", confluent, ltz));
    assertNull(EncodeFormat.of("avro-confluent", Map.of("url", "http://r:8081"), rowType));
    // Header-only registry auth resolves; the untranslated credential sources decline.
    assertNotNull(
        EncodeFormat.of(
            "avro-confluent",
            Map.of(
                "url", "http://r:8081",
                "subject", "t-value",
                "basic-auth.credentials-source", "USER_INFO",
                "basic-auth.user-info", "user:pass"),
            rowType));
    assertNull(
        EncodeFormat.of(
            "avro-confluent",
            Map.of(
                "url", "http://r:8081",
                "subject", "t-value",
                "basic-auth.credentials-source", "URL"),
            rowType));
  }

  @Test
  void collectsKeyAndValueFormatOptionsSeparately() {
    KafkaSinkTranslator.Result result =
        KafkaSinkTranslator.translate(
            Map.of(
                "connector", "upsert-kafka",
                "topic", "output",
                "properties.bootstrap.servers", "broker:9092",
                "key.format", "json",
                "value.format", "json",
                "key.json.timestamp-format.standard", "ISO-8601",
                "value.json.encode.decimal-as-plain-number", "true"));

    assertTrue(result.fallbackReason == null, () -> result.fallbackReason);
    assertEquals(
        Map.of("timestamp-format.standard", "ISO-8601"), result.planned().keyFormatOptions);
    assertEquals(
        Map.of("encode.decimal-as-plain-number", "true"), result.planned().valueFormatOptions);
  }

  /** Flink's factories complete a registry format's subject from the topic on a context copy the
   * planner hook never sees; the translator replays it, never overriding an explicit subject. */
  @Test
  void autoCompletesTheSchemaRegistrySubjectFromTheTopic() {
    KafkaSinkTranslator.Result result =
        KafkaSinkTranslator.translate(
            Map.of(
                "connector", "upsert-kafka",
                "topic", "orders",
                "properties.bootstrap.servers", "broker:9092",
                "value.format", "avro-confluent",
                "value.avro-confluent.url", "http://registry:8081",
                "key.format", "avro-confluent",
                "key.avro-confluent.url", "http://registry:8081",
                "key.avro-confluent.subject", "explicit-subject"));

    assertTrue(result.fallbackReason == null, () -> result.fallbackReason);
    assertEquals(
        "orders-value", result.planned().valueFormatOptions.get("schema-registry.subject"));
    assertEquals("explicit-subject", result.planned().keyFormatOptions.get("subject"));
    assertNull(result.planned().keyFormatOptions.get("schema-registry.subject"));
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
    assertTrue(result.fallbackReason != null);
    assertTrue(result.fallbackReason.contains("transactional-id-prefix"));
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
    assertTrue(result.fallbackReason != null);
    assertTrue(result.fallbackReason.contains("interceptor.classes"));
  }

  private static void assertFallback(Map<String, String> options, String expected) {
    KafkaSinkTranslator.Result result = KafkaSinkTranslator.translate(options);
    assertTrue(result.fallbackReason != null);
    assertTrue(result.fallbackReason.contains(expected));
  }

  private static Map<String, String> with(Map<String, String> base, String key, String value) {
    java.util.HashMap<String, String> copy = new java.util.HashMap<>(base);
    copy.put(key, value);
    return copy;
  }
}
