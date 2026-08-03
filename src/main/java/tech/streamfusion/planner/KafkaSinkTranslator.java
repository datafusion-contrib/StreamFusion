package tech.streamfusion.planner;

import tech.streamfusion.kafka.KafkaProducerConfigTranslator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;

/** Conservative table-option boundary for the native-serialization/stock-KafkaSink hybrid. */
final class KafkaSinkTranslator {

  private KafkaSinkTranslator() {}

  static final class Result {
    private final Planned planned;
    final String fallbackReason;

    private Result(Planned planned, String fallbackReason) {
      this.planned = planned;
      this.fallbackReason = fallbackReason;
    }

    static Result translated(Planned planned) {
      return new Result(planned, null);
    }

    static Result fallback(String reason) {
      return new Result(null, reason);
    }

    Planned planned() {
      return planned;
    }
  }

  static final class Planned {
    final String topic;
    final Properties producerProperties;
    final DeliveryGuarantee deliveryGuarantee;
    final String transactionalIdPrefix;
    final TransactionNamingStrategy transactionNamingStrategy;
    final Integer parallelism;
    final String valueFormat;
    final String keyFormat;
    final Map<String, String> valueFormatOptions;
    final Map<String, String> keyFormatOptions;
    final boolean upsert;
    final KafkaProducerConfigTranslator.Result nativeProducerConfig;

    private Planned(
        String topic,
        Properties producerProperties,
        DeliveryGuarantee deliveryGuarantee,
        String transactionalIdPrefix,
        TransactionNamingStrategy transactionNamingStrategy,
        Integer parallelism,
        String valueFormat,
        String keyFormat,
        Map<String, String> valueFormatOptions,
        Map<String, String> keyFormatOptions,
        boolean upsert,
        KafkaProducerConfigTranslator.Result nativeProducerConfig) {
      this.topic = topic;
      this.producerProperties = producerProperties;
      this.deliveryGuarantee = deliveryGuarantee;
      this.transactionalIdPrefix = transactionalIdPrefix;
      this.transactionNamingStrategy = transactionNamingStrategy;
      this.parallelism = parallelism;
      this.valueFormat = valueFormat;
      this.keyFormat = keyFormat;
      this.valueFormatOptions = valueFormatOptions;
      this.keyFormatOptions = keyFormatOptions;
      this.upsert = upsert;
      this.nativeProducerConfig = nativeProducerConfig;
    }
  }

  static Result translate(Map<String, String> options) {
    boolean upsert = "upsert-kafka".equals(options.get("connector"));
    String topic = options.get("topic");
    if (topic == null || topic.contains(";")) {
      return Result.fallback("native serialization currently requires one fixed topic");
    }
    if (options.containsKey("topic-pattern")) {
      return Result.fallback("topic-pattern requires writable topic metadata");
    }
    String valueFormat = options.getOrDefault("value.format", options.get("format"));
    String keyFormat = options.get("key.format");
    if (upsert && keyFormat == null) {
      return Result.fallback("upsert-kafka requires a key format");
    }
    if (!upsert && keyFormat != null) {
      return Result.fallback("a keyed ordinary kafka table is not yet natively encoded");
    }
    if (options.containsKey("key.fields")
        || options.containsKey("key.fields-prefix")
        || !"ALL".equalsIgnoreCase(options.getOrDefault("value.fields-include", "ALL"))) {
      return Result.fallback("key/value projection is not yet natively encoded");
    }
    if (!"default".equalsIgnoreCase(options.getOrDefault("sink.partitioner", "default"))) {
      return Result.fallback("non-default sink partitioner");
    }
    if (!"0".equals(options.getOrDefault("sink.buffer-flush.max-rows", "0"))
        || !isZeroDuration(options.getOrDefault("sink.buffer-flush.interval", "0 s"))) {
      return Result.fallback("sink buffer flushing is not yet supported");
    }

    DeliveryGuarantee guarantee;
    TransactionNamingStrategy naming;
    try {
      guarantee =
          DeliveryGuarantee.valueOf(
              options
                  .getOrDefault("sink.delivery-guarantee", "at-least-once")
                  .replace('-', '_')
                  .toUpperCase(Locale.ROOT));
      String namingOption = options.get("sink.transaction-naming-strategy");
      naming =
          namingOption == null || "default".equalsIgnoreCase(namingOption)
              ? TransactionNamingStrategy.DEFAULT
              : TransactionNamingStrategy.valueOf(
                  namingOption.replace('-', '_').toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException invalid) {
      return Result.fallback("invalid Kafka sink delivery or transaction option");
    }
    String transactionalIdPrefix = options.get("sink.transactional-id-prefix");
    if (guarantee == DeliveryGuarantee.EXACTLY_ONCE && transactionalIdPrefix == null) {
      return Result.fallback("exactly-once requires sink.transactional-id-prefix");
    }

    Properties producer = new Properties();
    options.forEach(
        (key, value) -> {
          if (key.startsWith("properties.")) {
            producer.setProperty(key.substring("properties.".length()), value);
          }
        });
    if (!producer.containsKey("bootstrap.servers")) {
      return Result.fallback("properties.bootstrap.servers is required");
    }
    KafkaProducerConfigTranslator.Result nativeProducerConfig = null;
    if (guarantee == DeliveryGuarantee.EXACTLY_ONCE) {
      if (naming != TransactionNamingStrategy.INCREMENTING) {
        return Result.fallback(
            "native exactly-once producer currently requires incremental transaction naming");
      }
      nativeProducerConfig = KafkaProducerConfigTranslator.translate(producer);
      if (nativeProducerConfig.fallbackReason != null) {
        return Result.fallback(nativeProducerConfig.fallbackReason);
      }
    }

    // Flink configures the key and value formats as two independent format instances: value
    // options live under `<format>.` / `value.<format>.`, key options only under `key.<format>.`
    // (with the format factory's own defaults when absent, never the value's settings).
    Map<String, String> valueFormatOptions = new LinkedHashMap<>();
    Map<String, String> keyFormatOptions = new LinkedHashMap<>();
    options.forEach(
        (key, value) -> {
          stripPrefix(key, valueFormat + ".", value, valueFormatOptions);
          stripPrefix(key, "value." + valueFormat + ".", value, valueFormatOptions);
          if (keyFormat != null) {
            stripPrefix(key, "key." + keyFormat + ".", value, keyFormatOptions);
          }
        });
    // Flink's Kafka factories auto-complete a schema-registry format's subject from the (single,
    // fixed) topic before the format factory validates it — on a copied context our planner hook
    // never sees, so the same completion happens here, under the same fallback spelling and never
    // overriding an explicit subject.
    autoCompleteSchemaRegistrySubject(valueFormat, valueFormatOptions, topic + "-value");
    autoCompleteSchemaRegistrySubject(keyFormat, keyFormatOptions, topic + "-key");
    Integer parallelism =
        options.containsKey("sink.parallelism")
            ? Integer.valueOf(options.get("sink.parallelism"))
            : null;
    return Result.translated(
        new Planned(
            topic,
            producer,
            guarantee,
            transactionalIdPrefix,
            naming,
            parallelism,
            valueFormat,
            keyFormat,
            valueFormatOptions,
            keyFormatOptions,
            upsert,
            nativeProducerConfig));
  }

  private static void stripPrefix(
      String key, String prefix, String value, Map<String, String> into) {
    if (key.startsWith(prefix)) {
      into.put(key.substring(prefix.length()), value);
    }
  }

  private static void autoCompleteSchemaRegistrySubject(
      String format, Map<String, String> formatOptions, String subject) {
    if (("avro-confluent".equals(format) || "debezium-avro-confluent".equals(format))
        && !formatOptions.containsKey("subject")
        && !formatOptions.containsKey("schema-registry.subject")) {
      formatOptions.put("schema-registry.subject", subject);
    }
  }

  private static boolean isZeroDuration(String value) {
    String normalized = value.trim().toLowerCase(Locale.ROOT);
    return normalized.matches("0+(\\.0+)?(\\s*(ms|s|sec|secs|second|seconds))?");
  }
}
