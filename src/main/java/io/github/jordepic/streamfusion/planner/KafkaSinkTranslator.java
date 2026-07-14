package io.github.jordepic.streamfusion.planner;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;

/** Conservative table-option boundary for the native-serialization/stock-KafkaSink hybrid. */
final class KafkaSinkTranslator {

  private KafkaSinkTranslator() {}

  static final class Result {
    private final Planned planned;
    private final String fallbackReason;

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

    boolean isTranslated() {
      return planned != null;
    }

    Planned planned() {
      return planned;
    }

    Optional<String> fallbackReason() {
      return Optional.ofNullable(fallbackReason);
    }
  }

  static final class Planned {
    final String topic;
    final Properties producerProperties;
    final DeliveryGuarantee deliveryGuarantee;
    final String transactionalIdPrefix;
    final TransactionNamingStrategy transactionNamingStrategy;
    final Integer parallelism;
    final Map<String, String> jsonOptions;
    final boolean upsert;

    private Planned(
        String topic,
        Properties producerProperties,
        DeliveryGuarantee deliveryGuarantee,
        String transactionalIdPrefix,
        TransactionNamingStrategy transactionNamingStrategy,
        Integer parallelism,
        Map<String, String> jsonOptions,
        boolean upsert) {
      this.topic = topic;
      this.producerProperties = producerProperties;
      this.deliveryGuarantee = deliveryGuarantee;
      this.transactionalIdPrefix = transactionalIdPrefix;
      this.transactionNamingStrategy = transactionNamingStrategy;
      this.parallelism = parallelism;
      this.jsonOptions = jsonOptions;
      this.upsert = upsert;
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
    String format = options.getOrDefault("value.format", options.get("format"));
    if (!"json".equals(format)) {
      return Result.fallback("value format " + format + " is not yet natively encoded");
    }
    if ((upsert && !"json".equals(options.get("key.format")))
        || (!upsert && options.containsKey("key.format"))) {
      return Result.fallback("key format is not yet natively encoded for this connector");
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
                  .toUpperCase(java.util.Locale.ROOT));
      String namingOption = options.get("sink.transaction-naming-strategy");
      naming =
          namingOption == null || "default".equalsIgnoreCase(namingOption)
              ? TransactionNamingStrategy.DEFAULT
              : TransactionNamingStrategy.valueOf(
                  namingOption.replace('-', '_').toUpperCase(java.util.Locale.ROOT));
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

    Map<String, String> jsonOptions = new LinkedHashMap<>();
    options.forEach(
        (key, value) -> {
          if (key.startsWith("json.")) {
            jsonOptions.put(key.substring("json.".length()), value);
          } else if (key.startsWith("value.json.")) {
            jsonOptions.put(key.substring("value.json.".length()), value);
          }
        });
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
            jsonOptions,
            upsert));
  }

  private static boolean isZeroDuration(String value) {
    String normalized = value.trim().toLowerCase(java.util.Locale.ROOT);
    return normalized.matches("0+(\\.0+)?(\\s*(ms|s|sec|secs|second|seconds))?");
  }
}
