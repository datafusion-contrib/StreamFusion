package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.Native;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Head-to-head Kafka ingest throughput: the shallow path (Flink's connector wraps the same Kafka
 * client — consume message values as heap {@code byte[]}s, batch them into an Arrow binary column,
 * decode natively) vs the native path (rdkafka consumes straight into an Arrow builder, no JVM
 * byte[]/JNI per record, then the same decode). Decode is shared, so the gap is purely the consume +
 * byte-delivery saving — ~5x here, which is what justifies building the production native source.
 *
 * <p>Opt-in via {@code SF_BENCHMARK=true}; requires Docker (Testcontainers Kafka). The native side
 * builds with the {@code kafka-bench} cargo feature, which statically links a bundled librdkafka (no
 * system install): {@code SF_BENCHMARK=true mvn test -Pbench
 * -Dnative.cargo.args="build --release --features kafka-bench" -Dtest=KafkaIngestBenchmark}. The
 * default build excludes rdkafka, so it needs none of this.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class KafkaIngestBenchmark {

  private static final int MESSAGES =
      System.getenv("SF_KAFKA_MESSAGES") != null
          ? Integer.parseInt(System.getenv("SF_KAFKA_MESSAGES"))
          : 1_000_000;
  private static final String TOPIC = "bench-json";

  @Test
  void shallowVsNativeKafkaIngest() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produce(brokers, MESSAGES);

      // Profiling mode: loop the native consume so a sampler has a long, stable window over the poll
      // thread (librdkafka poll + the byte copy) and the Rust decode thread.
      if ("1".equals(System.getenv("SF_KAFKA_PROFILE"))) {
        for (int i = 0; i < 30; i++) {
          nativeConsumeAndDecode(brokers);
        }
        return;
      }

      // SAME harness (plain JVM), both legs decode to Arrow and count IN RUST — no per-batch export to
      // the JVM, as they'd feed a downstream native operator. The only difference is who polls Kafka:
      // Java's batch poll vs librdkafka's per-message poll.
      double shallow = time(() -> consumeAndDecode(brokers)); // Java batch poll -> bytes -> native decode
      double nativeSecs = time(() -> nativeConsumeAndDecode(brokers)); // rdkafka poll + native decode

      System.out.printf(
          "%n[kafka -> Arrow IN RUST, %,d three-field JSON msgs, same plain-JVM harness]%n"
              + "  shallow (Java batch poll -> native decode): %.2fs  =  %,.0f msgs/s%n"
              + "  native  (rdkafka poll + native decode):     %.2fs  =  %,.0f msgs/s%n"
              + "  native speedup: %.2fx%n",
          MESSAGES, shallow, MESSAGES / shallow, nativeSecs, MESSAGES / nativeSecs, shallow / nativeSecs);
    }
  }

  /**
   * Runs {@code work} once to warm up (JIT, page-cache), then again timed; asserts both runs decoded
   * every message (a run that silently reads nothing would otherwise report an absurd rate) and returns
   * the timed seconds.
   */
  private static double time(java.util.function.LongSupplier work) {
    long warm = work.getAsLong();
    if (warm < MESSAGES) {
      throw new AssertionError("warm-up decoded " + warm + " rows, expected >= " + MESSAGES);
    }
    long start = System.nanoTime();
    long timed = work.getAsLong();
    if (timed < MESSAGES) {
      throw new AssertionError("timed run decoded " + timed + " rows, expected >= " + MESSAGES);
    }
    return (System.nanoTime() - start) / 1e9;
  }

  /** Produces MESSAGES three-field JSON documents to the topic. */
  private static void produce(String brokers, int messages) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < messages; i++) {
        byte[] value =
            String.format("{\"id\": %d, \"name\": \"row-%d\", \"score\": %d.5}", i, i, i % 100)
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(TOPIC, value));
      }
      producer.flush();
    }
  }

  /** Consumes the whole topic, decoding each poll batch natively; returns the decoded row count. */
  private static long consumeAndDecode(String brokers) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "bench-" + System.nanoTime());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 8192);
    props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 16 << 20);

    long rows = 0;
    long seen = 0;
    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(TOPIC));
      long handle = withExportedSchema(allocator, dictionaries, Native::createJsonDecoder);
      try {
        while (seen < MESSAGES) {
          ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));
          if (records.isEmpty()) {
            continue;
          }
          rows += decodeBatch(handle, records, allocator, dictionaries);
          seen += records.count();
        }
      } finally {
        Native.closeJsonDecoder(handle);
      }
    }
    return rows;
  }

  /**
   * Builds the Arrow binary column of a poll's values and decodes it natively, counting the decoded
   * rows in Rust (no export back to the JVM) — so the shallow path terminates with Arrow in Rust, the
   * same as the native consumer.
   */
  private static long decodeBatch(
      long handle,
      ConsumerRecords<byte[], byte[]> records,
      BufferAllocator allocator,
      CDataDictionaryProvider dictionaries) {
    int count = records.count();
    try (VarBinaryVector body = new VarBinaryVector("body", allocator)) {
      body.allocateNew(count);
      int i = 0;
      for (ConsumerRecord<byte[], byte[]> record : records) {
        body.setSafe(i++, record.value());
      }
      body.setValueCount(count);
      VectorSchemaRoot in = new VectorSchemaRoot(List.of(body));
      in.setRowCount(count);
      try (ArrowArray inArray = ArrowArray.allocateNew(allocator);
          ArrowSchema inSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, in, dictionaries, inArray, inSchema);
        return Native.decodeJsonCount(handle, inArray.memoryAddress(), inSchema.memoryAddress());
      }
    }
  }

  /**
   * Drives the production split reader (rdkafka poll + background decode thread) over the topic and
   * counts decoded rows in Rust — terminating with Arrow in Rust, no per-batch JVM export.
   */
  private static long nativeConsumeAndDecode(String brokers) {
    String[] keys = {
      "bootstrap.servers", "group.id", "enable.auto.commit", "fetch.queue.backoff.ms", "check.crcs"
    };
    String[] values = {brokers, "bench-native-" + System.nanoTime(), "false", "2", "false"};
    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
      return withExportedSchema(
          allocator,
          dictionaries,
          (arrayAddress, schemaAddress) ->
              Native.benchmarkNativeConsume(keys, values, TOPIC, arrayAddress, schemaAddress, MESSAGES));
    }
  }

  /**
   * Exports an empty batch of the {@code id BIGINT, name STRING, score DOUBLE} schema into C structs
   * and invokes {@code use} with their addresses (to create a decoder, or to start a native consume).
   */
  private static long withExportedSchema(
      BufferAllocator allocator,
      CDataDictionaryProvider dictionaries,
      java.util.function.LongBinaryOperator use) {
    try (BigIntVector id = new BigIntVector("id", allocator);
        VarCharVector name = new VarCharVector("name", allocator);
        Float8Vector score = new Float8Vector("score", allocator)) {
      for (FieldVector vector : List.of(id, name, score)) {
        vector.allocateNew();
        vector.setValueCount(0);
      }
      try (VectorSchemaRoot template = new VectorSchemaRoot(List.of(id, name, score));
          ArrowArray array = ArrowArray.allocateNew(allocator);
          ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        template.setRowCount(0);
        Data.exportVectorSchemaRoot(allocator, template, dictionaries, array, schema);
        return use.applyAsLong(array.memoryAddress(), schema.memoryAddress());
      }
    }
  }
}
