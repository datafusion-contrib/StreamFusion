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
 * needs a system librdkafka and the {@code kafka-bench} cargo feature:
 * {@code PKG_CONFIG_PATH=<dir with zlib.pc+libcurl.pc> SF_BENCHMARK=true mvn test -Pbench
 * -Dnative.cargo.args="build --release --features kafka-bench" -Dtest=KafkaIngestBenchmark}. macOS
 * ships libz/libcurl but not their .pc files — point PKG_CONFIG_PATH at stubs or `brew install zlib
 * curl`. The default build excludes rdkafka, so it needs none of this.
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

      double shallow = time(() -> consumeAndDecode(brokers)); // JVM client + heap byte[] + decode
      double nativeSecs = time(() -> nativeConsumeAndDecode(brokers)); // rdkafka + decode, all native

      System.out.printf(
          "%n[kafka ingest, %,d three-field JSON msgs]%n"
              + "  shallow (JVM client + off-heap copy + native decode): %.2fs  =  %,.0f msgs/s%n"
              + "  native  (rdkafka + native decode):                     %.2fs  =  %,.0f msgs/s%n"
              + "  native speedup: %.2fx%n",
          MESSAGES,
          shallow,
          MESSAGES / shallow,
          nativeSecs,
          MESSAGES / nativeSecs,
          shallow / nativeSecs);
    }
  }

  /** Runs {@code work} once to warm up (JIT, page-cache), then again timed; returns the timed seconds. */
  private static double time(Runnable work) {
    work.run();
    long start = System.nanoTime();
    work.run();
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

  /** Builds the binary batch of a poll's values, decodes it natively, and returns the decoded rows. */
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
          ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
          ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, in, dictionaries, inArray, inSchema);
        Native.decodeJson(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress());
        try (VectorSchemaRoot out =
            Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries)) {
          return out.getRowCount();
        }
      }
    }
  }

  /** Consumes and decodes the whole topic natively (rdkafka + native decode); returns the row count. */
  private static long nativeConsumeAndDecode(String brokers) {
    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
      return withExportedSchema(
          allocator,
          dictionaries,
          (arrayAddress, schemaAddress) ->
              Native.benchmarkKafkaConsume(brokers, TOPIC, arrayAddress, schemaAddress, MESSAGES));
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
