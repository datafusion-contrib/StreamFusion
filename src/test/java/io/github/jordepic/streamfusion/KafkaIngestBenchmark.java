package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.Native;
import java.time.Duration;
import java.util.List;
import java.util.Map;
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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
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
 * byte-delivery saving. Build the native side with {@code kafka-bench,mimalloc} — the allocator
 * rebind pays for librdkafka's per-message op malloc/free (see the feature's Cargo.toml note).
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
  // Partition count for the topic. >1 tests whether librdkafka's per-partition fetch (one FetchRequest
  // carrying all partitions, drained off one queue) closes the gap to Java's inline single-thread poll.
  private static final int PARTITIONS =
      System.getenv("SF_KAFKA_PARTITIONS") != null
          ? Integer.parseInt(System.getenv("SF_KAFKA_PARTITIONS"))
          : 1;
  private static final String TOPIC = "bench-json";
  private static final String AVRO_TOPIC = "bench-avro";
  private static final int SCHEMA_ID = 1;
  // Non-nullable row so the Avro schema is a bare record (not a top-level ["null", record] union).
  private static final RowType ROW_TYPE =
      RowType.of(
          false,
          new LogicalType[] {new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()},
          new String[] {"id", "name", "score"});

  @Test
  void shallowVsNativeKafkaIngest() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produce(brokers, MESSAGES);

      // Profiling mode: loop one JSON leg so a sampler has a long, stable window.
      // SF_KAFKA_PROFILE=1 loops the production-reader native path; =json-serial the hand-rolled
      // serial native path; =json-shallow the Java-consume-then-native-decode path.
      String jsonProfile = System.getenv("SF_KAFKA_PROFILE");
      if (jsonProfile != null
          && (jsonProfile.equals("1")
              || jsonProfile.equals("json-serial")
              || jsonProfile.equals("json-shallow"))) {
        System.err.println("PROFILE_CONSUME_LOOP_START");
        System.err.flush();
        for (int i = 0; i < 60; i++) {
          switch (jsonProfile) {
            case "json-shallow" -> consumeAndDecode(brokers);
            case "json-serial" -> nativeConsumeAndDecode(brokers, true);
            default -> nativeConsumeAndDecode(brokers); // 1 (production reader)
          }
        }
        return;
      }

      // SAME harness (plain JVM), both legs decode to Arrow and count IN RUST — no per-batch export to
      // the JVM, as they'd feed a downstream native operator. The only difference is who polls Kafka:
      // Java's batch poll vs librdkafka's per-message poll.
      double shallow = time(() -> consumeAndDecode(brokers)); // Java batch poll -> bytes -> native decode
      double nativeSecs = time(() -> nativeConsumeAndDecode(brokers)); // production split reader
      double serial = time(() -> nativeConsumeAndDecode(brokers, true)); // rdkafka poll + inline decode

      System.out.printf(
          "%n[kafka -> Arrow IN RUST, %,d three-field JSON msgs, same plain-JVM harness]%n"
              + "  shallow (Java batch poll -> native decode): %.2fs  =  %,.0f msgs/s%n"
              + "  native (production reader, inline decode):  %.2fs  =  %,.0f msgs/s%n"
              + "  native serial    (rdkafka poll + inline):   %.2fs  =  %,.0f msgs/s%n"
              + "  native(production) speedup vs shallow: %.2fx   production vs hand-rolled serial: %.2fx%n",
          MESSAGES, shallow, MESSAGES / shallow, nativeSecs, MESSAGES / nativeSecs, serial,
          MESSAGES / serial, shallow / nativeSecs, serial / nativeSecs);
    }
  }

  /**
   * Raw delivery throughput with NO decode on either side — isolates the consumer. Answers whether
   * librdkafka's batch-consume actually delivers slower than the Java client's poll on this box, or
   * whether the earlier gap was downstream (decode/pipeline). Same topic, both just count messages.
   */
  @Test
  void rawConsumeThroughput() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produce(brokers, MESSAGES);

      // Profiling mode: loop the native raw consume (NO decode, no decode thread) so a sampler gets a
      // long, stable window over JUST the poll loop — rd_kafka_consume_batch_queue + per-message destroy.
      String profile = System.getenv("SF_KAFKA_PROFILE");
      if (profile != null) {
        boolean java = "java".equals(profile);
        System.err.println("PROFILE_CONSUME_LOOP_START");
        System.err.flush();
        for (int i = 0; i < 60; i++) {
          if (java) {
            javaConsumeOnly(brokers);
          } else {
            nativeConsumeOnly(brokers);
          }
        }
        return;
      }

      double java = time(() -> javaConsumeOnly(brokers));
      double nativeSecs = time(() -> nativeConsumeOnly(brokers));

      System.out.printf(
          "%n[raw consume (no decode), %,d msgs]%n"
              + "  java   (KafkaConsumer poll, count):   %.2fs  =  %,.0f msgs/s%n"
              + "  native (rdkafka batch consume, count): %.2fs  =  %,.0f msgs/s%n"
              + "  native speedup: %.2fx%n",
          MESSAGES, java, MESSAGES / java, nativeSecs, MESSAGES / nativeSecs, java / nativeSecs);
    }
  }

  /** Java client raw delivery: poll and count, no decode. */
  private static long javaConsumeOnly(String brokers) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "bench-raw-" + System.nanoTime());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 8192);
    props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 16 << 20);
    long seen = 0;
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(TOPIC));
      while (seen < MESSAGES) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));
        seen += records.count();
      }
    }
    return seen;
  }

  /**
   * librdkafka raw delivery (no decode) using the EXACT config the production native source sends, so
   * this isolates how fast the production-tuned librdkafka delivers messages versus the Java client.
   */
  private static long nativeConsumeOnly(String brokers) {
    String[][] config = nativeConfig(brokers, "bench-raw-native-" + System.nanoTime());
    return Native.benchmarkConsumeOnly(config[0], config[1], TOPIC, MESSAGES);
  }

  /**
   * Same comparison as the JSON test but for Confluent-Avro — the format where native decode should
   * help most (Avro is heavier to deserialize than JSON, and the native path avoids the JVM byte[]).
   * Both legs decode to Arrow and count in Rust.
   */
  @Test
  void shallowVsNativeAvroIngest() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      String avroSchema =
          org.apache.flink.formats.avro.typeutils.AvroSchemaConverter.convertToSchema(ROW_TYPE)
              .toString();
      produceAvro(brokers, MESSAGES, avroSchema);

      // Profiling: loop one Avro leg so a sampler gets a stable window. SF_KAFKA_PROFILE=avro-native
      // loops the production-reader native path; =avro-shallow loops
      // the serial Java-consume-then-decode path.
      String profile = System.getenv("SF_KAFKA_PROFILE");
      if (profile != null && profile.startsWith("avro-")) {
        System.err.println("PROFILE_CONSUME_LOOP_START");
        System.err.flush();
        for (int i = 0; i < 60; i++) {
          switch (profile) {
            case "avro-shallow" -> consumeAndDecodeAvro(brokers, avroSchema);
            case "avro-serial" -> nativeConsumeAndDecodeAvro(brokers, avroSchema, true);
            default -> nativeConsumeAndDecodeAvro(brokers, avroSchema); // avro-native (production)
          }
        }
        return;
      }

      double shallow = time(() -> consumeAndDecodeAvro(brokers, avroSchema));
      double nativeSecs = time(() -> nativeConsumeAndDecodeAvro(brokers, avroSchema));
      double serial = time(() -> nativeConsumeAndDecodeAvro(brokers, avroSchema, true));

      System.out.printf(
          "%n[kafka -> Arrow IN RUST, %,d three-field Confluent-Avro msgs, same plain-JVM harness]%n"
              + "  shallow (Java batch poll -> native decode): %.2fs  =  %,.0f msgs/s%n"
              + "  native (production reader, inline decode):  %.2fs  =  %,.0f msgs/s%n"
              + "  native serial    (rdkafka poll + inline):   %.2fs  =  %,.0f msgs/s%n"
              + "  native(production) speedup vs shallow: %.2fx   production vs hand-rolled serial: %.2fx%n",
          MESSAGES, shallow, MESSAGES / shallow, nativeSecs, MESSAGES / nativeSecs, serial,
          MESSAGES / serial, shallow / nativeSecs, serial / nativeSecs);
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

  /** Produces MESSAGES three-field JSON documents to the topic, spread round-robin over PARTITIONS. */
  private static void produce(String brokers, int messages) {
    createTopic(brokers, TOPIC);
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
        producer.send(new ProducerRecord<>(TOPIC, i % PARTITIONS, null, value));
      }
      producer.flush();
    }
  }

  /** Creates {@code topic} with PARTITIONS partitions (an explicit partition needs the topic to exist). */
  private static void createTopic(String brokers, String topic) {
    try (Admin admin = Admin.create(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers))) {
      admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1))).all().get();
    } catch (Exception e) {
      throw new RuntimeException("failed to create topic " + topic, e);
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
      long handle =
          withExportedSchema(
              allocator,
              dictionaries,
              (arrayAddress, schemaAddress) ->
                  Native.createDecoder(0, arrayAddress, schemaAddress, "", "", 0, false));
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
        Native.closeDecoder(handle);
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
        return Native.decodeCount(handle, inArray.memoryAddress(), inSchema.memoryAddress());
      }
    }
  }

  /**
   * Drives the production split reader (rdkafka poll + background decode thread) over the topic and
   * counts decoded rows in Rust — terminating with Arrow in Rust, no per-batch JVM export.
   */
  private static long nativeConsumeAndDecode(String brokers) {
    return nativeConsumeAndDecode(brokers, false);
  }

  private static long nativeConsumeAndDecode(String brokers, boolean serial) {
    String[][] config = nativeConfig(brokers, "bench-native-" + System.nanoTime());
    String[] keys = config[0];
    String[] values = config[1];
    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
      return withExportedSchema(
          allocator,
          dictionaries,
          (arrayAddress, schemaAddress) ->
              serial
                  ? Native.benchmarkNativeConsumeSerial(
                      keys, values, TOPIC, 0, arrayAddress, schemaAddress, "", 0, MESSAGES)
                  : Native.benchmarkNativeConsume(
                      keys, values, TOPIC, 0, arrayAddress, schemaAddress, "", 0, MESSAGES));
    }
  }

  /** Produces Confluent-framed Avro (magic byte + 4-byte schema id + Avro body) to the Avro topic. */
  private static void produceAvro(String brokers, int messages, String avroSchemaJson) throws Exception {
    org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(avroSchemaJson);
    org.apache.avro.generic.GenericDatumWriter<org.apache.avro.generic.GenericRecord> writer =
        new org.apache.avro.generic.GenericDatumWriter<>(schema);
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < messages; i++) {
        org.apache.avro.generic.GenericRecord record = new org.apache.avro.generic.GenericData.Record(schema);
        record.put("id", (long) i);
        record.put("name", "row-" + i);
        record.put("score", (i % 100) + 0.5);
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        out.write(0);
        out.write((SCHEMA_ID >>> 24) & 0xFF);
        out.write((SCHEMA_ID >>> 16) & 0xFF);
        out.write((SCHEMA_ID >>> 8) & 0xFF);
        out.write(SCHEMA_ID & 0xFF);
        org.apache.avro.io.BinaryEncoder encoder =
            org.apache.avro.io.EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        producer.send(new ProducerRecord<>(AVRO_TOPIC, out.toByteArray()));
      }
      producer.flush();
    }
  }

  /** Shallow Avro: Java batch poll -> bytes -> native Confluent-Avro decode, counted in Rust. */
  private static long consumeAndDecodeAvro(String brokers, String avroSchema) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "bench-avro-" + System.nanoTime());
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
      consumer.subscribe(List.of(AVRO_TOPIC));
      long handle = Native.createDecoder(1, 0, 0, avroSchema, "", SCHEMA_ID, false);
      try {
        while (seen < MESSAGES) {
          ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));
          if (records.isEmpty()) {
            continue;
          }
          rows += decodeBatchAvro(handle, records, allocator, dictionaries);
          seen += records.count();
        }
      } finally {
        Native.closeDecoder(handle);
      }
    }
    return rows;
  }

  /** Builds the Arrow binary column of a poll's values and Avro-decodes it natively, counting in Rust. */
  private static long decodeBatchAvro(
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
        return Native.decodeCount(handle, inArray.memoryAddress(), inSchema.memoryAddress());
      }
    }
  }

  /** Native Avro: rdkafka batch consume + native Confluent-Avro decode, counted in Rust. */
  private static long nativeConsumeAndDecodeAvro(String brokers, String avroSchema) {
    return nativeConsumeAndDecodeAvro(brokers, avroSchema, false);
  }

  private static long nativeConsumeAndDecodeAvro(String brokers, String avroSchema, boolean serial) {
    String[][] config = nativeConfig(brokers, "bench-native-avro-" + System.nanoTime());
    String[] keys = config[0];
    String[] values = config[1];
    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
      return withExportedSchema(
          allocator,
          dictionaries,
          (arrayAddress, schemaAddress) ->
              serial
                  ? Native.benchmarkNativeConsumeSerial(
                      keys, values, AVRO_TOPIC, 1, arrayAddress, schemaAddress, avroSchema, SCHEMA_ID,
                      MESSAGES)
                  : Native.benchmarkNativeConsume(
                      keys, values, AVRO_TOPIC, 1, arrayAddress, schemaAddress, avroSchema, SCHEMA_ID,
                      MESSAGES));
    }
  }

  /**
   * The librdkafka config the production native source uses for a typical consumer: the {@link
   * io.github.jordepic.streamfusion.kafka.KafkaConfigTranslator} output plus the librdkafka-only
   * throughput tuning {@code KafkaTables} folds in. Returned as {@code {keys, values}} for the JNI string-array call. Keeping this identical to the
   * production path is the point: the benchmark measures what a real SQL Kafka table actually runs.
   */
  private static String[][] nativeConfig(String brokers, String group) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", brokers);
    props.setProperty("group.id", group);
    props.setProperty("enable.auto.commit", "false");
    props.setProperty("fetch.min.bytes", "1");
    props.setProperty("fetch.max.bytes", String.valueOf(64 << 20));
    props.setProperty("max.partition.fetch.bytes", String.valueOf(16 << 20));
    props.setProperty("fetch.max.wait.ms", "500");
    java.util.Map<String, String> config =
        new java.util.HashMap<>(
            io.github.jordepic.streamfusion.kafka.KafkaConfigTranslator.translate(props).config());
    config.putIfAbsent("fetch.queue.backoff.ms", "2");
    config.putIfAbsent("queued.min.messages", "1000000");
    config.putIfAbsent("queued.max.messages.kbytes", "2097151");
    // A/B knob: the translator leaves check.crcs at librdkafka's default (false — see the
    // translator's note; librdkafka has no hardware CRC32C on ARM, so verification costs real
    // delivery-thread CPU). SF_KAFKA_CHECK_CRCS=true re-enables it to measure that tax.
    if (System.getenv("SF_KAFKA_CHECK_CRCS") != null) {
      config.put("check.crcs", System.getenv("SF_KAFKA_CHECK_CRCS"));
    }
    String[] keys = config.keySet().toArray(new String[0]);
    String[] values = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      values[i] = config.get(keys[i]);
    }
    return new String[][] {keys, values};
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
