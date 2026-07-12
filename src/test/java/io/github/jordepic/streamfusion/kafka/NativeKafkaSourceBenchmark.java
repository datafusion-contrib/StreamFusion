package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeBytesDecodeOperator;
import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.json.JsonFormatProvider;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
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
 * Head-to-head source throughput on the same topic, both bounded to the latest offset so each job
 * reads the whole topic once and terminates: the production native source (rdkafka consumes and decodes
 * to Arrow in Rust, emitting {@link ArrowBatch}) vs Flink's own {@link KafkaSource} with its JSON →
 * {@code RowData} deserializer — i.e. "the one that converts rows after reading in Java". This is the
 * realistic payoff measurement: it isolates the source (a trivial counting sink consumes each side's
 * native output type, no Arrow↔Row transpose), so the gap is exactly the JVM Kafka client + per-record
 * {@code RowData} materialization the native path removes.
 *
 * <p>Opt-in via {@code SF_BENCHMARK=true}; release native build with the {@code kafka} feature:
 * {@code SF_BENCHMARK=true mvn test -Pbench -Dnative.cargo.args="build --release --features kafka"
 * -Dtest=NativeKafkaSourceBenchmark}. Debug Rust gives misleading numbers — always {@code -Pbench}.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeKafkaSourceBenchmark {

  private static final String TOPIC = "bench-source-json";
  private static final String AVRO_TOPIC = "bench-source-avro";
  private static final int SCHEMA_ID = 1;
  private static final int MESSAGES =
      System.getenv("SF_KAFKA_MESSAGES") != null
          ? Integer.parseInt(System.getenv("SF_KAFKA_MESSAGES"))
          : 1_000_000;
  private static final AtomicLong COUNTER = new AtomicLong();

  // Non-nullable at the row level so AvroSchemaConverter yields a bare record (not a ["null", record]
  // top-level union, which neither a plain Avro writer nor the Confluent wire format expects).
  private static final RowType ROW_TYPE =
      RowType.of(
          false,
          new LogicalType[] {
            new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()
          },
          new String[] {"id", "name", "score"});

  @Test
  void nativeSourceVsFlinkRowSource() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produce(brokers, MESSAGES);

      // Profiling mode: spend the whole JVM lifetime in the native source so a sampler (macOS `sample`)
      // has a stable window over the Rust/librdkafka stacks. Skips the Flink baseline.
      if ("1".equals(System.getenv("SF_KAFKA_NATIVE_ONLY"))) {
        for (int i = 0; i < 150; i++) {
          assertCount(runNativeSource(brokers));
        }
        return;
      }

      // Both paths END AT ARROW BATCHES — we're measuring the fastest way to get there.
      double shallow = time(() -> runShallowJsonSource(brokers)); // Java polls -> bytes -> Rust decode
      double nativeSecs = time(() -> runNativeSource(brokers)); // Rust polls + Rust decode

      System.out.printf(
          "%n[kafka -> Arrow, %,d three-field JSON msgs, bounded to latest]%n"
              + "  shallow (Java poll -> bytes -> native decode -> Arrow): %.2fs  =  %,.0f msgs/s%n"
              + "  native  (rdkafka poll + native decode -> Arrow):        %.2fs  =  %,.0f msgs/s%n"
              + "  native speedup: %.2fx%n",
          MESSAGES, shallow, MESSAGES / shallow, nativeSecs, MESSAGES / nativeSecs, shallow / nativeSecs);
    }
  }

  @Test
  void nativeAvroSourceVsFlinkRowSource() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      String avroSchema = AvroSchemaConverter.convertToSchema(ROW_TYPE).toString();
      produceAvro(brokers, MESSAGES, avroSchema);

      double flink = time(() -> runFlinkAvroSource(brokers)); // JVM client + Confluent Avro -> RowData
      double nativeSecs =
          time(() -> runNativeSource(brokers, AVRO_TOPIC, 1, avroSchema, SCHEMA_ID)); // rdkafka -> Arrow

      System.out.printf(
          "%n[kafka source, %,d three-field Confluent-Avro msgs, bounded to latest]%n"
              + "  flink  (KafkaSource + Avro->RowData):   %.2fs  =  %,.0f msgs/s%n"
              + "  native (rdkafka -> Arrow source):       %.2fs  =  %,.0f msgs/s%n"
              + "  native speedup: %.2fx%n",
          MESSAGES, flink, MESSAGES / flink, nativeSecs, MESSAGES / nativeSecs, flink / nativeSecs);
    }
  }

  /** Runs {@code job} once to warm up (JIT, page cache), then again timed; returns the timed seconds. */
  private static double time(RunnableJob job) throws Exception {
    assertCount(job.run());
    long start = System.nanoTime();
    assertCount(job.run());
    return (System.nanoTime() - start) / 1e9;
  }

  private static void assertCount(long count) {
    if (count != MESSAGES) {
      throw new AssertionError("expected " + MESSAGES + " rows, got " + count);
    }
  }

  // Per-fetch batch size: Flink's max.poll.records and the native reader's maxRecords, kept identical
  // so both clients do the same number of poll rounds. Optionally tune librdkafka's fetch backoff to
  // match the Java client's continuous prefetch (SF_KAFKA_BACKOFF) — off by default for a clean run.
  private static final int BATCH = 8192;

  /** The consumer settings both sources share verbatim — the only difference is the client. */
  private static Properties sharedConsumerProperties(String brokers) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", brokers);
    props.setProperty("group.id", "bench");
    props.setProperty("enable.auto.commit", "false");
    props.setProperty("fetch.min.bytes", "1");
    props.setProperty("fetch.max.bytes", String.valueOf(64 << 20));
    props.setProperty("max.partition.fetch.bytes", String.valueOf(16 << 20));
    props.setProperty("fetch.max.wait.ms", "500");
    props.setProperty("max.poll.records", String.valueOf(BATCH));
    return props;
  }

  /** Flink's Kafka source decoding JSON to RowData (the row-materializing baseline). */
  private long runFlinkRowSource(String brokers) throws Exception {
    KafkaSource<RowData> source =
        KafkaSource.<RowData>builder()
            .setProperties(sharedConsumerProperties(brokers))
            .setTopics(TOPIC)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setBounded(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(
                new JsonRowDataDeserializationSchema(
                    ROW_TYPE, InternalTypeInfo.of(ROW_TYPE), false, false, TimestampFormat.SQL))
            .build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    COUNTER.set(0);
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "flink-kafka")
        .map(new CountingMap<>(row -> 1L), InternalTypeInfo.of(ROW_TYPE))
        .sinkTo(new DiscardingSink<>());
    env.execute();
    return COUNTER.get();
  }

  /**
   * Shallow path: Flink/Java polls Kafka for raw value bytes, which are batched and handed to the same
   * native JSON format provider the native source uses — so this and the native source end at identical
   * Arrow batches and differ only in who polls Kafka (Java + a heap->native byte copy vs librdkafka in Rust).
   */
  private long runShallowJsonSource(String brokers) throws Exception {
    KafkaSource<byte[]> source =
        KafkaSource.<byte[]>builder()
            .setProperties(sharedConsumerProperties(brokers))
            .setTopics(TOPIC)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setBounded(OffsetsInitializer.latest())
            .setDeserializer(
                KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class))
            .build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    COUNTER.set(0);
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "shallow-kafka")
        .transform(
            "shallow-decode",
            ArrowBatchTypeInformation.INSTANCE,
            new NativeBytesDecodeOperator(
                ROW_TYPE,
                BATCH,
                new JsonFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(ROW_TYPE, ROW_TYPE, Map.of("format", "json"), false)),
                0))
        .map(new CountingMap<>(batch -> (long) batch.rowCount()), ArrowBatchTypeInformation.INSTANCE)
        .sinkTo(new DiscardingSink<>());
    env.execute();
    return COUNTER.get();
  }

  /** Flink's Kafka source decoding Confluent Avro to RowData (the row-materializing baseline). */
  private long runFlinkAvroSource(String brokers) throws Exception {
    KafkaSource<RowData> source =
        KafkaSource.<RowData>builder()
            .setProperties(sharedConsumerProperties(brokers))
            .setTopics(AVRO_TOPIC)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setBounded(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(
                new ConfluentStrippingDeserializationSchema(
                    new AvroRowDataDeserializationSchema(ROW_TYPE, InternalTypeInfo.of(ROW_TYPE))))
            .build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    COUNTER.set(0);
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "flink-kafka-avro")
        .map(new CountingMap<>(row -> 1L), InternalTypeInfo.of(ROW_TYPE))
        .sinkTo(new DiscardingSink<>());
    env.execute();
    return COUNTER.get();
  }

  /** The production native source over JSON: rdkafka consume + decode to Arrow, no RowData. */
  private long runNativeSource(String brokers) throws Exception {
    return runNativeSource(brokers, TOPIC, 0, "", 0);
  }

  /** The native source for a given topic/format (0 JSON, 1 Confluent Avro); decode to Arrow, no RowData. */
  private long runNativeSource(String brokers, String topic, int format, String avroSchema, int schemaId)
      throws Exception {
    Map<String, String> config = new java.util.HashMap<>(KafkaConfigTranslator.translate(sharedConsumerProperties(brokers)).config());
    // Parity, not cherry-picking: the Java consumer prefetches continuously, but librdkafka defaults
    // fetch.queue.backoff.ms to 1000ms — it idles up to a second before refetching a drained queue,
    // which has no Java analog and would otherwise handicap the native side for no real reason. Match
    // the Java client's eager prefetch. (librdkafka has no max.poll.records analog either; the native
    // batch cap below is the equivalent of the shared max.poll.records.)
    config.put("fetch.queue.backoff.ms", "2");
    // On ARM Macs librdkafka falls back to software crc32c; on x86 it uses SSE4.2 hardware CRC (~free,
    // like the JVM's CRC intrinsic the Java client uses). Toggle to estimate the x86-representative cost.
    if ("1".equals(System.getenv("SF_KAFKA_NOCRC"))) {
      config.put("check.crcs", "false");
    }
    // Ceiling experiment: max the prefetch buffers so librdkafka's background fetcher stays far ahead
    // of consumption (absorbs scheduling jitter under Flink's thread contention).
    if ("1".equals(System.getenv("SF_KAFKA_TUNE"))) {
      config.put("queued.min.messages", "10000000");
      config.put("queued.max.messages.kbytes", "2097151");
    }
    long pollTimeout = System.getenv("SF_KAFKA_POLLTO") != null
        ? Long.parseLong(System.getenv("SF_KAFKA_POLLTO"))
        : 250L;
    String[] keys = config.keySet().toArray(new String[0]);
    String[] values = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      values[i] = config.get(keys[i]);
    }
    NativeKafkaSource source =
        new NativeKafkaSource(
            KafkaSubscriber.getTopicListSubscriber(List.of(topic)),
            OffsetsInitializer.earliest(),
            OffsetsInitializer.latest(),
            Boundedness.BOUNDED,
            sharedConsumerProperties(brokers),
            keys,
            values,
            BATCH,
            pollTimeout,
            null,
            null);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    COUNTER.set(0);
    env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "native-kafka", ArrowBatchTypeInformation.INSTANCE)
        .map(new CountingMap<>(batch -> (long) batch.rowCount()), ArrowBatchTypeInformation.INSTANCE)
        .sinkTo(new DiscardingSink<>());
    env.execute();
    return COUNTER.get();
  }

  private interface Weight<T> extends Serializable {
    long of(T value);
  }

  /** Sums each record's weight (1 per RowData, rowCount per ArrowBatch) into the shared counter. */
  private static final class CountingMap<T> implements MapFunction<T, T> {
    private final Weight<T> weight;

    CountingMap(Weight<T> weight) {
      this.weight = weight;
    }

    @Override
    public T map(T value) {
      COUNTER.addAndGet(weight.of(value));
      return value;
    }
  }

  private interface RunnableJob {
    long run() throws Exception;
  }

  /** Strips the Confluent 5-byte prefix (magic + schema id), then delegates to a plain-Avro decoder. */
  private static final class ConfluentStrippingDeserializationSchema
      implements DeserializationSchema<RowData> {
    private final DeserializationSchema<RowData> inner;

    ConfluentStrippingDeserializationSchema(DeserializationSchema<RowData> inner) {
      this.inner = inner;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
      inner.open(context);
    }

    @Override
    public RowData deserialize(byte[] message) throws java.io.IOException {
      return inner.deserialize(java.util.Arrays.copyOfRange(message, 5, message.length));
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
      return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
      return inner.getProducedType();
    }
  }

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
                .getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(TOPIC, value));
      }
      producer.flush();
    }
  }

  /** Produces Confluent-framed Avro: magic byte + 4-byte big-endian schema id + Avro binary body. */
  private static void produceAvro(String brokers, int messages, String avroSchemaJson) throws Exception {
    Schema schema = new Schema.Parser().parse(avroSchemaJson);
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < messages; i++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", (long) i);
        record.put("name", "row-" + i);
        record.put("score", (i % 100) + 0.5);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0); // Confluent magic byte
        out.write((SCHEMA_ID >>> 24) & 0xFF);
        out.write((SCHEMA_ID >>> 16) & 0xFF);
        out.write((SCHEMA_ID >>> 8) & 0xFF);
        out.write(SCHEMA_ID & 0xFF);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        producer.send(new ProducerRecord<>(AVRO_TOPIC, 0, null, out.toByteArray()));
      }
      producer.flush();
    }
  }
}
