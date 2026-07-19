package io.github.jordepic.streamfusion.kafka;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Diagnostic probe, not a regression test: measures raw transactional produce+flush throughput of
 * the native librdkafka producer against the Java client on identical records, isolating the
 * producer drain rate from Flink entirely. Used to attribute exactly-once sink benchmark deltas.
 */
@Tag("streamfusion-kafka")
@EnabledIfEnvironmentVariable(named = "SF_PRODUCER_PROBE", matches = "true")
class NativeKafkaProducerThroughputProbe {

  private static final int RECORDS = Integer.getInteger("probe.records", 1_000_000);
  private static final int VALUE_BYTES = Integer.getInteger("probe.valueBytes", 150);
  private static final int ROUNDS = Integer.getInteger("probe.rounds", 3);

  @Test
  void comparesTransactionalProduceThroughput() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String bootstrap = kafka.getBootstrapServers();
      byte[] value = new byte[VALUE_BYTES];
      java.util.Arrays.fill(value, (byte) 'x');

      for (int round = 0; round < ROUNDS; round++) {
        double nativePerRecord = nativeRound(bootstrap, value, round, false);
        double nativeBatched = nativeRound(bootstrap, value, round, true);
        double javaRate = javaRound(bootstrap, value, round);
        System.out.printf(
            "[probe] round %d: native-per-record-jni %.3f, native-batched %.3f, java %.3f M rec/s"
                + " (batched/java %.2fx)%n",
            round,
            nativePerRecord / 1e6,
            nativeBatched / 1e6,
            javaRate / 1e6,
            nativeBatched / javaRate);
      }
    }
  }

  private double nativeRound(String bootstrap, byte[] value, int round, boolean batched)
      throws Exception {
    String topic = createTopic(bootstrap, "probe-native-" + round);
    Map<String, String> config = new LinkedHashMap<>();
    config.put("bootstrap.servers", bootstrap);
    config.put("transaction.timeout.ms", "600000");
    config.put("statistics.interval.ms", "100");
    config.put("batch.size", System.getProperty("probe.nativeBatchSize", "16384"));
    config.put("linger.ms", System.getProperty("probe.nativeLingerMs", "5"));
    config.put("acks", "all");
    config.put("enable.idempotence", "true");
    config.put("max.in.flight.requests.per.connection", "5");
    config.put("queue.buffering.max.messages", "0");
    config.put("queue.buffering.max.kbytes", "32768");
    config.put("socket.nagle.disable", "true");
    String[] keys = config.keySet().toArray(new String[0]);
    String[] values = config.values().toArray(new String[0]);
    long handle =
        NativeKafka.openTransactionalKafkaProducer(
            KafkaProducerConfigTranslator.ABI_VERSION,
            keys,
            values,
            "probe-native-" + round + "-" + UUID.randomUUID(),
            60_000,
            1_048_576);
    try {
      NativeKafka.initKafkaTransactions(handle, 60_000, new long[2]);
      NativeKafka.beginKafkaTransaction(handle);
      long start = System.nanoTime();
      if (batched) {
        NativeKafka.produceKafkaRecordRepeated(handle, topic, value, RECORDS);
      } else {
        for (int i = 0; i < RECORDS; i++) {
          NativeKafka.produceKafkaRecord(handle, topic, null, value);
        }
      }
      NativeKafka.flushKafkaProducer(handle, 600_000, new long[2]);
      double seconds = (System.nanoTime() - start) / 1e9;
      NativeKafka.abortKafkaTransaction(handle, 60_000);
      return RECORDS / seconds;
    } finally {
      NativeKafka.closeKafkaProducer(handle);
    }
  }

  private double javaRound(String bootstrap, byte[] value, int round) throws Exception {
    String topic = createTopic(bootstrap, "probe-java-" + round);
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "probe-java-" + round + "-" + UUID.randomUUID());
    props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 600_000);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      producer.initTransactions();
      producer.beginTransaction();
      long start = System.nanoTime();
      for (int i = 0; i < RECORDS; i++) {
        producer.send(new ProducerRecord<>(topic, null, value));
      }
      producer.flush();
      double seconds = (System.nanoTime() - start) / 1e9;
      producer.abortTransaction();
      return RECORDS / seconds;
    }
  }

  private static String createTopic(String bootstrap, String prefix) throws Exception {
    String topic = prefix + "-" + UUID.randomUUID();
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    try (AdminClient admin = AdminClient.create(props)) {
      admin.createTopics(java.util.List.of(new NewTopic(topic, 1, (short) 1))).all().get();
    }
    return topic;
  }
}
